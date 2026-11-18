#!/usr/bin/env python3
"""Extract MIMIC-CXR DICOM metadata for one p-folder without keeping any DICOMs on disk.

For each image in the folder the script fetches the file from PhysioNet into memory,
reads the header with the same functions as the notebooks, and discards the bytes.
No DICOM is ever written to disk, only Parquet.

PhysioNet only serves its files to wget (other clients get HTTP 403), so every fetch is
done by running the real `wget` program and reading its output; wget must be installed.

By default it reads only as much of each file as the header needs (the first
--first-bytes bytes, then more if the header turns out to be longer) and then stops
wget, because the header sits at the start of the file and the pixel data at the end. Pass --full-files
to download every file completely instead; the result is the same, only slower.

Work is saved every --chunk-size images as a part file, so the script can be stopped
and re-run at any time and carries on with the images that are still missing. When the
whole folder is done the parts are combined into

    <out-dir>/mimic_cxr_meta_<batch>.parquet        wide, one row per DICOM, index = path
    <out-dir>/mimic_cxr_sequences_<batch>.parquet   long, one row per nested element

which are the same files, in the same format, as mimic_cxr_metadata_batched.ipynb writes.

Example (from the repo root):
    uv run python scripts/mimic_cxr_stream_metadata.py \
        --user <physionet-username> --batch p19 --limit 20          # quick trial
    ... --user <physionet-username> --batch p19                     # the whole folder

Output goes to output/mimic_cxr/ in the repo by default (git-ignored); change with --out-dir.

If the DICOMs are already downloaded, pass --local-dir with the folder that holds MIMIC-CXR's
`files/` folder (files/p10/p10000032/s50414267/<id>.dcm). The headers are then read from disk:
no PhysioNet login, no wget and no network, and the output is the same.
What it expects on disk: the .dcm files as downloaded from PhysioNet, under their original
names and folders. Only the header at the start of each file is read (the pixel data is
skipped), so this is fast. Only the folder given as --batch has to be there, not all of
MIMIC-CXR; every .dcm found in it is used. If cxr-record-list.csv.gz is next to `files/`
(or in the output folder), images it lists that are not on disk are counted and reported.
    uv run python scripts/mimic_cxr_stream_metadata.py \
        --local-dir /path/to/mimic-cxr/2.1.0 --batch p19

A folder can take hours. On a Mac, put `caffeinate -i` in front of the command so the computer
does not go to sleep while it runs (with --user or --local-dir alike):
    caffeinate -i uv run python scripts/mimic_cxr_stream_metadata.py --user <physionet-username> --batch p19
caffeinate only prevents sleep: keep the terminal window open, or run inside tmux / screen
(also what to use on Linux or an HPC node). An interrupted run carries on when re-run.

The PhysioNet password is asked for at start (or read from $PHYSIONET_PASSWORD). wget needs
it in a config file, so it is kept in a private temporary file readable only by you, which
is removed when the script exits. Only counts are printed, never metadata values.
"""
import argparse
import atexit
import getpass
import io
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import warnings
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pyarrow.parquet as pq
import pydicom
from pydicom.filereader import read_partial

BASE_URL = 'https://physionet.org/files/mimic-cxr/2.1.0/'
RECORD_LIST = 'cxr-record-list.csv.gz'
PIXEL_TAGS = {0x7FE00010, 0x7FE00009, 0x7FE00008}
SEQ_COLUMNS = ['path', 'sequence', 'element', 'depth', 'tag', 'keyword', 'VR', 'value']
RETRIES = 3
WGET = shutil.which('wget')
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_OUT_DIR = os.path.join(REPO_ROOT, 'output', 'mimic_cxr')


# --- metadata functions, copied unchanged from dicom_metadata_all.ipynb -------------------

def _tag_name(elem):
    """Column name for an element: DICOM keyword, or '(gggg,eeee) <name>' for private/unknown tags."""
    return elem.keyword or f'{_tag_hex(elem)} {elem.name}'.rstrip()


def _tag_hex(elem):
    return f'({elem.tag.group:04X},{elem.tag.element:04X})'


def _scalar(value):
    """Convert a pydicom value into a string scalar (Parquet-safe)."""
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return f'<{len(value)} bytes>'
    if isinstance(value, (list, tuple, pydicom.multival.MultiValue)):
        return '|'.join(str(v) for v in value)
    return str(value)


def flat_elements(ds, prefix=''):
    """Top-level elements of a dataset as a flat dict.

    Non-sequence elements -> string value; sequence elements -> item count
    (the sequence contents are collected separately by walk_sequences).
    """
    out = {}
    for elem in ds:
        name = prefix + _tag_name(elem)
        out[name] = len(elem.value) if elem.VR == 'SQ' else _scalar(elem.value)
    return out


def walk_sequences(ds, prefix='', depth=0):
    """Yield (element_path, depth, elem) for every element nested inside a sequence, at any depth.

    Top-level (depth 0) non-sequence elements are *not* yielded: they live in the flat table.
    Nested sequence elements are yielded (value = item count) and then recursed into.
    """
    for elem in ds:
        if elem.tag.element == 0x0000:
            continue
        name = prefix + _tag_name(elem)
        if elem.VR != 'SQ':
            if depth > 0:
                yield name, depth, elem
            continue
        if depth > 0:
            yield name, depth, elem
        for i, item in enumerate(elem.value):
            yield from walk_sequences(item, prefix=f'{name}[{i}].', depth=depth + 1)


def read_dicom_metadata(fileobj, path):
    """Read header-only metadata from a file-like DICOM.

    Returns (flat_row, sequence_rows): one dict for the tabular output and a list of
    dicts (one per nested element) for the sequences output.
    """
    ds = pydicom.dcmread(fileobj, stop_before_pixels=True, force=True)

    flat = {'path': path}
    if getattr(ds, 'file_meta', None) is not None:
        flat.update(flat_elements(ds.file_meta, prefix='file_meta.'))
    flat.update(flat_elements(ds))

    seq_rows = [
        {
            'path': path,
            'sequence': name.split('[', 1)[0],
            'element': name,
            'depth': depth,
            'tag': _tag_hex(elem),
            'keyword': elem.keyword or None,
            'VR': str(elem.VR),
            'value': str(len(elem.value)) if elem.VR == 'SQ' else _scalar(elem.value),
        }
        for name, depth, elem in walk_sequences(ds)
    ]
    return flat, seq_rows


# --- download -----------------------------------------------------------------------------

class AuthError(Exception):
    """PhysioNet rejected the login; no point trying further files."""


class FetchError(Exception):
    """wget could not fetch the file."""


def make_wget_config(user, password):
    """Private temporary wget config holding the login, removed when the script exits."""
    folder = tempfile.mkdtemp(prefix='physionet_')          # created with mode 0700
    path = os.path.join(folder, 'wgetrc')
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as fh:
        fh.write(f'user = {user}\npassword = {password}\n')
    atexit.register(shutil.rmtree, folder, ignore_errors=True)
    return path


def _wget(url, config, want):
    """First `want` bytes of the file via wget (want=None = the whole file).

    Returns (data, complete), complete meaning `data` is the entire file. When only a
    prefix is wanted, wget is stopped as soon as those bytes have arrived.
    """
    proc = subprocess.Popen(
        [WGET, f'--config={config}', '--quiet', '--tries=1', '--timeout=120', '--output-document=-', url],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    try:
        data = proc.stdout.read() if want is None else proc.stdout.read(want)
        complete = want is None or len(data) < want
        if not complete:
            proc.kill()
    finally:
        proc.stdout.close()
        code = proc.wait()
    if complete and code != 0:
        if code == 6:                       # wget's exit code for a failed login
            raise AuthError('wget reported an authentication failure')
        raise FetchError(f'wget exit code {code}')
    return data, complete


def _wget_retrying(url, config, want):
    for attempt in range(RETRIES):
        try:
            return _wget(url, config, want)
        except FetchError:
            if attempt == RETRIES - 1:
                raise
            time.sleep(2 ** attempt)


def header_complete(data):
    """True if `data` (a prefix of a DICOM file) reaches the pixel data element.

    The reader walks the top-level elements in order; seeing the pixel data tag proves
    every header element before it was inside the prefix.
    """
    reached = []

    def at_pixel_data(tag, vr, length):
        if tag in PIXEL_TAGS:
            reached.append(True)
            return True
        return False

    try:
        read_partial(io.BytesIO(data), stop_when=at_pixel_data, force=True)
    except Exception:
        return False
    return bool(reached)


def fetch_header_bytes(url, config, first_bytes):
    """Bytes from the start of the file, guaranteed to contain the complete header."""
    if first_bytes is None:
        return _wget_retrying(url, config, None)[0]
    want = first_bytes
    while True:
        data, complete = _wget_retrying(url, config, want)
        if complete or header_complete(data):
            return data
        want *= 4


def process(job):
    """Fetch one DICOM into memory, extract its metadata, drop the bytes."""
    record_path, base_url, config, first_bytes, save_sequences, local_root = job
    key = record_path[len('files/'):]
    if local_root is not None:
        return process_local(os.path.join(local_root, record_path), record_path, key, save_sequences)
    try:
        data = fetch_header_bytes(base_url + record_path, config, first_bytes)
    except FetchError as e:
        return {'ok': False, 'record_path': record_path, 'reason': str(e)}
    try:
        flat, seqs = read_dicom_metadata(io.BytesIO(data), key)
    except Exception as e:
        flat, seqs = {'path': key, 'error': repr(e)}, []
    return {'ok': True, 'flat': flat, 'seqs': seqs if save_sequences else [], 'nbytes': len(data)}


def process_local(file_path, record_path, key, save_sequences):
    """Extract the metadata of one DICOM on disk; only the header is read, not the pixel data."""
    try:
        fh = open(file_path, 'rb')
    except OSError as e:
        return {'ok': False, 'record_path': record_path, 'reason': f'cannot open file: {e.strerror}'}
    with fh:
        try:
            flat, seqs = read_dicom_metadata(fh, key)
        except Exception as e:
            flat, seqs = {'path': key, 'error': repr(e)}, []
        nbytes = fh.tell()
    return {'ok': True, 'flat': flat, 'seqs': seqs if save_sequences else [], 'nbytes': nbytes}


# --- parquet helpers ------------------------------------------------------------------------

def _parquet_safe(frame):
    """Stringify any column holding mixed Python types, which Parquet cannot store."""
    for col in [c for c in frame.columns if frame[c].dtype == object]:
        values = frame[col]
        present = values[values.notna()]
        if not present.map(lambda v: isinstance(v, str)).all():
            frame[col] = values.map(lambda v: v if pd.isna(v) else str(v))
    return frame


def write_parquet(frame, path, **kwargs):
    """Write to a temporary name, then rename, so an interrupted write never looks finished."""
    tmp = path + '.partial'
    frame.to_parquet(tmp, **kwargs)
    os.replace(tmp, path)


def part_files(parts_dir, kind):
    if not os.path.isdir(parts_dir):
        return []
    return sorted(os.path.join(parts_dir, f) for f in os.listdir(parts_dir)
                  if re.fullmatch(rf'{kind}_\d+\.parquet', f))


def combine(parts_dir, meta_path, seq_path, save_sequences):
    meta = pd.concat([pd.read_parquet(f) for f in part_files(parts_dir, 'meta')], ignore_index=True)
    meta = _parquet_safe(meta.set_index('path').sort_index())
    if save_sequences:
        seq = pd.concat([pd.read_parquet(f) for f in part_files(parts_dir, 'seq')], ignore_index=True)
        write_parquet(seq[SEQ_COLUMNS], seq_path, index=False)
    write_parquet(meta, meta_path)
    return meta.shape


# --- main -----------------------------------------------------------------------------------

def load_record_paths(out_dir, base_url, config, batch):
    """Paths of every DICOM in the batch, from PhysioNet's official record list."""
    local = os.path.join(out_dir, RECORD_LIST)
    if not os.path.exists(local):
        print(f'downloading {RECORD_LIST} ...', flush=True)
        data, _ = _wget_retrying(base_url + RECORD_LIST, config, None)
        with open(local + '.partial', 'wb') as fh:
            fh.write(data)
        os.replace(local + '.partial', local)
    records = pd.read_csv(local, dtype=str)
    if 'path' not in records.columns:
        sys.exit(f'{RECORD_LIST} has no "path" column (columns: {list(records.columns)})')
    paths = records['path']
    return sorted(paths[paths.str.startswith(f'files/{batch}/') & paths.str.endswith('.dcm')])


def resolve_local_root(local_dir):
    """The folder that holds `files/`, given either that folder or the `files` folder itself."""
    path = os.path.abspath(os.path.expanduser(local_dir))
    if os.path.isdir(os.path.join(path, 'files')):
        return path
    if os.path.basename(path) == 'files' and os.path.isdir(path):
        return os.path.dirname(path)
    sys.exit(f'--local-dir: no "files" folder in {path}.\n'
             '--local-dir is where the MIMIC-CXR DICOMs were downloaded (the folder holding '
             'files/pXX/pXXXXXXXX/sXXXXXXXX/<id>.dcm), not the folder the Parquet output goes to.\n'
             'If the DICOMs are not downloaded, leave --local-dir out and pass --user to read them from PhysioNet.')


def load_local_record_paths(local_root, out_dir, batch):
    """Paths of every DICOM of the batch found on disk, as `files/pXX/.../<id>.dcm`.

    If PhysioNet's record list is next to the files (or in the output folder) it is used to
    report DICOMs that are listed but not on disk, i.e. an incomplete download.
    """
    batch_dir = os.path.join(local_root, 'files', batch)
    if not os.path.isdir(batch_dir):
        sys.exit(f'--local-dir: {batch_dir} does not exist')
    found = set()
    for folder, _, names in os.walk(batch_dir):
        rel = os.path.relpath(folder, local_root).replace(os.sep, '/')
        found.update(f'{rel}/{name}' for name in names if name.endswith('.dcm'))
    for record_list in (os.path.join(local_root, RECORD_LIST), os.path.join(out_dir, RECORD_LIST)):
        if os.path.exists(record_list):
            paths = pd.read_csv(record_list, dtype=str)['path']
            listed = set(paths[paths.str.startswith(f'files/{batch}/') & paths.str.endswith('.dcm')])
            absent = len(listed - found)
            print(f'{os.path.basename(record_list)} lists {len(listed)} images for {batch}; '
                  f'{absent} of them are not on disk' + (' (incomplete download?)' if absent else ''), flush=True)
            break
    else:
        print(f'no {RECORD_LIST} found to check the download is complete; using every .dcm on disk', flush=True)
    return sorted(found)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--batch', required=True, help='top-level folder to process, e.g. p19')
    ap.add_argument('--user', help='PhysioNet username (not needed with --local-dir)')
    ap.add_argument('--local-dir',
                    help="read DICOMs already on disk instead of PhysioNet: the folder holding MIMIC-CXR's files/ folder")
    ap.add_argument('--out-dir', default=DEFAULT_OUT_DIR,
                    help='where the Parquet files go (default: output/mimic_cxr in the repo)')
    ap.add_argument('--chunk-size', type=int, default=500, help='images per saved part file')
    ap.add_argument('--workers', type=int, default=4, help='images read at the same time (connections to PhysioNet)')
    ap.add_argument('--first-bytes', type=int, default=64 * 1024,
                    help='bytes read from the start of each file before checking the header is complete')
    ap.add_argument('--full-files', action='store_true', help='download every file completely')
    ap.add_argument('--limit', type=int, help='process at most this many images this run (for a trial)')
    ap.add_argument('--no-sequences', action='store_true', help='skip the nested-sequence table')
    ap.add_argument('--overwrite', action='store_true', help='redo a batch that is already combined')
    ap.add_argument('--base-url', default=BASE_URL)
    args = ap.parse_args()

    if not re.fullmatch(r'p1\d', args.batch):
        sys.exit('--batch must be one of p10 ... p19')
    local_root = resolve_local_root(args.local_dir) if args.local_dir else None
    if local_root is None and not args.user:
        sys.exit('pass --user (to read from PhysioNet) or --local-dir (to read DICOMs already on disk)')
    if local_root is None and WGET is None:
        sys.exit('wget is not installed (on a Mac: brew install wget)')
    os.makedirs(args.out_dir, exist_ok=True)
    warnings.filterwarnings('ignore', module='pydicom')
    logging.getLogger('pydicom').setLevel(logging.ERROR)

    save_sequences = not args.no_sequences
    first_bytes = None if args.full_files else args.first_bytes
    base_url = args.base_url.rstrip('/') + '/'
    meta_path = os.path.join(args.out_dir, f'mimic_cxr_meta_{args.batch}.parquet')
    seq_path = os.path.join(args.out_dir, f'mimic_cxr_sequences_{args.batch}.parquet')
    parts_dir = os.path.join(args.out_dir, f'mimic_cxr_meta_{args.batch}_parts')
    failed_path = os.path.join(args.out_dir, f'mimic_cxr_meta_{args.batch}_failed.txt')

    if os.path.exists(meta_path) and not args.overwrite:
        print(f'{os.path.basename(meta_path)} already exists: {args.batch} is done. Use --overwrite to redo it.')
        return 0
    if args.overwrite and os.path.isdir(parts_dir):
        shutil.rmtree(parts_dir)

    if local_root is not None:
        config = None
        record_paths = load_local_record_paths(local_root, args.out_dir, args.batch)
        if not record_paths:
            sys.exit(f'no .dcm files under {os.path.join(local_root, "files", args.batch)}')
    else:
        password = os.environ.get('PHYSIONET_PASSWORD') or getpass.getpass(f'PhysioNet password for {args.user}: ')
        config = make_wget_config(args.user, password)
        try:
            record_paths = load_record_paths(args.out_dir, base_url, config, args.batch)
        except FetchError as e:
            sys.exit(f'could not download {RECORD_LIST}: {e}')
        except AuthError as e:
            sys.exit(f'PhysioNet rejected the login ({e}). Check the username and password.')
        if not record_paths:
            sys.exit(f'no DICOMs listed for {args.batch} in {RECORD_LIST}')

    os.makedirs(parts_dir, exist_ok=True)
    done = set()
    for f in part_files(parts_dir, 'meta'):
        done.update(pq.read_table(f, columns=['path']).column('path').to_pylist())
    todo = [p for p in record_paths if p[len('files/'):] not in done]
    run = todo if args.limit is None else todo[:args.limit]

    free_gb = shutil.disk_usage(args.out_dir).free / 1e9
    print(f'output folder:   {args.out_dir}  ({free_gb:,.0f} GB free)')
    print(f'source:          {"DICOMs on disk in " + local_root if local_root else "PhysioNet"}')
    print(f'{args.batch}: {len(record_paths)} images listed, {len(done)} already done, '
          f'{len(run)} to do this run', flush=True)

    next_part = 1 + max([int(re.search(r'_(\d+)\.parquet$', f).group(1))
                         for f in part_files(parts_dir, 'meta')], default=-1)
    failures, n_ok, n_bytes, started = [], 0, 0, time.time()
    jobs = [(p, base_url, config, first_bytes, save_sequences, local_root) for p in run]

    try:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for start in range(0, len(jobs), args.chunk_size):
                chunk_started = time.time()
                results = list(pool.map(process, jobs[start:start + args.chunk_size]))
                good = [r for r in results if r['ok']]
                failures += [r for r in results if not r['ok']]
                if good:
                    name = f'{next_part:05d}.parquet'
                    if save_sequences:
                        seq_rows = [row for r in good for row in r['seqs']]
                        write_parquet(pd.DataFrame(seq_rows, columns=SEQ_COLUMNS),
                                      os.path.join(parts_dir, 'seq_' + name), index=False)
                    # The meta part is written last: it is what marks these images as done.
                    write_parquet(_parquet_safe(pd.DataFrame([r['flat'] for r in good])),
                                  os.path.join(parts_dir, 'meta_' + name), index=False)
                    next_part += 1
                    n_ok += len(good)
                    n_bytes += sum(r['nbytes'] for r in good)
                elapsed = time.time() - started
                rate = n_ok / elapsed if elapsed else 0
                recent = len(good) / max(time.time() - chunk_started, 1e-9)
                left = len(jobs) - start - len(results)
                eta = f'{left / rate / 3600:.1f} h' if rate else 'n/a'
                print(f'{time.strftime("%H:%M:%S")} | {len(done) + n_ok}/{len(record_paths)} done | '
                      f'{recent:.1f} images/s now, {rate:.1f} average | '
                      f'{n_bytes / max(n_ok, 1) / 1024:,.0f} KB read per image | '
                      f'{len(failures)} failed | this run finishes in {eta}', flush=True)
                if len(results) >= 5 and not good:
                    sys.exit(f'every image in the last chunk failed ({results[0]["reason"]}); stopping. '
                             'Completed chunks are saved; re-run the same command to continue.')
    except AuthError as e:
        sys.exit(f'PhysioNet rejected the login ({e}). Completed chunks are saved; fix the login and re-run.')
    except KeyboardInterrupt:
        sys.exit('\nstopped. Completed chunks are saved; re-run the same command to continue.')

    with open(failed_path, 'w') as fh:
        fh.writelines(f"{r['record_path']}\t{r['reason']}\n" for r in failures)
    if not failures:
        os.remove(failed_path)

    remaining = len(record_paths) - len(done) - n_ok
    if remaining:
        print(f'{remaining} images still to do ({len(failures)} failed this run'
              + (f', listed in {os.path.basename(failed_path)}' if failures else '')
              + '). Re-run the same command to continue.')
        return 1 if failures else 0

    rows, cols = combine(parts_dir, meta_path, seq_path, save_sequences)
    print(f'{args.batch} complete: {rows} dicoms x {cols} columns -> {os.path.basename(meta_path)}')
    print(f'the part files in {os.path.basename(parts_dir)}/ are no longer needed and can be deleted')
    return 0


if __name__ == '__main__':
    sys.exit(main())
