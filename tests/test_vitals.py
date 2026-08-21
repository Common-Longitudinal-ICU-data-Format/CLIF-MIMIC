import pytest
import sys
import types
from pathlib import Path
import importlib.util


def _load_normalize_temp_c():
    utils_stub = types.ModuleType("src.utils")
    utils_stub.construct_mapper_dict = lambda *args, **kwargs: {}
    utils_stub.fetch_mimic_events = lambda *args, **kwargs: None
    utils_stub.load_mapping_csv = lambda *args, **kwargs: None
    utils_stub.get_relevant_item_ids = lambda *args, **kwargs: []
    utils_stub.find_duplicates = lambda *args, **kwargs: None
    utils_stub.rename_and_reorder_cols = lambda *args, **kwargs: None
    utils_stub.save_to_rclif = lambda *args, **kwargs: None
    utils_stub.convert_and_sort_datetime = lambda *args, **kwargs: None
    utils_stub.EXCLUDED_LABELS_DEFAULT = []
    utils_stub.convert_tz_to_utc = lambda *args, **kwargs: None

    original_utils = sys.modules.get("src.utils")
    sys.modules["src.utils"] = utils_stub

    try:
        vitals_path = Path(__file__).resolve().parents[1] / "src" / "tables" / "vitals.py"
        spec = importlib.util.spec_from_file_location("vitals_under_test", vitals_path)
        vitals_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(vitals_module)
        return vitals_module.normalize_temp_c
    finally:
        if original_utils is None:
            del sys.modules["src.utils"]
        else:
            sys.modules["src.utils"] = original_utils


normalize_temp_c = _load_normalize_temp_c()


@pytest.mark.parametrize(
    "temp_c, expected",
    [
        (36.8, 36.8),
        (39.1, 39.1),
        (97.2, 36.2),
        (99.0, 37.2),
        (98.9, 37.2),
    ],
)
def test_normalize_temp_c(temp_c, expected):
    assert normalize_temp_c(temp_c) == expected
