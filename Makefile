.PHONY: help mo test-med freeze

mo:
	uv run marimo edit --watch

test-med:
	uv run pytest tests/test_med_admin.py -vv

run:
	uv run python main.py

freeze:
	uv pip compile pyproject.toml -o requirements.txt
