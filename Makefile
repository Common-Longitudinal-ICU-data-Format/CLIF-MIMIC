.PHONY: help mo test-med

mo:
	uv run marimo edit --watch

test-med:
	uv run pytest tests/test_med_admin.py -vv

run:
	uv run python main.py
