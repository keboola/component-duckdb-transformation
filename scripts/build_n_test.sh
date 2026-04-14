#!/bin/sh
set -e

uv run ruff check .
uv run python -m pytest tests/ --tb=short -q
