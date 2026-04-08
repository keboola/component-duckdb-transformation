#!/bin/sh
set -e

PYTHON=/opt/venvs/duckdb-1.5.1/bin/python
$PYTHON -m flake8 --config=flake8.cfg
$PYTHON -m unittest discover
