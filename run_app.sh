#!/bin/zsh
set -euo pipefail

export PYTHONPATH=src
exec ./venv/bin/streamlit run app.py
