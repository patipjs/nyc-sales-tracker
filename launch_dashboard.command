#!/bin/zsh
# Launch the NYC Sales Tracker Streamlit app without auto-starting on boot.

set -e

# If you move this script (e.g., to Desktop), still run against the project.
PROJECT_DIR="/Users/jstippets/projects/nyc-sales-tracker"
cd "$PROJECT_DIR"

VENV="$PROJECT_DIR/.venv"

# Create venv if missing and install dependencies once.
if [ ! -d "$VENV" ]; then
  python -m venv "$VENV"
  source "$VENV/bin/activate"
  pip install --upgrade pip
  pip install -r requirements.txt
else
  source "$VENV/bin/activate"
fi

# Start the app; Streamlit will open in your browser.
streamlit run dashboard/app.py
