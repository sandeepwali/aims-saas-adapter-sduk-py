#!/bin/sh

set -e

. /venv/bin/activate

echo "App LogLevel ${LOG_LEVEL:-INFO}"
echo "Azure LogLevel ${LOG_LEVEL_AZURE:-INFO}"

exec python app.py
