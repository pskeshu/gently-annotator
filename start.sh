#!/usr/bin/env bash
# Launch the Gently Annotator server.
#
# Usage:
#   ./start.sh                  # uses host/port from config.yaml
#   ./start.sh --port 8091      # any extra args pass through to annotator.server

set -euo pipefail
repo="$(cd "$(dirname "$0")" && pwd)"
py="$repo/venv/bin/python"

if [ ! -x "$py" ]; then
    echo "venv not found at: $py" >&2
    echo
    echo "Bootstrap it once with:" >&2
    echo "  python3 -m venv venv" >&2
    echo "  ./venv/bin/python -m pip install -e ." >&2
    exit 1
fi

cd "$repo"
exec "$py" -m annotator.server "$@"
