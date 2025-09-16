#!/usr/bin/env bash
set -euo pipefail
exec python3 -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000