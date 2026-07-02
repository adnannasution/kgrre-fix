#!/bin/bash
set -e

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

if [ ! -x ".venv/bin/python" ]; then
  echo "Python environment belum tersedia. Jalankan setup terlebih dahulu."
  exit 1
fi

if [ ! -f "dist/index.html" ]; then
  echo "Build web belum tersedia."
  exit 1
fi

echo "Kilang Graph berjalan di http://127.0.0.1:8765"
echo "Folder sumber: /Users/macbook/Documents/KGRRE/runtime/artifact_upload"
echo "Tekan Control+C untuk berhenti."

(sleep 1.5 && open "http://127.0.0.1:8765") &
exec .venv/bin/python -m uvicorn backend.app:app --host 127.0.0.1 --port 8765
