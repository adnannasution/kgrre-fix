"""Import sebuah ZIP output ETL langsung ke PostgreSQL (DATABASE_URL di .env),
dijalankan dari laptop — tanpa lewat upload browser, jadi tidak kena batas waktu
HTTP Railway. Memakai pipeline importer yang sama dengan aplikasi.

Pemakaian:
    set PYTHONPATH=.   (PowerShell: $env:PYTHONPATH=".")
    .venv/Scripts/python.exe scripts/import_local.py "knowledge_graph_upload.zip" "Nama Dataset"
"""
import sys
import time
import shutil
import tempfile
import threading
import zipfile
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from backend.config import ensure_dirs, ensure_schema
from backend.scanner import scan_package
from backend import importer


def main() -> int:
    if len(sys.argv) < 2:
        print("Pemakaian: import_local.py <path.zip> [nama dataset]")
        return 2
    zip_src = Path(sys.argv[1]).resolve()
    name = sys.argv[2] if len(sys.argv) > 2 else "Knowledge Graph ETL Dataset"
    if not zip_src.exists():
        print(f"File tidak ditemukan: {zip_src}")
        return 2

    print(f"[0] Menyiapkan skema di PostgreSQL...")
    ensure_dirs()
    ensure_schema()

    # Ekstrak sendiri (TIDAK pakai start_zip_import yang menghapus file asli).
    # Pakai folder temp di drive yang sama dengan ZIP (mis. D:) agar tidak
    # kehabisan ruang di C: untuk ekstraksi besar.
    temp_base = zip_src.parent / ".import_tmp"
    temp_base.mkdir(exist_ok=True)
    extract_dir = Path(tempfile.mkdtemp(prefix="kgrre_import_", dir=str(temp_base)))
    print(f"[1] Mengekstrak ZIP ke {extract_dir} ...")
    with zipfile.ZipFile(zip_src) as archive:
        for member in archive.infolist():
            mp = Path(member.filename)
            if member.is_dir() or mp.is_absolute() or ".." in mp.parts:
                continue
            if len(mp.parts) > 2:
                continue
            if len(mp.parts) == 2 and mp.parts[0] not in {"input_metadata", "analysis_ready"}:
                continue
            archive.extract(member, extract_dir)
    print("[2] Ekstraksi selesai. Memindai paket...")

    scan = scan_package(extract_dir, validate=True)
    files = importer._select_ready_files(scan, allow_partial=True)
    print(f"[3] {len(files)} file siap diimpor. Memulai ingest ke PostgreSQL...")

    job = importer._create_job(name)

    # Jalankan di thread agar bisa polling progres, lalu tunggu selesai.
    t = threading.Thread(target=importer._run_import, args=(job, files, extract_dir, True), daemon=True)
    t.start()

    last = None
    while t.is_alive():
        status = f"{job.phase} ({job.progress}%) - {job.message}"
        if status != last:
            print(f"    {status}")
            last = status
        time.sleep(2)
    t.join()

    # Bersihkan tempdir.
    shutil.rmtree(extract_dir, ignore_errors=True)

    if job.status == "completed":
        print(f"\n[OK] SELESAI. Dataset '{name}' (id={job.dataset_id})")
        print(f"     {job.message}")
        return 0
    print(f"\n[GAGAL] status={job.status} error={job.error}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
