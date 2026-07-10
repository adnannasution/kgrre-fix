from __future__ import annotations
 
import json
import re
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .config import (
    UPLOADS_DIR, delete_dataset as _delete_dataset_data, ensure_dirs, ensure_schema,
    get_config, get_dataset_row, list_datasets, rename_dataset as _rename_dataset,
    reset_all as _reset_all, save_config,
)
from .database import fetch_tuple, pool, scoped
from .etl_pipeline import start_etl_import
import threading
import uuid
from .importer import JOBS, JOBS_LOCK, ImportJob, cancel_job, start_chunked_import, start_import, start_zip_import
from .scanner import scan_folder

PRIORITY_RELATIONSHIPS = [
    "REFINERY_UNIT_HAS_PLANT",
    "REFINERY_UNIT_HAS_EQUIPMENT",
    "PLANT_HAS_FUNCTIONAL_LOCATION",
    "FUNCTIONAL_LOCATION_HAS_EQUIPMENT",
    "EQUIPMENT_HAS_ISSUE",
    "ISSUE_HAS_RCPS",
    "RCPS_HAS_RECOMMENDATION",
    "EQUIPMENT_HAS_RELIABILITY_OBSERVATION",
    "EQUIPMENT_HAS_MAINTENANCE_ORDER",
    "EQUIPMENT_HAS_NOTIFICATION",
    "EQUIPMENT_HAS_READINESS_RECORD",
    "EQUIPMENT_HAS_INSPECTION",
]

app = FastAPI(title="Kilang Knowledge Graph", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class FolderConfig(BaseModel):
    upload_folder: str


class ImportRequest(BaseModel):
    name: str = "Knowledge Graph Dataset"
    allow_partial: bool = True


class ChunkedFileInfo(BaseModel):
    name: str
    total_chunks: int


class ChunkedInitRequest(BaseModel):
    name: str = "Knowledge Graph Dataset"
    files: list[ChunkedFileInfo]
    mode: str = "csv"  # "csv" | "etl"
    existing_dataset_id: str | None = None


import threading as _threading
import uuid as _uuid

_CHUNK_SESSIONS: dict[str, dict] = {}
_CHUNK_LOCK = _threading.Lock()


class RenameRequest(BaseModel):
    name: str


class PropertyQueryRequest(BaseModel):
    query: str
    limit: int = 200


_SCHEMA_READY = False

# Cache hasil endpoint dashboard berat (reliability_insight, ru_summary) per dataset.
# Dataset bersifat statis (di-import sekali, dibaca berkali-kali), sedangkan query insight
# men-scan ratusan ribu baris kg_node/kg_relationship dan berjalan ~puluhan detik — cukup
# untuk melewati timeout proxy Railway sehingga halaman menampilkan 0/—. Dengan cache,
# hanya request pertama yang membayar biaya komputasi; sisanya instan. Cache dibersihkan
# otomatis saat proses restart dan saat dataset di-rename/hapus.
import threading
import time as _time

_INSIGHT_CACHE: dict[tuple[str, str], tuple[float, object]] = {}
_INSIGHT_WARMING: set[tuple[str, str]] = set()
_INSIGHT_CACHE_LOCK = threading.Lock()
_INSIGHT_CACHE_TTL = 3600.0  # 1 jam


def _cached_insight(dataset_id: str, key: str, compute, fallback=None):
    """Sajikan hasil compute() dari cache. Bila cache dingin, JANGAN hitung inline
    (query insight ~38 detik melewati timeout proxy Railway → request gagal & cache
    tak pernah terisi). Hitung di thread latar, kembalikan `fallback` dengan
    `computing=True` agar frontend menampilkan status memuat lalu polling lagi.

    `fallback` harus berupa dict dengan bentuk yang sama agar frontend tidak error."""
    cache_key = (dataset_id, key)
    now = _time.time()
    with _INSIGHT_CACHE_LOCK:
        hit = _INSIGHT_CACHE.get(cache_key)
        if hit and now - hit[0] < _INSIGHT_CACHE_TTL:
            return hit[1]
        already_warming = cache_key in _INSIGHT_WARMING
        if not already_warming:
            _INSIGHT_WARMING.add(cache_key)

    if not already_warming:
        def _warm():
            try:
                value = compute()
                with _INSIGHT_CACHE_LOCK:
                    _INSIGHT_CACHE[cache_key] = (_time.time(), value)
            except Exception:
                # Jangan biarkan thread mati diam-diam: kalau compute gagal, warming
                # flag dibersihkan & cache tak terisi → frontend polling selamanya
                # ("muter muter terus"). Cache hasil fallback + error agar polling
                # berhenti dan errornya kelihatan, lalu log tracebacknya.
                import traceback
                traceback.print_exc()
                err = dict(fallback or {})
                err["computing"] = False
                err["error"] = "Gagal menghitung insight."
                with _INSIGHT_CACHE_LOCK:
                    _INSIGHT_CACHE[cache_key] = (_time.time(), err)
            finally:
                with _INSIGHT_CACHE_LOCK:
                    _INSIGHT_WARMING.discard(cache_key)
        threading.Thread(target=_warm, name=f"warm-{key}", daemon=True).start()

    result = dict(fallback or {})
    result["computing"] = True
    return result


def _invalidate_insight_cache(dataset_id: str) -> None:
    with _INSIGHT_CACHE_LOCK:
        for k in [k for k in _INSIGHT_CACHE if k[0] == dataset_id]:
            _INSIGHT_CACHE.pop(k, None)


def ensure_schema_once() -> None:
    """Pastikan skema dibuat tepat sekali. Dipanggil saat startup (best-effort)
    dan dari db_for()/list_datasets agar tetap terbentuk walau DB belum siap
    saat boot."""
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    ensure_schema()
    _SCHEMA_READY = True


@app.on_event("startup")
def _startup() -> None:
    ensure_dirs()
    # Jangan sentuh DB di sini. Pool dibuka non-blocking (open=False + open(wait=False)),
    # jadi pool().connection() bisa hang kalau DB belum siap — startup uvicorn ikut hang
    # dan /api/health tidak pernah dijawab. Schema dibuat pada request pertama via
    # ensure_schema_once() yang dipanggil dari db_for() dan list_datasets().


@app.on_event("shutdown")
def _shutdown() -> None:
    from .database import close_pool
    close_pool()


@app.get("/api/health")
def health():
    return {"ok": True, "service": "kilang-knowledge-graph"}


@app.get("/api/folder")
def folder(validate: bool = False):
    return scan_folder(validate_sheets=validate)


@app.put("/api/folder")
def update_folder(payload: FolderConfig):
    path = Path(payload.upload_folder).expanduser()
    if not path.exists() or not path.is_dir():
        raise HTTPException(400, "Folder tidak tersedia.")
    save_config(str(path))
    return scan_folder()


@app.post("/api/imports")
def create_import(payload: ImportRequest):
    try:
        return start_import(payload.name, payload.allow_partial).public()
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.post("/api/imports/zip")
async def create_zip_import(
    file: UploadFile = File(...),
    name: str = Form("Knowledge Graph ETL Dataset"),
    allow_partial: bool = Form(True),
):
    if not file.filename or not file.filename.lower().endswith(".zip"):
        raise HTTPException(400, "Upload harus berupa file .zip.")
    target = UPLOADS_DIR / f"{quote(file.filename)}-{Path(file.filename).stem}-{len(JOBS)}.zip"
    with target.open("wb") as handle:
        while chunk := await file.read(1024 * 1024):
            handle.write(chunk)
    try:
        return start_zip_import(name, target, allow_partial).public()
    except ValueError as exc:
        target.unlink(missing_ok=True)
        raise HTTPException(400, str(exc)) from exc


@app.post("/api/imports/chunked/init")
def init_chunked_upload(payload: ChunkedInitRequest):
    upload_id = _uuid.uuid4().hex
    session_dir = UPLOADS_DIR / upload_id
    session_dir.mkdir(parents=True, exist_ok=True)
    with _CHUNK_LOCK:
        _CHUNK_SESSIONS[upload_id] = {
            "name": payload.name,
            "dir": str(session_dir),
            "mode": payload.mode,
            "existing_dataset_id": payload.existing_dataset_id,
            "files": {f.name: {"total_chunks": f.total_chunks, "received": set()} for f in payload.files},
        }
    return {"upload_id": upload_id}


@app.post("/api/imports/chunked/{upload_id}/chunk")
async def upload_chunk(
    upload_id: str,
    file_name: str = Form(...),
    chunk_index: int = Form(...),
    data: UploadFile = File(...),
):
    with _CHUNK_LOCK:
        session = _CHUNK_SESSIONS.get(upload_id)
    if not session:
        raise HTTPException(404, "Upload session tidak ditemukan.")
    if file_name not in session["files"]:
        raise HTTPException(400, f"File {file_name} tidak terdaftar dalam sesi ini.")
    chunk_path = Path(session["dir"]) / f"{file_name}.part{chunk_index}"
    with chunk_path.open("wb") as fh:
        while chunk := await data.read(1024 * 1024):
            fh.write(chunk)
    with _CHUNK_LOCK:
        session["files"][file_name]["received"].add(chunk_index)
        received = len(session["files"][file_name]["received"])
        total = session["files"][file_name]["total_chunks"]
    return {"file_name": file_name, "chunk_index": chunk_index, "received": received, "total": total}


@app.post("/api/imports/chunked/{upload_id}/commit")
def commit_chunked_upload(upload_id: str):
    with _CHUNK_LOCK:
        session = _CHUNK_SESSIONS.pop(upload_id, None)
    if not session:
        raise HTTPException(404, "Upload session tidak ditemukan.")
    session_dir = Path(session["dir"])
    for file_name, info in session["files"].items():
        total = info["total_chunks"]
        received = info["received"]
        if len(received) < total:
            raise HTTPException(400, f"File {file_name} belum lengkap: {len(received)}/{total} chunk.")
        out_path = session_dir / file_name
        with out_path.open("wb") as out:
            for i in range(total):
                part = session_dir / f"{file_name}.part{i}"
                out.write(part.read_bytes())
                part.unlink(missing_ok=True)
    try:
        mode = session.get("mode", "csv")
        if mode in ("etl", "etl_append"):
            excel_paths = [session_dir / fn for fn in session["files"]]
            existing = session.get("existing_dataset_id")
            return start_etl_import(
                session["name"], excel_paths,
                existing_dataset_id=existing,
                append=(mode == "etl_append"),
            ).public()
        return start_chunked_import(session["name"], session_dir).public()
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.post("/api/etl/upload")
async def etl_upload(
    files: list[UploadFile] = File(...),
    name: str = Form("Knowledge Graph ETL Dataset"),
):
    if not files:
        raise HTTPException(400, "Minimal satu file Excel diperlukan.")
    saved: list[Path] = []
    etl_dir = UPLOADS_DIR / _uuid.uuid4().hex
    etl_dir.mkdir(parents=True, exist_ok=True)
    for f in files:
        if not f.filename or not f.filename.lower().endswith((".xlsx", ".xls")):
            raise HTTPException(400, f"File {f.filename} bukan format Excel (.xlsx/.xls).")
        dest = etl_dir / f.filename
        with dest.open("wb") as fh:
            while chunk := await f.read(1024 * 1024):
                fh.write(chunk)
        saved.append(dest)
    try:
        job = start_etl_import(name, saved)
        return job.public()
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc


@app.post("/api/datasets/{dataset_id}/sync")
async def etl_sync(
    dataset_id: str,
    files: list[UploadFile] = File(...),
):
    """Sinkronisasi ulang knowledge graph dari file Excel baru tanpa membuat dataset baru."""
    row = get_dataset_row(dataset_id)
    if not row:
        raise HTTPException(404, "Dataset tidak ditemukan.")
    if not files:
        raise HTTPException(400, "Minimal satu file Excel diperlukan.")
    saved: list[Path] = []
    etl_dir = UPLOADS_DIR / _uuid.uuid4().hex
    etl_dir.mkdir(parents=True, exist_ok=True)
    for f in files:
        if not f.filename or not f.filename.lower().endswith((".xlsx", ".xls")):
            raise HTTPException(400, f"File {f.filename} bukan format Excel (.xlsx/.xls).")
        dest = etl_dir / f.filename
        with dest.open("wb") as fh:
            while chunk := await f.read(1024 * 1024):
                fh.write(chunk)
        saved.append(dest)
    try:
        job = start_etl_import(row["name"], saved, existing_dataset_id=dataset_id)
        return job.public()
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc


_NORM = "regexp_replace(upper(coalesce(%s, '')), '[^A-Z0-9]+', '', 'g')"


def _norm(expr: str) -> str:
    return _NORM % expr


def _run_rebuild(job: ImportJob, dataset_id: str, row: dict) -> None:
    """Jalankan rebuild relasi di background thread."""
    import time as _time
    try:
        job.status = "running"
        job.phase = "Menghapus relasi lama"
        job.progress = 5
        with scoped(dataset_id, autocommit=True) as connection:

            # 1. RU → Equipment
            connection.execute(f"""
                INSERT INTO kg_relationship
                    (relationship_id, source_node_id, target_node_id,
                     relationship_type, properties_json, is_candidate, confidence)
                SELECT
                    'rel_' || md5(ru.node_id || '|REFINERY_UNIT_HAS_EQUIPMENT|' || eq.node_id),
                    ru.node_id, eq.node_id,
                    'REFINERY_UNIT_HAS_EQUIPMENT',
                    '{{}}'::jsonb, false, 1.0
                FROM kg_node ru, kg_node eq
                WHERE ru.node_type = 'refinery_unit'
                  AND eq.node_type = 'equipment'
                  AND ru.label = eq.properties_json->>'refinery_unit'
                ON CONFLICT DO NOTHING
            """)

            job.progress = 20

            # Equipment → domain nodes
            for domain_type, rel_type, eq_prop in [
                ('maintenance_order',       'EQUIPMENT_HAS_MAINTENANCE_ORDER',       'equipment_raw'),
                ('rkap_program',            'EQUIPMENT_HAS_RKAP_PROGRAM',            'equipment_raw'),
                ('reliability_observation', 'EQUIPMENT_HAS_RELIABILITY_OBSERVATION', 'equipment_raw'),
                ('inspection',              'EQUIPMENT_HAS_INSPECTION',              'equipment_raw'),
                ('equipment_issue',         'EQUIPMENT_HAS_ISSUE',                   'equipment_raw'),
                ('readiness_record',        'EQUIPMENT_HAS_READINESS_RECORD',        'equipment_raw'),
                ('work_order',              'EQUIPMENT_HAS_WORK_ORDER',              'equipment_raw'),
                ('notification',            'EQUIPMENT_HAS_NOTIFICATION',            'equipment_raw'),
                ('bad_actor',               'EQUIPMENT_HAS_BAD_ACTOR',               'tag_raw'),
                ('rotor',                   'EQUIPMENT_HAS_ROTOR',                   'equipment_raw'),
                ('spm_workplan',            'EQUIPMENT_HAS_SPM_WORKPLAN',            'equipment_raw'),
                ('tank_workplan',           'EQUIPMENT_HAS_TANK_WORKPLAN',           'equipment_raw'),
                ('jetty_workplan',          'EQUIPMENT_HAS_JETTY_WORKPLAN',          'equipment_raw'),
                ('readiness_tank',          'EQUIPMENT_HAS_READINESS_TANK',          'equipment_raw'),
                ('readiness_spm',           'EQUIPMENT_HAS_READINESS_SPM',           'equipment_raw'),
                ('readiness_jetty',         'EQUIPMENT_HAS_READINESS_JETTY',         'equipment_raw'),
                ('zero_clamp',              'EQUIPMENT_HAS_ZERO_CLAMP',              'equipment_raw'),
                ('pipeline_inspection',     'EQUIPMENT_HAS_PIPELINE_INSPECTION',     'equipment_raw'),
                ('critical_equipment',      'EQUIPMENT_HAS_CRITICAL_EQUIPMENT',      'equipment_raw'),
                ('icu_issue',               'EQUIPMENT_HAS_ICU_ISSUE',               'equipment_raw'),
                ('power_steam',             'EQUIPMENT_HAS_POWER_STEAM',             'equipment_raw'),
                ('metering',               'EQUIPMENT_HAS_METERING',               'equipment_raw'),
            ]:
                connection.execute(f"""
                    INSERT INTO kg_relationship
                        (relationship_id, source_node_id, target_node_id,
                         relationship_type, properties_json, is_candidate, confidence)
                    SELECT DISTINCT
                        'rel_' || md5(eq.node_id || '|{rel_type}|' || dn.node_id),
                        eq.node_id, dn.node_id, '{rel_type}',
                        '{{}}'::jsonb, false, 1.0
                    FROM kg_node eq, kg_node dn
                    WHERE eq.node_type = 'equipment'
                      AND dn.node_type = '{domain_type}'
                      AND regexp_replace(upper(coalesce(eq.properties_json->>'equipment_code_raw','')), '[^A-Z0-9]+', '', 'g') != ''
                      AND regexp_replace(upper(coalesce(eq.properties_json->>'equipment_code_raw','')), '[^A-Z0-9]+', '', 'g')
                        = regexp_replace(upper(coalesce(dn.properties_json->>'{eq_prop}','')), '[^A-Z0-9]+', '', 'g')
                      AND regexp_replace(upper(coalesce(dn.properties_json->>'{eq_prop}','')), '[^A-Z0-9]+', '', 'g') != ''
                    ON CONFLICT DO NOTHING
                """)

            job.progress = 50

            # Monitoring Operasi: dua kolom equipment
            for mo_prop in ('equipment_process_raw', 'equipment_sts_raw'):
                connection.execute(f"""
                    INSERT INTO kg_relationship
                        (relationship_id, source_node_id, target_node_id,
                         relationship_type, properties_json, is_candidate, confidence)
                    SELECT DISTINCT
                        'rel_' || md5(eq.node_id || '|EQUIPMENT_HAS_MONITORING_OPERASI|' || dn.node_id),
                        eq.node_id, dn.node_id, 'EQUIPMENT_HAS_MONITORING_OPERASI',
                        '{{}}'::jsonb, false, 1.0
                    FROM kg_node eq, kg_node dn
                    WHERE eq.node_type = 'equipment'
                      AND dn.node_type = 'monitoring_operasi'
                      AND regexp_replace(trim(coalesce(eq.properties_json->>'equipment_code_raw','')), '/[0-9]+$', '')
                        = regexp_replace(trim(coalesce(dn.properties_json->>'{mo_prop}','')), '/[0-9]+$', '')
                      AND trim(coalesce(eq.properties_json->>'equipment_code_raw','')) != ''
                      AND trim(coalesce(dn.properties_json->>'{mo_prop}','')) != ''
                    ON CONFLICT DO NOTHING
                """)

            # ATG: via tag_tangki / tag_atg
            connection.execute(f"""
                INSERT INTO kg_relationship
                    (relationship_id, source_node_id, target_node_id,
                     relationship_type, properties_json, is_candidate, confidence)
                SELECT DISTINCT
                    'rel_' || md5(eq.node_id || '|EQUIPMENT_HAS_ATG|' || dn.node_id),
                    eq.node_id, dn.node_id, 'EQUIPMENT_HAS_ATG',
                    '{{}}'::jsonb, false, 1.0
                FROM kg_node eq, kg_node dn
                WHERE eq.node_type = 'equipment'
                  AND dn.node_type = 'atg'
                  AND (
                    regexp_replace(trim(coalesce(eq.properties_json->>'equipment_code_raw','')), '/[0-9]+$', '')
                      = regexp_replace(trim(coalesce(dn.properties_json->>'tag_tangki','')), '/[0-9]+$', '')
                    OR regexp_replace(trim(coalesce(eq.properties_json->>'equipment_code_raw','')), '/[0-9]+$', '')
                      = regexp_replace(trim(coalesce(dn.properties_json->>'tag_atg','')), '/[0-9]+$', '')
                  )
                  AND trim(coalesce(eq.properties_json->>'equipment_code_raw','')) != ''
                ON CONFLICT DO NOTHING
            """)

            job.progress = 65

            # Cross-domain
            for src_type, tgt_type, rel_type in [
                ('critical_equipment', 'bad_actor',           'CRITICAL_EQUIPMENT_HAS_BAD_ACTOR'),
                ('zero_clamp',         'inspection',           'ZERO_CLAMP_HAS_INSPECTION'),
                ('zero_clamp',         'pipeline_inspection',  'ZERO_CLAMP_HAS_PIPELINE_INSPECTION'),
                ('power_steam',        'monitoring_operasi',   'POWER_STEAM_HAS_MONITORING_OPERASI'),
            ]:
                connection.execute(f"""
                    INSERT INTO kg_relationship
                        (relationship_id, source_node_id, target_node_id,
                         relationship_type, properties_json, is_candidate, confidence)
                    SELECT DISTINCT
                        'rel_' || md5(s.node_id || '|{rel_type}|' || t.node_id),
                        s.node_id, t.node_id, '{rel_type}',
                        '{{}}'::jsonb, false, 1.0
                    FROM kg_node s, kg_node t
                    WHERE s.node_type = '{src_type}'
                      AND t.node_type = '{tgt_type}'
                      AND regexp_replace(trim(coalesce(s.properties_json->>'equipment_raw', s.properties_json->>'tag_raw','')), '/[0-9]+$', '')
                        = regexp_replace(trim(coalesce(t.properties_json->>'equipment_raw', t.properties_json->>'tag_raw','')), '/[0-9]+$', '')
                      AND trim(coalesce(s.properties_json->>'equipment_raw', s.properties_json->>'tag_raw','')) != ''
                    ON CONFLICT DO NOTHING
                """)

            job.progress = 75

            # Plant / FLoc hierarchy
            for src_t, tgt_t, rel_t, join_col in [
                ('refinery_unit', 'plant',               'REFINERY_UNIT_HAS_PLANT',               'refinery_unit'),
                ('plant',         'equipment',           'PLANT_HAS_EQUIPMENT',                   'plant'),
                ('plant',         'functional_location', 'PLANT_HAS_FUNCTIONAL_LOCATION',         'plant'),
                ('functional_location', 'equipment',     'FUNCTIONAL_LOCATION_HAS_EQUIPMENT',     'functional_location'),
            ]:
                connection.execute(f"""
                    INSERT INTO kg_relationship
                        (relationship_id, source_node_id, target_node_id,
                         relationship_type, properties_json, is_candidate, confidence)
                    SELECT DISTINCT
                        'rel_' || md5(s.node_id || '|{rel_t}|' || t.node_id),
                        s.node_id, t.node_id, '{rel_t}',
                        '{{}}'::jsonb, false, 1.0
                    FROM kg_node s, kg_node t
                    WHERE s.node_type = '{src_t}' AND t.node_type = '{tgt_t}'
                      AND s.label = t.properties_json->>'{join_col}'
                    ON CONFLICT DO NOTHING
                """)

            job.progress = 85

            # RU → domain nodes — auto-discover semua node type yang punya refinery_unit property
            # Kecualikan node yang memang bukan domain laporan
            _RU_EXCLUDE = {'equipment', 'refinery_unit', 'plant', 'functional_location'}
            _RU_REL_OVERRIDE = {
                'equipment_issue': 'REFINERY_UNIT_HAS_ISSUE',
                'icu_issue':       'REFINERY_UNIT_HAS_ICU_ISSUE',
            }
            domain_node_types = rows(connection, """
                SELECT DISTINCT node_type FROM kg_node
                WHERE properties_json->>'refinery_unit' IS NOT NULL
                  AND node_type != ALL(%s)
            """, [list(_RU_EXCLUDE)])
            for row in domain_node_types:
                domain_type = row['node_type']
                rel_type = _RU_REL_OVERRIDE.get(
                    domain_type,
                    'REFINERY_UNIT_HAS_' + domain_type.upper()
                )
                connection.execute(f"""
                    INSERT INTO kg_relationship
                        (relationship_id, source_node_id, target_node_id,
                         relationship_type, properties_json, is_candidate, confidence)
                    SELECT DISTINCT
                        'rel_' || md5(ru.node_id || '|{rel_type}|' || dn.node_id),
                        ru.node_id, dn.node_id, '{rel_type}',
                        '{{}}'::jsonb, false, 1.0
                    FROM kg_node ru, kg_node dn
                    WHERE ru.node_type = 'refinery_unit'
                      AND dn.node_type = '{domain_type}'
                      AND ru.label = dn.properties_json->>'refinery_unit'
                    ON CONFLICT DO NOTHING
                """)

            job.progress = 95
            node_count = fetch_tuple(connection, "SELECT count(*) FROM kg_node")[0]
            edge_count = fetch_tuple(connection,
                "SELECT count(*) FROM kg_relationship WHERE NOT is_candidate")[0]

        from .config import update_dataset_counts
        update_dataset_counts(dataset_id, node_count, edge_count,
                              row.get("issue_count", 0), row.get("workbooks") or [])
        job.status = "completed"
        job.phase = "Selesai"
        job.progress = 100
        job.message = f"{edge_count:,} relasi berhasil dibangun"
    except Exception as exc:
        job.status = "failed"
        job.error = str(exc)
    finally:
        job.finished_at = _time.time()

@app.post("/api/datasets/{dataset_id}/rebuild-relationships")
def rebuild_relationships(dataset_id: str):
    """Rebuild semua relasi — berjalan di background, kembalikan job_id untuk polling."""
    row = get_dataset_row(dataset_id)
    if not row:
        raise HTTPException(404, "Dataset tidak ditemukan.")

    job = ImportJob(id=str(uuid.uuid4()), name=f"Rebuild relasi — {row['name']}", dataset_id=dataset_id)
    with JOBS_LOCK:
        JOBS[job.id] = job
    threading.Thread(target=_run_rebuild, args=(job, dataset_id, row), daemon=True).start()
    return job.public()


@app.get("/api/imports/{job_id}")
def import_status(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "Import job tidak ditemukan.")
    return job.public()


@app.delete("/api/imports/{job_id}")
def stop_import(job_id: str):
    if job_id not in JOBS:
        raise HTTPException(404, "Import job tidak ditemukan.")
    cancel_job(job_id)
    return {"ok": True}


@app.get("/api/datasets")
def datasets():
    return list_datasets()


@app.get("/api/datasets/{dataset_id}")
def dataset(dataset_id: str):
    return get_dataset(dataset_id)


@app.patch("/api/datasets/{dataset_id}")
def rename_dataset(dataset_id: str, payload: RenameRequest):
    get_dataset(dataset_id)
    name = payload.name.strip()
    if not name:
        raise HTTPException(400, "Nama tidak boleh kosong.")
    return _rename_dataset(dataset_id, name)


@app.delete("/api/datasets/{dataset_id}")
def delete_dataset(dataset_id: str):
    get_dataset(dataset_id)
    _delete_dataset_data(dataset_id)
    _invalidate_insight_cache(dataset_id)
    return {"ok": True}


@app.post("/api/reset")
def reset_all():
    """Hapus semua dataset, node, dan relasi — kembali ke kondisi awal kosong."""
    _INSIGHT_CACHE.clear()
    return _reset_all()


@app.get("/api/datasets/{dataset_id}/load-summary")
def load_summary_endpoint(dataset_id: str):
    get_dataset(dataset_id)
    with db_for(dataset_id) as connection:
        return rows(
            connection,
            "SELECT workbook, sheet_name, row_count, node_count, edge_count, issue_count, status "
            "FROM load_summary ORDER BY workbook, sheet_name",
        )


@app.get("/api/datasets/{dataset_id}/stats")
def stats(dataset_id: str):
    with_db(dataset_id)
    connection = db_for(dataset_id)
    try:
        node_types = rows(connection, "SELECT node_type, count(*) count FROM kg_node GROUP BY node_type ORDER BY count DESC")
        edge_types = rows(connection, "SELECT relationship_type, is_candidate, count(*) count FROM kg_relationship GROUP BY relationship_type,is_candidate ORDER BY count DESC")
        totals = fetch_tuple(
            connection,
            "SELECT (SELECT count(*) FROM kg_node), "
            "(SELECT count(*) FROM kg_relationship WHERE NOT is_candidate), "
            "(SELECT count(*) FROM kg_relationship WHERE is_candidate), "
            "(SELECT count(*) FROM import_issue)"
        )
        return {
            "nodes": totals[0], "verified_edges": totals[1],
            "candidate_edges": totals[2], "issues": totals[3],
            "node_types": node_types, "edge_types": edge_types,
        }
    finally:
        connection.close()


@app.get("/api/datasets/{dataset_id}/query-metadata")
def query_metadata(dataset_id: str):
    connection = db_for(dataset_id)
    try:
        node_types = rows(connection, "SELECT node_type, count(*) count FROM kg_node GROUP BY node_type ORDER BY count DESC")
        edge_types = rows(connection, "SELECT relationship_type, is_candidate, count(*) count FROM kg_relationship GROUP BY relationship_type,is_candidate ORDER BY count DESC")
        core_node_fields = list(NODE_COLUMNS.keys())
        core_edge_fields = list(EDGE_COLUMNS.keys())
        return {
            "node_types": [
                {
                    "type": item["node_type"],
                    "count": item["count"],
                    "fields": _query_fields_for_type(connection, "kg_node", "node_type", item["node_type"], core_node_fields),
                }
                for item in node_types
            ],
            "edge_types": [
                {
                    "type": item["relationship_type"],
                    "is_candidate": item["is_candidate"],
                    "count": item["count"],
                    "fields": _query_fields_for_type(connection, "kg_relationship", "relationship_type", item["relationship_type"], core_edge_fields),
                }
                for item in edge_types
            ],
            "core_node_fields": core_node_fields,
            "core_edge_fields": core_edge_fields,
        }
    finally:
        connection.close()


@app.get("/api/datasets/{dataset_id}/readiness-context/{node_id:path}")
def readiness_context(dataset_id: str, node_id: str):
    connection = db_for(dataset_id)
    try:
        # Read berat (scan readiness/inspection per-RU): cap waktu agar gagal cepat,
        # tidak menumpuk backend nyangkut di proxy (aturan hard-won di CLAUDE.md).
        connection.execute("SET LOCAL statement_timeout = '20s'")
        return _readiness_context_for_node(connection, node_id)
    finally:
        connection.close()


@app.get("/api/datasets/{dataset_id}/search")
def search(
    dataset_id: str,
    q: str = "",
    node_type: str = "",
    domain: str = "",
    refinery_unit: str = "",
    equipment_code: str = "",
    limit: int = Query(50, ge=1, le=200),
):
    connection = db_for(dataset_id)
    try:
        # Batasi waktu query: tanpa ini, satu pencarian lambat tetap men-scan di
        # server walau klien sudah menyerah (proxy timeout), sehingga backend
        # menumpuk sebagai koneksi orphaned berjam-jam (pernah terlihat 9 query
        # 'SELECT * FROM kg_node' nyangkut 4,5 jam). 20 detik cukup untuk dataset
        # 1,5jt baris dengan pencarian kolom (≈0,6 dtk), dan menggagalkan cepat
        # bila ada yang patologis.
        connection.execute("SET LOCAL statement_timeout = '20s'")
        clauses, params = [], []
        if q:
            # Cari hanya di kolom ringkas (label, business_key, node_id) + identifier.
            # JANGAN cast properties_json::text lalu LIKE — itu memaksa seq-scan penuh
            # 1,5jt baris jsonb (≈15 dtk) tanpa bisa pakai index; versi kolom ≈0,6 dtk
            # dengan recall hampir identik. Pencarian bebas di dalam properties_json
            # dilayani fitur Property Query yang terpisah.
            clauses.append("""
                (
                    lower(coalesce(label,'') || ' ' || coalesce(business_key,'') || ' ' || node_id) LIKE %s
                    OR EXISTS (
                        SELECT 1 FROM kg_identifier i
                        WHERE i.equipment_node_id=kg_node.node_id
                          AND lower(i.identifier) LIKE %s
                    )
                )
            """)
            params.extend([f"%{q.lower()}%", f"%{q.lower()}%"])
        if node_type:
            clauses.append("node_type=%s")
            params.append(node_type)
        if domain:
            clauses.append("domain=%s")
            params.append(domain)
        if refinery_unit:
            clauses.append("""
                (
                    lower(coalesce((properties_json ->> 'refinery_unit'), '')) LIKE %s
                    OR lower(coalesce((properties_json ->> 'ru'), '')) LIKE %s
                    OR lower(coalesce(label, '')) = lower(%s)
                    OR lower(coalesce(business_key, '')) = lower(%s)
                )
            """)
            params.extend([f"%{refinery_unit.lower()}%", f"%{refinery_unit.lower()}%", refinery_unit, refinery_unit])
        if equipment_code:
            clauses.append("""
                (
                    lower(coalesce((properties_json ->> 'equipment_code_normalized'), '')) LIKE %s
                    OR lower(coalesce((properties_json ->> 'equipment_id'), '')) LIKE %s
                    OR lower(coalesce(label, '')) LIKE %s
                    OR lower(coalesce(business_key, '')) LIKE %s
                )
            """)
            pattern = f"%{equipment_code.lower()}%"
            params.extend([pattern, pattern, pattern, pattern])
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return [
            node_to_api(item)
            for item in rows(connection, f"SELECT * FROM kg_node {where} ORDER BY label NULLS LAST LIMIT %s", params + [limit])
        ]
    finally:
        connection.close()


@app.get("/api/datasets/{dataset_id}/nodes/{node_id:path}")
def node_detail(dataset_id: str, node_id: str):
    connection = db_for(dataset_id)
    try:
        item = one(connection, "SELECT * FROM kg_node WHERE node_id=%s", [node_id])
        if not item:
            raise HTTPException(404, "Node tidak ditemukan.")
        result = node_to_api(item, connection)
        result["domain_record"] = one(
            connection,
            "SELECT * FROM domain_record WHERE source_record_id=%s LIMIT 1",
            [item.get("source_record_id")],
        )
        if result["domain_record"]:
            result["domain_record"]["record"] = parse_json(result["domain_record"].pop("record_json"))
        return result
    finally:
        connection.close()


@app.get("/api/datasets/{dataset_id}/relationships/{relationship_id}")
def relationship_detail(dataset_id: str, relationship_id: str):
    connection = db_for(dataset_id)
    try:
        item = one(connection, "SELECT * FROM kg_relationship WHERE relationship_id=%s", [relationship_id])
        if not item:
            raise HTTPException(404, "Relationship tidak ditemukan.")
        source = one(connection, "SELECT * FROM kg_node WHERE node_id=%s", [item["source_node_id"]])
        target = one(connection, "SELECT * FROM kg_node WHERE node_id=%s", [item["target_node_id"]])
        result = edge_to_api(item, connection, source, target)
        result["source_node"] = node_to_api(source) if source else None
        result["target_node"] = node_to_api(target) if target else None
        return result
    finally:
        connection.close()


@app.post("/api/datasets/{dataset_id}/property-query")
def property_query(dataset_id: str, payload: PropertyQueryRequest):
    connection = db_for(dataset_id)
    try:
        return _run_property_query(connection, payload.query, payload.limit)
    finally:
        connection.close()


@app.get("/api/datasets/{dataset_id}/neighbors/{node_id:path}")
def neighbors(
    dataset_id: str,
    node_id: str,
    depth: int = Query(1, ge=1, le=5),
    limit: int = Query(300, ge=10, le=3000),
    include_candidates: bool = False,
    min_confidence: float = Query(0.8, ge=0, le=1),
    relationship_type: str = "",
    domain: str = "",
    node_type: str = "",
    refinery_unit: str = "",
    equipment_code: str = "",
):
    connection = db_for(dataset_id)
    try:
        # Read berat (BFS + sintesis tag-match): cap waktu agar gagal cepat (aturan CLAUDE.md).
        connection.execute("SET LOCAL statement_timeout = '20s'")
        root = one(connection, "SELECT * FROM kg_node WHERE node_id=%s", [node_id])
        if not root:
            raise HTTPException(404, "Node tidak ditemukan.")

        visible_ids: list[str] = [node_id]
        visible_set = {node_id}
        frontier = [node_id]
        edge_by_id: dict[str, dict] = {}
        truncated = False
        next_frontier_count = 0
        high_degree_warning = ""

        effective_limit = min(limit, 300 if root.get("node_type") == "refinery_unit" else limit)
        max_edges_per_frontier = max(25, min(250, effective_limit * 2))

        for _hop in range(depth):
            if not frontier or len(visible_ids) >= effective_limit:
                break
            edge_rows = _frontier_edges(
                connection, frontier, include_candidates, min_confidence,
                relationship_type, domain, max_edges_per_frontier + 1,
            )
            if len(edge_rows) > max_edges_per_frontier:
                truncated = True
                edge_rows = edge_rows[:max_edges_per_frontier]
            next_frontier: list[str] = []
            for edge in edge_rows:
                edge_by_id.setdefault(edge["relationship_id"], edge)
                other = edge["target_node_id"] if edge["source_node_id"] in frontier else edge["source_node_id"]
                expand_other = not _is_parent_hub_backtrack(edge, other)
                if other not in visible_set:
                    if len(visible_ids) >= effective_limit:
                        truncated = True
                        next_frontier_count += 1
                        continue
                    visible_set.add(other)
                    visible_ids.append(other)
                    if expand_other:
                        next_frontier.append(other)
            frontier = next_frontier[:50]

        if len(visible_ids) >= effective_limit:
            truncated = True
        if root.get("node_type") == "refinery_unit" and truncated:
            high_degree_warning = f"Refinery Unit memiliki banyak koneksi; menampilkan {len(visible_ids)} node pertama. Gunakan filter atau expand bertahap."

        placeholders = ",".join("%s" for _ in visible_ids)
        node_rows = rows(connection, f"SELECT * FROM kg_node WHERE node_id IN ({placeholders})", visible_ids)
        if node_type or refinery_unit or equipment_code:
            node_rows = [
                item for item in node_rows
                if _node_filter(item, node_type, refinery_unit, equipment_code) or item["node_id"] == node_id
            ]
            allowed = {item["node_id"] for item in node_rows}
            edge_by_id = {
                key: edge for key, edge in edge_by_id.items()
                if edge["source_node_id"] in allowed and edge["target_node_id"] in allowed
            }

        allowed_node_ids = {n["node_id"] for n in node_rows}
        visible_edges = [
            edge for edge in edge_by_id.values()
            if edge["source_node_id"] in allowed_node_ids and edge["target_node_id"] in allowed_node_ids
        ]
        degree = _visible_degree_summary(node_id, visible_edges, truncated)
        api_nodes = [node_to_api(item) for item in node_rows]
        api_edges = [edge_to_api(item) for item in visible_edges]

        # Link sintetis: readiness/inspection tak punya edge nyata ke equipment, jadi cocokkan
        # via tag (exact-boundary) dan tampilkan sebagai edge candidate (putus-putus) HANYA saat
        # toggle candidate aktif dan root-nya equipment. Reuse logika match jalur prompt.
        if include_candidates and root.get("node_type") == "equipment":
            # Sintesis link tag-match tak boleh pernah menggagalkan seluruh graph. Bila ada
            # data/row tak terduga yang memicu exception, kembalikan graph dasar apa adanya
            # (pola sama dengan _reliability_engineering_signals). Tanpa ini, satu error di sini
            # membuat endpoint 500 dan frontend menahan graph lama -> "Candidate seolah tak berefek".
            try:
                root_properties = parse_json(root.get("properties_json"))
                present_ids = {item["id"] for item in api_nodes}
                budget = max(0, effective_limit - len(api_nodes))
                for config, row, matched_token in _iter_tag_match_candidates(
                    connection, root, root_properties, present_ids, budget,
                    relationship_type=relationship_type, node_type=node_type,
                ):
                    api_nodes.append(node_to_api(row))
                    api_edges.append(edge_to_api(_synthetic_tag_link_edge(node_id, row, config, matched_token)))
            except Exception:
                pass

        return {
            "nodes": api_nodes,
            "edges": api_edges,
            "truncated": truncated,
            "next_frontier_count": next_frontier_count,
            "high_degree_warning": high_degree_warning,
            "degree": degree,
        }
    finally:
        connection.close()


@app.get("/api/datasets/{dataset_id}/nodes/{node_id:path}/degree")
def node_degree(dataset_id: str, node_id: str, include_candidates: bool = False):
    connection = db_for(dataset_id)
    try:
        if not one(connection, "SELECT node_id FROM kg_node WHERE node_id=%s LIMIT 1", [node_id]):
            raise HTTPException(404, "Node tidak ditemukan.")
        return _node_degree_summary(connection, node_id, include_candidates)
    finally:
        connection.close()


@app.get("/api/datasets/{dataset_id}/directed-descendants/{node_id:path}")
def directed_descendants(
    dataset_id: str,
    node_id: str,
    min_depth: int = Query(3, ge=1, le=5),
    max_depth: int = Query(5, ge=1, le=5),
    limit: int = Query(300, ge=10, le=3000),
    relationship_type: str = "",
    include_candidates: bool = False,
):
    connection = db_for(dataset_id)
    try:
        root = one(connection, "SELECT * FROM kg_node WHERE node_id=%s", [node_id])
        if not root:
            raise HTTPException(404, "Node tidak ditemukan.")
        if min_depth > max_depth:
            min_depth = max_depth

        visible_ids: list[str] = [node_id]
        visible_set = {node_id}
        edge_by_id: dict[str, dict] = {}
        paths: list[dict] = []
        frontier = [{
            "node_id": node_id,
            "depth": 0,
            "node_path": [node_id],
            "label_path": [root.get("label") or node_id],
            "relationship_path": [],
        }]
        truncated = False
        max_depth_found = 0
        effective_limit = min(limit, 300 if root.get("node_type") == "refinery_unit" else limit)
        # Directed mode must preserve budget for deeper descendants. If the first
        # hop from a high-degree RU consumes the whole limit, no 3+ hop path can
        # be shown even when it exists.
        max_edges_per_frontier = max(15, min(160, effective_limit // max_depth))

        for _hop in range(max_depth):
            if not frontier or len(visible_ids) >= effective_limit:
                break
            frontier_ids = [item["node_id"] for item in frontier]
            edge_rows = _directed_frontier_edges(
                connection, frontier_ids, include_candidates, relationship_type, max_edges_per_frontier + 1
            )
            if len(edge_rows) > max_edges_per_frontier:
                truncated = True
                edge_rows = edge_rows[:max_edges_per_frontier]
            labels = _node_labels(connection, [edge["target_node_id"] for edge in edge_rows])
            by_source: dict[str, list[dict]] = {}
            for edge in edge_rows:
                by_source.setdefault(edge["source_node_id"], []).append(edge)

            next_frontier = []
            for state in frontier:
                for edge in by_source.get(state["node_id"], []):
                    target = edge["target_node_id"]
                    if target in state["node_path"]:
                        continue
                    edge_by_id.setdefault(edge["relationship_id"], edge)
                    if target not in visible_set:
                        if len(visible_ids) >= effective_limit:
                            truncated = True
                            continue
                        visible_set.add(target)
                        visible_ids.append(target)
                    depth = state["depth"] + 1
                    max_depth_found = max(max_depth_found, depth)
                    next_state = {
                        "node_id": target,
                        "depth": depth,
                        "node_path": state["node_path"] + [target],
                        "label_path": state["label_path"] + [labels.get(target, target)],
                        "relationship_path": state["relationship_path"] + [edge["relationship_type"]],
                    }
                    if depth >= min_depth:
                        paths.append({
                            "depth": depth,
                            "node_id_path": next_state["node_path"],
                            "label_path": next_state["label_path"],
                            "relationship_path": next_state["relationship_path"],
                        })
                    if depth < max_depth:
                        next_frontier.append(next_state)
            frontier = next_frontier[:50]

        placeholders = ",".join("%s" for _ in visible_ids)
        node_rows = rows(connection, f"SELECT * FROM kg_node WHERE node_id IN ({placeholders})", visible_ids)
        allowed_node_ids = {item["node_id"] for item in node_rows}
        visible_edges = [
            edge for edge in edge_by_id.values()
            if edge["source_node_id"] in allowed_node_ids and edge["target_node_id"] in allowed_node_ids
        ]
        if len(visible_ids) >= effective_limit:
            truncated = True
        return {
            "nodes": [node_to_api(item) for item in node_rows],
            "edges": [edge_to_api(item) for item in visible_edges],
            "paths": paths[:100],
            "has_deep_descendants": any(path["depth"] >= min_depth for path in paths),
            "max_depth_found": max_depth_found,
            "truncated": truncated,
        }
    finally:
        connection.close()


def _frontier_edges(
    connection,
    frontier: list[str],
    include_candidates: bool,
    min_confidence: float,
    relationship_type: str,
    domain: str,
    limit: int,
) -> list[dict]:
    placeholders = ",".join("%s" for _ in frontier)
    filter_clauses = [
        "(NOT is_candidate OR %s)",
        "(confidence IS NULL OR confidence >= %s)",
    ]
    filter_params: list = [include_candidates, min_confidence]
    if relationship_type:
        filter_clauses.append("relationship_type=%s")
        filter_params.append(relationship_type)
    if domain:
        filter_clauses.append("domain=%s")
        filter_params.append(domain)
    priority_placeholders = ",".join("%s" for _ in PRIORITY_RELATIONSHIPS)
    filter_sql = " AND ".join(filter_clauses)
    params: list = (
        frontier + filter_params +
        frontier + filter_params +
        PRIORITY_RELATIONSHIPS +
        [limit]
    )
    return rows(
        connection,
        f"""
        SELECT *
        FROM (
          SELECT * FROM kg_relationship
          WHERE source_node_id IN ({placeholders}) AND {filter_sql}
          UNION ALL
          SELECT * FROM kg_relationship
          WHERE target_node_id IN ({placeholders}) AND {filter_sql}
        )
        ORDER BY
          CASE WHEN relationship_type IN ({priority_placeholders}) THEN 0 ELSE 1 END,
          confidence DESC NULLS LAST,
          relationship_type
        LIMIT %s
        """,
        params,
    )


def _directed_frontier_edges(
    connection,
    frontier: list[str],
    include_candidates: bool,
    relationship_type: str,
    limit: int,
) -> list[dict]:
    if not frontier:
        return []
    placeholders = ",".join("%s" for _ in frontier)
    clauses = [f"source_node_id IN ({placeholders})", "(NOT is_candidate OR %s)"]
    params: list = frontier + [include_candidates]
    if relationship_type:
        clauses.append("relationship_type=%s")
        params.append(relationship_type)
    priority_placeholders = ",".join("%s" for _ in PRIORITY_RELATIONSHIPS)
    params.extend(PRIORITY_RELATIONSHIPS)
    params.append(limit)
    return rows(
        connection,
        f"""
        SELECT *
        FROM kg_relationship
        WHERE {' AND '.join(clauses)}
        ORDER BY
          CASE WHEN relationship_type IN ({priority_placeholders}) THEN 0 ELSE 1 END,
          confidence DESC NULLS LAST,
          relationship_type
        LIMIT %s
        """,
        params,
    )


def _node_labels(connection, node_ids: list[str]) -> dict[str, str]:
    if not node_ids:
        return {}
    unique_ids = list(dict.fromkeys(node_ids))
    placeholders = ",".join("%s" for _ in unique_ids)
    return {
        item["node_id"]: item.get("label") or item["node_id"]
        for item in rows(connection, f"SELECT node_id,label FROM kg_node WHERE node_id IN ({placeholders})", unique_ids)
    }


def _is_parent_hub_backtrack(edge: dict, other_node_id: str) -> bool:
    relationship_type = edge.get("relationship_type") or ""
    parent_first_relationships = {
        "REFINERY_UNIT_HAS_PLANT",
        "REFINERY_UNIT_HAS_EQUIPMENT",
        "PLANT_HAS_FUNCTIONAL_LOCATION",
        "FUNCTIONAL_LOCATION_HAS_EQUIPMENT",
    }
    return relationship_type in parent_first_relationships and other_node_id == edge.get("source_node_id")


def _node_degree_summary(connection, node_id: str, include_candidates: bool = False) -> dict:
    edge_filter = "" if include_candidates else "AND NOT is_candidate"
    total = fetch_tuple(
        connection,
        f"SELECT count(*) FROM kg_relationship WHERE (source_node_id=%s OR target_node_id=%s) {edge_filter}",
        [node_id, node_id],
    )[0]
    candidate_count = fetch_tuple(
        connection,
        "SELECT count(*) FROM kg_relationship WHERE (source_node_id=%s OR target_node_id=%s) AND is_candidate",
        [node_id, node_id],
    )[0]
    by_type = rows(
        connection,
        f"""
        SELECT relationship_type, count(*) count
        FROM kg_relationship
        WHERE (source_node_id=%s OR target_node_id=%s) {edge_filter}
        GROUP BY relationship_type
        ORDER BY count DESC
        LIMIT 50
        """,
        [node_id, node_id],
    )
    return {
        "node_id": node_id,
        "total_edges": total,
        "candidate_edges": candidate_count,
        "by_relationship_type": by_type,
        "high_degree": total > 500,
    }


def _visible_degree_summary(node_id: str, edge_rows: list[dict], high_degree: bool) -> dict:
    counts: dict[str, int] = {}
    candidate_edges = 0
    total_edges = 0
    for edge in edge_rows:
        if edge.get("source_node_id") != node_id and edge.get("target_node_id") != node_id:
            continue
        total_edges += 1
        if edge.get("is_candidate"):
            candidate_edges += 1
        relationship_type = edge.get("relationship_type") or "UNKNOWN"
        counts[relationship_type] = counts.get(relationship_type, 0) + 1
    return {
        "node_id": node_id,
        "total_edges": total_edges,
        "candidate_edges": candidate_edges,
        "by_relationship_type": [
            {"relationship_type": key, "count": value}
            for key, value in sorted(counts.items(), key=lambda item: item[1], reverse=True)
        ],
        "high_degree": high_degree,
    }


@app.get("/api/datasets/{dataset_id}/equipment/{node_id:path}/360")
def equipment_360(dataset_id: str, node_id: str, include_candidates: bool = True):
    connection = db_for(dataset_id)
    try:
        connection.execute("SET LOCAL statement_timeout = '15s'")
        equipment = one(connection, "SELECT * FROM kg_node WHERE node_id=%s", [node_id])
        if not equipment:
            raise HTTPException(404, "Equipment tidak ditemukan.")
        related = rows(connection, """
            SELECT n.*, r.relationship_type, r.is_candidate, r.confidence
            FROM kg_relationship r
            JOIN kg_node n ON n.node_id=CASE WHEN r.source_node_id=%s THEN r.target_node_id ELSE r.source_node_id END
            WHERE (r.source_node_id=%s OR r.target_node_id=%s) AND NOT r.is_candidate
            ORDER BY coalesce((n.properties_json ->> 'reference_date'),
                              (n.properties_json ->> 'status_date'),
                              (n.properties_json ->> 'plan_date'),
                              (n.properties_json ->> 'month_update')) DESC NULLS LAST
            LIMIT 1000
        """, [node_id, node_id, node_id])
        related_api = [node_to_api(item) | {
            "relationship_type": item["relationship_type"],
            "is_candidate": item["is_candidate"],
            "confidence": item["confidence"],
        } for item in related]

        # Domain tanpa edge nyata ke equipment ini (readiness/inspection/reliability/RKAP/issue)
        # disurfacing sebagai kartu kandidat lewat tag-match exact-boundary — matcher SAMA dengan
        # endpoint `neighbors` (lihat _iter_tag_match_candidates). Dedupe terhadap relasi nyata di
        # atas via present_ids. Dibungkus try/except agar satu baris aneh tak pernah 500 halaman.
        if include_candidates and equipment["node_type"] == "equipment":
            try:
                equipment_properties = parse_json(equipment.get("properties_json"))
                present_ids = {item["id"] for item in related_api}
                for config, row, matched_token in _iter_tag_match_candidates(
                    connection, equipment, equipment_properties, present_ids, budget=300,
                ):
                    related_api.append(node_to_api(row) | {
                        "relationship_type": config["relationship_type"],
                        "is_candidate": True,
                        "confidence": None,
                        "matched_token": matched_token,
                    })
            except Exception:
                pass

        return {
            "equipment": node_to_api(equipment),
            "related": related_api,
        }
    finally:
        connection.close()


@app.get("/api/datasets/{dataset_id}/issues")
def issues(
    dataset_id: str,
    issue_type: str = "",
    limit: int = Query(200, ge=1, le=2000),
    offset: int = Query(0, ge=0),
):
    connection = db_for(dataset_id)
    try:
        where = "WHERE issue_type=%s" if issue_type else ""
        params = [issue_type] if issue_type else []
        total = fetch_tuple(connection, f"SELECT count(*) FROM import_issue {where}", params)[0]
        return {
            "total": total,
            "items": rows(
                connection,
                f"SELECT * FROM import_issue {where} ORDER BY source_file,source_sheet,source_row LIMIT %s OFFSET %s",
                params + [limit, offset],
            ),
        }
    finally:
        connection.close()


@app.get("/api/datasets/{dataset_id}/audit/{issue_type}")
def audit(dataset_id: str, issue_type: str, limit: int = Query(500, ge=1, le=2000), offset: int = Query(0, ge=0)):
    if issue_type == "relationship_candidates":
        connection = db_for(dataset_id)
        try:
            total = fetch_tuple(connection, "SELECT count(*) FROM kg_relationship WHERE is_candidate")[0]
            return {
                "total": total,
                "items": [edge_to_api(item) for item in rows(
                    connection,
                    "SELECT * FROM kg_relationship WHERE is_candidate ORDER BY confidence DESC NULLS LAST LIMIT %s OFFSET %s",
                    [limit, offset],
                )],
            }
        finally:
            connection.close()
    return issues(dataset_id, issue_type, limit, offset)


@app.get("/api/datasets/{dataset_id}/ru-summary")
def ru_summary(dataset_id: str):
    def compute():
        connection = db_for(dataset_id)
        try:
            equipment_summary = _analysis_rows(connection, "ru_equipment_summary", 100)
            readiness_summary = _readiness_association_summary(connection)
            for row in equipment_summary:
                key = _ru_key(row.get("refinery_unit"))
                if key in readiness_summary:
                    row.update(readiness_summary[key])

            # Fallback: hitung langsung dari graph jika ETL summary tidak tersedia
            if not equipment_summary:
                equipment_summary = rows(connection, """
                    SELECT
                        coalesce(nullif(properties_json->>'refinery_unit',''), 'Unknown') AS refinery_unit,
                        count(*) AS equipment_count
                    FROM kg_node WHERE node_type='equipment'
                    GROUP BY refinery_unit
                    ORDER BY equipment_count DESC
                """)
                # Gabungkan maintenance orders per RU
                mo_by_ru = {r['refinery_unit']: r['c'] for r in rows(connection, """
                    SELECT coalesce(nullif(properties_json->>'refinery_unit',''), 'Unknown') AS refinery_unit,
                           count(*) AS c
                    FROM kg_node WHERE node_type='maintenance_order'
                    GROUP BY refinery_unit
                """)}
                rkap_by_ru = {r['refinery_unit']: r['c'] for r in rows(connection, """
                    SELECT coalesce(nullif(properties_json->>'refinery_unit',''), 'Unknown') AS refinery_unit,
                           count(*) AS c
                    FROM kg_node WHERE node_type='rkap_program'
                    GROUP BY refinery_unit
                """)}
                equipment_summary = [
                    {
                        **row,
                        'maintenance_orders': mo_by_ru.get(row['refinery_unit'], 0),
                        'rkap_programs': rkap_by_ru.get(row['refinery_unit'], 0),
                        'unmatched_identifiers': 0,
                    }
                    for row in equipment_summary
                ]
                # Juga gabungkan readiness
                readiness_summary = _readiness_association_summary(connection)
                for row in equipment_summary:
                    key = _ru_key(row.get("refinery_unit"))
                    if key in readiness_summary:
                        row.update(readiness_summary[key])

            refinery_units = _analysis_rows(connection, "refinery_units", 100)
            if not refinery_units:
                refinery_units = [{'refinery_unit': r['refinery_unit']} for r in equipment_summary]

            return {
                "refinery_units": refinery_units,
                "equipment_summary": equipment_summary,
                "data_coverage": _analysis_rows(connection, "ru_data_coverage", 1000),
                "relationship_quality": _analysis_rows(connection, "ru_relationship_quality", 1000),
            }
        finally:
            connection.close()

    return _cached_insight(
        dataset_id, "ru_summary", compute,
        fallback={"refinery_units": [], "equipment_summary": [], "data_coverage": [], "relationship_quality": []},
    )


@app.get("/api/datasets/{dataset_id}/schema")
def schema(dataset_id: str):
    connection = db_for(dataset_id)
    try:
        graph_schema = _analysis_rows(connection, "graph_schema", 2000)
        ontology_depth = _analysis_rows(connection, "ontology_depth", 2000)
        deepest_paths = _analysis_rows(connection, "deepest_paths", 2000)

        # Fallback: hitung langsung dari graph jika ETL tidak menyertakan file summary
        if not graph_schema:
            graph_schema = rows(connection, """
                SELECT
                    r.relationship_type,
                    s.node_type AS source_node_type,
                    t.node_type AS target_node_type,
                    count(*) AS relationship_count
                FROM kg_relationship r
                JOIN kg_node s ON s.node_id = r.source_node_id
                JOIN kg_node t ON t.node_id = r.target_node_id
                GROUP BY r.relationship_type, s.node_type, t.node_type
                ORDER BY relationship_count DESC
                LIMIT 500
            """)

        if not ontology_depth:
            # Bangun ontology_depth dari graph_schema: satu baris per relationship
            ontology_depth = [
                {
                    "relationship_path": row.get("relationship_type", ""),
                    "node_path": f"{row.get('source_node_type','')} → {row.get('target_node_type','')}",
                    "depth": 1,
                    "relationship_count": row.get("relationship_count", 0),
                }
                for row in graph_schema
            ]

        if not deepest_paths:
            # Bangun deepest_paths dari graph_schema: tampilkan top relationship terbanyak
            deepest_paths = [
                {
                    "path_id": f"live-{i}",
                    "path_pattern": f"{row.get('source_node_type','')} → {row.get('target_node_type','')}",
                    "path_depth": 1,
                    "label_path": f"{row.get('source_node_type','')} → {row.get('target_node_type','')}",
                    "analysis_scope": f"{row.get('relationship_count', 0)} relasi ({row.get('relationship_type','')})",
                    "relationship_type": row.get("relationship_type", ""),
                }
                for i, row in enumerate(graph_schema[:50])
            ]

        return {
            "graph_schema": graph_schema,
            "ontology_depth": ontology_depth,
            "deepest_paths": deepest_paths,
        }
    finally:
        connection.close()


_RELIABILITY_FALLBACK = {
    "kpis": {}, "cross_domain_kpis": {}, "ru_ranking": [], "ru_reliability_portfolio": [],
    "mtbf_mttr_by_ru": [], "status_distribution": [], "high_risk_equipment": [],
    "equipment_action_queue": [], "coverage_alerts": [], "relationship_quality_alerts": [],
    "data_quality_backlog": [], "reliability_trend": [],
}


@app.get("/api/datasets/{dataset_id}/insights/reliability")
def reliability_insight(dataset_id: str):
    return _cached_insight(
        dataset_id, "reliability",
        lambda: _compute_reliability_insight(dataset_id),
        fallback=_RELIABILITY_FALLBACK,
    )


def _compute_reliability_insight(dataset_id: str):
    connection = db_for(dataset_id)
    try:
        kpis = one(connection, """
            WITH obs AS (
                SELECT
                    coalesce((properties_json ->> 'refinery_unit'), (properties_json ->> 'ru'), 'Unknown') refinery_unit,
                    nullif((properties_json ->> 'equipment'), '') equipment_code,
                    (CASE WHEN (nullif((properties_json ->> 'mtbf'), '')) ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN (nullif((properties_json ->> 'mtbf'), ''))::double precision END) mtbf,
                    (CASE WHEN (nullif((properties_json ->> 'mttr'), '')) ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN (nullif((properties_json ->> 'mttr'), ''))::double precision END) mttr,
                    (CASE WHEN (nullif((properties_json ->> 'running_hours'), '')) ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN (nullif((properties_json ->> 'running_hours'), ''))::double precision END) running_hours,
                    lower(coalesce((properties_json ->> 'status'), '')) status
                FROM kg_node
                WHERE node_type='reliability_observation'
            )
            SELECT
                count(*) observations,
                count(DISTINCT equipment_code) observed_equipment,
                avg(mtbf) avg_mtbf,
                avg(mttr) avg_mttr,
                avg(running_hours) avg_running_hours,
                sum(CASE WHEN mtbf IS NULL OR mtbf=0 THEN 1 ELSE 0 END) zero_mtbf_count,
                sum(CASE WHEN mttr IS NOT NULL AND mttr>24 THEN 1 ELSE 0 END) high_mttr_count,
                sum(CASE WHEN status <> '' AND status NOT IN ('running','run','operation','operating','normal','standby') THEN 1 ELSE 0 END) abnormal_status_count
            FROM obs
        """) or {}

        mtbf_mttr_by_ru = rows(connection, """
            WITH obs AS (
                SELECT
                    coalesce((properties_json ->> 'refinery_unit'), (properties_json ->> 'ru'), 'Unknown') refinery_unit,
                    nullif((properties_json ->> 'equipment'), '') equipment_code,
                    (CASE WHEN (nullif((properties_json ->> 'mtbf'), '')) ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN (nullif((properties_json ->> 'mtbf'), ''))::double precision END) mtbf,
                    (CASE WHEN (nullif((properties_json ->> 'mttr'), '')) ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN (nullif((properties_json ->> 'mttr'), ''))::double precision END) mttr,
                    (CASE WHEN (nullif((properties_json ->> 'running_hours'), '')) ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN (nullif((properties_json ->> 'running_hours'), ''))::double precision END) running_hours,
                    lower(coalesce((properties_json ->> 'status'), '')) status
                FROM kg_node
                WHERE node_type='reliability_observation'
            )
            SELECT
                refinery_unit,
                count(*) observations,
                count(DISTINCT equipment_code) observed_equipment,
                avg(mtbf) avg_mtbf,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY mtbf) median_mtbf,
                avg(mttr) avg_mttr,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY mttr) median_mttr,
                avg(running_hours) avg_running_hours,
                sum(CASE WHEN mtbf IS NULL OR mtbf=0 THEN 1 ELSE 0 END) zero_mtbf_count,
                sum(CASE WHEN mttr IS NOT NULL AND mttr>24 THEN 1 ELSE 0 END) high_mttr_count,
                sum(CASE WHEN status <> '' AND status NOT IN ('running','run','operation','operating','normal','standby') THEN 1 ELSE 0 END) abnormal_status_count
            FROM obs
            GROUP BY refinery_unit
            ORDER BY observations DESC
        """)

        status_distribution = rows(connection, """
            SELECT
                coalesce((properties_json ->> 'refinery_unit'), (properties_json ->> 'ru'), 'Unknown') refinery_unit,
                coalesce(nullif((properties_json ->> 'status'), ''), 'Unknown') status,
                count(*) count
            FROM kg_node
            WHERE node_type='reliability_observation'
            GROUP BY refinery_unit, status
            ORDER BY refinery_unit, count DESC
        """)

        high_risk_equipment = rows(connection, """
            WITH eq_obs AS (
                SELECT
                    e.node_id equipment_node_id,
                    e.label equipment_label,
                    e.business_key equipment_key,
                    coalesce((o.properties_json ->> 'refinery_unit'), (e.properties_json ->> 'refinery_unit'), (e.properties_json ->> 'ru'), 'Unknown') refinery_unit,
                    (CASE WHEN (nullif((o.properties_json ->> 'mtbf'), '')) ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN (nullif((o.properties_json ->> 'mtbf'), ''))::double precision END) mtbf,
                    (CASE WHEN (nullif((o.properties_json ->> 'mttr'), '')) ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN (nullif((o.properties_json ->> 'mttr'), ''))::double precision END) mttr,
                    (CASE WHEN (nullif((o.properties_json ->> 'running_hours'), '')) ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN (nullif((o.properties_json ->> 'running_hours'), ''))::double precision END) running_hours,
                    lower(coalesce((o.properties_json ->> 'status'), '')) status
                FROM kg_relationship r
                JOIN kg_node e ON e.node_id=r.source_node_id AND e.node_type='equipment'
                JOIN kg_node o ON o.node_id=r.target_node_id AND o.node_type='reliability_observation'
                WHERE r.relationship_type='EQUIPMENT_HAS_RELIABILITY_OBSERVATION'
                  AND NOT r.is_candidate
            ),
            scored AS (
                SELECT
                    equipment_node_id, equipment_label, equipment_key, refinery_unit,
                    count(*) observations,
                    avg(mtbf) avg_mtbf,
                    avg(mttr) avg_mttr,
                    max(running_hours) max_running_hours,
                    sum(CASE WHEN status <> '' AND status NOT IN ('running','run','operation','operating','normal','standby') THEN 1 ELSE 0 END) abnormal_status_count,
                    (
                        CASE WHEN avg(mtbf) IS NULL THEN 0 WHEN avg(mtbf)=0 THEN 35 WHEN avg(mtbf)<100 THEN 30 WHEN avg(mtbf)<500 THEN 18 ELSE 0 END +
                        CASE WHEN avg(mttr) IS NULL THEN 0 WHEN avg(mttr)>72 THEN 25 WHEN avg(mttr)>24 THEN 18 WHEN avg(mttr)>8 THEN 8 ELSE 0 END +
                        CASE WHEN max(running_hours) IS NULL THEN 0 WHEN max(running_hours)>8000 THEN 12 WHEN max(running_hours)>4000 THEN 6 ELSE 0 END +
                        CASE WHEN sum(CASE WHEN status <> '' AND status NOT IN ('running','run','operation','operating','normal','standby') THEN 1 ELSE 0 END)>0 THEN 15 ELSE 0 END
                    ) risk_score
                FROM eq_obs
                GROUP BY equipment_node_id, equipment_label, equipment_key, refinery_unit
            )
            SELECT *
            FROM scored
            WHERE risk_score > 0
            ORDER BY risk_score DESC, avg_mtbf ASC NULLS LAST, avg_mttr DESC NULLS LAST
            LIMIT 50
        """)

        equipment_action_queue = rows(connection, """
            WITH reliability AS (
                SELECT
                    e.node_id equipment_node_id,
                    e.label equipment_label,
                    e.business_key equipment_key,
                    coalesce((o.properties_json ->> 'refinery_unit'), (e.properties_json ->> 'refinery_unit'), 'Unknown') refinery_unit,
                    count(*) observations,
                    avg((CASE WHEN (nullif((o.properties_json ->> 'mtbf'), '')) ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN (nullif((o.properties_json ->> 'mtbf'), ''))::double precision END)) avg_mtbf,
                    avg((CASE WHEN (nullif((o.properties_json ->> 'mttr'), '')) ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN (nullif((o.properties_json ->> 'mttr'), ''))::double precision END)) avg_mttr,
                    max((CASE WHEN (nullif((o.properties_json ->> 'running_hours'), '')) ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN (nullif((o.properties_json ->> 'running_hours'), ''))::double precision END)) max_running_hours,
                    sum(CASE WHEN lower(coalesce((o.properties_json ->> 'status'), '')) <> ''
                              AND lower(coalesce((o.properties_json ->> 'status'), '')) NOT IN ('running','run','operation','operating','normal','standby')
                             THEN 1 ELSE 0 END) abnormal_status_count
                FROM kg_relationship r
                JOIN kg_node e ON e.node_id=r.source_node_id AND e.node_type='equipment'
                JOIN kg_node o ON o.node_id=r.target_node_id AND o.node_type='reliability_observation'
                WHERE r.relationship_type='EQUIPMENT_HAS_RELIABILITY_OBSERVATION'
                  AND NOT r.is_candidate
                GROUP BY e.node_id, e.label, e.business_key, refinery_unit
            ),
            issue AS (
                SELECT source_node_id equipment_node_id, count(DISTINCT target_node_id) issue_count
                FROM kg_relationship
                WHERE relationship_type='EQUIPMENT_HAS_ISSUE' AND NOT is_candidate
                GROUP BY source_node_id
            ),
            readiness AS (
                SELECT source_node_id equipment_node_id, count(DISTINCT target_node_id) readiness_records
                FROM kg_relationship
                WHERE relationship_type='EQUIPMENT_HAS_READINESS_RECORD' AND NOT is_candidate
                GROUP BY source_node_id
            ),
            rkap AS (
                -- termasuk candidate: ~93 persen data RKAP dari ETL berstatus candidate
                SELECT source_node_id equipment_node_id, count(DISTINCT target_node_id) rkap_programs
                FROM kg_relationship
                WHERE relationship_type='EQUIPMENT_HAS_RKAP_PROGRAM'
                GROUP BY source_node_id
            ),
            scored AS (
                SELECT
                    rel.*,
                    coalesce(issue.issue_count, 0) issue_count,
                    coalesce(readiness.readiness_records, 0) readiness_records,
                    coalesce(rkap.rkap_programs, 0) rkap_programs,
                    (
                        CASE WHEN rel.avg_mtbf IS NULL THEN 0 WHEN rel.avg_mtbf=0 THEN 35 WHEN rel.avg_mtbf<100 THEN 30 WHEN rel.avg_mtbf<500 THEN 18 ELSE 0 END +
                        CASE WHEN rel.avg_mttr IS NULL THEN 0 WHEN rel.avg_mttr>72 THEN 25 WHEN rel.avg_mttr>24 THEN 18 WHEN rel.avg_mttr>8 THEN 8 ELSE 0 END +
                        CASE WHEN rel.max_running_hours IS NULL THEN 0 WHEN rel.max_running_hours>8000 THEN 12 WHEN rel.max_running_hours>4000 THEN 6 ELSE 0 END +
                        CASE WHEN rel.abnormal_status_count>0 THEN 15 ELSE 0 END +
                        least(coalesce(issue.issue_count, 0) * 4, 16) +
                        CASE WHEN coalesce(readiness.readiness_records, 0)>0 THEN 8 ELSE 0 END +
                        CASE WHEN coalesce(rkap.rkap_programs, 0)=0 THEN 6 ELSE 0 END
                    ) risk_score
                FROM reliability rel
                LEFT JOIN issue USING(equipment_node_id)
                LEFT JOIN readiness USING(equipment_node_id)
                LEFT JOIN rkap USING(equipment_node_id)
            )
            SELECT *
            FROM scored
            WHERE risk_score > 0
            ORDER BY risk_score DESC, issue_count DESC, readiness_records DESC, avg_mtbf ASC NULLS LAST, avg_mttr DESC NULLS LAST
            LIMIT 25
        """)

        reliability_trend = rows(connection, """
            SELECT
                coalesce((properties_json ->> 'refinery_unit'), (properties_json ->> 'ru'), 'Unknown') refinery_unit,
                coalesce(nullif((properties_json ->> 'year'), ''), nullif((properties_json ->> 'tahun'), ''), 'Unknown') period_year,
                coalesce(nullif((properties_json ->> 'month'), ''), nullif((properties_json ->> 'bulan'), ''), 'Unknown') period_month,
                count(*) observations,
                avg((CASE WHEN (nullif((properties_json ->> 'mtbf'), '')) ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN (nullif((properties_json ->> 'mtbf'), ''))::double precision END)) avg_mtbf,
                avg((CASE WHEN (nullif((properties_json ->> 'mttr'), '')) ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN (nullif((properties_json ->> 'mttr'), ''))::double precision END)) avg_mttr
            FROM kg_node
            WHERE node_type='reliability_observation'
            GROUP BY refinery_unit, period_year, period_month
            ORDER BY refinery_unit, period_year, period_month
            LIMIT 500
        """)

        ru_summary = _analysis_rows(connection, "ru_equipment_summary", 100)
        coverage = _analysis_rows(connection, "ru_data_coverage", 1000)
        relationship_quality = _analysis_rows(connection, "ru_relationship_quality", 1000)
        candidate_relationships = rows(connection, """
            SELECT
                coalesce((n.properties_json ->> 'refinery_unit'), 'Unknown') refinery_unit,
                r.domain,
                r.relationship_type,
                r.match_method,
                count(*) candidate_count,
                avg(r.confidence) average_confidence
            FROM kg_relationship r
            LEFT JOIN kg_node n ON n.node_id = r.source_node_id
            WHERE r.is_candidate
            GROUP BY refinery_unit, r.domain, r.relationship_type, r.match_method
            ORDER BY candidate_count DESC
            LIMIT 50
        """)
        mtbf_by_ru = {item["refinery_unit"]: item for item in mtbf_mttr_by_ru}
        ru_ranking = []
        for row in ru_summary:
            ru = row.get("refinery_unit") or "Unknown"
            reliability = mtbf_by_ru.get(ru, {})
            link_percentage = _float(row.get("overall_equipment_link_percentage"))
            issue_count = _float(row.get("equipment_issues"))
            recommendations = _float(row.get("recommendation_count"))
            avg_mtbf = _float(reliability.get("avg_mtbf"))
            avg_mttr = _float(reliability.get("avg_mttr"))
            risk_score = (
                (35 if avg_mtbf == 0 else 25 if avg_mtbf is not None and avg_mtbf < 100 else 12 if avg_mtbf is not None and avg_mtbf < 500 else 0)
                + (25 if avg_mttr is not None and avg_mttr > 72 else 16 if avg_mttr is not None and avg_mttr > 24 else 0)
                + min(issue_count / 40, 20)
                + min(recommendations / 10, 10)
                + (15 if link_percentage is not None and link_percentage < 95 else 0)
            )
            ru_ranking.append({
                **row,
                "avg_mtbf": avg_mtbf,
                "avg_mttr": avg_mttr,
                "observed_equipment": reliability.get("observed_equipment"),
                "risk_score": round(risk_score, 2),
            })
        ru_ranking.sort(key=lambda item: item["risk_score"], reverse=True)

        # Readiness ditautkan ETL di level RU (REFINERY_UNIT_HAS_READINESS_RECORD),
        # bukan per-equipment, sehingga ru_equipment_summary.readiness_records = 0.
        # Hitung jumlah readiness per RU dari graph agar kolom READY tidak 0.
        ru_readiness_counts: dict[str, float] = {}
        for r in rows(connection, """
            SELECT
                coalesce(nullif((e.properties_json ->> 'refinery_unit'), ''), e.label) refinery_unit,
                count(DISTINCT r.target_node_id) readiness_records
            FROM kg_relationship r
            JOIN kg_node e ON e.node_id = r.source_node_id
            WHERE r.relationship_type = 'REFINERY_UNIT_HAS_READINESS_RECORD'
            GROUP BY refinery_unit
        """):
            ru = (r.get("refinery_unit") or "").strip()
            if ru:
                ru_readiness_counts[ru] = _float(r.get("readiness_records")) or 0

        ru_reliability_portfolio = []
        for item in ru_ranking:
            equipment_count = _float(item.get("equipment_count")) or 0
            readiness_records = _float(item.get("readiness_records")) or 0
            if not readiness_records:
                readiness_records = ru_readiness_counts.get((item.get("refinery_unit") or "").strip(), 0)
                item = {**item, "readiness_records": readiness_records}
            rkap_programs = _float(item.get("rkap_programs")) or 0
            link_percentage = _float(item.get("overall_equipment_link_percentage"))
            data_confidence = max(0, min(100, link_percentage if link_percentage is not None else 0))
            ru_reliability_portfolio.append({
                **item,
                "readiness_per_1k_equipment": round(readiness_records * 1000 / equipment_count, 2) if equipment_count else None,
                "rkap_per_1k_equipment": round(rkap_programs * 1000 / equipment_count, 2) if equipment_count else None,
                "data_confidence": round(data_confidence, 2),
            })

        coverage_alerts = [
            item for item in coverage
            if _float(item.get("equipment_link_percentage")) is not None and _float(item.get("equipment_link_percentage")) < 95
        ]
        relationship_quality_alerts = [
            item for item in relationship_quality
            if (
                (_float(item.get("average_confidence")) is not None and _float(item.get("average_confidence")) < 0.98)
                or (_float(item.get("minimum_confidence")) is not None and _float(item.get("minimum_confidence")) < 0.95)
            )
        ]
        coverage_backlog = []
        for item in coverage_alerts:
            percentage = _float(item.get("equipment_link_percentage"))
            total = _float(item.get("total_records")) or 0
            linked = _float(item.get("linked_to_equipment")) or 0
            coverage_backlog.append({
                **item,
                "backlog_type": "coverage",
                "priority_score": round((100 - (percentage or 0)) + min(max(total - linked, 0) / 1000, 20), 2),
            })
        candidate_backlog = [
            {
                **item,
                "backlog_type": "candidate_relationships",
                "priority_score": round(min((_float(item.get("candidate_count")) or 0) / 100, 40), 2),
            }
            for item in candidate_relationships
        ]
        data_quality_backlog = sorted(
            coverage_backlog + candidate_backlog,
            key=lambda item: _float(item.get("priority_score")) or 0,
            reverse=True,
        )[:25]
        avg_link = sum((_float(item.get("equipment_link_percentage")) or 0) for item in coverage) / len(coverage) if coverage else 0

        # Hitung langsung dari graph agar tidak bergantung pada file summary ETL
        _readiness_linked = one(connection, """
            SELECT count(DISTINCT target_node_id) AS c FROM kg_relationship
            WHERE relationship_type IN ('EQUIPMENT_HAS_READINESS_RECORD','REFINERY_UNIT_HAS_READINESS_RECORD')
              AND NOT is_candidate
        """) or {}
        _rkap_linked = one(connection, """
            SELECT count(DISTINCT target_node_id) AS c FROM kg_relationship
            WHERE relationship_type='EQUIPMENT_HAS_RKAP_PROGRAM'
        """) or {}
        # Data confidence: pakai avg_link dari coverage ETL jika ada, fallback hitung dari graph
        if not avg_link:
            _eq_total = one(connection, "SELECT count(*) AS c FROM kg_node WHERE node_type='equipment'") or {}
            _eq_linked = one(connection, """
                SELECT count(DISTINCT source_node_id) AS c FROM kg_relationship
                WHERE NOT is_candidate
            """) or {}
            _t = int(_eq_total.get('c') or 0)
            _l = int(_eq_linked.get('c') or 0)
            avg_link = round(_l * 100 / _t, 2) if _t else 0

        cross_domain_kpis = {
            "reliability_risk_equipment": len(equipment_action_queue),
            "readiness_linked_records": int(_readiness_linked.get('c') or 0),
            "rkap_linked_programs": int(_rkap_linked.get('c') or 0),
            "candidate_relationships": sum(int(_float(item.get("candidate_count")) or 0) for item in candidate_relationships),
            "data_confidence": round(avg_link, 2),
        }

        return {
            "kpis": kpis,
            "cross_domain_kpis": cross_domain_kpis,
            "ru_ranking": ru_ranking,
            "ru_reliability_portfolio": ru_reliability_portfolio,
            "mtbf_mttr_by_ru": mtbf_mttr_by_ru,
            "status_distribution": status_distribution,
            "high_risk_equipment": high_risk_equipment,
            "equipment_action_queue": equipment_action_queue,
            "coverage_alerts": coverage_alerts,
            "relationship_quality_alerts": relationship_quality_alerts,
            "data_quality_backlog": data_quality_backlog,
            "reliability_trend": reliability_trend,
        }
    finally:
        connection.close()


@app.get("/api/datasets/{dataset_id}/analysis/{name}")
def analysis(dataset_id: str, name: str, limit: int = Query(200, ge=1, le=2000)):
    connection = db_for(dataset_id)
    try:
        return _analysis_rows(connection, name, limit)
    finally:
        connection.close()


# Kolom equipment_raw untuk domain yang menggunakan nama kolom berbeda
_EQ_COL_OVERRIDE: dict[str, str] = {
    'EQUIPMENT_HAS_BAD_ACTOR':        'tag_raw',
    'EQUIPMENT_HAS_ZERO_CLAMP':       'tag_raw',
    'EQUIPMENT_HAS_MONITORING_OPERASI': 'equipment_process_raw',
    'EQUIPMENT_HAS_ATG':              'tag_tangki',
}

_ALWAYS_SHOW_DOMAINS: list[tuple[str, str, str]] = [
    ('readiness_jetty',  'EQUIPMENT_HAS_READINESS_JETTY',  'equipment_raw'),
    ('readiness_spm',    'EQUIPMENT_HAS_READINESS_SPM',    'equipment_raw'),
    ('readiness_tank',   'EQUIPMENT_HAS_READINESS_TANK',   'equipment_raw'),
    ('jetty_workplan',   'EQUIPMENT_HAS_JETTY_WORKPLAN',   'equipment_raw'),
    ('spm_workplan',     'EQUIPMENT_HAS_SPM_WORKPLAN',     'equipment_raw'),
    ('tank_workplan',    'EQUIPMENT_HAS_TANK_WORKPLAN',    'equipment_raw'),
    ('zero_clamp',       'EQUIPMENT_HAS_ZERO_CLAMP',       'equipment_raw'),
    ('metering',         'EQUIPMENT_HAS_METERING',         'equipment_raw'),
]


def _discover_domains(connection) -> list[tuple[str, str, str]]:
    """Auto-discover semua domain non-SAP dari EQUIPMENT_HAS_* relationships.
    Return: list of (node_type, rel_type, eq_col)"""
    discovered = rows(connection, """
        SELECT DISTINCT r.relationship_type, dn.node_type
        FROM kg_relationship r
        JOIN kg_node dn ON dn.node_id = r.target_node_id
        WHERE r.relationship_type LIKE 'EQUIPMENT_HAS_%%'
          AND NOT r.is_candidate
        ORDER BY dn.node_type
    """)
    result = []
    seen: set[str] = set()
    for d in discovered:
        rel_type = d['relationship_type']
        node_type = d['node_type']
        eq_col = _EQ_COL_OVERRIDE.get(rel_type, 'equipment_raw')
        result.append((node_type, rel_type, eq_col))
        seen.add(node_type)
    # Tambahkan domain yang selalu ditampilkan jika ada node-nya di dataset
    existing_types = {r['node_type'] for r in rows(connection, """
        SELECT DISTINCT node_type FROM kg_node
        WHERE node_type = ANY(%s)
    """, [[nt for nt, _, _ in _ALWAYS_SHOW_DOMAINS]])}
    for node_type, rel_type, eq_col in _ALWAYS_SHOW_DOMAINS:
        if node_type not in seen and node_type in existing_types:
            result.append((node_type, rel_type, eq_col))
    return result


@app.get("/api/datasets/{dataset_id}/equipment-coverage")
def equipment_coverage(dataset_id: str):
    """Coverage penulisan equipment di laporan non-SAP per domain per RU.
    Auto-discover semua EQUIPMENT_HAS_* dari dataset — tidak hardcode."""
    get_dataset(dataset_id)
    connection = db_for(dataset_id)
    try:
        connection.execute("SET LOCAL statement_timeout = '60s'")
        domains = _discover_domains(connection)
        result = []
        for domain_type, rel_type, eq_col in domains:
            domain_rows = rows(connection, f"""
                SELECT
                    coalesce(dn.properties_json->>'refinery_unit', 'Tidak diketahui') AS ru,
                    count(*) AS total,
                    count(r.target_node_id) AS matched,
                    count(*) - count(r.target_node_id) AS unmatched
                FROM kg_node dn
                LEFT JOIN kg_relationship r
                    ON r.target_node_id = dn.node_id
                   AND r.relationship_type = '{rel_type}'
                   AND NOT r.is_candidate
                WHERE dn.node_type = '{domain_type}'
                GROUP BY ru
                ORDER BY ru
            """)
            if domain_rows:
                result.append({
                    "domain": domain_type,
                    "rel_type": rel_type,
                    "eq_col": eq_col,
                    "rows": domain_rows,
                })
        return result
    finally:
        connection.close()


@app.get("/api/datasets/{dataset_id}/equipment-coverage/{domain}/unmatched")
def equipment_coverage_unmatched(dataset_id: str, domain: str, ru: str = "", limit: int = Query(200, ge=1, le=1000)):
    """List nilai equipment_raw yang tidak sama dengan master equipment, diurutkan dari paling sering."""
    get_dataset(dataset_id)
    connection = db_for(dataset_id)
    try:
        connection.execute("SET LOCAL statement_timeout = '20s'")
        # Cari rel_type & eq_col: dari relasi yang ada, atau dari _ALWAYS_SHOW_DOMAINS
        domain_info = rows(connection, """
            SELECT DISTINCT r.relationship_type
            FROM kg_relationship r
            JOIN kg_node dn ON dn.node_id = r.target_node_id
            WHERE dn.node_type = %s
              AND r.relationship_type LIKE 'EQUIPMENT_HAS_%%'
              AND NOT r.is_candidate
            LIMIT 1
        """, [domain])
        if domain_info:
            rel_type = domain_info[0]['relationship_type']
        else:
            # Cek _ALWAYS_SHOW_DOMAINS
            always = {nt: (rt, ec) for nt, rt, ec in _ALWAYS_SHOW_DOMAINS}
            if domain not in always:
                raise HTTPException(400, "Domain tidak dikenal atau belum ada relasi.")
            rel_type, _ = always[domain]
        eq_col = _EQ_COL_OVERRIDE.get(rel_type, 'equipment_raw')
        ru_filter = "AND dn.properties_json->>'refinery_unit' = %s" if ru else ""
        params = [ru] if ru else []
        unmatched = rows(connection, f"""
            SELECT
                trim(coalesce(dn.properties_json->>'{eq_col}', '')) AS equipment_raw_value,
                coalesce(dn.properties_json->>'refinery_unit', 'Tidak diketahui') AS ru,
                count(*) AS jumlah
            FROM kg_node dn
            WHERE dn.node_type = '{domain}'
              AND NOT EXISTS (
                SELECT 1 FROM kg_relationship r
                WHERE r.target_node_id = dn.node_id
                  AND r.relationship_type = '{rel_type}'
                  AND NOT r.is_candidate
              )
              AND trim(coalesce(dn.properties_json->>'{eq_col}', '')) != ''
              {ru_filter}
            GROUP BY equipment_raw_value, ru
            ORDER BY jumlah DESC
            LIMIT %s
        """, params + [limit])

        # Cari closest match dari master equipment: token exact OR substring (raw contains business_key)
        raw_vals_list = [r['equipment_raw_value'] for r in unmatched]
        closest: dict = {}
        if raw_vals_list:
            try:
                match_rows = rows(connection, """
                    WITH raw_vals AS (
                        SELECT unnest(%s::text[]) AS raw_val
                    )
                    SELECT DISTINCT ON (rv.raw_val)
                        rv.raw_val,
                        e.business_key AS closest_key,
                        e.label AS closest_label
                    FROM raw_vals rv
                    JOIN kg_node e ON e.node_type = 'equipment'
                        AND length(e.business_key) >= 4
                        AND (
                            lower(e.business_key) = ANY(
                                SELECT lower(t)
                                FROM regexp_split_to_table(rv.raw_val, '[\\s\\-\\/\\&\\(\\)\\,\\.\\:]+') AS t
                                WHERE length(t) >= 3
                            )
                            OR lower(rv.raw_val) LIKE '%%' || lower(e.business_key) || '%%'
                        )
                    ORDER BY rv.raw_val, length(e.business_key) DESC
                    LIMIT 2000
                """, [raw_vals_list])
                for mr in match_rows:
                    if mr['raw_val'] not in closest:
                        closest[mr['raw_val']] = (mr['closest_key'], mr['closest_label'])
            except Exception:
                pass  # jika gagal, kembalikan tanpa closest match

        for r in unmatched:
            ck, cl = closest.get(r['equipment_raw_value'], (None, None))
            r['closest_key'] = ck
            r['closest_label'] = cl

        return unmatched
    finally:
        connection.close()


@app.get("/api/datasets/{dataset_id}/export/{kind}")
def export(dataset_id: str, kind: str):
    dataset_item = get_dataset(dataset_id)
    safe_name = quote(dataset_item["name"].replace(" ", "-"))
    if kind == "review":
        query = "SELECT * FROM import_issue"
        filename, media = f"{safe_name}-data-review.csv", "text/csv"
    elif kind == "summary":
        query = "SELECT * FROM load_summary"
        filename, media = f"{safe_name}-load-summary.csv", "text/csv"
    elif kind == "graph":
        return StreamingResponse(
            stream_ndjson(dataset_id),
            media_type="application/x-ndjson",
            headers={"Content-Disposition": f'attachment; filename="{safe_name}-graph.ndjson"'},
        )
    else:
        raise HTTPException(400, "Jenis export tidak dikenal.")
    return StreamingResponse(
        stream_csv(dataset_id, query),
        media_type=media,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


class _ScopedConnection:
    """Pinjam koneksi dari pool, set app.dataset_id (RLS), dan tiru API lama
    (execute/close) agar endpoint existing tidak perlu diubah polanya."""

    def __init__(self, dataset_id: str):
        self._cm = pool().connection()
        self.conn = self._cm.__enter__()
        self.conn.execute("SELECT set_config('app.dataset_id', %s, false)", [dataset_id])

    def execute(self, query, params=None):
        return self.conn.execute(query, params or [])

    def cursor(self, *args, **kwargs):
        return self.conn.cursor(*args, **kwargs)

    def close(self):
        self._cm.__exit__(None, None, None)


def db_for(dataset_id: str) -> _ScopedConnection:
    get_dataset(dataset_id)  # 404 bila tidak ada
    return _ScopedConnection(dataset_id)


def with_db(dataset_id: str):
    return get_dataset(dataset_id)


def get_dataset(dataset_id: str):
    ensure_schema_once()
    item = get_dataset_row(dataset_id)
    if not item:
        raise HTTPException(404, "Dataset tidak ditemukan.")
    return item


def rows(connection, query: str, params=None) -> list[dict]:
    return connection.execute(query, params or []).fetchall()


def one(connection, query: str, params=None) -> dict | None:
    return connection.execute(query, params or []).fetchone()


QUERY_RE = re.compile(r"^(NODE|EDGE)\s+([A-Za-z0-9_]+)(?:\s+WHERE\s+(.+))?$", re.IGNORECASE)
COND_RE = re.compile(r"^([A-Za-z0-9_.]+)\s*(NOT\s+LIKE|LIKE|=|!=|>=|<=|>|<|CONTAINS|EXISTS)\s*(.*)$", re.IGNORECASE)
IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
NODE_COLUMNS = {
    "id": "node_id", "node_id": "node_id", "type": "node_type", "node_type": "node_type",
    "label": "label", "business_key": "business_key", "domain": "domain",
    "source_file": "source_file", "source_sheet": "source_sheet", "source_row": "source_row",
}
EDGE_COLUMNS = {
    "id": "relationship_id", "relationship_id": "relationship_id", "type": "relationship_type",
    "relationship_type": "relationship_type", "domain": "domain", "confidence": "confidence",
    "match_method": "match_method", "is_candidate": "is_candidate", "source_file": "source_file",
    "source_sheet": "source_sheet", "source_row": "source_row",
}

PROMPT_DOMAIN_LIMIT = 8
PROMPT_PROPERTY_FIELDS = {
    "equipment": [
        "refinery_unit", "ru", "plant", "functional_location", "criticallity",
        "criticality", "equipment_group", "plant_area", "description",
        "derived_risk_score", "derived_issue_count", "derived_open_issue_count",
        "derived_avg_mtbf", "derived_avg_mttr", "derived_abnormal_status_count",
    ],
    "reliability_observation": [
        "equipment", "status", "running_hours", "mtbf", "mttr", "year", "month",
        "week", "derived_period_key", "derived_is_abnormal_status",
        "derived_status_bucket", "derived_mtbf_bucket", "derived_mttr_bucket",
        "derived_low_mtbf_flag", "derived_high_mttr_flag",
    ],
    "maintenance_order": [
        "order", "order_type", "priority", "status", "plant", "refinery_unit",
        "reference_date", "derived_reference_date", "derived_order_age_days",
        "derived_is_open_order", "derived_planned_cost", "derived_actual_cost",
        "derived_cost_variance", "derived_priority_bucket", "derived_status_bucket",
        "derived_work_center",
    ],
    "maintenance_notification": [
        "notification", "notification_type", "priority", "status", "plant",
        "refinery_unit", "reference_date", "derived_reference_date",
        "derived_notification_age_days", "derived_status_bucket",
    ],
    "inspection": [
        "tag", "inspection_type", "work_type", "plan_date", "actual_date",
        "result", "derived_tag_compact", "derived_inspection_delay_days",
        "derived_is_overdue", "derived_is_late_actual",
        "derived_is_nonconformity", "derived_work_type_bucket",
    ],
    "rkap_program": [
        "program_number", "program_name", "fiscal_year", "total_equivalent_idr",
        "cost_group", "discipline", "status_actual", "status_prognosa",
        "actual_comp", "step_long_desc", "top_risk",
        "derived_total_equivalent_idr_num", "derived_schedule_variance_days",
        "derived_budget_bucket", "derived_is_high_value", "derived_is_top_risk",
        "derived_is_delayed", "derived_progress_stage_bucket",
    ],
    "equipment_issue": [
        "tag", "status", "report_date", "mitigation", "permanent_solution",
        "irkap_mitigation", "irkap_solution", "derived_issue_age_days",
        "derived_status_bucket", "derived_has_mitigation",
        "derived_has_permanent_solution", "derived_has_irkap_reference",
        "derived_actionability_score",
    ],
    "operational_issue": [
        "tag", "status", "report_date", "issue", "description", "mitigation",
        "permanent_solution", "derived_issue_age_days", "derived_status_bucket",
    ],
    "readiness_record": [
        "record_type", "period", "equipment_or_tag", "refinery_unit",
        "derived_readiness_tag_compact", "derived_readiness_family",
        "derived_record_month", "derived_has_bad_status", "derived_has_rtl",
        "derived_has_external_resource", "derived_action_category",
    ],
    "rcps": [
        "rcps_no", "criticality", "progress", "refinery_unit",
        "derived_progress_num", "derived_criticality_bucket",
    ],
    "rcps_recommendation": [
        "rcps_no", "pic", "target", "category", "status",
        "derived_target_date", "derived_is_overdue", "derived_owner_pic",
        "derived_recommendation_category", "derived_has_irkap",
    ],
}
PROMPT_NODE_DOMAIN = {
    "equipment": "asset",
    "refinery_unit": "asset",
    "plant": "asset",
    "functional_location": "asset",
    "reliability_observation": "reliability",
    "maintenance_order": "maintenance",
    "maintenance_notification": "maintenance",
    "readiness_record": "readiness",
    "equipment_issue": "issue_rcps",
    "operational_issue": "issue_rcps",
    "rcps": "issue_rcps",
    "rcps_recommendation": "issue_rcps",
    "rkap_program": "cost_program_rkap",
    "inspection": "inspection_operational",
}


def _run_property_query(connection, query_text: str, limit: int) -> dict:
    text = " ".join((query_text or "").strip().split())
    match = QUERY_RE.match(text)
    if not match:
        raise HTTPException(400, "Gunakan format: NODE equipment WHERE field = value atau EDGE REL_TYPE WHERE field CONTAINS value.")
    entity, entity_type, where_text = match.groups()
    limit = max(1, min(int(limit or 200), 1000))
    if entity.upper() == "NODE":
        if entity_type == "equipment" and where_text and "derived_" in where_text:
            return _run_equipment_property_query(connection, where_text, limit)
        clauses, params = ["node_type=%s"], [entity_type]
        extra_clauses, extra_params = _query_conditions(where_text, NODE_COLUMNS)
        clauses.extend(extra_clauses)
        params.extend(extra_params)
        node_rows = rows(
            connection,
            f"SELECT * FROM kg_node WHERE {' AND '.join(clauses)} ORDER BY label NULLS LAST LIMIT %s",
            params + [limit],
        )
        return {"nodes": [node_to_api(item) for item in node_rows], "edges": [], "truncated": len(node_rows) >= limit}

    clauses, params = ["relationship_type=%s"], [entity_type]
    extra_clauses, extra_params = _query_conditions(where_text, EDGE_COLUMNS)
    clauses.extend(extra_clauses)
    params.extend(extra_params)
    edge_rows = rows(
        connection,
        f"""
        SELECT *
        FROM kg_relationship
        WHERE {' AND '.join(clauses)}
        ORDER BY confidence DESC NULLS LAST, relationship_id
        LIMIT %s
        """,
        params + [limit],
    )
    node_ids = list(dict.fromkeys([node_id for edge in edge_rows for node_id in [edge["source_node_id"], edge["target_node_id"]]]))
    node_rows = _nodes_by_id(connection, node_ids)
    return {
        "nodes": [node_to_api(item) for item in node_rows],
        "edges": [edge_to_api(item) for item in edge_rows],
        "truncated": len(edge_rows) >= limit,
    }


def _query_conditions(
    where_text: str | None,
    columns: dict[str, str],
    json_expression_template: str = "(properties_json ->> '{field}')",
) -> tuple[list[str], list]:
    if not where_text:
        return [], []
    clauses, params = [], []
    for raw_condition in re.split(r"\s+AND\s+", where_text, flags=re.IGNORECASE):
        condition = raw_condition.strip()
        if not condition:
            continue
        match = COND_RE.match(condition)
        if not match:
            raise HTTPException(400, f"Filter tidak dikenali: {condition}")
        field, operator, raw_value = match.groups()
        field = field.split(".", 1)[-1]
        if not IDENT_RE.match(field):
            raise HTTPException(400, f"Field tidak valid: {field}")
        expression = columns.get(field) or json_expression_template.format(field=field)
        operator = " ".join(operator.upper().split())
        if operator == "EXISTS":
            clauses.append(f"nullif({expression}, '') IS NOT NULL")
            continue
        value = _query_value(raw_value)
        if operator == "CONTAINS":
            clauses.append(f"lower(coalesce(cast({expression} AS VARCHAR), '')) LIKE %s")
            params.append(f"%{str(value).lower()}%")
        elif operator in {"LIKE", "NOT LIKE"}:
            clauses.append(f"lower(coalesce(cast({expression} AS VARCHAR), '')) {'NOT LIKE' if operator == 'NOT LIKE' else 'LIKE'} %s")
            params.append(str(value).lower())
        elif operator in {"=", "!="}:
            clauses.append(f"lower(coalesce(cast({expression} AS VARCHAR), '')) {'<>' if operator == '!=' else '='} lower(%s)")
            params.append(str(value))
        elif operator in {">", "<", ">=", "<="}:
            clauses.append(f"(CASE WHEN (nullif(cast({expression} AS VARCHAR), '')) ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN (nullif(cast({expression} AS VARCHAR), ''))::double precision END) {operator} %s")
            params.append(_query_number(value))
    return clauses, params


def _run_equipment_property_query(connection, where_text: str, limit: int) -> dict:
    columns = {
        "id": "e.node_id", "node_id": "e.node_id", "type": "e.node_type", "node_type": "e.node_type",
        "label": "e.label", "business_key": "e.business_key", "domain": "e.domain",
        "source_file": "e.source_file", "source_sheet": "e.source_sheet", "source_row": "e.source_row",
        "derived_issue_count": "coalesce(m.issue_count, 0)",
        "derived_maintenance_order_count": "coalesce(m.maintenance_order_count, 0)",
        "derived_notification_count": "coalesce(m.notification_count, 0)",
        "derived_reliability_observation_count": "coalesce(m.reliability_observation_count, 0)",
        "derived_inspection_count": "coalesce(m.inspection_count, 0)",
        "derived_readiness_record_count": "coalesce(m.readiness_record_count, 0)",
        "derived_rkap_program_count": "coalesce(m.rkap_program_count, 0)",
        "derived_abnormal_status_count": "coalesce(m.abnormal_status_count, 0)",
        "derived_avg_mtbf": "m.avg_mtbf",
        "derived_avg_mttr": "m.avg_mttr",
        "derived_risk_score": "coalesce(m.risk_score, 0)",
    }
    clauses, params = _query_conditions(where_text, columns, "(e.properties_json ->> '{field}')")
    where_sql = " AND ".join(["e.node_type='equipment'"] + clauses)
    node_rows = rows(
        connection,
        f"""
        WITH rel_counts AS (
            SELECT
                source_node_id equipment_node_id,
                sum(CASE WHEN relationship_type='EQUIPMENT_HAS_ISSUE' AND NOT is_candidate THEN 1 ELSE 0 END) issue_count,
                sum(CASE WHEN relationship_type='EQUIPMENT_HAS_MAINTENANCE_ORDER' AND NOT is_candidate THEN 1 ELSE 0 END) maintenance_order_count,
                sum(CASE WHEN relationship_type='EQUIPMENT_HAS_NOTIFICATION' AND NOT is_candidate THEN 1 ELSE 0 END) notification_count,
                sum(CASE WHEN relationship_type='EQUIPMENT_HAS_RELIABILITY_OBSERVATION' AND NOT is_candidate THEN 1 ELSE 0 END) reliability_observation_count,
                sum(CASE WHEN relationship_type='EQUIPMENT_HAS_INSPECTION' AND NOT is_candidate THEN 1 ELSE 0 END) inspection_count,
                sum(CASE WHEN relationship_type='EQUIPMENT_HAS_READINESS_RECORD' AND NOT is_candidate THEN 1 ELSE 0 END) readiness_record_count,
                sum(CASE WHEN relationship_type='EQUIPMENT_HAS_RKAP_PROGRAM' AND NOT is_candidate THEN 1 ELSE 0 END) rkap_program_count
            FROM kg_relationship
            GROUP BY source_node_id
        ),
        reliability AS (
            SELECT
                r.source_node_id equipment_node_id,
                avg((CASE WHEN (nullif((n.properties_json ->> 'mtbf'), '')) ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN (nullif((n.properties_json ->> 'mtbf'), ''))::double precision END)) avg_mtbf,
                avg((CASE WHEN (nullif((n.properties_json ->> 'mttr'), '')) ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN (nullif((n.properties_json ->> 'mttr'), ''))::double precision END)) avg_mttr,
                sum(CASE WHEN lower(coalesce((n.properties_json ->> 'status'), '')) NOT IN ('', 'running', 'run', 'operation', 'operating', 'normal', 'standby') THEN 1 ELSE 0 END) abnormal_status_count
            FROM kg_relationship r
            JOIN kg_node n ON n.node_id=r.target_node_id
            WHERE r.relationship_type='EQUIPMENT_HAS_RELIABILITY_OBSERVATION' AND NOT r.is_candidate
            GROUP BY r.source_node_id
        ),
        metrics AS (
            SELECT
                e.node_id equipment_node_id,
                coalesce(c.issue_count, 0) issue_count,
                coalesce(c.maintenance_order_count, 0) maintenance_order_count,
                coalesce(c.notification_count, 0) notification_count,
                coalesce(c.reliability_observation_count, 0) reliability_observation_count,
                coalesce(c.inspection_count, 0) inspection_count,
                coalesce(c.readiness_record_count, 0) readiness_record_count,
                coalesce(c.rkap_program_count, 0) rkap_program_count,
                r.avg_mtbf,
                r.avg_mttr,
                coalesce(r.abnormal_status_count, 0) abnormal_status_count,
                (
                    CASE WHEN r.avg_mtbf = 0 THEN 35 WHEN r.avg_mtbf < 100 THEN 30 WHEN r.avg_mtbf < 500 THEN 18 ELSE 0 END +
                    CASE WHEN r.avg_mttr > 72 THEN 25 WHEN r.avg_mttr > 24 THEN 18 ELSE 0 END +
                    least(cast(coalesce(c.issue_count, 0) AS INTEGER) * 4, 16) +
                    CASE WHEN coalesce(r.abnormal_status_count, 0) > 0 THEN 15 ELSE 0 END +
                    CASE WHEN coalesce(c.rkap_program_count, 0) = 0 THEN 6 ELSE 0 END
                ) risk_score
            FROM kg_node e
            LEFT JOIN rel_counts c ON c.equipment_node_id=e.node_id
            LEFT JOIN reliability r ON r.equipment_node_id=e.node_id
            WHERE e.node_type='equipment'
        )
        SELECT e.*
        FROM kg_node e
        LEFT JOIN metrics m ON m.equipment_node_id=e.node_id
        WHERE {where_sql}
        ORDER BY e.label NULLS LAST
        LIMIT %s
        """,
        params + [limit],
    )
    return {"nodes": [node_to_api(item, connection) for item in node_rows], "edges": [], "truncated": len(node_rows) >= limit}


def _query_value(raw_value: str):
    value = raw_value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    if value.lower() == "true":
        return "true"
    if value.lower() == "false":
        return "false"
    return value


def _query_number(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(400, f"Operator numerik membutuhkan angka: {value}") from exc


def _query_fields_for_type(
    connection,
    table: str,
    type_column: str,
    type_value: str,
    core_fields: list[str],
    sample_limit: int = 300,
) -> list[str]:
    items = rows(
        connection,
        f"SELECT properties_json FROM {table} WHERE {type_column}=%s AND properties_json IS NOT NULL LIMIT %s",
        [type_value, sample_limit],
    )
    property_fields = set()
    for item in items:
        properties = parse_json(item.get("properties_json"))
        if isinstance(properties, dict):
            property_fields.update(key for key in properties if IDENT_RE.match(str(key)))
    return sorted(set(core_fields) | property_fields)


def _source_reference(item: dict) -> dict:
    return {
        "workbook": item.get("source_file") or "",
        "sheet": item.get("source_sheet") or "",
        "row": item.get("source_row"),
        "record_id": item.get("source_record_id"),
    }


def _prompt_domain_for_node(node_type: str, fallback_domain: str | None = None) -> str:
    return PROMPT_NODE_DOMAIN.get(node_type) or (fallback_domain or "other")


def _prompt_properties(node_type: str, properties: dict) -> dict:
    fields = PROMPT_PROPERTY_FIELDS.get(node_type, [])
    selected = {
        key: properties.get(key)
        for key in fields
        if properties.get(key) not in (None, "")
    }
    if selected:
        return selected
    return {
        key: value
        for key, value in list(properties.items())[:10]
        if value not in (None, "")
    }


def _relationship_association_type(edge: dict | None) -> str:
    if not edge:
        return "selected_node"
    return "candidate_relationship" if edge.get("is_candidate") else "direct_verified"


def _prompt_evidence_item(
    node: dict,
    edge: dict | None = None,
    association_type: str | None = None,
    matched_token: str = "",
) -> dict:
    properties = parse_json(node.get("properties_json"))
    node_type = node.get("node_type") or ""
    domain = _prompt_domain_for_node(node_type, node.get("domain") or (edge or {}).get("domain"))
    item = {
        "node_id": node.get("node_id"),
        "node_type": node_type,
        "label": node.get("label") or node.get("node_id"),
        "domain": domain,
        "association_type": association_type or _relationship_association_type(edge),
        "relationship_type": (edge or {}).get("relationship_type"),
        "confidence": (edge or {}).get("confidence"),
        "match_method": (edge or {}).get("match_method"),
        "is_candidate": bool((edge or {}).get("is_candidate")) if edge else False,
        "matched_token": matched_token,
        "source": _source_reference(node),
        "properties": _prompt_properties(node_type, properties),
    }
    if not item["matched_token"]:
        item.pop("matched_token")
    return item


def _append_prompt_evidence(grouped: dict[str, list[dict]], item: dict) -> None:
    domain = item.get("domain") or "other"
    bucket = grouped.setdefault(domain, [])
    if len(bucket) >= PROMPT_DOMAIN_LIMIT:
        return
    identity = (
        item.get("node_id"),
        item.get("relationship_type"),
        item.get("association_type"),
        item.get("matched_token"),
    )
    for existing in bucket:
        if (
            existing.get("node_id"),
            existing.get("relationship_type"),
            existing.get("association_type"),
            existing.get("matched_token"),
        ) == identity:
            return
    bucket.append(item)


def _direct_prompt_evidence_for_node(connection, node_id: str, root: dict) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    _append_prompt_evidence(grouped, _prompt_evidence_item(root, association_type="selected_node"))
    edge_rows = rows(
        connection,
        """
        SELECT r.*, n.node_id, n.node_type, n.label, n.business_key, n.domain node_domain,
               n.properties_json, n.source_file node_source_file,
               n.source_sheet node_source_sheet, n.source_row node_source_row,
               n.source_record_id node_source_record_id
        FROM kg_relationship r
        JOIN kg_node n ON n.node_id=CASE WHEN r.source_node_id=%s THEN r.target_node_id ELSE r.source_node_id END
        WHERE r.source_node_id=%s OR r.target_node_id=%s
        ORDER BY
          r.is_candidate ASC,
          CASE WHEN r.domain='asset' THEN 0 WHEN r.domain='issue' THEN 1 WHEN r.domain='maintenance' THEN 2
               WHEN r.domain='readiness' THEN 3 WHEN r.domain='reliability' THEN 4
               WHEN r.domain='cost_program' THEN 5 ELSE 6 END,
          r.confidence DESC NULLS LAST,
          r.relationship_type
        LIMIT 500
        """,
        [node_id, node_id, node_id],
    )
    for row in edge_rows:
        node = {
            "node_id": row.get("node_id"),
            "node_type": row.get("node_type"),
            "label": row.get("label"),
            "business_key": row.get("business_key"),
            "domain": row.get("node_domain"),
            "properties_json": row.get("properties_json"),
            "source_file": row.get("node_source_file"),
            "source_sheet": row.get("node_source_sheet"),
            "source_row": row.get("node_source_row"),
            "source_record_id": row.get("node_source_record_id"),
        }
        _append_prompt_evidence(grouped, _prompt_evidence_item(node, row))
    return grouped


def _ru_context_prompt_evidence(connection, ru: str, grouped: dict[str, list[dict]]) -> None:
    if not ru:
        return
    ru_pattern = rf"\y{ru}\y"
    context_types = [
        "readiness_record", "rkap_program", "equipment_issue", "operational_issue",
        "reliability_observation", "maintenance_order", "maintenance_notification", "inspection",
    ]
    for context_type in context_types:
        domain = _prompt_domain_for_node(context_type)
        if len(grouped.get(domain, [])) >= PROMPT_DOMAIN_LIMIT:
            continue
        context_rows = rows(
            connection,
            """
            SELECT *
            FROM kg_node
            WHERE node_type=%s
              AND (
                (upper(coalesce((properties_json ->> 'refinery_unit'), '')) ~ %s)
                OR (upper(coalesce((properties_json ->> 'ru'), '')) ~ %s)
              )
            ORDER BY source_file, source_sheet, source_row
            LIMIT 20
            """,
            [context_type, ru_pattern, ru_pattern],
        )
        for node in context_rows:
            item = _prompt_evidence_item(node, association_type="ru_context")
            _append_prompt_evidence(grouped, item)


def _readiness_context_for_node(connection, node_id: str) -> dict:
    item = one(connection, "SELECT * FROM kg_node WHERE node_id=%s", [node_id])
    if not item:
        raise HTTPException(404, "Node tidak ditemukan.")
    properties = parse_json(item.get("properties_json"))
    ru = _ru_key(properties.get("refinery_unit") or properties.get("ru") or item.get("business_key"))
    domain_evidence = _direct_prompt_evidence_for_node(connection, node_id, item)
    direct_count = 0
    if item.get("node_type") == "equipment":
        direct = one(
            connection,
            """
            SELECT count(DISTINCT target_node_id) count
            FROM kg_relationship
            WHERE source_node_id=%s AND relationship_type='EQUIPMENT_HAS_READINESS_RECORD' AND NOT is_candidate
            """,
            [node_id],
        )
        direct_count = int(direct.get("count") or 0) if direct else 0

    ru_level_count = 0
    tag_match_count = 0
    samples: list[dict] = []
    if ru:
        ru_pattern = rf"\y{ru}\y"
        ru_clause = (
            "((upper(coalesce((properties_json ->> 'refinery_unit'), '')) ~ %s)"
            " OR (upper(coalesce((properties_json ->> 'ru'), '')) ~ %s))"
        )
        ru_level = one(
            connection,
            f"SELECT count(*) count FROM kg_node WHERE node_type='readiness_record' AND {ru_clause}",
            [ru_pattern, ru_pattern],
        )
        ru_level_count = int(ru_level.get("count") or 0) if ru_level else 0
        if item.get("node_type") == "equipment":
            equipment_tokens = list(_equipment_match_tokens(item, properties))
            if equipment_tokens:
                # Dorong tag-match ke SQL (exact-boundary, kolom dipadatkan) supaya hanya
                # baris yang cocok yang ditarik ke Python — bukan scan tak terbatas.
                tag_columns = [
                    "label",
                    "properties_json ->> 'derived_readiness_tag_compact'",
                    "properties_json ->> 'equipment_or_tag'",
                    "properties_json ->> 'tag_no'",
                    "properties_json ->> 'tag_number'",
                    "properties_json ->> 'process_equipment'",
                    "properties_json ->> 'equipment'",
                ]
                tag_condition = _compact_match_condition(tag_columns)
                matched = one(
                    connection,
                    f"""
                    SELECT count(*) count
                    FROM kg_node
                    WHERE node_type='readiness_record' AND {ru_clause} AND {tag_condition}
                    """,
                    [ru_pattern, ru_pattern, equipment_tokens],
                )
                tag_match_count = int(matched.get("count") or 0) if matched else 0
                if tag_match_count:
                    sample_rows = rows(
                        connection,
                        f"""
                        SELECT node_id, node_type, label, domain, properties_json,
                               source_file, source_sheet, source_row, source_record_id
                        FROM kg_node
                        WHERE node_type='readiness_record' AND {ru_clause} AND {tag_condition}
                        LIMIT %s
                        """,
                        [ru_pattern, ru_pattern, equipment_tokens, PROMPT_DOMAIN_LIMIT],
                    )
                    equipment_token_set = set(equipment_tokens)
                    for readiness in sample_rows:
                        readiness_properties = parse_json(readiness.get("properties_json"))
                        readiness_tokens = {
                            _compact_token(value)
                            for value in [
                                readiness.get("label"),
                                readiness_properties.get("derived_readiness_tag_compact"),
                                readiness_properties.get("equipment_or_tag"),
                                readiness_properties.get("tag_no"),
                                readiness_properties.get("tag_number"),
                                readiness_properties.get("process_equipment"),
                                readiness_properties.get("equipment"),
                            ]
                        }
                        matched_token = next(
                            (token for token in readiness_tokens if token in equipment_token_set), ""
                        )
                        if len(samples) < 5:
                            sample = _prompt_evidence_item(
                                readiness, association_type="tag_secondary", matched_token=matched_token
                            )
                            sample["equipment_or_tag"] = (
                                readiness_properties.get("equipment_or_tag")
                                or readiness_properties.get("tag_no")
                                or readiness_properties.get("tag_number")
                            )
                            samples.append(sample)
                        _append_prompt_evidence(
                            domain_evidence,
                            _prompt_evidence_item(
                                readiness, association_type="tag_secondary", matched_token=matched_token
                            ),
                        )
        _ru_context_prompt_evidence(connection, ru, domain_evidence)

    semantic_status = (
        "Direct linked" if direct_count else "Tag matched" if tag_match_count else "RU only" if ru_level_count else "No readiness"
    )
    reliability_engineering = None
    if item.get("node_type") == "equipment":
        readiness_association = (
            "direct" if direct_count else "tag" if tag_match_count else "ru" if ru_level_count else "none"
        )
        reliability_engineering = _reliability_engineering_signals(
            connection, node_id, item, properties,
            readiness={
                "readiness_direct": direct_count,
                "readiness_tag_match": tag_match_count,
                "readiness_ru_level": ru_level_count,
                "readiness_association": readiness_association,
                "readiness_tag_samples": [s.get("equipment_or_tag") or s.get("label") for s in samples[:3] if s],
            },
        )
    return {
        "node_id": node_id,
        "node_type": item.get("node_type"),
        "label": item.get("label"),
        "refinery_unit": ru,
        "direct_count": direct_count,
        "tag_match_count": tag_match_count,
        "ru_level_count": ru_level_count,
        "semantic_status": semantic_status,
        "tag_match_samples": samples,
        "domain_evidence": domain_evidence,
        "reliability_engineering": reliability_engineering,
    }


def _reliability_engineering_signals(connection, node_id: str, item: dict, properties: dict, readiness: dict | None = None) -> dict:
    """Sinyal diagnostik CMRP per-equipment (faktual, untuk dipakai keempat prompt).

    Mengikuti pola equipment_action_queue: agregasi per-equipment lewat edge
    EQUIPMENT_HAS_* + properti turunan (derived_*) yang sudah dihitung ETL. Query
    sempit (per node_id, pakai index kg_relationship) + statement_timeout supaya
    tidak pernah jadi recompute graph besar di page-open path. `readiness` adalah
    hasil tag-match (exact-boundary) yang sudah dihitung pemanggil — disertakan agar
    prompt punya angka readiness konsisten dengan badge/dashboard.
    """
    NUM_RE = "'^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$'"

    def num(expr: str) -> str:
        # cast jsonb text -> double precision hanya bila benar-benar numerik
        return f"CASE WHEN (nullif(({expr}), '')) ~ {NUM_RE} THEN (nullif(({expr}), ''))::double precision END"

    signals: dict = {}
    try:
        connection.execute("SET LOCAL statement_timeout = '12s'")

        # (a) Reliability observation: MTBF/MTTR/status/running_hours
        # Utama: confirmed edges. Fallback: candidate edges (tag-match) jika confirmed tidak punya MTBF/MTTR.
        # Banyak equipment (tank, dll) tidak punya confirmed edge ke reliability_observation
        # tapi nilai MTBF/MTTR-nya nyata ada di node yang di-tag-match.
        def _rel_obs_query(include_candidates: bool) -> dict:
            cand_filter = "" if include_candidates else "AND NOT r.is_candidate"
            return one(connection, f"""
                SELECT
                    count(*) observations,
                    avg({num("o.properties_json ->> 'mtbf'")}) avg_mtbf,
                    avg({num("o.properties_json ->> 'mttr'")}) avg_mttr,
                    max({num("o.properties_json ->> 'running_hours'")}) max_running_hours,
                    sum(CASE WHEN lower(coalesce((o.properties_json ->> 'status'), '')) <> ''
                              AND lower(coalesce((o.properties_json ->> 'status'), '')) NOT IN ('running','run','operation','operating','normal','standby')
                             THEN 1 ELSE 0 END) abnormal_status_count,
                    mode() WITHIN GROUP (ORDER BY nullif((o.properties_json ->> 'hasil'), '')) function_status,
                    bool_or(r.is_candidate) used_candidates
                FROM kg_relationship r
                JOIN kg_node o ON o.node_id=r.target_node_id AND o.node_type='reliability_observation'
                WHERE r.source_node_id=%s
                  AND r.relationship_type='EQUIPMENT_HAS_RELIABILITY_OBSERVATION'
                  {cand_filter}
            """, [node_id]) or {}

        rel = _rel_obs_query(include_candidates=False)
        # Fallback ke candidate jika confirmed tidak punya MTBF/MTTR
        if not rel.get('avg_mtbf') and not rel.get('avg_mttr'):
            rel_cand = _rel_obs_query(include_candidates=True)
            if rel_cand.get('avg_mtbf') or rel_cand.get('avg_mttr'):
                rel = rel_cand

        # issue count (bad-actor / FRACAS)
        iss = one(connection, """
            SELECT count(DISTINCT target_node_id) issue_count
            FROM kg_relationship
            WHERE source_node_id=%s AND relationship_type='EQUIPMENT_HAS_ISSUE' AND NOT is_candidate
        """, [node_id]) or {}

        # (b) Maintenance order: backlog/aging/schedule/cost/material-block (derived_* ETL)
        wm = one(connection, f"""
            SELECT
                count(*) total_orders,
                sum(CASE WHEN (m.properties_json ->> 'derived_is_open_order')='true' THEN 1 ELSE 0 END) open_orders,
                sum(CASE WHEN (m.properties_json ->> 'derived_is_open_order')='false' THEN 1 ELSE 0 END) closed_orders,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY {num("m.properties_json ->> 'derived_order_age_days'")}) backlog_age_median,
                percentile_cont(0.9) WITHIN GROUP (ORDER BY {num("m.properties_json ->> 'derived_order_age_days'")}) backlog_age_p90,
                sum(CASE WHEN (m.properties_json ->> 'derived_is_open_order')='true'
                          AND (upper(coalesce((m.properties_json ->> 'derived_status_bucket'), '')) ~ 'WAMA|WASR')
                         THEN 1 ELSE 0 END) material_blocked_count,
                sum(CASE WHEN lower(coalesce((m.properties_json ->> 'derived_priority_bucket'), ''))='high' THEN 1 ELSE 0 END) priority_high_count,
                sum({num("m.properties_json ->> 'derived_planned_cost'")}) planned_cost,
                sum({num("m.properties_json ->> 'derived_actual_cost'")}) actual_cost
            FROM kg_relationship r
            JOIN kg_node m ON m.node_id=r.target_node_id AND m.node_type='maintenance_order'
            WHERE r.source_node_id=%s
              AND r.relationship_type='EQUIPMENT_HAS_MAINTENANCE_ORDER'
        """, [node_id]) or {}

        # (c) RKAP program: business case (cost/top-risk/delay). Sebagian besar keterkaitan
        # RKAP bersifat candidate (prefix-substring) -> pisahkan exact vs candidate agar
        # tingkat keyakinan transparan ke prompt.
        rkap = one(connection, f"""
            SELECT
                count(*) rkap_program_count,
                sum(CASE WHEN NOT r.is_candidate THEN 1 ELSE 0 END) rkap_exact_count,
                sum(CASE WHEN r.is_candidate THEN 1 ELSE 0 END) rkap_candidate_count,
                sum({num("k.properties_json ->> 'derived_total_equivalent_idr_num'")}) rkap_total_cost,
                sum(CASE WHEN (k.properties_json ->> 'derived_is_top_risk')='true' THEN 1 ELSE 0 END) rkap_top_risk_count,
                sum(CASE WHEN (k.properties_json ->> 'derived_is_delayed')='true' THEN 1 ELSE 0 END) rkap_delayed_count,
                sum(CASE WHEN (k.properties_json ->> 'derived_is_high_value')='true' THEN 1 ELSE 0 END) rkap_high_value_count
            FROM kg_relationship r
            JOIN kg_node k ON k.node_id=r.target_node_id AND k.node_type='rkap_program'
            WHERE r.source_node_id=%s
              AND r.relationship_type='EQUIPMENT_HAS_RKAP_PROGRAM'
        """, [node_id]) or {}

        # (d) Inspection: tidak punya edge ke equipment -> tag-match exact-boundary dalam RU sama.
        inspection_match_count, inspection_findings = _inspection_tag_match(connection, item, properties)

        criticality = (
            properties.get("criticallity")
            or properties.get("criticality")
            or properties.get("kgrre_normalized_criticality")
        )

        # (e) ICU Issue: isu integritas kondisi
        icu = one(connection, """
            SELECT count(*) icu_count,
                   sum(CASE WHEN lower(coalesce(n.properties_json->>'icu_status','')) NOT IN ('closed','selesai','done','resolved') THEN 1 ELSE 0 END) icu_open_count
            FROM kg_relationship r
            JOIN kg_node n ON n.node_id=r.target_node_id AND n.node_type='icu_issue'
            WHERE r.source_node_id=%s AND r.relationship_type='EQUIPMENT_HAS_ICU_ISSUE' AND NOT r.is_candidate
        """, [node_id]) or {}

        # (f) Critical Equipment: apakah equipment ini masuk daftar critical
        crit_eq = one(connection, """
            SELECT count(*) ce_count,
                   string_agg(DISTINCT nullif(n.properties_json->>'criticality_class',''), ', ' ORDER BY nullif(n.properties_json->>'criticality_class','')) ce_class
            FROM kg_relationship r
            JOIN kg_node n ON n.node_id=r.target_node_id AND n.node_type='critical_equipment'
            WHERE r.source_node_id=%s AND r.relationship_type='EQUIPMENT_HAS_CRITICAL_EQUIPMENT' AND NOT r.is_candidate
        """, [node_id]) or {}

        # (g) Zero Clamp: pemasangan klem sementara (indikasi kerusakan aktif)
        zc = one(connection, f"""
            SELECT count(*) zc_count,
                   sum(CASE WHEN lower(coalesce(n.properties_json->>'status','')) NOT IN ('dilepas','removed','done','selesai','closed') THEN 1 ELSE 0 END) zc_active_count,
                   mode() WITHIN GROUP (ORDER BY nullif(n.properties_json->>'type_damage','')) dominant_damage
            FROM kg_relationship r
            JOIN kg_node n ON n.node_id=r.target_node_id AND n.node_type='zero_clamp'
            WHERE r.source_node_id=%s AND r.relationship_type='EQUIPMENT_HAS_ZERO_CLAMP' AND NOT r.is_candidate
        """, [node_id]) or {}

        # (h) Pipeline Inspection: remaining life & integritas pipa
        pi = one(connection, f"""
            SELECT count(*) pi_count,
                   min({num("n.properties_json->>'rem_life'")}) min_rem_life,
                   sum(CASE WHEN {num("n.properties_json->>'rem_life'")} IS NOT NULL
                              AND {num("n.properties_json->>'rem_life'")} < 5 THEN 1 ELSE 0 END) pi_near_eol
            FROM kg_relationship r
            JOIN kg_node n ON n.node_id=r.target_node_id AND n.node_type='pipeline_inspection'
            WHERE r.source_node_id=%s AND r.relationship_type='EQUIPMENT_HAS_PIPELINE_INSPECTION' AND NOT r.is_candidate
        """, [node_id]) or {}

        # (i) Power & Steam: monitoring utilitas (count saja, sinyal ketersediaan)
        ps = one(connection, """
            SELECT count(*) ps_count
            FROM kg_relationship r
            JOIN kg_node n ON n.node_id=r.target_node_id AND n.node_type='power_steam'
            WHERE r.source_node_id=%s AND r.relationship_type='EQUIPMENT_HAS_POWER_STEAM' AND NOT r.is_candidate
        """, [node_id]) or {}

        # (i2) Metering: sertifikasi alat ukur (expired = risiko kepatuhan)
        meter = one(connection, """
            SELECT count(*) meter_count,
                   sum(CASE WHEN lower(coalesce(n.properties_json->>'status_metering','')) NOT IN ('operasi normal','normal','valid','ok') THEN 1 ELSE 0 END) meter_not_normal,
                   min(nullif(n.properties_json->>'date_expired','')) AS nearest_expired
            FROM kg_relationship r
            JOIN kg_node n ON n.node_id=r.target_node_id AND n.node_type='metering'
            WHERE r.source_node_id=%s AND r.relationship_type='EQUIPMENT_HAS_METERING' AND NOT r.is_candidate
        """, [node_id]) or {}

        # (j) Readiness infrastruktur (Jetty/SPM/Tangki): langsung via edge
        readiness_infra = {}
        for ri_type, ri_rel in [
            ('readiness_jetty', 'EQUIPMENT_HAS_READINESS_JETTY'),
            ('readiness_spm',   'EQUIPMENT_HAS_READINESS_SPM'),
            ('readiness_tank',  'EQUIPMENT_HAS_READINESS_TANK'),
        ]:
            ri = one(connection, f"""
                SELECT count(*) cnt,
                       sum(CASE WHEN lower(coalesce(n.properties_json->>'status_operation',''))
                                  NOT IN ('running','operasi','normal','standby','operate') THEN 1 ELSE 0 END) not_normal
                FROM kg_relationship r
                JOIN kg_node n ON n.node_id=r.target_node_id AND n.node_type='{ri_type}'
                WHERE r.source_node_id=%s AND r.relationship_type='{ri_rel}' AND NOT r.is_candidate
            """, [node_id]) or {}
            if int(ri.get('cnt') or 0):
                readiness_infra[ri_type] = {'count': int(ri['cnt']), 'not_normal': int(ri.get('not_normal') or 0)}

        observations = int(rel.get("observations") or 0)
        signals = {
            # (a) keandalan
            "observations": observations,
            "avg_mtbf": _round(rel.get("avg_mtbf")),
            "avg_mttr": _round(rel.get("avg_mttr")),
            "max_running_hours": _round(rel.get("max_running_hours")),
            "abnormal_status_count": int(rel.get("abnormal_status_count") or 0),
            "function_status": rel.get("function_status"),
            "issue_count": int(iss.get("issue_count") or 0),
            # (b) work-management
            "total_orders": int(wm.get("total_orders") or 0),
            "open_orders": int(wm.get("open_orders") or 0),
            "closed_orders": int(wm.get("closed_orders") or 0),
            "backlog_age_median": _round(wm.get("backlog_age_median")),
            "backlog_age_p90": _round(wm.get("backlog_age_p90")),
            "material_blocked_count": int(wm.get("material_blocked_count") or 0),
            "priority_high_count": int(wm.get("priority_high_count") or 0),
            "planned_cost": _round(wm.get("planned_cost")),
            "actual_cost": _round(wm.get("actual_cost")),
            # (c) business case RKAP (pisah exact vs candidate)
            "rkap_program_count": int(rkap.get("rkap_program_count") or 0),
            "rkap_exact_count": int(rkap.get("rkap_exact_count") or 0),
            "rkap_candidate_count": int(rkap.get("rkap_candidate_count") or 0),
            "rkap_total_cost": _round(rkap.get("rkap_total_cost")),
            "rkap_top_risk_count": int(rkap.get("rkap_top_risk_count") or 0),
            "rkap_delayed_count": int(rkap.get("rkap_delayed_count") or 0),
            "rkap_high_value_count": int(rkap.get("rkap_high_value_count") or 0),
            # (d) inspeksi (tag-match exact-boundary, indikatif)
            "inspection_match_count": inspection_match_count,
            "inspection_findings": inspection_findings,
            # (e) ICU issue
            "icu_count": int(icu.get("icu_count") or 0),
            "icu_open_count": int(icu.get("icu_open_count") or 0),
            # (f) critical equipment
            "ce_count": int(crit_eq.get("ce_count") or 0),
            "ce_class": crit_eq.get("ce_class") or None,
            # (g) zero clamp
            "zc_count": int(zc.get("zc_count") or 0),
            "zc_active_count": int(zc.get("zc_active_count") or 0),
            "zc_dominant_damage": zc.get("dominant_damage") or None,
            # (h) pipeline inspection
            "pi_count": int(pi.get("pi_count") or 0),
            "pi_min_rem_life": _round(pi.get("min_rem_life")),
            "pi_near_eol": int(pi.get("pi_near_eol") or 0),
            # (i) power & steam
            "ps_count": int(ps.get("ps_count") or 0),
            # (i2) metering
            "meter_count": int(meter.get("meter_count") or 0),
            "meter_not_normal": int(meter.get("meter_not_normal") or 0),
            "meter_nearest_expired": meter.get("nearest_expired") or None,
            # (j) readiness infrastruktur (jetty/spm/tank)
            "readiness_infra": readiness_infra or None,
            # (k) kritikalitas + keyakinan
            "criticality": str(criticality) if criticality not in (None, "") else None,
            "confidence_note": (
                "kuat" if observations >= 6 else "indikasi" if observations >= 1 else "lemah"
            ),
        }
        # readiness (hasil tag-match exact-boundary yang sudah dihitung pemanggil)
        if readiness:
            signals.update({k: v for k, v in readiness.items() if v not in (None, "")})
    except Exception:
        # Jangan pernah menggagalkan readiness-context hanya karena sinyal tambahan
        signals = {}
    return signals


def _inspection_tag_match(connection, item: dict, properties: dict) -> tuple[int, list[str]]:
    """Cocokkan inspection ke equipment lewat exact-boundary tag (tidak ada edge).

    Inspection tidak punya EQUIPMENT_HAS_INSPECTION; satu-satunya jalur adalah
    tag-match. Dibatasi ke RU yang sama (sempit) + exact-boundary token (kode penuh)
    supaya tidak salah-alamat. Kembalikan (jumlah match, beberapa temuan ringkas).
    """
    ru = _ru_key(properties.get("refinery_unit") or properties.get("ru") or item.get("business_key"))
    if not ru:
        return 0, []
    equipment_tokens = list(_equipment_match_tokens(item, properties))
    if not equipment_tokens:
        return 0, []
    ru_pattern = rf"\y{ru}\y"
    ru_clause = (
        "((upper(coalesce((properties_json ->> 'refinery_unit'), '')) ~ %s)"
        " OR (upper(coalesce((properties_json ->> 'ru'), '')) ~ %s))"
    )
    # Tag-match didorong ke SQL (exact-boundary) supaya jumlah match akurat (tidak
    # terpotong LIMIT) dan hanya baris yang cocok yang ditarik untuk contoh temuan.
    tag_condition = _compact_match_condition(
        [
            "properties_json ->> 'derived_tag_compact'",
            "properties_json ->> 'tag'",
            "properties_json ->> 'tag_no'",
            "properties_json ->> 'tag_no_ln'",
            "label",
        ]
    )
    matched = one(
        connection,
        f"SELECT count(*) count FROM kg_node WHERE node_type='inspection' AND {ru_clause} AND {tag_condition}",
        [ru_pattern, ru_pattern, equipment_tokens],
    )
    match_count = int(matched.get("count") or 0) if matched else 0
    findings: list[str] = []
    if match_count:
        finding_rows = rows(
            connection,
            f"""
            SELECT properties_json
            FROM kg_node
            WHERE node_type='inspection' AND {ru_clause} AND {tag_condition}
            LIMIT 20
            """,
            [ru_pattern, ru_pattern, equipment_tokens],
        )
        for insp in finding_rows:
            insp_props = parse_json(insp.get("properties_json"))
            detail = (
                insp_props.get("grand_result")
                or insp_props.get("result_remaining_life")
                or insp_props.get("damage_type")
                or insp_props.get("status")
            )
            if detail:
                findings.append(str(detail))
            if len(findings) >= 3:
                break
    return match_count, findings


def _round(value, digits: int = 2):
    try:
        if value is None:
            return None
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def _equipment_match_tokens(item: dict, properties: dict) -> set[str]:
    """Token KODE PENUH equipment untuk exact-boundary match (anti salah-alamat).

    Dulu helper ini juga membangkitkan SEMUA prefix 4..n dari tiap kode
    (token[:size]), sehingga tag domain (readiness/inspection/RKAP) bisa nyangkut
    hanya karena 4 char awal sama — mis. equipment 011E-114 menghasilkan prefix
    "011E" yang menarik readiness milik 011E-115, dst (terukur ~15% salah alamat,
    set token membengkak 354rb -> 2,2jt). Sekarang kembalikan hanya kode penuh;
    pencocokan menjadi kesetaraan token (kode == kode), bukan prefix-substring.
    """
    return {
        token
        for token in (
            _compact_token(value)
            for value in [
                item.get("business_key"),
                item.get("label"),
                properties.get("equipment_code_normalized"),
                properties.get("equipment_id"),
                properties.get("functional_location"),
                properties.get("functional_loc"),
                properties.get("tag"),
                properties.get("tag_no"),
                properties.get("tag_number"),
            ]
        )
        if len(token) >= 4
    }


def _readiness_association_summary(connection) -> dict[str, dict]:
    # Exact-boundary match (kode penuh == kode penuh), sejalan _equipment_match_tokens.
    # Tidak ada lagi set prefix 4..n (sumber salah-alamat) supaya angka readiness
    # konsisten antara dashboard ini dan prompt diagnosis.
    equipment_tokens: dict[str, set[str]] = {}
    equipment_rows = rows(
        connection,
        """
        SELECT business_key, label, properties_json
        FROM kg_node
        WHERE node_type='equipment'
        """,
    )
    for item in equipment_rows:
        properties = parse_json(item.get("properties_json"))
        ru = _ru_key(properties.get("refinery_unit") or properties.get("ru") or item.get("business_key"))
        if not ru:
            continue
        # Pakai helper yang sama dengan jalur prompt (termasuk functional_location +
        # normalisasi suffix /NN) supaya angka readiness selaras dashboard <-> prompt.
        equipment_tokens.setdefault(ru, set()).update(_equipment_match_tokens(item, properties))

    summary: dict[str, dict] = {}
    readiness_rows = rows(
        connection,
        """
        SELECT node_id, properties_json
        FROM kg_node
        WHERE node_type='readiness_record'
        """,
    )
    for item in readiness_rows:
        properties = parse_json(item.get("properties_json"))
        ru = _ru_key(properties.get("refinery_unit") or properties.get("ru"))
        if not ru:
            continue
        row = summary.setdefault(
            ru,
            {
                "readiness_records_total": 0,
                "readiness_records_tag_matched": 0,
                "readiness_records_direct_linked": 0,
            },
        )
        row["readiness_records_total"] += 1
        readiness_tokens = {
            _compact_token(value)
            for value in [
                properties.get("derived_readiness_tag_compact"),
                properties.get("equipment_or_tag"),
                properties.get("tag_no"),
                properties.get("tag_number"),
                properties.get("process_equipment"),
                properties.get("equipment"),
                item.get("label"),
            ]
        }
        readiness_tokens = {token for token in readiness_tokens if len(token) >= 4}
        if any(token in equipment_tokens.get(ru, set()) for token in readiness_tokens):
            row["readiness_records_tag_matched"] += 1

    direct_rows = rows(
        connection,
        """
        SELECT coalesce((e.properties_json ->> 'refinery_unit'), (e.properties_json ->> 'ru')) refinery_unit,
               count(DISTINCT r.target_node_id) direct_count
        FROM kg_relationship r
        JOIN kg_node e ON e.node_id=r.source_node_id
        WHERE r.relationship_type='EQUIPMENT_HAS_READINESS_RECORD' AND NOT r.is_candidate
        GROUP BY 1
        """,
    )
    for item in direct_rows:
        ru = _ru_key(item.get("refinery_unit"))
        if not ru:
            continue
        row = summary.setdefault(
            ru,
            {
                "readiness_records_total": 0,
                "readiness_records_tag_matched": 0,
                "readiness_records_direct_linked": 0,
            },
        )
        row["readiness_records_direct_linked"] = int(item.get("direct_count") or 0)

    for row in summary.values():
        total = row["readiness_records_total"]
        tag_matched = row["readiness_records_tag_matched"]
        direct_linked = row["readiness_records_direct_linked"]
        row["readiness_tag_match_percentage"] = round(tag_matched * 100 / total, 2) if total else None
        row["readiness_direct_link_percentage"] = round(direct_linked * 100 / total, 2) if total else None
        row["readiness_semantic_status"] = (
            "Direct linked" if direct_linked else "Tag matched" if tag_matched else "RU only" if total else "No readiness"
        )
    return summary


def _ru_key(value) -> str:
    text = str(value or "").upper()
    match = re.search(r"\bRU\s+([IVX]+)\b", text)
    return f"RU {match.group(1)}" if match else ""


def _compact_token(value) -> str:
    text = str(value or "").upper()
    if "|" in text:
        text = text.split("|")[-1]
    # Buang akhiran posisi "/NN" (mis. 42-T-107A/00 -> 42-T-107A) supaya tag domain
    # (readiness/inspection) cocok dengan kode equipment yang tanpa suffix. Hanya
    # slash+digit di ujung; "P-101/A" tak tersentuh.
    text = re.sub(r"/\d+$", "", text)
    return re.sub(r"[^A-Z0-9]", "", text)


def _compact_sql(expr: str) -> str:
    """Versi SQL dari `_compact_token`: ambil segmen setelah '|' terakhir, buang
    akhiran posisi '/NN', buang karakter non-alfanumerik, upper-case. Dipakai untuk
    mendorong tag-match ke SQL (hanya baris yang cocok yang ditarik ke Python)
    alih-alih scan tak terbatas. Normalisasi disamakan dengan `_compact_token`."""
    after_pipe = f"regexp_replace(coalesce({expr}, ''), '^.*[|]', '')"
    no_suffix = f"regexp_replace({after_pipe}, '/[0-9]+$', '')"
    return f"upper(regexp_replace({no_suffix}, '[^A-Za-z0-9]', '', 'g'))"


def _compact_match_condition(exprs: list[str]) -> str:
    """Kondisi SQL: salah satu kolom (setelah dipadatkan) cocok exact-boundary dengan
    salah satu token equipment. Token dilewatkan sebagai satu parameter array text;
    `&&` = array overlap. Token kosong/<4 char tak akan overlap (token equipment >=4)."""
    array = ", ".join(_compact_sql(expr) for expr in exprs)
    return f"(ARRAY[{array}] && %s::text[])"


# Domain yang dicocokkan ke equipment lewat tag exact-boundary (dalam RU yang sama). Dipakai
# untuk mensintesis link candidate, BERSAMA oleh graph (endpoint `neighbors`) dan Equipment 360
# (endpoint `equipment_360`) — lihat `_iter_tag_match_candidates`. readiness_record/inspection
# memang tak punya edge nyata ke equipment. reliability_observation/rkap_program/issue PUNYA edge
# nyata secara umum, tapi sebagian equipment (mis. tangki) tak punya edge tercatat — di situ
# tag-match memunculkannya sebagai kandidat. Dedupe via present_ids memastikan record yang sudah
# terhubung lewat edge nyata ke equipment ini tak pernah diduplikasi sebagai kandidat. Match
# tetap exact-boundary kode penuh (anti salah-alamat) + selalu ditandai is_candidate. `tag_columns`
# = kolom SQL untuk match; `tag_props` = properti (mentah) untuk ringkasan + cari matched_token.
TAG_MATCH_GRAPH_LINKS = [
    {
        "node_type": "readiness_record",
        "relationship_type": "EQUIPMENT_HAS_READINESS_RECORD",
        "domain": "readiness",
        "tag_columns": [
            "label",
            "properties_json ->> 'derived_readiness_tag_compact'",
            "properties_json ->> 'equipment_or_tag'",
            "properties_json ->> 'tag_no'",
            "properties_json ->> 'tag_number'",
            "properties_json ->> 'process_equipment'",
            "properties_json ->> 'equipment'",
        ],
        "tag_props": ["derived_readiness_tag_compact", "equipment_or_tag", "tag_no", "tag_number", "process_equipment", "equipment"],
    },
    {
        "node_type": "inspection",
        "relationship_type": "EQUIPMENT_HAS_INSPECTION",
        "domain": "inspection_operational",
        "tag_columns": [
            "properties_json ->> 'derived_tag_compact'",
            "properties_json ->> 'tag'",
            "properties_json ->> 'tag_no'",
            "properties_json ->> 'tag_no_ln'",
            "label",
        ],
        "tag_props": ["derived_tag_compact", "tag", "tag_no", "tag_no_ln"],
    },
    {
        "node_type": "reliability_observation",
        "relationship_type": "EQUIPMENT_HAS_RELIABILITY_OBSERVATION",
        "domain": "reliability",
        "tag_columns": [
            "properties_json ->> 'equipment'",
            "properties_json ->> 'derived_tag_compact'",
            "properties_json ->> 'tag'",
            "properties_json ->> 'tag_no'",
            "properties_json ->> 'tag_number'",
            "label",
        ],
        "tag_props": ["equipment", "derived_tag_compact", "tag", "tag_no", "tag_number"],
    },
    {
        "node_type": "rkap_program",
        "relationship_type": "EQUIPMENT_HAS_RKAP_PROGRAM",
        "domain": "cost_program",
        "tag_columns": [
            "properties_json ->> 'equipment'",
            "properties_json ->> 'derived_tag_compact'",
            "properties_json ->> 'tag'",
            "properties_json ->> 'functional_location'",
            "label",
        ],
        "tag_props": ["equipment", "derived_tag_compact", "tag", "functional_location"],
    },
    {
        "node_type": "issue",
        "relationship_type": "EQUIPMENT_HAS_ISSUE",
        "domain": "issue",
        "tag_columns": [
            "properties_json ->> 'equipment'",
            "properties_json ->> 'derived_tag_compact'",
            "properties_json ->> 'tag'",
            "properties_json ->> 'tag_no'",
            "label",
        ],
        "tag_props": ["equipment", "derived_tag_compact", "tag", "tag_no"],
    },
]


def _tag_matched_domain_rows(
    connection, node_type: str, ru: str, equipment_tokens: list[str], tag_columns: list[str], limit: int
) -> list[dict]:
    """Baris kg_node domain (readiness/inspection/reliability/RKAP/issue) yang cocok
    exact-boundary dengan token equipment dalam RU yang sama. Reuse ru-filter (`\\y`) +
    `_compact_match_condition`. Dipakai `_iter_tag_match_candidates` untuk mensintesis link
    candidate di graph (`neighbors`) dan Equipment 360 (`equipment_360`)."""
    if not ru or not equipment_tokens:
        return []
    ru_pattern = rf"\y{ru}\y"
    ru_clause = (
        "((upper(coalesce((properties_json ->> 'refinery_unit'), '')) ~ %s)"
        " OR (upper(coalesce((properties_json ->> 'ru'), '')) ~ %s))"
    )
    tag_condition = _compact_match_condition(tag_columns)
    return rows(
        connection,
        f"SELECT * FROM kg_node WHERE node_type=%s AND {ru_clause} AND {tag_condition} LIMIT %s",
        [node_type, ru_pattern, ru_pattern, equipment_tokens, limit],
    )


def _synthetic_tag_link_edge(root_id: str, row: dict, config: dict, matched_token: str) -> dict:
    """Edge sintetis (candidate) equipment -> node domain hasil tag-match, dibentuk seperti
    baris kg_relationship lalu dilewatkan ke `edge_to_api`. is_candidate=True → render
    putus-putus; confidence=None → selalu tampil saat toggle candidate aktif."""
    row_props = parse_json(row.get("properties_json"))
    raw_tag = next((row_props.get(key) for key in config["tag_props"] if row_props.get(key)), None)
    return {
        "relationship_id": f"tagmatch::{root_id}::{row['node_id']}",
        "source_node_id": root_id,
        "target_node_id": row["node_id"],
        "relationship_type": config["relationship_type"],
        "domain": config["domain"],
        "confidence": None,
        "match_method": "tag_exact_boundary",
        "is_candidate": True,
        "properties_json": json.dumps({
            "matched_token": matched_token,
            "derived_candidate_reason": "Cocok tag exact-boundary dalam RU yang sama (belum ada relasi tercatat ke equipment).",
            "matched_tag_raw": raw_tag,
        }),
        "source_file": row.get("source_file"),
        "source_sheet": row.get("source_sheet"),
        "source_row": row.get("source_row"),
        "source_record_id": row.get("source_record_id"),
    }


def _iter_tag_match_candidates(
    connection, root: dict, root_properties: dict, present_ids: set[str], budget: int,
    relationship_type: str = "", node_type: str = "",
):
    """Hasilkan (config, row, matched_token) untuk domain kandidat hasil tag-match
    (exact-boundary, dalam RU yang sama). Dipakai BERSAMA oleh endpoint `neighbors`
    dan `equipment_360` supaya kedua tampilan konsisten (DRY). Memutakhirkan
    `present_ids` (set node_id yang sudah tampil) agar tak ada duplikasi terhadap
    relasi nyata maupun antar-kandidat; berhenti saat `budget` habis. Reuse jalur
    match yang sama dengan prompt: _ru_key + _equipment_match_tokens +
    _tag_matched_domain_rows + _compact_token + TAG_MATCH_GRAPH_LINKS."""
    ru = _ru_key(root_properties.get("refinery_unit") or root_properties.get("ru") or root.get("business_key"))
    equipment_tokens = list(_equipment_match_tokens(root, root_properties))
    if not ru or not equipment_tokens:
        return
    equipment_token_set = set(equipment_tokens)
    for config in TAG_MATCH_GRAPH_LINKS:
        if budget <= 0:
            break
        if relationship_type and relationship_type != config["relationship_type"]:
            continue
        if node_type and node_type != config["node_type"]:
            continue
        matched_rows = _tag_matched_domain_rows(
            connection, config["node_type"], ru, equipment_tokens,
            config["tag_columns"], min(budget, 60),
        )
        for row in matched_rows:
            if row["node_id"] in present_ids:
                continue
            row_props = parse_json(row.get("properties_json"))
            row_tokens = {
                _compact_token(value)
                for value in [row.get("label"), *(row_props.get(key) for key in config["tag_props"])]
            }
            matched_token = next((token for token in row_tokens if token in equipment_token_set), "")
            present_ids.add(row["node_id"])
            budget -= 1
            yield config, row, matched_token
            if budget <= 0:
                break


def _nodes_by_id(connection, node_ids: list[str]) -> list[dict]:
    if not node_ids:
        return []
    placeholders = ",".join("%s" for _ in node_ids)
    return rows(connection, f"SELECT * FROM kg_node WHERE node_id IN ({placeholders})", node_ids)


def node_to_api(item: dict, connection=None) -> dict:
    properties = parse_json(item.get("properties_json"))
    if connection:
        properties = {**properties, **_virtual_node_properties(connection, item, properties)}
    return {
        "id": item["node_id"], "kind": item["node_type"], "label": item.get("label") or item["node_id"],
        "subtitle": item.get("business_key"), "domain": item.get("domain"),
        "properties": properties,
        "refinery_unit": properties.get("refinery_unit") or properties.get("ru"),
        "equipment_code_normalized": properties.get("equipment_code_normalized") or properties.get("equipment_id"),
        "source": {
            "workbook": item.get("source_file") or "",
            "sheet": item.get("source_sheet") or "",
            "row": item.get("source_row"),
            "record_id": item.get("source_record_id"),
        },
    }


def edge_to_api(item: dict, connection=None, source: dict | None = None, target: dict | None = None) -> dict:
    properties = parse_json(item.get("properties_json"))
    if connection:
        if source is None:
            source = one(connection, "SELECT * FROM kg_node WHERE node_id=%s", [item["source_node_id"]])
        if target is None:
            target = one(connection, "SELECT * FROM kg_node WHERE node_id=%s", [item["target_node_id"]])
        properties = {**properties, **_virtual_edge_properties(item, properties, source, target)}
    return {
        "id": item["relationship_id"], "source": item["source_node_id"],
        "target": item["target_node_id"], "type": item["relationship_type"],
        "domain": item.get("domain"), "confidence": item.get("confidence"),
        "match_method": item.get("match_method"), "is_candidate": item.get("is_candidate"),
        "properties": properties,
        "source_ref": {
            "workbook": item.get("source_file") or "",
            "sheet": item.get("source_sheet") or "",
            "row": item.get("source_row"),
            "record_id": item.get("source_record_id"),
        },
    }


def _virtual_node_properties(connection, item: dict, properties: dict) -> dict:
    node_id, node_type = item["node_id"], item["node_type"]
    derived: dict = {}
    if node_type == "equipment":
        for rel_type, key in [
            ("EQUIPMENT_HAS_ISSUE", "issue_count"),
            ("EQUIPMENT_HAS_MAINTENANCE_ORDER", "maintenance_order_count"),
            ("EQUIPMENT_HAS_NOTIFICATION", "notification_count"),
            ("EQUIPMENT_HAS_INSPECTION", "inspection_count"),
            ("EQUIPMENT_HAS_READINESS_RECORD", "readiness_record_count"),
            ("EQUIPMENT_HAS_RKAP_PROGRAM", "rkap_program_count"),
            ("EQUIPMENT_HAS_RELIABILITY_OBSERVATION", "reliability_observation_count"),
        ]:
            derived[key] = fetch_tuple(
                connection,
                "SELECT count(*) FROM kg_relationship WHERE source_node_id=%s AND relationship_type=%s AND NOT is_candidate",
                [node_id, rel_type],
            )[0]
        reliability = one(connection, """
            SELECT
                avg((CASE WHEN (nullif((n.properties_json ->> 'mtbf'), '')) ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN (nullif((n.properties_json ->> 'mtbf'), ''))::double precision END)) avg_mtbf,
                avg((CASE WHEN (nullif((n.properties_json ->> 'mttr'), '')) ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN (nullif((n.properties_json ->> 'mttr'), ''))::double precision END)) avg_mttr,
                sum(CASE WHEN lower(coalesce((n.properties_json ->> 'status'), '')) NOT IN ('', 'running', 'run', 'operation', 'operating', 'normal', 'standby') THEN 1 ELSE 0 END) abnormal_status_count
            FROM kg_relationship r
            JOIN kg_node n ON n.node_id=r.target_node_id
            WHERE r.source_node_id=%s AND r.relationship_type='EQUIPMENT_HAS_RELIABILITY_OBSERVATION' AND NOT r.is_candidate
        """, [node_id]) or {}
        derived.update({key: _round_metric(value) for key, value in reliability.items()})
        rkap = one(connection, """
            SELECT sum(CASE WHEN (CASE WHEN (nullif((n.properties_json ->> 'total_equivalent_idr'), '')) ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN (nullif((n.properties_json ->> 'total_equivalent_idr'), ''))::double precision END) >= 10000000000 THEN 1 ELSE 0 END) high_value_rkap_count
            FROM kg_relationship r
            JOIN kg_node n ON n.node_id=r.target_node_id
            WHERE r.source_node_id=%s AND r.relationship_type='EQUIPMENT_HAS_RKAP_PROGRAM' AND NOT r.is_candidate
        """, [node_id]) or {}
        derived.update({key: int(value or 0) for key, value in rkap.items()})
        derived["open_issue_count"] = derived["issue_count"]
        derived["has_issue"] = derived["issue_count"] > 0
        derived["has_readiness"] = derived["readiness_record_count"] > 0
        derived["has_rkap"] = derived["rkap_program_count"] > 0
        derived["risk_score"] = _equipment_risk_score(derived)
    elif node_type == "reliability_observation":
        derived["period_key"] = "-".join(str(properties.get(key, "")).strip() for key in ["year", "month", "week"] if properties.get(key))
        status = str(properties.get("status") or "").lower()
        derived["is_abnormal_status"] = bool(status and status not in {"running", "run", "operation", "operating", "normal", "standby"})
        derived["mtbf_bucket"] = _bucket(_float(properties.get("mtbf")), [(100, "critical"), (500, "low"), (2000, "medium")], "healthy")
        derived["mttr_bucket"] = _bucket(_float(properties.get("mttr")), [(8, "normal"), (24, "elevated"), (72, "high")], "critical")
    elif node_type == "inspection":
        delay = _days_between(properties.get("plan_date"), properties.get("actual_date"))
        derived["inspection_delay_days"] = delay
        derived["is_overdue_or_late"] = delay is not None and delay > 0
        derived["is_nonconformity"] = "non" in str(properties.get("result") or "").lower()
    elif node_type == "rkap_program":
        derived["schedule_variance_days"] = _days_between(properties.get("plan_finish"), properties.get("actual_finish"))
        amount = _float(properties.get("total_equivalent_idr"))
        derived["budget_bucket"] = _bucket(amount, [(1_000_000_000, "medium"), (10_000_000_000, "high"), (50_000_000_000, "major")], "strategic")
        derived["is_high_value"] = amount is not None and amount >= 10_000_000_000
        derived["is_top_risk"] = str(properties.get("top_risk")).lower() == "true"
    elif node_type == "equipment_issue":
        derived["has_mitigation"] = _meaningful_text(properties.get("mitigation"))
        derived["has_permanent_solution"] = _meaningful_text(properties.get("permanent_solution"))
        derived["has_irkap_reference"] = _meaningful_text(properties.get("irkap_solution")) or _meaningful_text(properties.get("irkap_mitigation"))
        report_date = _parse_date(properties.get("report_date"))
        derived["issue_age_days"] = (date.today() - report_date).days if report_date else None
    elif node_type == "rcps_recommendation":
        target = _parse_date(properties.get("target"))
        derived["target_date_normalized"] = target.isoformat() if target else properties.get("target")
        derived["is_overdue"] = bool(target and target < date.today())
        derived["owner_pic"] = properties.get("pic")
    return {f"derived_{key}": value for key, value in derived.items() if value is not None}


def _virtual_edge_properties(item: dict, properties: dict, source: dict | None, target: dict | None) -> dict:
    source_label = source.get("label") if source else item["source_node_id"]
    target_label = target.get("label") if target else item["target_node_id"]
    source_type = source.get("node_type") if source else ""
    target_type = target.get("node_type") if target else ""
    confidence = item.get("confidence")
    match_token = properties.get("matched_token") or properties.get("readiness_equipment_token") or properties.get("equipment_code_raw")
    derived = {
        "source_label": source_label,
        "source_type": source_type,
        "target_label": target_label,
        "target_type": target_type,
        "direction_label": f"{source_label} -> {target_label}",
        "is_shortcut": bool(properties.get("shortcut")),
        "provenance_label": " / ".join(str(value) for value in [item.get("source_file"), item.get("source_sheet"), item.get("source_row")] if value not in (None, "")),
        "match_quality_bucket": _confidence_bucket(confidence),
    }
    if match_token:
        derived["match_token"] = match_token
        derived["match_explain"] = f"Matched by {item.get('match_method') or 'rule'} using token {match_token}"
    if item.get("is_candidate"):
        derived["review_priority"] = "high" if (confidence or 0) >= 0.9 else "medium" if (confidence or 0) >= 0.7 else "low"
        derived["candidate_reason"] = item.get("match_method") or "candidate relationship"
    if properties.get("shortcut"):
        derived["shortcut_warning"] = "Shortcut edge for fast exploration; inspect hierarchy edge for natural path."
    return {f"derived_{key}": value for key, value in derived.items() if value not in (None, "")}


def _equipment_risk_score(values: dict) -> int:
    score = 0
    avg_mtbf = _float(values.get("avg_mtbf"))
    avg_mttr = _float(values.get("avg_mttr"))
    if avg_mtbf == 0:
        score += 35
    elif avg_mtbf is not None and avg_mtbf < 100:
        score += 30
    elif avg_mtbf is not None and avg_mtbf < 500:
        score += 18
    if avg_mttr is not None and avg_mttr > 72:
        score += 25
    elif avg_mttr is not None and avg_mttr > 24:
        score += 18
    score += min(int(values.get("issue_count") or 0) * 4, 16)
    if values.get("abnormal_status_count"):
        score += 15
    if not values.get("has_rkap"):
        score += 6
    return score


def _round_metric(value):
    numeric = _float(value)
    return round(numeric, 2) if numeric is not None else value


def _bucket(value: float | None, thresholds: list[tuple[float, str]], default: str):
    if value is None:
        return "unknown"
    for ceiling, label in thresholds:
        if value <= ceiling:
            return label
    return default


def _confidence_bucket(value) -> str:
    confidence = _float(value)
    if confidence is None:
        return "unknown"
    if confidence >= 0.95:
        return "verified"
    if confidence >= 0.8:
        return "strong"
    if confidence >= 0.6:
        return "review"
    return "weak"


def _meaningful_text(value) -> bool:
    text = str(value or "").strip().lower()
    return bool(text and text not in {"-", "n/a", "na", "none", "null"})


def _days_between(start, finish) -> int | None:
    start_date, finish_date = _parse_date(start), _parse_date(finish)
    if not start_date or not finish_date:
        return None
    return (finish_date - start_date).days


def _parse_date(value) -> date | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if text.isdigit():
        serial = int(text)
        if 20_000 <= serial <= 80_000:
            return date.fromordinal(date(1899, 12, 30).toordinal() + serial)
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except ValueError:
            continue
    return None


def parse_json(value):
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {"value": value}


def _analysis_rows(connection, name: str, limit: int) -> list[dict]:
    items = rows(connection, "SELECT row_json FROM graph_analysis WHERE analysis_name=%s LIMIT %s", [name, limit])
    return [parse_json(item["row_json"]) for item in items]


def _float(value):
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _node_filter(item: dict, node_type: str, refinery_unit: str, equipment_code: str) -> bool:
    if node_type and item.get("node_type") != node_type:
        return False
    props = parse_json(item.get("properties_json"))
    if refinery_unit:
        haystack = " ".join(str(value or "") for value in [
            props.get("refinery_unit"), props.get("ru"), item.get("label"), item.get("business_key")
        ]).lower()
        if refinery_unit.lower() not in haystack:
            return False
    if equipment_code:
        haystack = " ".join(str(value or "") for value in [
            props.get("equipment_code_normalized"), props.get("equipment_id"), props.get("equipment"),
            item.get("label"), item.get("business_key")
        ]).lower()
        if equipment_code.lower() not in haystack:
            return False
    return True


def stream_csv(dataset_id: str, query: str):
    connection = db_for(dataset_id)
    try:
        cursor = connection.execute(query)
        columns = [item.name for item in cursor.description]
        yield ",".join(csv_cell(item) for item in columns) + "\n"
        while batch := cursor.fetchmany(1000):
            for row in batch:
                yield ",".join(csv_cell(row.get(c)) for c in columns) + "\n"
    finally:
        connection.close()


def stream_ndjson(dataset_id: str):
    connection = db_for(dataset_id)
    try:
        cursor = connection.execute("SELECT * FROM kg_node")
        while batch := cursor.fetchmany(1000):
            for row in batch:
                yield json.dumps({"recordType": "node", **row}, default=str) + "\n"
        cursor = connection.execute("SELECT * FROM kg_relationship")
        while batch := cursor.fetchmany(1000):
            for row in batch:
                yield json.dumps({"recordType": "edge", **row}, default=str) + "\n"
    finally:
        connection.close()


def csv_cell(value) -> str:
    return '"' + ("" if value is None else str(value)).replace('"', '""') + '"'


class DiagnosisGenerateRequest(BaseModel):
    prompt: str
    role: str = "engineer"


_DINOIKI_URL = "https://ai.dinoiki.com/v1/chat/completions"


@app.post("/api/diagnosis/generate")
def diagnosis_generate(req: DiagnosisGenerateRequest):
    import os, requests as _req
    api_key = os.environ.get("DINOIKI_API_KEY", "")
    if not api_key:
        raise HTTPException(status_code=503, detail="DINOIKI_API_KEY belum dikonfigurasi di server.")

    def _stream():
        try:
            resp = _req.post(
                _DINOIKI_URL,
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
                json={"model": "gpt-4o", "messages": [{"role": "user", "content": req.prompt}],
                      "max_tokens": 4096, "temperature": 0.7, "stream": True},
                stream=True, timeout=120,
            )
            if not resp.ok:
                yield f"data: {json.dumps({'error': f'HTTP Error {resp.status_code}: {resp.reason}'})}\n\n"
                yield "data: [DONE]\n\n"
                return
            for raw in resp.iter_lines():
                if not raw:
                    continue
                line = raw.decode("utf-8") if isinstance(raw, bytes) else raw
                if not line.startswith("data:"):
                    continue
                payload_str = line[5:].strip()
                if payload_str == "[DONE]":
                    break
                try:
                    obj = json.loads(payload_str)
                    text = obj.get("choices", [{}])[0].get("delta", {}).get("content") or ""
                    if text:
                        yield f"data: {json.dumps({'text': text})}\n\n"
                except Exception:
                    pass
        except Exception as exc:
            yield f"data: {json.dumps({'error': str(exc)})}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


class AnalysisGenerateRequest(BaseModel):
    scope: str = "dataset"      # 'dataset' | 'ru' | 'equipment'
    ru: str = ""
    equipment_id: str = ""
    focus: str = "general"      # 'reliability' | 'readiness' | 'coverage' | 'risk' | 'general'


def _analysis_ai_stream(prompt: str):
    import os, requests as _req
    api_key = os.environ.get("DINOIKI_API_KEY", "")
    if not api_key:
        yield f"data: {json.dumps({'error': 'DINOIKI_API_KEY belum dikonfigurasi.'})}\n\n"
        yield "data: [DONE]\n\n"
        return
    try:
        resp = _req.post(
            _DINOIKI_URL,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
            json={"model": "gpt-4o", "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 4096, "temperature": 0.7, "stream": True},
            stream=True, timeout=120,
        )
        if not resp.ok:
            yield f"data: {json.dumps({'error': f'HTTP Error {resp.status_code}: {resp.reason}'})}\n\n"
            yield "data: [DONE]\n\n"
            return
        for raw in resp.iter_lines():
            if not raw:
                continue
            line = raw.decode("utf-8") if isinstance(raw, bytes) else raw
            if not line.startswith("data:"):
                continue
            ps = line[5:].strip()
            if ps == "[DONE]":
                break
            try:
                obj = json.loads(ps)
                text = obj.get("choices", [{}])[0].get("delta", {}).get("content") or ""
                if text:
                    yield f"data: {json.dumps({'text': text})}\n\n"
            except Exception:
                pass
    except Exception as exc:
        yield f"data: {json.dumps({'error': str(exc)})}\n\n"
    yield "data: [DONE]\n\n"


def _fmt_int(v) -> str:
    try:
        return f"{int(v):,}"
    except Exception:
        return str(v) if v is not None else "0"


def _gather_ru_ctx(connection, ru: str) -> dict:
    ru_like = f'%{ru}%'
    eq = rows(connection, """
        SELECT count(*) AS total,
               count(DISTINCT properties_json->>'equipment_type') AS types
        FROM kg_node WHERE node_type='equipment'
          AND properties_json->>'refinery_unit' ILIKE %s
    """, [ru_like])
    top_eq = rows(connection, """
        SELECT n.label, n.business_key,
               n.properties_json->>'equipment_type' AS eq_type,
               count(r.relationship_id) AS rel_count
        FROM kg_node n
        LEFT JOIN kg_relationship r ON r.source_node_id = n.node_id AND NOT r.is_candidate
        WHERE n.node_type='equipment' AND n.properties_json->>'refinery_unit' ILIKE %s
        GROUP BY n.node_id, n.label, n.business_key, eq_type
        ORDER BY rel_count DESC LIMIT 15
    """, [ru_like])
    domain_counts: dict = {}
    for domain in ['reliability_observation','rkap_program','equipment_issue','icu_issue',
                   'bad_actor','critical_equipment','readiness_record','monitoring_operasi',
                   'power_steam','paf_issue','rotor','atg','zero_clamp',
                   'readiness_jetty','readiness_spm','readiness_tank',
                   'jetty_workplan','spm_workplan','tank_workplan',
                   'tkdn','oa_availability','plo_permit','pipeline_inspection',
                   'work_order','notification']:
        r = rows(connection, "SELECT count(*) AS c FROM kg_node WHERE node_type=%s AND properties_json->>'refinery_unit' ILIKE %s", [domain, ru_like])
        if r and int(r[0]['c']) > 0:
            domain_counts[domain] = int(r[0]['c'])

    # Work Order — ringkasan per status
    wo = rows(connection, """
        SELECT count(*) AS total,
               sum(CASE WHEN properties_json->>'derived_is_open_order'='true' THEN 1 ELSE 0 END) AS open_count,
               sum(CASE WHEN properties_json->>'derived_status_bucket' IN ('WAMA','WASR') THEN 1 ELSE 0 END) AS waiting_material,
               sum(CASE WHEN properties_json->>'order_type' IS NOT NULL THEN 1 ELSE 0 END) AS with_type,
               sum(CASE WHEN (properties_json->>'derived_planned_cost')::float > 0
                        AND properties_json->>'derived_planned_cost' ~ '^[0-9]' THEN
                        (properties_json->>'derived_planned_cost')::float ELSE 0 END) AS total_planned_cost
        FROM kg_node WHERE node_type='work_order'
          AND properties_json->>'refinery_unit' ILIKE %s
    """, [ru_like])
    wo_by_type = rows(connection, """
        SELECT properties_json->>'order_type' AS otype, count(*) AS c
        FROM kg_node WHERE node_type='work_order'
          AND properties_json->>'refinery_unit' ILIKE %s
          AND properties_json->>'order_type' IS NOT NULL
        GROUP BY otype ORDER BY c DESC LIMIT 8
    """, [ru_like])

    # Notification — ringkasan
    notif = rows(connection, """
        SELECT count(*) AS total,
               count(DISTINCT properties_json->>'notification_type') AS types
        FROM kg_node WHERE node_type='notification'
          AND properties_json->>'refinery_unit' ILIKE %s
    """, [ru_like])

    # Readiness infrastruktur — status breakdown per tipe
    readiness_infra = {}
    for rtype in ['readiness_jetty', 'readiness_spm', 'readiness_tank']:
        status_rows = rows(connection, """
            SELECT properties_json->>'status_operation' AS status,
                   count(*) AS c
            FROM kg_node WHERE node_type=%s
              AND properties_json->>'refinery_unit' ILIKE %s
            GROUP BY status
        """, [rtype, ru_like])
        if status_rows:
            readiness_infra[rtype] = {r.get('status') or 'Unknown': int(r.get('c') or 0) for r in status_rows}

    # Workplan RTL — status breakdown per tipe
    workplan_infra = {}
    for wtype in ['jetty_workplan', 'spm_workplan', 'tank_workplan']:
        wp_rows = rows(connection, """
            SELECT properties_json->>'status' AS status,
                   count(*) AS c
            FROM kg_node WHERE node_type=%s
              AND properties_json->>'refinery_unit' ILIKE %s
            GROUP BY status
        """, [wtype, ru_like])
        if wp_rows:
            workplan_infra[wtype] = {r.get('status') or 'Unknown': int(r.get('c') or 0) for r in wp_rows}

    # TKDN — ringkasan per RU
    tkdn = rows(connection, """
        SELECT count(*) AS total,
               avg(CASE WHEN properties_json->>'persentase' ~ '^[0-9]'
                   THEN (properties_json->>'persentase')::float END) AS avg_pct,
               sum(CASE WHEN properties_json->>'nominal' ~ '^[0-9]'
                   THEN (properties_json->>'nominal')::float ELSE 0 END) AS total_nominal
        FROM kg_node WHERE node_type='tkdn'
          AND properties_json->>'refinery_unit' ILIKE %s
    """, [ru_like])

    # OA Availability — rata-rata value_perc per RU
    oa = rows(connection, """
        SELECT count(*) AS total,
               avg(CASE WHEN properties_json->>'value_perc' ~ '^[0-9]'
                   THEN (properties_json->>'value_perc')::float END) AS avg_value,
               avg(CASE WHEN properties_json->>'actual_target' ~ '^[0-9]'
                   THEN (properties_json->>'actual_target')::float END) AS avg_target
        FROM kg_node WHERE node_type='oa_availability'
          AND properties_json->>'refinery_unit' ILIKE %s
    """, [ru_like])

    # PLO Permit — jumlah, expired vs valid
    plo = rows(connection, """
        SELECT count(*) AS total,
               sum(CASE WHEN properties_json->>'masa_berlaku' IS NOT NULL
                        AND properties_json->>'masa_berlaku' < to_char(CURRENT_DATE,'YYYY-MM-DD')
                   THEN 1 ELSE 0 END) AS expired
        FROM kg_node WHERE node_type='plo_permit'
          AND properties_json->>'refinery_unit' ILIKE %s
    """, [ru_like])

    # Pipeline Inspection — jumlah, rem_life rendah, temp_repair
    pipeline = rows(connection, """
        SELECT count(*) AS total,
               sum(CASE WHEN properties_json->>'temp_repair' ~ '^[1-9]'
                   THEN 1 ELSE 0 END) AS has_temp_repair,
               avg(CASE WHEN properties_json->>'rem_life' ~ '^[0-9]'
                   THEN (properties_json->>'rem_life')::float END) AS avg_rem_life,
               sum(CASE WHEN properties_json->>'rem_life' ~ '^[0-9]'
                        AND (properties_json->>'rem_life')::float < 5
                   THEN 1 ELSE 0 END) AS near_eol
        FROM kg_node WHERE node_type='pipeline_inspection'
          AND properties_json->>'refinery_unit' ILIKE %s
    """, [ru_like])

    rel_obs = rows(connection, """
        SELECT count(*) AS total,
               sum(CASE WHEN (properties_json->>'derived_is_top_risk')='true' THEN 1 ELSE 0 END) AS top_risk,
               avg(CASE WHEN properties_json->>'mtbf' ~ '^[0-9]' THEN (properties_json->>'mtbf')::float END) AS avg_mtbf,
               avg(CASE WHEN properties_json->>'mttr' ~ '^[0-9]' THEN (properties_json->>'mttr')::float END) AS avg_mttr
        FROM kg_node WHERE node_type='reliability_observation'
          AND properties_json->>'refinery_unit' ILIKE %s
    """, [ru_like])
    rkap = rows(connection, """
        SELECT count(*) AS total,
               sum(CASE WHEN (properties_json->>'derived_is_delayed')='true' THEN 1 ELSE 0 END) AS delayed,
               sum(CASE WHEN properties_json->>'derived_planned_cost' ~ '^[0-9]'
                   THEN (properties_json->>'derived_planned_cost')::float ELSE 0 END) AS budget
        FROM kg_node WHERE node_type='rkap_program'
          AND properties_json->>'refinery_unit' ILIKE %s
    """, [ru_like])
    bad = rows(connection, """
        SELECT properties_json->>'failure_mode' AS fm, count(*) AS c
        FROM kg_node WHERE node_type='bad_actor'
          AND properties_json->>'refinery_unit' ILIKE %s
        GROUP BY fm ORDER BY c DESC LIMIT 10
    """, [ru_like])

    # --- True knowledge-graph metrics (multi-hop traversal via kg_relationship) ---

    # Equipment isolated in graph: no confirmed edges at all (zero graph coverage)
    isolated = rows(connection, """
        SELECT count(*) AS c
        FROM kg_node n
        WHERE n.node_type='equipment' AND n.properties_json->>'refinery_unit' ILIKE %s
          AND NOT EXISTS (
              SELECT 1 FROM kg_relationship r
              WHERE r.source_node_id = n.node_id AND NOT r.is_candidate
          )
    """, [ru_like])

    # Multi-domain intersection: equipment connected to 3+ distinct domain types
    multi_domain = rows(connection, """
        SELECT n.label, n.business_key,
               count(DISTINCT r.relationship_type) AS domain_count,
               string_agg(DISTINCT r.relationship_type, ', ' ORDER BY r.relationship_type) AS domains
        FROM kg_node n
        JOIN kg_relationship r ON r.source_node_id = n.node_id AND NOT r.is_candidate
        WHERE n.node_type='equipment' AND n.properties_json->>'refinery_unit' ILIKE %s
        GROUP BY n.node_id, n.label, n.business_key
        HAVING count(DISTINCT r.relationship_type) >= 3
        ORDER BY domain_count DESC LIMIT 10
    """, [ru_like])

    # Cross-domain high-risk: equipment linked to BOTH bad_actor AND critical_equipment in the graph
    high_risk_cross = rows(connection, """
        SELECT n.label, n.business_key
        FROM kg_node n
        WHERE n.node_type='equipment' AND n.properties_json->>'refinery_unit' ILIKE %s
          AND EXISTS (
              SELECT 1 FROM kg_relationship r1
              JOIN kg_node t1 ON t1.node_id = r1.target_node_id
              WHERE r1.source_node_id = n.node_id AND NOT r1.is_candidate
                AND t1.node_type = 'bad_actor'
          )
          AND EXISTS (
              SELECT 1 FROM kg_relationship r2
              JOIN kg_node t2 ON t2.node_id = r2.target_node_id
              WHERE r2.source_node_id = n.node_id AND NOT r2.is_candidate
                AND t2.node_type = 'critical_equipment'
          )
        LIMIT 15
    """, [ru_like])

    # Graph density: ratio equipment with >=1 confirmed edge vs total
    total_eq = int((eq[0].get('total') or 0)) if eq else 0
    isolated_count = int((isolated[0].get('c') or 0)) if isolated else 0
    connected_count = total_eq - isolated_count

    # Equipment with delayed RKAP traversed through graph edges
    delayed_rkap_eq = rows(connection, """
        SELECT count(DISTINCT n.node_id) AS c
        FROM kg_node n
        JOIN kg_relationship r ON r.source_node_id = n.node_id AND NOT r.is_candidate
        JOIN kg_node rk ON rk.node_id = r.target_node_id AND rk.node_type = 'rkap_program'
        WHERE n.node_type='equipment' AND n.properties_json->>'refinery_unit' ILIKE %s
          AND (rk.properties_json->>'derived_is_delayed') = 'true'
    """, [ru_like])

    return {
        'ru': ru,
        'equipment': eq[0] if eq else {},
        'top_equipment': top_eq,
        'domain_counts': domain_counts,
        'reliability': rel_obs[0] if rel_obs else {},
        'rkap': rkap[0] if rkap else {},
        'bad_actors': bad,
        'work_order': wo[0] if wo else {},
        'wo_by_type': wo_by_type,
        'notification': notif[0] if notif else {},
        'readiness_infra': readiness_infra,
        'workplan_infra': workplan_infra,
        'tkdn': tkdn[0] if tkdn else {},
        'oa': oa[0] if oa else {},
        'plo': plo[0] if plo else {},
        'pipeline': pipeline[0] if pipeline else {},
        # KG-specific metrics
        'kg': {
            'total_equipment': total_eq,
            'isolated_nodes': isolated_count,
            'connected_nodes': connected_count,
            'graph_coverage_pct': round(connected_count / total_eq * 100, 1) if total_eq else 0,
            'multi_domain_equipment': multi_domain,
            'high_risk_cross_domain': high_risk_cross,
            'delayed_rkap_equipment_count': int((delayed_rkap_eq[0].get('c') or 0)) if delayed_rkap_eq else 0,
        },
    }


def _gather_dataset_ctx(connection) -> dict:
    counts = rows(connection, """
        SELECT node_type, count(*) AS c FROM kg_node
        WHERE node_type NOT IN ('refinery_unit','plant')
        GROUP BY node_type ORDER BY c DESC LIMIT 30
    """)
    rels = rows(connection, "SELECT count(*) AS total, sum(CASE WHEN is_candidate THEN 1 ELSE 0 END) AS candidates FROM kg_relationship")
    eq_ru = rows(connection, """
        SELECT properties_json->>'refinery_unit' AS ru, count(*) AS c
        FROM kg_node WHERE node_type='equipment' AND properties_json->>'refinery_unit' IS NOT NULL
        GROUP BY ru ORDER BY c DESC LIMIT 10
    """)

    # --- True knowledge-graph metrics across the whole dataset ---

    # Top equipment by degree centrality (most connections in graph)
    top_degree = rows(connection, """
        SELECT n.label, n.business_key,
               n.properties_json->>'refinery_unit' AS ru,
               count(r.relationship_id) AS degree
        FROM kg_node n
        JOIN kg_relationship r ON r.source_node_id = n.node_id AND NOT r.is_candidate
        WHERE n.node_type = 'equipment'
        GROUP BY n.node_id, n.label, n.business_key, ru
        ORDER BY degree DESC LIMIT 10
    """)

    # Relationship type distribution (which domains are richest in edges)
    rel_type_dist = rows(connection, """
        SELECT relationship_type, count(*) AS c
        FROM kg_relationship WHERE NOT is_candidate
        GROUP BY relationship_type ORDER BY c DESC LIMIT 15
    """)

    # Isolated equipment dataset-wide (zero confirmed edges)
    isolated = rows(connection, """
        SELECT count(*) AS c FROM kg_node n
        WHERE n.node_type = 'equipment'
          AND NOT EXISTS (
              SELECT 1 FROM kg_relationship r
              WHERE r.source_node_id = n.node_id AND NOT r.is_candidate
          )
    """)

    # Total equipment count
    total_eq_row = rows(connection, "SELECT count(*) AS c FROM kg_node WHERE node_type='equipment'")
    total_eq = int((total_eq_row[0].get('c') or 0)) if total_eq_row else 0
    isolated_count = int((isolated[0].get('c') or 0)) if isolated else 0

    # Multi-domain equipment (connected to 3+ distinct relationship types)
    multi_domain_count = rows(connection, """
        SELECT count(*) AS c FROM (
            SELECT n.node_id
            FROM kg_node n
            JOIN kg_relationship r ON r.source_node_id = n.node_id AND NOT r.is_candidate
            WHERE n.node_type = 'equipment'
            GROUP BY n.node_id
            HAVING count(DISTINCT r.relationship_type) >= 3
        ) sub
    """)

    return {
        'node_counts': counts,
        'relationships': rels[0] if rels else {},
        'equipment_per_ru': eq_ru,
        'kg': {
            'total_equipment': total_eq,
            'isolated_nodes': isolated_count,
            'graph_coverage_pct': round((total_eq - isolated_count) / total_eq * 100, 1) if total_eq else 0,
            'multi_domain_equipment_count': int((multi_domain_count[0].get('c') or 0)) if multi_domain_count else 0,
            'top_degree_equipment': top_degree,
            'relationship_type_distribution': rel_type_dist,
        },
    }


def _build_analysis_prompt(scope: str, ru: str, focus: str, ctx: dict) -> str:
    focus_map = {
        'reliability': 'keandalan dan reliability engineering (MTBF, MTTR, bad actor, failure mode)',
        'readiness': 'kesiapan operasi (readiness record, jetty, SPM, tank)',
        'coverage': 'kualitas data (coverage penulisan kode equipment, kelengkapan laporan)',
        'risk': 'manajemen risiko (critical equipment, bad actor, program RKAP yang terlambat)',
        'general': 'analisis menyeluruh mencakup keandalan, kesiapan, kualitas data, dan risiko',
    }
    focus_desc = focus_map.get(focus, focus_map['general'])

    if scope == 'equipment':
        item = ctx.get('equipment', {})
        props = item.get('properties', {})
        related = ctx.get('related', [])
        signals = ctx.get('reliability_engineering', {})

        rel_lines = []
        for r in related[:30]:
            rel_lines.append(f"  - [{r.get('relationship_type','')}] {r.get('label','')} (kandidat: {r.get('is_candidate', False)})")

        signal_lines = []
        for k, v in (signals or {}).items():
            if v not in (None, '', 0, '0', 0.0):
                signal_lines.append(f"  {k}: {v}")
        # Pastikan MTBF/MTTR selalu muncul dengan label jelas, bukan hilang
        for mtkey, label in [('avg_mtbf', 'MTBF rata-rata (jam)'), ('avg_mttr', 'MTTR rata-rata (jam)'), ('max_running_hours', 'Running hours maks')]:
            if signals and signals.get(mtkey) not in (None, 0, 0.0):
                pass  # sudah masuk di loop atas
            elif signals and signals.get(mtkey) is None:
                signal_lines.append(f"  {label}: Belum tercatat di reliability observation yang terhubung")

        prop_lines = [f"  {k}: {v}" for k, v in props.items() if v not in (None, '', 'None', '0')][:30]

        data_block = f"""
## Data Equipment
- ID: {item.get('id','')}
- Label: {item.get('label','')}
- Refinery Unit: {item.get('refinery_unit','')}

### Properties
{chr(10).join(prop_lines) or '  (tidak ada)'}

### Relasi di Knowledge Graph ({len(related)} relasi)
{chr(10).join(rel_lines) or '  (tidak ada relasi)'}

### Sinyal Reliability Engineering
{chr(10).join(signal_lines) or '  (tidak ada sinyal)'}
"""
        scope_desc = f"satu equipment spesifik: **{item.get('label', item.get('id', ''))}** di {item.get('refinery_unit','')}"

    elif scope == 'ru':
        eq = ctx.get('equipment', {})
        top_eq = ctx.get('top_equipment', [])
        domain_counts = ctx.get('domain_counts', {})
        rel = ctx.get('reliability', {})
        rkap = ctx.get('rkap', {})
        bad = ctx.get('bad_actors', [])
        kg = ctx.get('kg', {})
        wo_ctx = ctx.get('work_order', {})
        wo_by_type = ctx.get('wo_by_type', [])
        notif_ctx = ctx.get('notification', {})
        readiness_infra = ctx.get('readiness_infra', {})
        workplan_infra = ctx.get('workplan_infra', {})

        top_eq_lines = '\n'.join(f"  {i+1}. {e.get('label','')} ({e.get('business_key','')}) — {e.get('rel_count',0)} relasi, tipe: {e.get('eq_type','')}" for i, e in enumerate(top_eq))
        domain_lines = '\n'.join(f"  - {k}: {_fmt_int(v)} baris" for k, v in domain_counts.items())

        def _infra_lines(infra: dict, label_map: dict) -> str:
            lines = []
            for k, statuses in infra.items():
                label = label_map.get(k, k)
                status_str = ', '.join(f"{s}: {c}" for s, c in statuses.items() if s and c)
                lines.append(f"  - {label}: {status_str or '(tidak ada data status)'}")
            return '\n'.join(lines)

        readiness_infra_lines = _infra_lines(readiness_infra, {
            'readiness_jetty': 'Jetty', 'readiness_spm': 'SPM', 'readiness_tank': 'Tangki'
        })
        workplan_infra_lines = _infra_lines(workplan_infra, {
            'jetty_workplan': 'Jetty RTL Workplan', 'spm_workplan': 'SPM RTL Workplan', 'tank_workplan': 'Tangki RTL Workplan'
        })
        tkdn_ctx = ctx.get('tkdn', {})
        oa_ctx = ctx.get('oa', {})
        plo_ctx = ctx.get('plo', {})
        pipeline_ctx = ctx.get('pipeline', {})
        bad_lines = '\n'.join(f"  - {b.get('fm','')}: {b.get('c','')}x" for b in bad[:8])
        multi_dom_lines = '\n'.join(
            f"  - {e.get('label','')} ({e.get('business_key','')}) → {e.get('domain_count','')} domain: {e.get('domains','')}"
            for e in kg.get('multi_domain_equipment', [])[:8]
        )
        high_risk_lines = '\n'.join(
            f"  - {e.get('label','')} ({e.get('business_key','')})"
            for e in kg.get('high_risk_cross_domain', [])[:10]
        )

        data_block = f"""
## Data Refinery Unit: {ru}
### Equipment
- Total equipment terdaftar: {_fmt_int(eq.get('total', 0))}
- Jumlah tipe equipment: {_fmt_int(eq.get('types', 0))}

### Metrik Knowledge Graph (Berbasis Traversal Graf)
- Equipment terhubung dalam graf: {_fmt_int(kg.get('connected_nodes', 0))} dari {_fmt_int(kg.get('total_equipment', 0))} ({kg.get('graph_coverage_pct', 0)}%)
- Equipment terisolasi (nol relasi di graf): {_fmt_int(kg.get('isolated_nodes', 0))} — ini adalah kesenjangan data yang tidak terlihat dari laporan biasa
- Equipment terhubung di 3+ domain laporan sekaligus (multi-hop): {len(kg.get('multi_domain_equipment', []))}
- Equipment yang punya program RKAP terlambat (ditelusuri melalui edges di graf): {_fmt_int(kg.get('delayed_rkap_equipment_count', 0))}

### Equipment dengan Konektivitas Graf Tertinggi (Degree Centrality)
{top_eq_lines or '  (tidak ada)'}

### Equipment Konvergen di Multiple Domain (Lintas Laporan — Knowledge Graph Cross-Domain)
{multi_dom_lines or '  (tidak ada — semua equipment hanya ada di ≤2 domain)'}

### Equipment Risiko Tinggi Lintas Domain (Bad Actor + Critical — Ditemukan via Graph Traversal)
{high_risk_lines or '  (tidak ada equipment yang terdeteksi di keduanya sekaligus)'}

### Volume Laporan per Domain
{domain_lines or '  (tidak ada laporan)'}

### Sinyal Reliability Observation
- Total catatan: {_fmt_int(rel.get('total', 0))}
- Masuk kategori top risk: {_fmt_int(rel.get('top_risk', 0))}
- Rata-rata MTBF: {round(float(rel.get('avg_mtbf') or 0), 1)} jam
- Rata-rata MTTR: {round(float(rel.get('avg_mttr') or 0), 1)} jam

### Program RKAP
- Total program: {_fmt_int(rkap.get('total', 0))}
- Program terlambat: {_fmt_int(rkap.get('delayed', 0))}
- Total anggaran: Rp {round(float(rkap.get('budget') or 0) / 1e9, 1)} miliar

### Bad Actor (Failure Mode Teratas)
{bad_lines or '  (tidak ada data bad actor)'}

### Work Order (SAP)
- Total work order: {_fmt_int(wo_ctx.get('total', 0))}
- Work order masih open: {_fmt_int(wo_ctx.get('open_count', 0))}
- Menunggu material/spare (WAMA/WASR): {_fmt_int(wo_ctx.get('waiting_material', 0))}
- Total biaya planned: Rp {round(float(wo_ctx.get('total_planned_cost') or 0) / 1e9, 1)} miliar
- Breakdown per tipe order: {', '.join(f"{r.get('otype','')}: {r.get('c',0)}" for r in wo_by_type) or '(tidak ada)'}

### Notifikasi (SAP)
- Total notifikasi: {_fmt_int(notif_ctx.get('total', 0))}
- Jumlah tipe notifikasi: {_fmt_int(notif_ctx.get('types', 0))}

### Kesiapan Infrastruktur (Readiness Status — Jetty / SPM / Tangki)
{readiness_infra_lines or '  (tidak ada data readiness infrastruktur untuk RU ini)'}

### RTL Action Plan Infrastruktur (Workplan — Item Belum Selesai)
{workplan_infra_lines or '  (tidak ada data workplan infrastruktur untuk RU ini)'}

### TKDN (Tingkat Komponen Dalam Negeri)
- Total rekaman TKDN: {_fmt_int(tkdn_ctx.get('total', 0))}
- Rata-rata persentase TKDN: {round(float(tkdn_ctx.get('avg_pct') or 0), 1)}%
- Total nominal TKDN: Rp {round(float(tkdn_ctx.get('total_nominal') or 0) / 1e9, 2)} miliar

### OA Availability (Overall Availability)
- Total rekaman OA: {_fmt_int(oa_ctx.get('total', 0))}
- Rata-rata value OA aktual: {round(float(oa_ctx.get('avg_value') or 0), 1)}%
- Rata-rata target OA: {round(float(oa_ctx.get('avg_target') or 0), 1)}%

### PLO Permit (Izin Operasi)
- Total izin PLO: {_fmt_int(plo_ctx.get('total', 0))}
- Izin kadaluarsa: {_fmt_int(plo_ctx.get('expired', 0))}
- Izin masih berlaku: {_fmt_int(int(plo_ctx.get('total') or 0) - int(plo_ctx.get('expired') or 0))}

### Pipeline Inspection (Pemeriksaan Pipa)
- Total rekaman inspeksi pipa: {_fmt_int(pipeline_ctx.get('total', 0))}
- Pipa dengan temporary repair: {_fmt_int(pipeline_ctx.get('has_temp_repair', 0))}
- Rata-rata remaining life: {round(float(pipeline_ctx.get('avg_rem_life') or 0), 1)} tahun
- Pipa mendekati end-of-life (rem_life < 5 tahun): {_fmt_int(pipeline_ctx.get('near_eol', 0))}
"""
        scope_desc = f"Refinery Unit **{ru}**"

    else:  # dataset
        nc = ctx.get('node_counts', [])
        rels = ctx.get('relationships', {})
        eq_ru = ctx.get('equipment_per_ru', [])
        kg = ctx.get('kg', {})
        nc_lines = '\n'.join(f"  - {r.get('node_type','')}: {_fmt_int(r.get('c',0))}" for r in nc)
        eq_ru_lines = '\n'.join(f"  - {r.get('ru','')}: {_fmt_int(r.get('c',0))} equipment" for r in eq_ru)
        top_deg_lines = '\n'.join(
            f"  {i+1}. {e.get('label','')} ({e.get('business_key','')}, {e.get('ru','')}) — {e.get('degree',0)} koneksi"
            for i, e in enumerate(kg.get('top_degree_equipment', []))
        )
        rel_type_lines = '\n'.join(
            f"  - {r.get('relationship_type','')}: {_fmt_int(r.get('c',0))} edges"
            for r in kg.get('relationship_type_distribution', [])
        )
        data_block = f"""
## Overview Dataset Knowledge Graph
### Jumlah Node per Tipe
{nc_lines}

### Metrik Graf Dataset (Knowledge Graph Analytics)
- Total relasi terverifikasi (edges): {_fmt_int(rels.get('total',0))}
- Relasi kandidat (belum terverifikasi): {_fmt_int(rels.get('candidates',0))}
- Total equipment: {_fmt_int(kg.get('total_equipment', 0))}
- Equipment terhubung di graf: {_fmt_int(kg.get('total_equipment', 0) - kg.get('isolated_nodes', 0))} ({kg.get('graph_coverage_pct', 0)}%)
- Equipment terisolasi (nol relasi): {_fmt_int(kg.get('isolated_nodes', 0))}
- Equipment terhubung di 3+ domain sekaligus (multi-domain node): {_fmt_int(kg.get('multi_domain_equipment_count', 0))}

### Distribusi Edge per Tipe Relasi (Domain Coverage di Graf)
{rel_type_lines or '  (tidak ada)'}

### Equipment dengan Degree Centrality Tertinggi di Seluruh Dataset
(Equipment yang menjadi hub/pusat dalam network knowledge graph kilang)
{top_deg_lines or '  (tidak ada)'}

### Distribusi Equipment per Refinery Unit
{eq_ru_lines}
"""
        scope_desc = "seluruh dataset knowledge graph kilang"

    return f"""Kamu adalah seorang ahli Asset Integrity & Reliability Engineering untuk kilang minyak dengan pengalaman 20 tahun. Kamu menganalisis data dari sistem knowledge graph kilang (KGRRE) — sebuah jaringan relasi antar entitas operasional yang menghubungkan equipment, laporan reliability, program RKAP, bad actor, readiness, monitoring, dan domain lainnya melalui edges (relasi terverifikasi) dan multi-hop traversal.

PENTING: Data di bawah ini BUKAN sekadar agregasi SQL flat. Data ini adalah hasil analisis struktur jaringan (graph analytics), termasuk:
- **Degree centrality**: equipment yang menjadi hub/pusat jaringan (banyak relasi)
- **Multi-domain convergence**: equipment yang muncul sekaligus di 3+ laporan berbeda — tidak bisa dilihat dari tiap laporan secara terpisah
- **Isolated node detection**: equipment yang tidak terhubung ke laporan apapun — gap yang tersembunyi
- **Cross-domain risk**: equipment yang terdeteksi di bad actor DAN critical equipment secara bersamaan melalui traversal edges
- **Graph coverage**: berapa persen equipment yang benar-benar terpantau dalam jaringan

Gunakan wawasan graf ini sebagai keunggulan analisis dibanding laporan konvensional.

## Permintaan Analisis
Lakukan {focus_desc} untuk {scope_desc}.
Berikan analisis narasi mendalam dalam Bahasa Indonesia yang dapat langsung digunakan oleh manajemen kilang.

{data_block}

## Instruksi Output
Tulis analisis mendalam dengan struktur berikut:

### 1. Ringkasan Eksekutif
Paragraf singkat (3-4 kalimat) kondisi keseluruhan berdasarkan data, sebutkan temuan kritis dari perspektif jaringan knowledge graph (degree centrality, isolated nodes, konvergensi multi-domain).

### 2. Temuan Utama dari Knowledge Graph
Poin-poin temuan paling signifikan (minimal 4, maksimal 8). Prioritaskan temuan yang hanya bisa dilihat dari struktur jaringan (bukan laporan individual): equipment dengan konektivitas tinggi, gap data (isolated nodes), konvergensi risiko lintas domain. Setiap poin harus spesifik dengan angka dan implikasinya.

### 3. Analisis Mendalam: Pola Jaringan & Konvergensi Risiko
Narasi analitik 3-5 paragraf yang menjelaskan: (a) pola konektivitas di jaringan, (b) equipment mana yang muncul di persimpangan banyak domain — dan apa artinya bagi operasional, (c) kesenjangan data yang terlihat dari isolated nodes, (d) keterkaitan antar domain yang tidak tampak dari laporan terpisah.

### 4. Risiko Prioritas
Daftar 3-5 risiko utama, dengan penekanan pada risiko yang teridentifikasi melalui konvergensi multi-domain atau gap dalam jaringan knowledge graph.

### 5. Rekomendasi Aksi
Rekomendasi konkret dan terurut prioritas (minimal 5). Sertakan timeframe (jangka pendek <3 bulan, menengah 3-12 bulan, panjang >12 bulan). Hubungkan rekomendasi dengan temuan spesifik dari jaringan.

### 6. Penutup
Kalimat penutup yang menyatakan tingkat kepercayaan analisis berdasarkan kelengkapan data dan cakupan jaringan yang tersedia.

Gunakan bahasa teknis yang tepat namun dapat dipahami manajemen. Jangan menyebutkan nama kolom database, nama field teknis sistem, atau istilah pemrograman. Fokus pada makna operasional dari setiap angka dan pola jaringan."""


@app.post("/api/datasets/{dataset_id}/analysis/generate")
def analysis_generate(dataset_id: str, req: AnalysisGenerateRequest):
    """Generate analisis AI berdasarkan data knowledge graph — per dataset, RU, atau equipment."""
    get_dataset(dataset_id)
    connection = db_for(dataset_id)
    try:
        connection.execute("SET LOCAL statement_timeout = '30s'")
        if req.scope == 'equipment' and req.equipment_id:
            ctx = _readiness_context_for_node(connection, req.equipment_id)
        elif req.scope == 'ru' and req.ru:
            ctx = _gather_ru_ctx(connection, req.ru)
        else:
            ctx = _gather_dataset_ctx(connection)
    finally:
        connection.close()

    prompt = _build_analysis_prompt(req.scope, req.ru, req.focus, ctx)
    return StreamingResponse(
        _analysis_ai_stream(prompt),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


class SaveAnalysisRequest(BaseModel):
    scope: str
    focus: str
    ru: str = ""
    equipment_id: str = ""
    title: str
    content: str

@app.post("/api/datasets/{dataset_id}/analysis/saved")
def save_analysis(dataset_id: str, req: SaveAnalysisRequest):
    get_dataset(dataset_id)
    connection = db_for(dataset_id)
    try:
        result = fetch_tuple(connection, """
            INSERT INTO ai_analysis (scope, focus, ru, equipment_id, title, content)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id, created_at
        """, [req.scope, req.focus, req.ru, req.equipment_id, req.title, req.content])
        return {"id": result[0], "created_at": result[1].isoformat() if result[1] else None}
    finally:
        connection.close()

@app.get("/api/datasets/{dataset_id}/analysis/saved")
def list_saved_analyses(dataset_id: str):
    get_dataset(dataset_id)
    connection = db_for(dataset_id)
    try:
        result = rows(connection, """
            SELECT id, scope, focus, ru, equipment_id, title, created_at
            FROM ai_analysis ORDER BY created_at DESC LIMIT 50
        """)
        return [dict(r, created_at=r['created_at'].isoformat() if r.get('created_at') else None) for r in result]
    finally:
        connection.close()

@app.get("/api/datasets/{dataset_id}/analysis/saved/{analysis_id}")
def get_saved_analysis(dataset_id: str, analysis_id: int):
    get_dataset(dataset_id)
    connection = db_for(dataset_id)
    try:
        result = rows(connection, "SELECT * FROM ai_analysis WHERE id = %s", [analysis_id])
        if not result:
            raise HTTPException(status_code=404, detail="Analisis tidak ditemukan.")
        r = result[0]
        return dict(r, created_at=r['created_at'].isoformat() if r.get('created_at') else None)
    finally:
        connection.close()

@app.delete("/api/datasets/{dataset_id}/analysis/saved/{analysis_id}")
def delete_saved_analysis(dataset_id: str, analysis_id: int):
    get_dataset(dataset_id)
    connection = db_for(dataset_id)
    try:
        connection.execute("DELETE FROM ai_analysis WHERE id = %s", [analysis_id])
        return {"ok": True}
    finally:
        connection.close()


class ChatRequest(BaseModel):
    question: str
    history: list = []   # [{role, content}]


def _props_to_text(props: dict) -> str:
    """Ubah properties_json dict menjadi teks ringkas untuk konteks AI."""
    skip = {'node_id', 'dataset_id'}
    parts = []
    for k, v in props.items():
        if k in skip or v is None or str(v).strip() in ('', '0', 'None'):
            continue
        parts.append(f"{k}: {v}")
    return ', '.join(parts[:30])  # max 30 field agar tidak terlalu panjang


def _entity_lookup(connection, question: str) -> str:
    """Cari entitas spesifik (equipment/node) yang disebut di pertanyaan dan ambil detail propertinya."""
    import re
    lines = []

    # Token kandidat: kata 2+ karakter, angka-huruf (kode equipment), atau frasa dalam kutip
    tokens = re.findall(r'"([^"]+)"|\'([^\']+)\'|([A-Z0-9][A-Z0-9\-\/]{2,})', question.upper())
    # Juga ekstrak kata bermakna panjang (bukan stopword umum)
    stopwords = {'APA', 'YANG', 'DAN', 'DARI', 'UNTUK', 'PADA', 'DENGAN', 'DI', 'INI',
                 'ITU', 'ADALAH', 'BERAPA', 'BAGAIMANA', 'DIMANA', 'KAPAN', 'SIAPA',
                 'STATUS', 'DATA', 'DETAIL', 'INFO', 'LIST', 'SEMUA', 'TOTAL', 'JUMLAH'}
    candidates = set()
    for t in tokens:
        word = (t[0] or t[1] or t[2]).strip()
        if word and word not in stopwords and len(word) >= 3:
            candidates.add(word)

    # Juga cari kata-kata panjang dari pertanyaan asli (nama equipment biasanya unik)
    for word in re.findall(r'\b\w{4,}\b', question):
        upper = word.upper()
        if upper not in stopwords and not upper.startswith('PPMS') and len(upper) >= 4:
            candidates.add(upper)

    if not candidates:
        return ''

    found_nodes = []
    for candidate in list(candidates)[:8]:  # max 8 token dicari
        try:
            hits = rows(connection, """
                SELECT node_id, node_type, label, business_key, properties_json
                FROM kg_node
                WHERE lower(label) LIKE lower(%s)
                   OR lower(business_key) LIKE lower(%s)
                   OR lower(node_id) LIKE lower(%s)
                LIMIT 3
            """, [f'%{candidate}%', f'%{candidate}%', f'%{candidate}%'])
            found_nodes.extend(hits)
        except Exception:
            pass

    if not found_nodes:
        return ''

    # Deduplikasi by node_id
    seen = set()
    unique_nodes = []
    for n in found_nodes:
        if n['node_id'] not in seen:
            seen.add(n['node_id'])
            unique_nodes.append(n)

    lines.append(f"\n=== ENTITAS SPESIFIK YANG DITEMUKAN ({len(unique_nodes)} node) ===")
    for n in unique_nodes[:6]:
        props = n.get('properties_json') or {}
        lines.append(f"\n[{n['node_type']}] {n['label']} (ID: {n['business_key']})")
        props_text = _props_to_text(props)
        if props_text:
            lines.append(f"  Properties: {props_text}")
        # Ambil relasi langsung node ini
        try:
            rels = rows(connection, """
                SELECT r.relationship_type, t.label, t.node_type, t.business_key,
                       t.properties_json->>'status' AS status,
                       t.properties_json->>'derived_is_open_order' AS open_order,
                       t.properties_json->>'derived_status_bucket' AS status_bucket,
                       t.properties_json->>'mtbf' AS mtbf,
                       t.properties_json->>'failure_mode' AS failure_mode,
                       t.properties_json->>'status_optimasi' AS status_optimasi,
                       t.properties_json->>'type_pekerjaan' AS type_pekerjaan
                FROM kg_relationship r
                JOIN kg_node t ON t.node_id = r.target_node_id
                WHERE r.source_node_id = %s AND NOT r.is_candidate
                LIMIT 20
            """, [n['node_id']])
            if rels:
                lines.append(f"  Relasi langsung ({len(rels)}):")
                for r_ in rels:
                    detail_parts = []
                    for fld in ('status', 'open_order', 'status_bucket', 'mtbf', 'failure_mode', 'status_optimasi', 'type_pekerjaan'):
                        val = r_.get(fld)
                        if val and str(val).strip() not in ('', 'None', 'false'):
                            detail_parts.append(f"{fld}={val}")
                    detail = f" [{', '.join(detail_parts)}]" if detail_parts else ''
                    lines.append(f"    - {r_['relationship_type']} → {r_['node_type']}: {r_['label']} ({r_['business_key']}){detail}")
        except Exception:
            pass

    return '\n'.join(lines)


def _gather_chat_context(connection, question: str) -> str:
    """Kumpulkan fakta relevan dari KG berdasarkan kata kunci di pertanyaan."""
    import re
    q_lower = question.lower()

    # Deteksi RU dari pertanyaan
    ru_pattern = re.compile(r'\bru\s*(i{1,4}|iv|vi{0,3}|[1-9])\b', re.IGNORECASE)
    ru_matches = ru_pattern.findall(q_lower)
    ru_filter = None
    if ru_matches:
        raw = ru_matches[0].upper().strip()
        ru_filter = f'%{raw}%'

    # Deteksi node type dari pertanyaan
    type_keywords = {
        'equipment': ['equipment', 'alat', 'mesin', 'pump', 'compressor', 'vessel', 'heat exchanger', 'turbin'],
        'reliability_observation': ['reliability', 'mtbf', 'mttr', 'bad actor', 'failure', 'kegagalan', 'keandalan'],
        'maintenance_order': ['work order', 'wo ', 'maintenance order', 'perawatan', 'perbaikan', 'order'],
        'notification': ['notification', 'notifikasi', 'laporan kerusakan'],
        'rkap_program': ['rkap', 'program', 'anggaran', 'budget', 'biaya'],
        'inspection_plan': ['inspection plan', 'rencana inspeksi', 'inspeksi'],
        'ppms': ['ppms', 'predictive', 'prediktif', 'kondisi', 'model'],
        'bad_actor': ['bad actor', 'paling sering rusak', 'breakdown', 'failure mode'],
        'critical_equipment': ['critical', 'kritis', 'prioritas'],
        'readiness_record': ['readiness', 'kesiapan', 'siap operasi'],
    }
    focus_types = [ntype for ntype, kws in type_keywords.items() if any(kw in q_lower for kw in kws)]
    if not focus_types:
        focus_types = ['equipment', 'reliability_observation', 'maintenance_order']

    lines = []

    # Statistik umum dataset
    try:
        total_eq = fetch_tuple(connection, "SELECT count(*) FROM kg_node WHERE node_type='equipment'")[0]
        total_rel = fetch_tuple(connection, "SELECT count(*) FROM kg_relationship WHERE NOT is_candidate")[0]
        lines.append(f"Dataset: {total_eq} equipment, {total_rel} relasi terverifikasi.")
    except Exception:
        pass

    # Statistik per RU jika ada filter
    if ru_filter:
        try:
            eq_ru = fetch_tuple(connection,
                "SELECT count(*) FROM kg_node WHERE node_type='equipment' AND properties_json->>'refinery_unit' ILIKE %s",
                [ru_filter])[0]
            lines.append(f"Jumlah equipment di RU '{ru_matches[0].upper()}': {eq_ru}.")
        except Exception:
            pass

    # Data per domain yang relevan
    for ntype in focus_types[:4]:
        try:
            params = [ntype]
            ru_clause = ""
            if ru_filter:
                ru_clause = " AND properties_json->>'refinery_unit' ILIKE %s"
                params.append(ru_filter)
            cnt = fetch_tuple(connection,
                f"SELECT count(*) FROM kg_node WHERE node_type=%s{ru_clause}", params)[0]
            if cnt == 0:
                continue
            lines.append(f"Jumlah node '{ntype}'{' di RU ini' if ru_filter else ''}: {cnt}.")

            if ntype == 'reliability_observation':
                agg = rows(connection,
                    f"""SELECT
                        sum(CASE WHEN (properties_json->>'derived_is_top_risk')='true' THEN 1 ELSE 0 END) AS top_risk,
                        avg(CASE WHEN properties_json->>'mtbf' ~ '^[0-9]' THEN (properties_json->>'mtbf')::float END) AS avg_mtbf,
                        avg(CASE WHEN properties_json->>'mttr' ~ '^[0-9]' THEN (properties_json->>'mttr')::float END) AS avg_mttr
                    FROM kg_node WHERE node_type='reliability_observation'{ru_clause}""", params[1:] if ru_filter else [])
                if agg:
                    a = agg[0]
                    lines.append(f"  Top risk: {a.get('top_risk') or 0}, Avg MTBF: {round(float(a['avg_mtbf'] or 0),1)}h, Avg MTTR: {round(float(a['avg_mttr'] or 0),1)}h.")
                top_ba = rows(connection,
                    f"""SELECT label, properties_json->>'failure_mode' AS fm, properties_json->>'mtbf' AS mtbf
                    FROM kg_node WHERE node_type='reliability_observation'{ru_clause}
                    AND (properties_json->>'derived_is_top_risk')='true'
                    ORDER BY (CASE WHEN properties_json->>'mtbf' ~ '^[0-9]' THEN (properties_json->>'mtbf')::float END) ASC NULLS LAST
                    LIMIT 5""", params[1:] if ru_filter else [])
                if top_ba:
                    lines.append("  Top bad actor (MTBF terendah):")
                    for r_ in top_ba:
                        lines.append(f"    - {r_.get('label','?')} | failure: {r_.get('fm','?')} | MTBF: {r_.get('mtbf','?')}h")

            elif ntype in ('maintenance_order', 'work_order'):
                agg = rows(connection,
                    f"""SELECT
                        sum(CASE WHEN (properties_json->>'derived_is_open_order')='true' THEN 1 ELSE 0 END) AS open_wos,
                        sum(CASE WHEN properties_json->>'derived_status_bucket' IN ('WAMA','WASR') THEN 1 ELSE 0 END) AS waiting_mat
                    FROM kg_node WHERE node_type=%s{ru_clause}""", params)
                if agg:
                    a = agg[0]
                    lines.append(f"  Open WO: {a.get('open_wos') or 0}, Menunggu material: {a.get('waiting_mat') or 0}.")

            elif ntype == 'rkap_program':
                agg = rows(connection,
                    f"""SELECT
                        sum(CASE WHEN (properties_json->>'derived_is_delayed')='true' THEN 1 ELSE 0 END) AS delayed,
                        sum(CASE WHEN properties_json->>'derived_planned_cost' ~ '^[0-9]'
                            THEN (properties_json->>'derived_planned_cost')::float ELSE 0 END) AS total_budget
                    FROM kg_node WHERE node_type='rkap_program'{ru_clause}""", params[1:] if ru_filter else [])
                if agg:
                    a = agg[0]
                    lines.append(f"  Program terlambat: {a.get('delayed') or 0}, Total budget: IDR {int(a.get('total_budget') or 0):,}.")

            elif ntype == 'bad_actor':
                top = rows(connection,
                    f"""SELECT properties_json->>'failure_mode' AS fm, count(*) AS c
                    FROM kg_node WHERE node_type='bad_actor'{ru_clause}
                    GROUP BY fm ORDER BY c DESC LIMIT 5""", params[1:] if ru_filter else [])
                if top:
                    lines.append("  Top failure mode bad actor:")
                    for r_ in top:
                        lines.append(f"    - {r_.get('fm','?')}: {r_.get('c','?')} kasus")

            elif ntype == 'inspection_plan':
                top = rows(connection,
                    f"""SELECT properties_json->>'type_pekerjaan' AS tp, count(*) AS c
                    FROM kg_node WHERE node_type='inspection_plan'{ru_clause}
                    GROUP BY tp ORDER BY c DESC LIMIT 5""", params[1:] if ru_filter else [])
                if top:
                    lines.append("  Top type pekerjaan inspeksi:")
                    for r_ in top:
                        lines.append(f"    - {r_.get('tp','?')}: {r_.get('c','?')} rencana")

            elif ntype == 'ppms':
                agg = rows(connection,
                    f"""SELECT properties_json->>'status_optimasi' AS st, count(*) AS c
                    FROM kg_node WHERE node_type='ppms'{ru_clause}
                    GROUP BY st ORDER BY c DESC LIMIT 5""", params[1:] if ru_filter else [])
                if agg:
                    lines.append("  Status optimasi PPMS:")
                    for r_ in agg:
                        lines.append(f"    - {r_.get('st','?')}: {r_.get('c','?')}")

        except Exception:
            pass

    # Equipment dengan paling banyak relasi (paling sering muncul di graph)
    try:
        ru_clause = " AND n.properties_json->>'refinery_unit' ILIKE %s" if ru_filter else ""
        params_eq = [ru_filter] if ru_filter else []
        top_eq = rows(connection,
            f"""SELECT n.label, n.business_key, count(r.relationship_id) AS deg
            FROM kg_node n
            JOIN kg_relationship r ON r.source_node_id = n.node_id AND NOT r.is_candidate
            WHERE n.node_type='equipment'{ru_clause}
            GROUP BY n.node_id, n.label, n.business_key
            ORDER BY deg DESC LIMIT 5""", params_eq)
        if top_eq:
            lines.append("Top equipment berdasarkan jumlah koneksi di graph:")
            for r_ in top_eq:
                lines.append(f"  - {r_.get('label','?')} ({r_.get('business_key','?')}): {r_.get('deg','?')} relasi")
    except Exception:
        pass

    return '\n'.join(lines) if lines else "Tidak ada data konteks yang berhasil diambil."


@app.post("/api/datasets/{dataset_id}/chat")
def dataset_chat(dataset_id: str, req: ChatRequest):
    get_dataset(dataset_id)
    import os, requests as _req
    api_key = os.environ.get("DINOIKI_API_KEY", "")
    if not api_key:
        raise HTTPException(status_code=503, detail="DINOIKI_API_KEY belum dikonfigurasi di server.")

    connection = db_for(dataset_id)
    try:
        connection.execute("SET LOCAL statement_timeout = '15s'")
        ctx = _gather_chat_context(connection, req.question)
        entity_ctx = _entity_lookup(connection, req.question)
        if entity_ctx:
            ctx = ctx + '\n' + entity_ctx
    except Exception:
        ctx = "Konteks tidak berhasil diambil."
    finally:
        connection.close()

    system_msg = (
        "Kamu adalah asisten AI untuk sistem knowledge graph kilang minyak (KGRRE). "
        "Jawab pertanyaan user berdasarkan data faktual dari knowledge graph berikut. "
        "Jika data tidak mencukupi untuk menjawab secara pasti, katakan demikian. "
        "Gunakan Bahasa Indonesia. Jangan mengarang data yang tidak ada di konteks.\n\n"
        f"=== DATA KNOWLEDGE GRAPH ===\n{ctx}\n==========================="
    )

    messages = [{"role": "system", "content": system_msg}]
    for h in (req.history or [])[-10:]:
        if h.get("role") in ("user", "assistant") and h.get("content"):
            messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": req.question})

    def _stream():
        try:
            resp = _req.post(
                _DINOIKI_URL,
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
                json={"model": "gpt-4o", "messages": messages, "max_tokens": 2048, "temperature": 0.3, "stream": True},
                stream=True, timeout=120,
            )
            if not resp.ok:
                yield f"data: {json.dumps({'error': f'HTTP Error {resp.status_code}: {resp.reason}'})}\n\n"
                yield "data: [DONE]\n\n"
                return
            for raw in resp.iter_lines():
                if not raw:
                    continue
                line = raw.decode("utf-8") if isinstance(raw, bytes) else raw
                if not line.startswith("data:"):
                    continue
                ps = line[5:].strip()
                if ps == "[DONE]":
                    break
                try:
                    obj = json.loads(ps)
                    text = obj.get("choices", [{}])[0].get("delta", {}).get("content") or ""
                    if text:
                        yield f"data: {json.dumps({'text': text})}\n\n"
                except Exception:
                    pass
        except Exception as exc:
            yield f"data: {json.dumps({'error': str(exc)})}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


class NodeChatRequest(BaseModel):
    node_id: str
    question: str
    history: list = []


def _build_node_context(connection, node_id: str) -> str:
    """Bangun konteks lengkap untuk satu node: properties + semua relasi langsung + domain record."""
    lines = []

    # Data node utama
    node = rows(connection, "SELECT * FROM kg_node WHERE node_id = %s", [node_id])
    if not node:
        return "Node tidak ditemukan."
    n = node[0]
    props = n.get('properties_json') or {}
    lines.append(f"=== NODE UTAMA ===")
    lines.append(f"ID: {n['node_id']}")
    lines.append(f"Tipe: {n['node_type']}")
    lines.append(f"Label: {n['label']}")
    lines.append(f"Business Key: {n['business_key']}")
    if props:
        lines.append("Properties:")
        for k, v in props.items():
            if v is not None and str(v).strip() not in ('', '0', 'None', 'false'):
                lines.append(f"  {k}: {v}")

    # Domain record tambahan (jika ada)
    try:
        dr = rows(connection, "SELECT * FROM domain_record WHERE node_id = %s LIMIT 1", [node_id])
        if dr:
            domain_props = {k: v for k, v in dict(dr[0]).items()
                           if k not in ('node_id', 'dataset_id', 'id') and v is not None and str(v).strip() not in ('', 'None')}
            if domain_props:
                lines.append("Domain record tambahan:")
                for k, v in domain_props.items():
                    lines.append(f"  {k}: {v}")
    except Exception:
        pass

    # Semua relasi keluar (source = node ini)
    try:
        out_rels = rows(connection, """
            SELECT r.relationship_type, r.relationship_id,
                   t.node_id AS target_id, t.node_type AS target_type,
                   t.label AS target_label, t.business_key AS target_key,
                   t.properties_json AS target_props
            FROM kg_relationship r
            JOIN kg_node t ON t.node_id = r.target_node_id
            WHERE r.source_node_id = %s AND NOT r.is_candidate
            LIMIT 50
        """, [node_id])
        if out_rels:
            lines.append(f"\n=== RELASI KELUAR ({len(out_rels)}) ===")
            for r_ in out_rels:
                tp = r_.get('target_props') or {}
                # Ambil field paling informatif
                detail_fields = ['status', 'derived_is_open_order', 'derived_status_bucket',
                                 'mtbf', 'mttr', 'failure_mode', 'status_optimasi', 'model_name',
                                 'type_pekerjaan', 'due_year', 'plan_year', 'order_type',
                                 'derived_planned_cost', 'derived_is_delayed', 'derived_is_top_risk',
                                 'notification_type', 'description', 'resume_kondisi']
                detail_parts = []
                for fld in detail_fields:
                    val = tp.get(fld)
                    if val and str(val).strip() not in ('', 'None', 'false', '0'):
                        label = fld.replace('derived_', '').replace('_', ' ')
                        detail_parts.append(f"{label}={val}")
                detail = f" [{', '.join(detail_parts[:6])}]" if detail_parts else ''
                lines.append(f"  → {r_['relationship_type']} | {r_['target_type']}: {r_['target_label']} ({r_['target_key']}){detail}")
    except Exception:
        pass

    # Relasi masuk (target = node ini) — siapa yang referensikan node ini
    try:
        in_rels = rows(connection, """
            SELECT r.relationship_type, s.node_type AS src_type,
                   s.label AS src_label, s.business_key AS src_key
            FROM kg_relationship r
            JOIN kg_node s ON s.node_id = r.source_node_id
            WHERE r.target_node_id = %s AND NOT r.is_candidate
            LIMIT 20
        """, [node_id])
        if in_rels:
            lines.append(f"\n=== DIREFERENSIKAN OLEH ({len(in_rels)}) ===")
            for r_ in in_rels:
                lines.append(f"  ← {r_['relationship_type']} | {r_['src_type']}: {r_['src_label']} ({r_['src_key']})")
    except Exception:
        pass

    return '\n'.join(lines)


@app.post("/api/datasets/{dataset_id}/node-chat")
def node_chat(dataset_id: str, req: NodeChatRequest):
    get_dataset(dataset_id)
    import os, requests as _req
    api_key = os.environ.get("DINOIKI_API_KEY", "")
    if not api_key:
        raise HTTPException(status_code=503, detail="DINOIKI_API_KEY belum dikonfigurasi di server.")

    connection = db_for(dataset_id)
    try:
        connection.execute("SET LOCAL statement_timeout = '15s'")
        node_ctx = _build_node_context(connection, req.node_id)
    except Exception as exc:
        node_ctx = f"Gagal memuat data node: {exc}"
    finally:
        connection.close()

    system_msg = (
        "Kamu adalah asisten AI untuk sistem knowledge graph kilang minyak (KGRRE). "
        "User sedang melihat detail sebuah node/entitas dan bertanya tentangnya. "
        "Jawab berdasarkan data faktual berikut saja. Jangan mengarang. "
        "Gunakan Bahasa Indonesia. Jika data tidak ada, katakan 'tidak tercatat'.\n\n"
        f"{node_ctx}"
    )

    messages = [{"role": "system", "content": system_msg}]
    for h in (req.history or [])[-10:]:
        if h.get("role") in ("user", "assistant") and h.get("content"):
            messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": req.question})

    def _stream():
        try:
            resp = _req.post(
                _DINOIKI_URL,
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
                json={"model": "gpt-4o", "messages": messages, "max_tokens": 2048, "temperature": 0.3, "stream": True},
                stream=True, timeout=120,
            )
            if not resp.ok:
                yield f"data: {json.dumps({'error': f'HTTP Error {resp.status_code}: {resp.reason}'})}\n\n"
                yield "data: [DONE]\n\n"
                return
            for raw in resp.iter_lines():
                if not raw:
                    continue
                line = raw.decode("utf-8") if isinstance(raw, bytes) else raw
                if not line.startswith("data:"):
                    continue
                ps = line[5:].strip()
                if ps == "[DONE]":
                    break
                try:
                    obj = json.loads(ps)
                    text = obj.get("choices", [{}])[0].get("delta", {}).get("content") or ""
                    if text:
                        yield f"data: {json.dumps({'text': text})}\n\n"
                except Exception:
                    pass
        except Exception as exc:
            yield f"data: {json.dumps({'error': str(exc)})}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


DIST = Path(__file__).resolve().parents[1] / "dist"
if DIST.exists():
    app.mount("/", StaticFiles(directory=DIST, html=True), name="artifact")
