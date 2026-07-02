from __future__ import annotations

import json
import shutil
import threading
import time
import uuid
import zipfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import csv as csvmod

from .config import UPLOADS_DIR, insert_dataset
from .database import enable_rls, fetch_tuple, finalize, initialize, scoped
from .scanner import REQUIRED_FILES, package_file_type, scan_folder, scan_package, sha256

ANALYSIS_FILES = {
    "refinery_units.csv": "refinery_units",
    "ru_equipment_summary.csv": "ru_equipment_summary",
    "ru_data_coverage.csv": "ru_data_coverage",
    "ru_relationship_quality.csv": "ru_relationship_quality",
    "graph_schema.csv": "graph_schema",
    "ontology_depth.csv": "ontology_depth",
    "deepest_paths.csv": "deepest_paths",
    "output_manifest.csv": "output_manifest",
}

AUDIT_FILES = {
    "unmatched_identifier.csv": "unmatched_identifier",
    "ambiguous_match.csv": "ambiguous_match",
    "invalid_value.csv": "invalid_value",
}


@dataclass
class ImportJob:
    id: str
    name: str
    status: str = "queued"
    phase: str = "Menunggu"
    progress: int = 0
    message: str = ""
    dataset_id: str | None = None
    error: str | None = None
    started_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    cancelled: bool = False

    def public(self) -> dict:
        return {
            "id": self.id, "name": self.name, "status": self.status,
            "phase": self.phase, "progress": self.progress, "message": self.message,
            "dataset_id": self.dataset_id, "error": self.error,
            "started_at": self.started_at, "finished_at": self.finished_at,
        }


JOBS: dict[str, ImportJob] = {}
JOBS_LOCK = threading.Lock()


def start_import(name: str, allow_partial: bool = True) -> ImportJob:
    scan = scan_folder(validate_sheets=True)
    files = _select_ready_files(scan, allow_partial)
    job = _create_job(name)
    threading.Thread(target=_run_import, args=(job, files, Path(scan["folder"]), False), daemon=True).start()
    return job


def start_chunked_import(name: str, folder: Path, allow_partial: bool = True) -> ImportJob:
    """Import dari folder yang sudah berisi file hasil chunked upload."""
    scan = scan_package(folder, validate=True)
    files = _select_ready_files(scan, allow_partial)
    job = _create_job(name)
    threading.Thread(target=_run_import, args=(job, files, folder, True), daemon=True).start()
    return job


def start_zip_import(name: str, zip_path: Path, allow_partial: bool = True) -> ImportJob:
    extract_dir = UPLOADS_DIR / uuid.uuid4().hex
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as archive:
        for member in archive.infolist():
            member_path = Path(member.filename)
            if member.is_dir() or member_path.is_absolute() or ".." in member_path.parts:
                continue
            if len(member_path.parts) > 2:
                continue
            if len(member_path.parts) == 2 and member_path.parts[0] not in {"input_metadata", "analysis_ready"}:
                continue
            archive.extract(member, extract_dir)
    zip_path.unlink(missing_ok=True)
    scan = scan_package(extract_dir, validate=True)
    files = _select_ready_files(scan, allow_partial)
    job = _create_job(name)
    threading.Thread(target=_run_import, args=(job, files, extract_dir, True), daemon=True).start()
    return job


def cancel_job(job_id: str) -> None:
    if job_id in JOBS:
        JOBS[job_id].cancelled = True


def _create_job(name: str) -> ImportJob:
    job = ImportJob(id=uuid.uuid4().hex, name=name.strip() or "Knowledge Graph ETL Dataset")
    with JOBS_LOCK:
        JOBS[job.id] = job
    return job


def _select_ready_files(scan: dict, allow_partial: bool) -> list[dict]:
    missing_required = [
        name for name in REQUIRED_FILES
        if not any(Path(item["name"]).name == name and item["status"] in {"Ready", "Changed", "Already imported"} for item in scan["files"])
    ]
    if missing_required:
        raise ValueError(f"File wajib belum tersedia: {', '.join(missing_required)}")
    invalid = [item["name"] for item in scan["files"] if item["path"] and item["status"] == "Invalid"]
    if invalid and not allow_partial:
        raise ValueError(f"File invalid: {', '.join(invalid)}")
    changed_or_new = [item for item in scan["files"] if item["path"] and item["status"] in {"Ready", "Changed"}]
    selected = [item for item in scan["files"] if item["path"] and item["status"] in {"Ready", "Changed", "Already imported"}]
    if not changed_or_new:
        raise ValueError("Tidak ada file CSV/JSON baru atau berubah yang siap diimpor.")
    return selected


def _run_import(job: ImportJob, files: list[dict], package_root: Path, uploaded_package: bool) -> None:
    dataset_id = uuid.uuid4().hex
    try:
        job.status = "running"
        job.phase = "Fingerprint"
        fingerprints = _fingerprint_files(job, files)
        job.progress = 10

        # autocommit=True: tiap langkah ingest langsung commit agar server tidak
        # menahan satu transaksi raksasa (penyebab koneksi diputus saat file ~1 GB).
        with scoped(dataset_id, autocommit=True) as connection:
            initialize(connection)
            enable_rls(connection)
            _register_source_files(connection, fingerprints)

            file_by_name = {Path(item["name"]).name: item for item in fingerprints}
            job.phase = "Node master"
            _ingest_nodes(connection, Path(file_by_name["nodes.csv"]["path"]), job)
            job.progress = 30

            job.phase = "Verified edges"
            _ingest_relationships(connection, Path(file_by_name["relationships.csv"]["path"]), job, candidates=False)
            job.progress = 52

            if "relationship_candidates.csv" in file_by_name:
                job.phase = "Candidate edges"
                _ingest_relationships(connection, Path(file_by_name["relationship_candidates.csv"]["path"]), job, candidates=True)
            job.progress = 60

            job.phase = "Analysis & audit"
            _ingest_supporting_files(connection, fingerprints, job)
            job.progress = 76

            job.phase = "Validation"
            _derive_identifiers(connection)
            _validate_graph(connection)
            finalize(connection)
            counts = fetch_tuple(
                connection,
                "SELECT (SELECT count(*) FROM kg_node), "
                "(SELECT count(*) FROM kg_relationship), "
                "(SELECT count(*) FROM import_issue)"
            )

            insert_dataset({
                "id": dataset_id,
                "name": job.name,
                "mode": "etl_csv_graph",
                "node_count": counts[0],
                "edge_count": counts[1],
                "issue_count": counts[2],
                "workbooks": [item["name"] for item in fingerprints],
                "uploaded_package": uploaded_package,
            })

        job.dataset_id = dataset_id
        job.progress = 100
        job.phase = "Selesai"
        job.message = f"{counts[0]:,} node dan {counts[1]:,} relationship tersedia."
        job.status = "completed"
    except Exception as exc:
        _drop_dataset_data(dataset_id)
        job.status = "cancelled" if job.cancelled else "failed"
        job.error = str(exc)
        job.message = str(exc)
    finally:
        job.finished_at = time.time()


def _drop_dataset_data(dataset_id: str) -> None:
    """Bersihkan partial import yang gagal (RLS membatasi DELETE ke dataset ini)."""
    try:
        with scoped(dataset_id) as conn:
            for table in (
                "kg_node", "kg_relationship", "domain_record", "kg_identifier",
                "import_issue", "load_summary", "graph_analysis", "source_file",
            ):
                conn.execute(f"DELETE FROM {table}")
    except Exception:
        pass


def _fingerprint_files(job: ImportJob, files: list[dict]) -> list[dict]:
    fingerprints = []
    total = len(files) or 1
    for index, item in enumerate(files):
        _check_cancel(job)
        path = Path(item["path"])
        job.message = f"Memeriksa {item['name']}"
        fingerprints.append({**item, "sha256": sha256(path, lambda: job.cancelled)})
        job.progress = 2 + round((index + 1) / total * 8)
    return fingerprints


def _register_source_files(connection, fingerprints: list[dict]) -> None:
    connection.cursor().executemany(
        "INSERT INTO source_file (file_name, file_type, path, size, mtime_ns, sha256, row_count) VALUES (%s, %s, %s, %s, %s, %s, NULL)",
        [(item["name"], item.get("file_type") or item.get("workbook_type"), item["path"], item["size"], item["mtime_ns"], item["sha256"]) for item in fingerprints],
    )


def _bigint(expr: str) -> str:
    """Safe text->bigint cast (NULL on failure), setara try_cast DuckDB."""
    return f"CASE WHEN {expr} ~ '^-?[0-9]+$' THEN ({expr})::bigint END"


def _double(expr: str) -> str:
    """Safe text->double cast (NULL on failure)."""
    return f"CASE WHEN {expr} ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN ({expr})::double precision END"


def _jsonb(expr: str, default: str = "'{}'") -> str:
    """Cast text JSON ke jsonb; fallback ke default bila NULL/kosong."""
    return f"CASE WHEN {expr} IS NULL OR {expr} = '' THEN {default}::jsonb ELSE ({expr})::jsonb END"


def _py_bigint(v):
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _py_double(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _py_jsonb(v):
    # Kembalikan string JSON valid (psycopg cast ::jsonb di SQL). Default '{}'.
    if v is None or v == "":
        return "{}"
    s = str(v)
    try:
        json.loads(s)
        return s
    except (json.JSONDecodeError, TypeError):
        return json.dumps({"value": s}, ensure_ascii=False)


def _pick(header_idx: dict, *names):
    """Index kolom pertama yang cocok dari header (toleran), atau None."""
    for n in names:
        if n in header_idx:
            return header_idx[n]
    return None


def _chunked_csv_insert(connection, path: Path, job: ImportJob, insert_sql: str,
                        row_builder, batch: int = 5000) -> int:
    """Baca CSV per-batch, bangun baris via row_builder(header_idx, raw_row),
    lalu INSERT ... ON CONFLICT per batch dengan commit (autocommit). Ringan RAM
    server karena tiap transaksi kecil. row_builder return tuple/None (skip).
    header_idx adalah dict {nama_kolom: index}; row_builder bisa pakai .keys()
    untuk daftar kolom terurut bila perlu seluruh baris jadi JSON."""
    inserted = 0
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csvmod.reader(handle)
        header = [h.strip() for h in next(reader, [])]
        header_idx = {name: i for i, name in enumerate(header)}
        buf = []
        for raw in reader:
            built = row_builder(header_idx, raw)
            if built is not None:
                buf.append(built)
            if len(buf) >= batch:
                _check_cancel(job)
                with connection.cursor() as cur:
                    cur.executemany(insert_sql, buf)
                inserted += len(buf)
                job.message = f"{path.name}: {inserted:,} baris dimuat"
                buf.clear()
        if buf:
            with connection.cursor() as cur:
                cur.executemany(insert_sql, buf)
            inserted += len(buf)
    return inserted


def _ingest_nodes(connection, path: Path, job: ImportJob) -> None:
    _check_cancel(job)
    job.message = "Memuat nodes.csv sebagai node master"
    insert_sql = """
        INSERT INTO kg_node
            (node_id, node_type, business_key, label, domain, properties_json,
             source_file, source_sheet, source_row, source_record_id)
        VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s)
        ON CONFLICT (dataset_id, node_id) DO NOTHING
    """

    def build(h, r):
        def g(i):
            return r[i] if i is not None and i < len(r) else None
        i_id = _pick(h, "node_id")
        node_id = g(i_id)
        if not node_id:
            return None
        i_label = _pick(h, "label")
        label = g(i_label) if i_label is not None else None
        if not label:
            label = node_id
        return (
            node_id,
            g(_pick(h, "node_type")),
            g(_pick(h, "business_key")),
            label,
            g(_pick(h, "domain")),
            _py_jsonb(g(_pick(h, "properties_json"))),
            g(_pick(h, "source_file")),
            g(_pick(h, "source_sheet")),
            _py_bigint(g(_pick(h, "source_row"))),
            g(_pick(h, "source_record_id")),
        )

    _chunked_csv_insert(connection, path, job, insert_sql, build)
    count = fetch_tuple(connection, "SELECT count(*) FROM kg_node")[0]
    connection.execute("INSERT INTO load_summary (workbook, sheet_name, row_count, node_count, edge_count, issue_count, status) VALUES (%s, %s, %s, %s, 0, 0, %s)", [path.name, "nodes", count, count, "loaded"])


def _ingest_relationships(connection, path: Path, job: ImportJob, candidates: bool) -> None:
    _check_cancel(job)
    job.message = f"Memuat {path.name} sebagai {'candidate edge' if candidates else 'edge final'}"
    insert_sql = """
        INSERT INTO kg_relationship
            (relationship_id, source_node_id, target_node_id, relationship_type, domain,
             confidence, match_method, is_candidate, properties_json,
             source_file, source_sheet, source_row, source_record_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s)
        ON CONFLICT (dataset_id, relationship_id) DO NOTHING
    """

    def build(h, r):
        def g(i):
            return r[i] if i is not None and i < len(r) else None
        rid = g(_pick(h, "relationship_id"))
        if not rid:
            return None
        return (
            rid,
            g(_pick(h, "source_node_id")),
            g(_pick(h, "target_node_id")),
            g(_pick(h, "relationship_type")),
            g(_pick(h, "domain")),
            _py_double(g(_pick(h, "confidence"))),
            g(_pick(h, "match_method")),
            candidates,
            _py_jsonb(g(_pick(h, "properties_json"))),
            g(_pick(h, "source_file")),
            g(_pick(h, "source_sheet")),
            _py_bigint(g(_pick(h, "source_row"))),
            g(_pick(h, "source_record_id")),
        )

    _chunked_csv_insert(connection, path, job, insert_sql, build)
    is_candidate_expr = "true" if candidates else "false"
    loaded = fetch_tuple(connection, f"SELECT count(*) FROM kg_relationship WHERE is_candidate={is_candidate_expr}")[0]
    connection.execute("INSERT INTO load_summary (workbook, sheet_name, row_count, node_count, edge_count, issue_count, status) VALUES (%s, %s, %s, 0, %s, 0, %s)", [path.name, "relationships", loaded, loaded, "loaded"])


def _ingest_supporting_files(connection, fingerprints: list[dict], job: ImportJob) -> None:
    by_name = {Path(item["name"]).name: item for item in fingerprints}
    for file_name, analysis_name in ANALYSIS_FILES.items():
        item = by_name.get(file_name)
        if item:
            _ingest_analysis_csv(connection, Path(item["path"]), analysis_name, job)
    if "etl_summary.json" in by_name:
        _ingest_json_analysis(connection, Path(by_name["etl_summary.json"]["path"]), "etl_summary")

    metadata = [item for item in fingerprints if str(item.get("file_type", "")).startswith("metadata_")]
    for item in metadata:
        path = Path(item["path"])
        if path.suffix.lower() == ".json":
            _ingest_json_analysis(connection, path, f"metadata_{path.stem}")
        else:
            _ingest_analysis_csv(connection, path, f"metadata_{path.stem}", job)

    analysis_ready = [item for item in fingerprints if str(item.get("file_type", "")).startswith("analysis_ready_")]
    for item in analysis_ready:
        path = Path(item["path"])
        _ingest_analysis_csv(connection, path, f"analysis_ready_{path.stem}", job)

    for file_name, issue_type in AUDIT_FILES.items():
        item = by_name.get(file_name)
        if item:
            _ingest_audit_csv(connection, Path(item["path"]), issue_type, job)

    for item in fingerprints:
        path = Path(item["path"])
        if path.name.startswith("domain_") and path.suffix.lower() == ".csv":
            _ingest_domain_csv(connection, path, job)


def _row_to_json(header_idx: dict, raw: list) -> str:
    """Bangun objek JSON {kolom: nilai} dari satu baris CSV (ganti to_jsonb(t))."""
    obj = {name: (raw[i] if i < len(raw) else None) for name, i in header_idx.items()}
    return json.dumps(obj, ensure_ascii=False, default=str)


def _ingest_analysis_csv(connection, path: Path, analysis_name: str, job: ImportJob) -> None:
    _check_cancel(job)
    job.message = f"Memuat analisis {path.name}"
    insert_sql = "INSERT INTO graph_analysis (analysis_name, row_json) VALUES (%s, %s::jsonb)"

    def build(h, r):
        return (analysis_name, _row_to_json(h, r))

    _chunked_csv_insert(connection, path, job, insert_sql, build)
    count = fetch_tuple(connection, "SELECT count(*) FROM graph_analysis WHERE analysis_name=%s", [analysis_name])[0]
    connection.execute("INSERT INTO load_summary (workbook, sheet_name, row_count, node_count, edge_count, issue_count, status) VALUES (%s, %s, %s, 0, 0, 0, %s)", [path.name, analysis_name, count, "loaded"])


def _ingest_json_analysis(connection, path: Path, analysis_name: str) -> None:
    payload = json.loads(path.read_text("utf-8"))
    rows = payload if isinstance(payload, list) else [payload]
    connection.cursor().executemany(
        "INSERT INTO graph_analysis (analysis_name, row_json) VALUES (%s, %s::jsonb)",
        [(analysis_name, json.dumps(row, ensure_ascii=False, default=str)) for row in rows],
    )
    connection.execute("INSERT INTO load_summary (workbook, sheet_name, row_count, node_count, edge_count, issue_count, status) VALUES (%s, %s, %s, 0, 0, 0, %s)", [path.name, analysis_name, len(rows), "loaded"])


def _ingest_audit_csv(connection, path: Path, issue_type: str, job: ImportJob) -> None:
    _check_cancel(job)
    job.message = f"Memuat audit {path.name}"
    default_msg = issue_type.replace("_", " ")
    insert_sql = """
        INSERT INTO import_issue
            (issue_type, identifier, message, source_file, source_sheet, source_row, details_json)
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
    """

    def build(h, r):
        def g(*names):
            i = _pick(h, *names)
            return r[i] if i is not None and i < len(r) else None
        message = g("reason", "message", "dq_issues", "details") or default_msg
        return (
            issue_type,
            g("identifier_raw", "identifier", "raw_identifier", "relationship_id", "source_record_id"),
            message,
            g("source_file"),
            g("source_sheet"),
            _py_bigint(g("source_row")),
            _row_to_json(h, r),
        )

    _chunked_csv_insert(connection, path, job, insert_sql, build)
    count = fetch_tuple(connection, "SELECT count(*) FROM import_issue WHERE issue_type=%s", [issue_type])[0]
    connection.execute("INSERT INTO load_summary (workbook, sheet_name, row_count, node_count, edge_count, issue_count, status) VALUES (%s, %s, %s, 0, 0, %s, %s)", [path.name, issue_type, count, count, "loaded"])


def _ingest_domain_csv(connection, path: Path, job: ImportJob) -> None:
    _check_cancel(job)
    job.message = f"Memuat supporting domain {path.name}"
    import hashlib

    insert_sql = """
        INSERT INTO domain_record
            (source_record_id, source_file, source_sheet, source_row, equipment_id, record_json)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
    """

    def build(h, r):
        def g(*names):
            i = _pick(h, *names)
            return r[i] if i is not None and i < len(r) else None
        record_json = _row_to_json(h, r)
        srec = g("source_record_id", "_source_record_id", "record_id")
        if not srec:
            srec = hashlib.md5(record_json.encode("utf-8")).hexdigest()
        return (
            srec,
            g("source_file", "_input_source_file", "_input_file") or path.name,
            g("source_sheet", "_input_source_sheet", "_logical_table") or path.stem,
            _py_bigint(g("source_row", "_source_row", "source_row_number")),
            g("equipment_id", "equipment", "tag_no", "tag_no_ln", "equipment_code_normalized"),
            record_json,
        )

    _chunked_csv_insert(connection, path, job, insert_sql, build)
    count = fetch_tuple(connection, "SELECT count(*) FROM domain_record WHERE source_file=%s OR source_sheet=%s", [path.name, path.stem])[0]
    connection.execute("INSERT INTO load_summary (workbook, sheet_name, row_count, node_count, edge_count, issue_count, status) VALUES (%s, %s, %s, 0, 0, 0, %s)", [path.name, path.stem, count, "loaded"])


def _derive_identifiers(connection) -> None:
    connection.execute("""
        INSERT INTO kg_identifier (identifier, equipment_node_id, identifier_type)
        SELECT DISTINCT label, node_id, 'label'
        FROM kg_node
        WHERE node_type='equipment' AND label IS NOT NULL AND label <> ''
    """)
    connection.execute("""
        INSERT INTO kg_identifier (identifier, equipment_node_id, identifier_type)
        SELECT DISTINCT business_key, node_id, 'business_key'
        FROM kg_node
        WHERE node_type='equipment' AND business_key IS NOT NULL AND business_key <> ''
    """)
    for key in ["equipment_code_normalized", "equipment_id", "equipment", "tag_no"]:
        connection.execute("""
            INSERT INTO kg_identifier (identifier, equipment_node_id, identifier_type)
            SELECT DISTINCT properties_json->>%s, node_id, %s
            FROM kg_node
            WHERE node_type='equipment'
              AND properties_json->>%s IS NOT NULL
              AND properties_json->>%s <> ''
        """, [key, key, key, key])


def _validate_graph(connection) -> None:
    connection.execute("""
        INSERT INTO import_issue
            (issue_type, identifier, message, source_file, source_sheet, source_row, details_json)
        SELECT 'broken_relationship', relationship_id,
               'Relationship final memiliki endpoint yang tidak tersedia.',
               source_file, source_sheet, source_row,
               jsonb_build_object('source_node_id', source_node_id, 'target_node_id', target_node_id)
        FROM kg_relationship r
        WHERE NOT r.is_candidate
          AND (
            NOT EXISTS (SELECT 1 FROM kg_node n WHERE n.node_id=r.source_node_id)
            OR NOT EXISTS (SELECT 1 FROM kg_node n WHERE n.node_id=r.target_node_id)
          )
    """)
    connection.execute("""
        INSERT INTO import_issue
            (issue_type, identifier, message, source_file, source_sheet, source_row, details_json)
        SELECT 'invalid_value', relationship_id,
               'Confidence relationship berada di luar rentang 0–1.',
               source_file, source_sheet, source_row, '{}'::jsonb
        FROM kg_relationship
        WHERE confidence IS NOT NULL AND (confidence < 0 OR confidence > 1)
    """)
    connection.execute("""
        DELETE FROM kg_relationship
        WHERE NOT EXISTS (SELECT 1 FROM kg_node n WHERE n.node_id=source_node_id)
           OR NOT EXISTS (SELECT 1 FROM kg_node n WHERE n.node_id=target_node_id)
    """)


def _read_csv_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csvmod.reader(handle)
        header = next(reader, [])
    return [name.strip() for name in header]


def _raw_table(connection, path: Path, table_name: str) -> tuple[str, list[str]]:
    """Create a session TEMP table with all-text columns matching the CSV header
    and bulk load via COPY in row-batches (commit per batch when autocommit).
    Returns (table_name, columns). Mirrors DuckDB read_csv_auto(all_varchar=true):
    setiap kolom text, kolom ekstra ditoleransi. Memuat bertahap mencegah server
    menutup koneksi saat file sangat besar (mis. nodes.csv ~1 GB)."""
    table = safe_ident(table_name)
    columns = _read_csv_header(path)
    if not columns:
        connection.execute(f'CREATE TEMP TABLE {table} (dummy text)')
        return table, []
    col_defs = ", ".join(f"{quote_ident(name)} text" for name in columns)
    connection.execute(f"DROP TABLE IF EXISTS {table}")
    connection.execute(f"CREATE TEMP TABLE {table} ({col_defs})")
    col_list = ", ".join(quote_ident(name) for name in columns)
    copy_sql = f"COPY {table} ({col_list}) FROM STDIN"
    ncols = len(columns)
    batch = 50_000
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csvmod.reader(handle)
        next(reader, None)  # skip header
        rows: list[tuple] = []
        for row in reader:
            # Normalisasi jumlah kolom agar cocok dengan header (toleran kolom ekstra/kurang).
            if len(row) != ncols:
                row = (row + [""] * ncols)[:ncols]
            rows.append(tuple(row))
            if len(rows) >= batch:
                with connection.cursor().copy(copy_sql) as copy:
                    for r in rows:
                        copy.write_row(r)
                rows.clear()
        if rows:
            with connection.cursor().copy(copy_sql) as copy:
                for r in rows:
                    copy.write_row(r)
    return table, columns


def _columns(connection, table_columns) -> set[str]:
    # table_columns adalah list kolom yang sudah dikembalikan _raw_table.
    return set(table_columns)


def col(cols: set[str], name: str | None, default: str = "NULL", fallback: str | None = None) -> str:
    if name and name in cols:
        return quote_ident(name)
    if fallback and fallback in cols:
        return quote_ident(fallback)
    return default


def first_col(cols: set[str], names: list[str]) -> str | None:
    return next((name for name in names if name in cols), None)


def safe_ident(value: str) -> str:
    return '"' + "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in value) + '"'


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _check_cancel(job: ImportJob) -> None:
    if job.cancelled:
        raise RuntimeError("Import dibatalkan.")
