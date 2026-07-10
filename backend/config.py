from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from .database import connection, initialize

# Direktori sementara untuk ekstraksi ZIP saat import. Ephemeral (di Railway pun
# tidak masalah) karena hanya dipakai selama proses import berlangsung.
UPLOADS_DIR = Path(os.environ.get("UPLOADS_DIR", tempfile.gettempdir())) / "kgrre_uploads"


def ensure_dirs() -> None:
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)


def get_config() -> dict:
    # Fitur "scan folder lokal" tidak relevan di deployment; alur utama = upload via frontend.
    return {
        "upload_folder": os.environ.get("UPLOAD_FOLDER", ""),
        "stability_seconds": int(os.environ.get("STABILITY_SECONDS", 0)),
        "scan_interval_seconds": int(os.environ.get("SCAN_INTERVAL_SECONDS", 10)),
    }


def save_config(upload_folder: str) -> dict:
    # Disimpan hanya di env-proses; tidak persist. Dipertahankan agar kontrak API tidak berubah.
    os.environ["UPLOAD_FOLDER"] = upload_folder
    return get_config()


# --- Catalog (dataset registry) tersimpan di PostgreSQL, bukan file JSON ---

_SCHEMA_READY = False


def ensure_schema() -> None:
    """Pastikan tabel ada (idempotent, sekali per proses).

    HANYA menjalankan initialize() = CREATE TABLE IF NOT EXISTS, yang instan dan
    TIDAK mengunci data. SENGAJA TIDAK memanggil enable_rls() di sini.

    enable_rls() menjalankan ALTER TABLE kg_node ENABLE/FORCE ROW LEVEL SECURITY
    pada tabel 1,5 juta baris; ALTER TABLE butuh lock eksklusif. Dulu dipanggil di
    setiap request /api/datasets sehingga puluhan ALTER TABLE menumpuk saling-blok
    (terlihat di pg_stat_activity: 50+ koneksi 'ALTER TABLE ... ENABLE RLS' nyangkut
    berjam-jam menunggu Lock). RLS sudah dibuat permanen saat import dan tidak perlu
    dibuat ulang; alur baca tidak boleh menyentuhnya. RLS dipasang hanya saat import
    (importer.py memanggil enable_rls langsung)."""
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    with connection() as conn:
        initialize(conn)
    _SCHEMA_READY = True


def _dataset_row(row: dict) -> dict:
    return {
        "id": row["id"],
        "name": row["name"],
        "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
        "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else None,
        "mode": row.get("mode"),
        "node_count": row.get("node_count") or 0,
        "edge_count": row.get("edge_count") or 0,
        "issue_count": row.get("issue_count") or 0,
        "workbooks": row.get("workbooks") or [],
        "uploaded_package": row.get("uploaded_package") or False,
    }


def list_datasets() -> list[dict]:
    ensure_schema()
    with connection() as conn:
        rows = conn.execute(
            "SELECT * FROM dataset_catalog ORDER BY created_at DESC"
        ).fetchall()
    return [_dataset_row(row) for row in rows]


def get_dataset_row(dataset_id: str) -> dict | None:
    with connection() as conn:
        row = conn.execute(
            "SELECT * FROM dataset_catalog WHERE id = %s", [dataset_id]
        ).fetchone()
    return _dataset_row(row) if row else None


def insert_dataset(dataset: dict) -> None:
    with connection() as conn:
        conn.execute(
            """
            INSERT INTO dataset_catalog
                (id, name, mode, node_count, edge_count, issue_count, workbooks, uploaded_package)
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            """,
            [
                dataset["id"], dataset["name"], dataset.get("mode"),
                dataset.get("node_count", 0), dataset.get("edge_count", 0),
                dataset.get("issue_count", 0),
                json.dumps(dataset.get("workbooks", [])),
                dataset.get("uploaded_package", False),
            ],
        )


def update_dataset_counts(dataset_id: str, node_count: int, edge_count: int, issue_count: int, workbooks: list) -> None:
    with connection() as conn:
        conn.execute(
            """
            UPDATE dataset_catalog
            SET node_count = %s, edge_count = %s, issue_count = %s,
                workbooks = %s::jsonb, updated_at = now()
            WHERE id = %s
            """,
            [node_count, edge_count, issue_count, json.dumps(workbooks), dataset_id],
        )


def rename_dataset(dataset_id: str, name: str) -> dict | None:
    with connection() as conn:
        conn.execute(
            "UPDATE dataset_catalog SET name = %s, updated_at = now() WHERE id = %s",
            [name, dataset_id],
        )
    return get_dataset_row(dataset_id)


# Tabel yang menyimpan data per-dataset; dihapus saat dataset dihapus.
_DATA_TABLES = (
    "kg_node", "kg_relationship", "domain_record", "kg_identifier",
    "import_issue", "load_summary", "graph_analysis", "source_file",
)


def delete_dataset(dataset_id: str) -> None:
    with connection() as conn:
        for table in _DATA_TABLES:
            conn.execute(f"DELETE FROM {table} WHERE dataset_id = %s", [dataset_id])
        conn.execute("DELETE FROM dataset_catalog WHERE id = %s", [dataset_id])


def reset_all() -> dict:
    """Hapus semua dataset dan seluruh data — kembali ke kondisi kosong."""
    with connection() as conn:
        for table in _DATA_TABLES:
            conn.execute(f"TRUNCATE {table}")
        conn.execute("TRUNCATE dataset_catalog")
    return {"ok": True}
