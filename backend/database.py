from __future__ import annotations

import json
import os
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

# Lazily-created process-wide connection pool. The backend is multi-threaded
# (import jobs run on background threads) so a pool is required.
_POOL: ConnectionPool | None = None


def _dsn() -> str:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL belum di-set. Isi di file .env (dev) atau variabel Railway (produksi)."
        )
    # Railway/Heroku kadang memakai skema 'postgres://'; psycopg butuh 'postgresql://'.
    if dsn.startswith("postgres://"):
        dsn = "postgresql://" + dsn[len("postgres://"):]
    # Batasi waktu connect. Tanpa ini, DATABASE_URL yang salah (mis. port keliru
    # di balik proxy internal Railway yang menerima TCP tapi tak pernah membalas
    # handshake) membuat percobaan koneksi menggantung tanpa batas — startup
    # uvicorn ikut menggantung sehingga healthcheck /api/health gagal selama
    # seluruh jendela retry, padahal endpoint itu sendiri tidak butuh DB. Dengan
    # connect_timeout, koneksi gagal cepat, _startup() menangkap errornya, dan
    # aplikasi tetap melayani /api/health.
    if "connect_timeout=" not in dsn:
        sep = "&" if "?" in dsn else "?"
        dsn = f"{dsn}{sep}connect_timeout=10"
    return dsn


def pool() -> ConnectionPool:
    global _POOL
    if _POOL is None:
        # open=True (default) membuka koneksi secara sinkron saat pertama kali pool
        # dibuat. Ini terjadi di request pertama (bukan startup), sehingga healthcheck
        # /api/health tetap menjawab. connect_timeout=10 di DSN memastikan koneksi
        # gagal cepat (10 detik) jika DB tidak tersedia, bukan hang selamanya.
        _POOL = ConnectionPool(
            _dsn(),
            min_size=1,
            max_size=10,
            kwargs={"row_factory": dict_row},
            open=True,
        )
    return _POOL


def close_pool() -> None:
    global _POOL
    if _POOL is not None:
        _POOL.close()
        _POOL = None


@contextmanager
def connection():
    """Yield a pooled connection (autocommit off; commit on clean exit)."""
    with pool().connection() as conn:
        yield conn


def initialize(conn: psycopg.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dataset_catalog (
            id text PRIMARY KEY,
            name text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            mode text,
            node_count bigint DEFAULT 0,
            edge_count bigint DEFAULT 0,
            issue_count bigint DEFAULT 0,
            workbooks jsonb DEFAULT '[]'::jsonb,
            uploaded_package boolean DEFAULT false
        );

        CREATE TABLE IF NOT EXISTS kg_node (
            dataset_id text NOT NULL DEFAULT current_setting('app.dataset_id', true),
            node_id text NOT NULL,
            node_type text NOT NULL,
            business_key text,
            label text,
            domain text,
            properties_json jsonb DEFAULT '{}'::jsonb,
            source_file text,
            source_sheet text,
            source_row bigint,
            source_record_id text,
            PRIMARY KEY (dataset_id, node_id)
        );

        CREATE TABLE IF NOT EXISTS kg_relationship (
            dataset_id text NOT NULL DEFAULT current_setting('app.dataset_id', true),
            relationship_id text NOT NULL,
            source_node_id text NOT NULL,
            target_node_id text NOT NULL,
            relationship_type text NOT NULL,
            domain text,
            confidence double precision,
            match_method text,
            is_candidate boolean DEFAULT false,
            properties_json jsonb DEFAULT '{}'::jsonb,
            source_file text,
            source_sheet text,
            source_row bigint,
            source_record_id text,
            PRIMARY KEY (dataset_id, relationship_id)
        );

        CREATE TABLE IF NOT EXISTS domain_record (
            dataset_id text NOT NULL DEFAULT current_setting('app.dataset_id', true),
            source_record_id text,
            source_file text,
            source_sheet text,
            source_row bigint,
            equipment_id text,
            record_json jsonb DEFAULT '{}'::jsonb
        );

        CREATE TABLE IF NOT EXISTS kg_identifier (
            dataset_id text NOT NULL DEFAULT current_setting('app.dataset_id', true),
            identifier text,
            equipment_node_id text,
            identifier_type text
        );

        CREATE TABLE IF NOT EXISTS import_issue (
            dataset_id text NOT NULL DEFAULT current_setting('app.dataset_id', true),
            issue_type text,
            identifier text,
            message text,
            source_file text,
            source_sheet text,
            source_row bigint,
            details_json jsonb DEFAULT '{}'::jsonb
        );

        CREATE TABLE IF NOT EXISTS load_summary (
            dataset_id text NOT NULL DEFAULT current_setting('app.dataset_id', true),
            workbook text,
            sheet_name text,
            row_count bigint,
            node_count bigint,
            edge_count bigint,
            issue_count bigint,
            status text
        );

        CREATE TABLE IF NOT EXISTS graph_analysis (
            dataset_id text NOT NULL DEFAULT current_setting('app.dataset_id', true),
            analysis_name text,
            row_json jsonb DEFAULT '{}'::jsonb
        );

        CREATE TABLE IF NOT EXISTS source_file (
            dataset_id text NOT NULL DEFAULT current_setting('app.dataset_id', true),
            file_name text,
            file_type text,
            path text,
            size bigint,
            mtime_ns bigint,
            sha256 text,
            row_count bigint
        );
    """)


def finalize(conn: psycopg.Connection) -> None:
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_node_type ON kg_node(dataset_id, node_type);
        CREATE INDEX IF NOT EXISTS idx_node_business ON kg_node(dataset_id, business_key);
        CREATE INDEX IF NOT EXISTS idx_node_source_record ON kg_node(dataset_id, source_record_id);
        CREATE INDEX IF NOT EXISTS idx_node_props ON kg_node USING gin (properties_json);
        CREATE INDEX IF NOT EXISTS idx_rel_source ON kg_relationship(dataset_id, source_node_id);
        CREATE INDEX IF NOT EXISTS idx_rel_target ON kg_relationship(dataset_id, target_node_id);
        CREATE INDEX IF NOT EXISTS idx_rel_type ON kg_relationship(dataset_id, relationship_type);
        CREATE INDEX IF NOT EXISTS idx_rel_candidate ON kg_relationship(dataset_id, is_candidate, confidence);
        CREATE INDEX IF NOT EXISTS idx_domain_record ON domain_record(dataset_id, source_record_id);
        CREATE INDEX IF NOT EXISTS idx_domain_equipment ON domain_record(dataset_id, equipment_id);
        CREATE INDEX IF NOT EXISTS idx_identifier ON kg_identifier(dataset_id, identifier);
        CREATE INDEX IF NOT EXISTS idx_source_file_type ON source_file(dataset_id, file_type);
        CREATE INDEX IF NOT EXISTS idx_analysis_name ON graph_analysis(dataset_id, analysis_name);
        CREATE INDEX IF NOT EXISTS idx_issue_type ON import_issue(dataset_id, issue_type);
    """)


# Tabel berisi data per-dataset yang harus terisolasi via Row-Level Security.
_RLS_TABLES = (
    "kg_node", "kg_relationship", "domain_record", "kg_identifier",
    "import_issue", "load_summary", "graph_analysis", "source_file",
)


def enable_rls(conn: psycopg.Connection) -> None:
    """Aktifkan Row-Level Security agar setiap query otomatis ter-filter ke
    dataset aktif (session var app.dataset_id). Menghilangkan kebutuhan menulis
    'WHERE dataset_id=...' manual di puluhan query, sekaligus mencegah kebocoran
    antar-dataset. Idempotent."""
    for table in _RLS_TABLES:
        conn.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        conn.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
        conn.execute(f"DROP POLICY IF EXISTS dataset_isolation ON {table}")
        conn.execute(
            f"""
            CREATE POLICY dataset_isolation ON {table}
            USING (dataset_id = current_setting('app.dataset_id', true))
            WITH CHECK (dataset_id = current_setting('app.dataset_id', true))
            """
        )


def fetch_tuple(conn, query: str, params=None):
    """Jalankan query dan kembalikan satu baris sebagai tuple (bukan dict),
    untuk akses posisional seperti fetchone()[0]. Pool memakai dict_row global,
    jadi gunakan cursor dengan row_factory tuple di sini."""
    from psycopg.rows import tuple_row
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(query, params or [])
        return cur.fetchone()


@contextmanager
def scoped(dataset_id: str, autocommit: bool = False):
    """Yield a pooled connection with app.dataset_id set, so RLS scopes all
    queries to this dataset. Use for both reads and writes of per-dataset data.

    autocommit=True dipakai untuk import besar: tiap COPY/INSERT langsung di-commit
    agar server tidak menahan satu transaksi raksasa (penyebab koneksi diputus saat
    memuat file 1 GB). app.dataset_id di-set session-level (is_local=false) sehingga
    tetap berlaku lintas commit, jadi RLS & DEFAULT dataset_id tetap benar."""
    with pool().connection() as conn:
        if autocommit:
            conn.autocommit = True
        conn.execute("SELECT set_config('app.dataset_id', %s, false)", [dataset_id])
        yield conn


def json_value(value) -> str:
    """Normalize an arbitrary value into a JSON string suitable for a jsonb cast."""
    if value is None:
        return "{}"
    if isinstance(value, str):
        try:
            json.loads(value)
            return value
        except json.JSONDecodeError:
            return json.dumps({"value": value}, ensure_ascii=False)
    return json.dumps(value, ensure_ascii=False, default=str)
