"""Kosongkan SEMUA data di PostgreSQL (TRUNCATE) tanpa menghapus skema/volume.
Aman dijalankan ulang. Dipakai untuk import full yang bersih."""
import os
from dotenv import load_dotenv
load_dotenv()
import psycopg

url = os.environ["DATABASE_URL"]
if url.startswith("postgres://"):
    url = "postgresql://" + url[len("postgres://"):]

TABLES = [
    "kg_node", "kg_relationship", "domain_record", "kg_identifier",
    "import_issue", "load_summary", "graph_analysis", "source_file",
    "dataset_catalog",
]

with psycopg.connect(url, connect_timeout=20, autocommit=True) as conn:
    # cek tabel ada dulu
    existing = {r[0] for r in conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    ).fetchall()}
    to_truncate = [t for t in TABLES if t in existing]
    if to_truncate:
        # TRUNCATE bypass RLS hanya untuk owner; kita owner DB Railway.
        sql = "TRUNCATE TABLE " + ", ".join(to_truncate) + " RESTART IDENTITY CASCADE"
        conn.execute(sql)
        print("TRUNCATED:", ", ".join(to_truncate))
    else:
        print("Tidak ada tabel untuk di-truncate (skema mungkin belum dibuat).")

    size = conn.execute("SELECT pg_size_pretty(pg_database_size(current_database()))").fetchone()[0]
    print("DB size sekarang:", size)
print("RESET OK")
