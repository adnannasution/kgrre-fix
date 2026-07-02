# Kilang Graph

Knowledge graph lokal untuk membuka output ETL refinery equipment sebagai graph interaktif, Equipment 360, dashboard Refinery Unit, schema/depth explorer, dan audit review.

## Menjalankan aplikasi

1. Pastikan output ETL tersedia di `/Users/macbook/Documents/KGRRE/runtime/artifact_upload`.
   - Required: `nodes.csv` dan `relationships.csv`.
   - Recommended: `relationship_candidates.csv`, `etl_summary.json`, `ru_*.csv`, `graph_schema.csv`, `ontology_depth.csv`, `deepest_paths.csv`, audit CSV, `domain_*.csv`, dan `input_metadata/*`.
2. Tunggu proses copy selesai sedikitnya 30 detik.
3. Double-click [`run_kg.command`](/Users/macbook/Documents/KGRRE/live_artifact/run_kg.command), atau jalankan:

```bash
./run_kg.command
```

4. Buka `http://127.0.0.1:8765`.
5. Di Import Center, pilih **Scan folder** lalu **Import folder**. Alternatifnya upload ZIP berisi struktur file output ETL yang sama.

File sumber di `KGRRE/runtime/artifact_upload` hanya dibaca oleh aplikasi live artifact. Database hasil import disimpan di `data/datasets`.

## Kontrak graph

- `nodes.csv` adalah node master. `node_id` menjadi primary key, `node_type` menjadi tipe node, dan `label` menjadi label visual.
- `relationships.csv` adalah edge final/produksi. `relationship_id` menjadi primary key, dengan arah `source_node_id → target_node_id`.
- `relationship_candidates.csv` hanya untuk audit/review. Candidate edge disembunyikan default dan muncul hanya saat mode candidate diaktifkan.
- `properties_json` ditampilkan di detail panel dan dipakai untuk filter tambahan seperti Refinery Unit dan equipment code bila kolom fisiknya tidak tersedia.
- Aplikasi tidak membuat ulang knowledge graph dari raw Excel/domain data.

## Pengembangan

```bash
source .venv/bin/activate
pnpm dev
```

Frontend berjalan di port `5173`; API lokal di port `8765`.
