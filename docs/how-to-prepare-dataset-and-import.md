# Cara Menyiapkan Dataset & Impor ke KGRRE

Panduan ini menjelaskan **seluruh proses di luar aplikasi** — mulai dari ETL Excel
di Google Colab sampai dataset tampil di viewer KGRRE. Ikuti berurutan; setiap
tahap punya kontrak yang harus dipenuhi agar impor berhasil.

> **Batas tanggung jawab (penting).** KGRRE adalah **viewer** knowledge graph, bukan
> pembangun graph. Aplikasi ini **tidak pernah** membangun ulang graph dari Excel/raw
> domain. Ia hanya meng-*ingest* output ETL yang **sudah berbentuk CSV/JSON**. Jadi
> semua logika pembentukan node & edge terjadi di Colab (Tahap 1), bukan di KGRRE.

```
┌─────────────────┐   ┌──────────────────┐   ┌───────────────┐   ┌──────────────┐
│  Tahap 1        │   │  Tahap 2         │   │  Tahap 3      │   │  Tahap 4     │
│  ETL di Colab   │──▶│  Paket output    │──▶│  Siapkan DB   │──▶│  Impor       │
│  (Excel → CSV)  │   │  (folder / ZIP)  │   │  (PostgreSQL) │   │  (browser /  │
│                 │   │                  │   │               │   │   laptop)    │
└─────────────────┘   └──────────────────┘   └───────────────┘   └──────────────┘
```

---

## Tahap 1 — ETL Excel di Google Colab

ETL berjalan **sepenuhnya di luar** repo ini (mis. notebook Colab). Tugasnya: membaca
sumber domain (Excel/CSV mentah dari SAP, inspeksi, RKAP, dsb.) lalu **memodelkan**
data menjadi node & edge dan **menuliskannya sebagai CSV/JSON** dengan skema di Tahap 2.

Yang harus dihasilkan ETL:

1. **Node master** — satu baris per entitas (equipment, refinery unit, maintenance
   order, notification, inspection, dst.). Setiap node punya `node_id` unik.
2. **Edge final** — relasi yang sudah terverifikasi antar node (`source → target`).
3. **Edge kandidat (opsional)** — relasi hasil pencocokan fuzzy yang belum pasti;
   dipisah ke file audit sendiri.
4. **File pendukung (opsional)** — ringkasan ETL, summary per Refinery Unit, schema
   graph, kedalaman ontologi, file audit kualitas data, dan tabel domain mentah.

Prinsip pemodelan yang harus dipatuhi ETL:

- **`node_type` dan `relationship_type` bebas teks (bukan enum).** Tipe baru langsung
  ter-ingest tanpa perubahan kode. (Agar diberi warna khusus di graph, tambahkan
  warna di `src/components/GraphView.tsx`; tipe tak dikenal memakai warna default.)
- **Atribut tambahan masuk ke `properties_json`**, bukan sebagai kolom baru. Kolom
  mentah yang tak dikenal diabaikan importer dengan aman.
- **Angka yang sering kosong/`0`** (mis. MTBF/MTTR/biaya) tetap ditulis apa adanya;
  jangan direkayasa. KGRRE memperlakukan `0`/kosong sebagai "belum tercatat".
- Nilai turunan yang dipakai dashboard/prompt (mis. `derived_order_age_days`,
  `derived_is_open_order`, `derived_planned_cost`, `derived_status_bucket`,
  `derived_total_equivalent_idr_num`, `derived_is_top_risk`, `derived_is_delayed`)
  **dihitung di ETL** dan disimpan di dalam `properties_json` node terkait.

Ekspor semua output sebagai **CSV UTF-8** (JSON UTF-8 untuk `etl_summary.json`).
Delimiter koma. Boleh ada BOM (importer membaca `utf-8-sig`).

---

## Tahap 2 — Struktur paket output ETL

Kumpulkan seluruh output ke **satu folder** dengan struktur berikut. Nama file penting
— scanner mengenali file **berdasarkan nama** (`backend/scanner.py`).

```
paket_output/
├── nodes.csv                     ← WAJIB (node master)
├── relationships.csv             ← WAJIB (edge final)
├── relationship_candidates.csv   ← disarankan (edge kandidat, audit-only)
├── etl_summary.json              ← disarankan (ringkasan ETL)
├── refinery_units.csv            ← disarankan (dashboard RU)
├── ru_equipment_summary.csv      ← disarankan
├── ru_data_coverage.csv          ← disarankan
├── ru_relationship_quality.csv   ← disarankan
├── graph_schema.csv              ← disarankan (schema explorer)
├── ontology_depth.csv            ← disarankan (depth explorer)
├── deepest_paths.csv             ← disarankan
├── unmatched_identifier.csv      ← disarankan (audit review)
├── ambiguous_match.csv           ← disarankan (audit review)
├── invalid_value.csv             ← disarankan (audit review)
├── output_manifest.csv           ← disarankan
├── domain_<apa saja>.csv         ← opsional, boleh banyak (tabel domain mentah)
├── input_metadata/               ← opsional (CSV/JSON metadata sumber)
│   └── *.csv | *.json
└── analysis_ready/               ← opsional (CSV siap-analisis)
    └── *.csv
```

Aturan yang diberlakukan importer/scanner:

- **Hanya `nodes.csv` + `relationships.csv` yang wajib.** Tanpa keduanya, impor ditolak.
- File dengan awalan **`domain_`** otomatis masuk tabel `domain_record` — tambah file
  domain baru **tanpa perubahan kode**.
- File di dalam **`input_metadata/`** dan **`analysis_ready/`** otomatis ter-ingest.
- **Kedalaman maksimal 2 level.** Sub-folder hanya boleh `input_metadata/` atau
  `analysis_ready/`. File di sub-folder lain diabaikan (juga proteksi path-traversal
  ZIP: tanpa path absolut / `..`).
- Nama file lain yang tak dikenal **diabaikan diam-diam** (bukan error). Untuk
  mendukungnya, daftarkan di `scanner.py`/`importer.py`.

### Kontrak kolom `nodes.csv`

| Kolom              | Wajib | Keterangan                                              |
|--------------------|:-----:|---------------------------------------------------------|
| `node_id`          |  ✅   | Primary key node. Baris tanpa `node_id` dilewati.       |
| `node_type`        |  ✅   | Tipe node (bebas teks). Mis. `equipment`, `refinery_unit`. |
| `label`            |  ✅   | Label visual. Jika kosong, fallback ke `node_id`.       |
| `properties_json`  |  ✅   | Objek JSON atribut tambahan. Kosong → `{}`.             |
| `business_key`     |  —    | Kunci bisnis (dipakai untuk identifier equipment).      |
| `domain`           |  —    | Domain sumber node.                                     |
| `source_file` / `source_sheet` / `source_row` / `source_record_id` | — | Jejak asal baris (untuk audit). |

- Duplikat `node_id` di-*dedupe* (`ON CONFLICT DO NOTHING`) — baris pertama menang.
- `properties_json` harus JSON valid. Jika bukan JSON, importer membungkusnya jadi
  `{"value": "<teks asli>"}` agar tidak gagal.

### Kontrak kolom `relationships.csv` (dan `relationship_candidates.csv`)

| Kolom               | Wajib | Keterangan                                             |
|---------------------|:-----:|--------------------------------------------------------|
| `relationship_id`   |  ✅   | Primary key edge. Baris tanpa ini dilewati.            |
| `source_node_id`    |  ✅   | `node_id` asal (arah `source → target`).               |
| `target_node_id`    |  ✅   | `node_id` tujuan.                                       |
| `relationship_type` |  ✅   | Tipe relasi (bebas teks). Mis. `EQUIPMENT_HAS_ORDER`.  |
| `confidence`        |  —    | Skor 0–1. Di luar rentang → dicatat sebagai issue.     |
| `match_method`      |  —    | Metode pencocokan.                                     |
| `domain`            |  —    | Domain relasi.                                          |
| `properties_json`   |  —    | Atribut tambahan edge (JSON).                          |
| `source_*`          |  —    | Jejak asal baris.                                       |

- **`relationships.csv`** = edge final/produksi (selalu tampil).
- **`relationship_candidates.csv`** = edge kandidat; disembunyikan default dan hanya
  muncul saat mode candidate diaktifkan (untuk audit/review).
- **Edge dengan endpoint yang tidak ada** (source/target bukan `node_id` valid)
  otomatis **dihapus** dari edge final dan dicatat sebagai `broken_relationship`
  di audit. Pastikan setiap `source_node_id`/`target_node_id` benar-benar ada di
  `nodes.csv`.

### Mengemas jadi ZIP (untuk upload/laptop)

Zip **isi** folder (bukan folder pembungkusnya) agar `nodes.csv` ada di root ZIP:

```bash
cd paket_output
zip -r ../knowledge_graph_upload.zip .          # macOS/Linux
# atau di PowerShell:
# Compress-Archive -Path * -DestinationPath ..\knowledge_graph_upload.zip
```

> Struktur `input_metadata/` dan `analysis_ready/` boleh tetap sebagai sub-folder di
> dalam ZIP — importer mengizinkan kedua nama itu di level ke-2.

---

## Tahap 3 — Menyiapkan database (PostgreSQL)

KGRRE menyimpan semua data di **PostgreSQL** (satu database bersama; setiap dataset
diisolasi lewat `dataset_id` + Row-Level Security). Anda **tidak perlu membuat tabel
manual** — skema dibuat otomatis pada impor pertama (`ensure_schema()` /
`CREATE TABLE IF NOT EXISTS`).

Yang perlu disiapkan hanyalah **`DATABASE_URL`**:

1. Di Railway, buka plugin **PostgreSQL** → tab **Variables**.
2. Untuk impor **dari laptop**, salin **`DATABASE_PUBLIC_URL`** (host `*.proxy.rlwy.net`).
3. Di root repo, salin `.env.example` → `.env` dan isi:

   ```dotenv
   DATABASE_URL=postgresql://USER:PASSWORD@HOST.proxy.rlwy.net:PORT/railway
   ```

   `.env` sudah di-`.gitignore` — **jangan commit** kredensial.

> Saat aplikasi berjalan **di Railway**, `DATABASE_URL` disuntik otomatis (host
> `*.railway.internal`) dan tidak perlu diisi manual. Pastikan variabel
> `DATABASE_URL = ${{Postgres.DATABASE_URL}}` di-set pada **service KGRRE**, bukan
> hanya pada service Postgres.

---

## Tahap 4 — Mengimpor dataset

Ada dua jalur. Pilih berdasarkan **ukuran data**.

### Jalur A — Upload lewat browser (dataset kecil)

Cocok untuk paket kecil (bukan dump penuh refinery).

1. Buka aplikasi KGRRE → menu **Import Center**.
2. **Upload ZIP** berisi struktur Tahap 2, lalu beri **nama dataset**.
3. Progres tampil real-time; tunggu status `Selesai`.

> ⚠️ **Jangan pakai jalur ini untuk data besar.** Proxy HTTP Railway timeout ~5 menit,
> dan upload >±400 MB akan gagal. Untuk dump penuh gunakan Jalur B.

### Jalur B — Impor dari laptop (dataset besar / dump penuh) — **disarankan**

Output ETL asli sangat besar (`nodes.csv` ~990 MB / ~1,5 juta baris,
`relationships.csv` ~1,2 GB / ~2,3 juta baris, domain 376 MB+ → ~417 MB zip,
~3,2 GB ekstrak). Untuk ini gunakan skrip lokal yang memanggil **pipeline importer
yang sama** tapi berjalan **sinkron** dari laptop (tanpa timeout proxy, error langsung
terlihat):

```bash
# 1) Aktifkan venv & pastikan .env berisi DATABASE_URL (DATABASE_PUBLIC_URL Railway)
# 2) Set PYTHONPATH ke root repo
export PYTHONPATH=.                 # PowerShell: $env:PYTHONPATH="."

# 3) Jalankan impor
.venv/bin/python scripts/import_local.py "knowledge_graph_upload.zip" "Nama Dataset"
#   Windows: .venv/Scripts/python.exe scripts/import_local.py "...zip" "Nama Dataset"
```

Yang dilakukan skrip:

- Mengekstrak ZIP ke **`<folder_zip>/.import_tmp/`** (drive yang sama dengan ZIP, agar
  tidak menghabiskan ruang `C:`), lalu memindai & meng-ingest.
- Meng-ingest **per-batch 5000 baris** dengan commit tiap batch (`autocommit`), casting
  angka/JSON di Python — sehingga transaksi tetap kecil dan koneksi tidak putus pada
  file berukuran GB.
- **Tidak menghapus ZIP sumber** (berbeda dari upload browser yang menghapusnya).
- Mencetak progres tiap fase: `Node master → Verified edges → Candidate edges →
  Analysis & audit → Validation → Selesai`.

Setelah selesai, skrip mencetak `dataset_id` dan jumlah node/relationship. Dataset
langsung muncul di daftar dataset aplikasi.

---

## Verifikasi & troubleshooting

Setelah impor:

1. Buka aplikasi → **pilih dataset** yang baru.
2. Cek **Overview** untuk jumlah node/edge sesuai ekspektasi.
3. Buka **Audit Review** untuk melihat issue (broken relationship, unmatched
   identifier, ambiguous match, invalid value).

Masalah umum:

| Gejala                                   | Penyebab & solusi                                                      |
|------------------------------------------|-----------------------------------------------------------------------|
| Impor ditolak "File wajib belum tersedia" | `nodes.csv`/`relationships.csv` tidak ada di **root** paket/ZIP.       |
| File `Invalid` di scan                    | Kolom wajib hilang. Cek header sesuai kontrak Tahap 2.                 |
| Banyak `broken_relationship` di audit     | `source_node_id`/`target_node_id` tak cocok `node_id` di `nodes.csv`.  |
| Node/edge lebih sedikit dari perkiraan    | Duplikat `node_id`/`relationship_id` di-dedupe; endpoint rusak dihapus.|
| File domain/metadata tak masuk            | Nama tak sesuai (`domain_*.csv`) atau di luar `input_metadata/`/`analysis_ready/`. |
| Upload browser timeout                    | Data terlalu besar — pakai **Jalur B** (`scripts/import_local.py`).    |
| Skrip lokal gagal connect DB              | `DATABASE_URL` di `.env` salah/kosong (pakai `DATABASE_PUBLIC_URL`).   |

> **Jangan** `pg_terminate_backend` untuk "membersihkan" impor yang menggantung — itu
> mengganggu infra bersama. Hentikan proses Python lokal saja; server akan mereap
> koneksi yatimnya sendiri.
