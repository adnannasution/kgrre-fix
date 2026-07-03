"""
ETL Pipeline — baca file Excel mentah → knowledge graph CSV → import ke PostgreSQL.
Dijalankan di background thread; browser bisa ditutup setelah upload selesai.
"""
from __future__ import annotations

import json
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import duckdb
import openpyxl

from .config import UPLOADS_DIR
from .importer import JOBS, ImportJob, _create_job, _run_import
from .scanner import scan_package

ETL_JOBS: dict[str, ImportJob] = {}
ETL_JOBS_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# Sheet domain detection — berbasis konten kolom (nama file & sheet bebas)
# ---------------------------------------------------------------------------

def _col_set(headers: list[str]) -> set[str]:
    """Normalisasi header kolom ke set lowercase underscore."""
    return {re.sub(r'[^a-z0-9]+', '_', h.strip().lower()).strip('_') for h in headers if h and str(h).strip()}


def _detect_domain_by_columns(headers: list[str]) -> str | None:
    """Deteksi domain dari nama kolom — tidak perlu nama file tertentu."""
    cols = _col_set(headers)

    def has(*names: str) -> bool:
        return any(c in cols for c in names)

    def has_sub(sub: str) -> bool:
        return any(sub in c for c in cols)

    # PLO — kolom sangat spesifik
    if has('nomor_ijin', 'no_ijin', 'permit_number', 'nama_plo', 'status_plo'):
        return 'plo'
    if has('date_expired', 'masa_berlaku') and has('kapasitas', 'cakupan', 'nama_plo', 'nomor_ijin'):
        return 'plo'

    # OA Issue — mi_pi sangat khas
    if has('mi_pi', 'mip'):
        return 'oa_issue'

    # ICU Issue — mitigation/permanent_solution sangat spesifik
    if has('mitigation', 'permanent_solution') or (has('icu_status', 'icu') and has('issue', 'deskripsi_issue')):
        return 'icu_issue'
    if has('target_closed', 'tanggal_target') and has('mitigation') and has('issue', 'deskripsi_issue'):
        return 'icu_issue'

    # Readiness — status_operation sangat khas dan tidak ada di domain lain
    if has('status_operation', 'status_operasi'):
        return 'readiness'
    if has('status_item', 'rtl') and has('period_date', 'month_update'):
        return 'readiness'

    # OA Availability
    if has('actual_target') and has('value_perc', 'month_update', 'bulan'):
        return 'oa_availability'
    if has('operational_availability') or (has_sub('oa') and has('value_perc', 'actual_target')):
        return 'oa_availability'

    # Reliability — mtbf/mttr/running hours sangat spesifik
    if has('mtbf', 'mttr', 'reliability_index'):
        return 'reliability'
    if has('running_hours', 'jam_operasi', 'run_hours', 'operating_hours'):
        return 'reliability'
    if has('failure_date', 'failure_mode', 'breakdown_date') and has('equipment', 'tag_number', 'tag_no'):
        return 'reliability'

    # RKAP — cost_program/cost_element spesifik
    if has('cost_program', 'cost_element', 'cost_center'):
        return 'rkap'
    if has_sub('rkap') or has_sub('irkap'):
        return 'rkap'
    if has('plan_idr', 'actual_idr', 'budget_idr') or (has('plan', 'actual', 'realisasi') and has('program', 'kegiatan')):
        return 'rkap'

    # Inspection plan
    if has_sub('inspection') and has('equipment', 'tag_number', 'tag_no', 'functional_loc', 'functional_location'):
        return 'inspection'
    if has('next_inspection', 'last_inspection', 'inspection_date', 'due_inspection'):
        return 'inspection'

    # RCPS — root cause
    if has('root_cause', 'akar_masalah') or (has('recommendation', 'rekomendasi') and has('finding', 'temuan')):
        return 'rcps_recommendation'
    if has_sub('rcps') or has('fishbone', 'why_1', 'why_2', 'why_3'):
        return 'rcps'

    # Org issue (sederhana) — issue list tanpa mitigation detail
    if has('issue', 'deskripsi_issue', 'paf') and has('responsible', 'pic', 'penanggung_jawab', 'priority'):
        return 'org_issue'

    # Equipment Master vs Maintenance Order — paling ambigu
    eq_score = 0
    mo_score = 0

    # Equipment master signals — SAP & Indonesian naming
    if has('functional_loc', 'functional_location', 'floc', 'lokasi_fungsi', 'functional_loc_no'): eq_score += 4
    if has('maintplant', 'planning_plant', 'maint_plant', 'pabrik', 'plant_kode'): eq_score += 3
    if has('catalog_profile', 'equipment_group', 'object_type', 'tipe_equipment', 'jenis_equipment', 'equipment_type', 'object_class'): eq_score += 3
    if has('criticallity', 'criticality', 'critical_rank', 'kritikalitas', 'tingkat_kritis', 'criticality_rank', 'kelas_kritis'): eq_score += 3
    if has('description_of_technical_object', 'keterangan_equipment', 'nama_equipment'): eq_score += 3
    if has('equipment', 'tag_number', 'tag_no', 'tag_num', 'kode_equipment', 'no_equipment', 'equipment_no', 'nomor_equipment', 'nomor_tag', 'tag_alat'): eq_score += 2
    if has('plant_area', 'location', 'plant_section', 'area', 'area_plant', 'lokasi'): eq_score += 1
    if has('date_update_data', 'tanggal_update', 'last_update', 'update_date') and not has('order', 'aufnr'): eq_score += 2

    # Maintenance order signals
    if has('order', 'order_number', 'aufnr', 'maint_order', 'order_no', 'nomor_order'): mo_score += 4
    if has('work_center', 'main_workcenter', 'arbpl', 'workcenter', 'pusat_kerja'):     mo_score += 4
    if has('order_type', 'auart', 'order_category', 'tipe_order'):                      mo_score += 3
    if has('notification', 'notification_no', 'qmnum', 'nomor_notifikasi'):             mo_score += 3
    if has('system_status', 'user_status', 'sttxt', 'status_sistem'):                  mo_score += 3
    if has('actual_start', 'actual_finish', 'basic_start', 'basic_finish', 'gstrp', 'gltrp'): mo_score += 2
    if has('equipment', 'tag_number', 'tag_no'):                                         mo_score += 1

    if eq_score >= 5 and eq_score > mo_score:
        return 'equipment'
    if mo_score >= 5 and mo_score > eq_score:
        return 'maintenance'
    if eq_score >= 4 and eq_score > mo_score + 2:
        return 'equipment'
    if mo_score >= 4 and mo_score > eq_score + 2:
        return 'maintenance'

    return None


def _detect_domain(filename: str, sheet: str, headers: list[str] | None = None) -> str | None:
    """Deteksi domain sheet. Kolom adalah sinyal utama; nama file/sheet hanya fallback."""
    # 1. Deteksi dari konten kolom (paling reliable — bebas nama file)
    if headers:
        by_col = _detect_domain_by_columns(headers)
        if by_col:
            return by_col

    # 2. Fallback ke nama file/sheet untuk kasus ambigu atau kolom kosong
    stem = Path(filename).stem.lower()
    sheet_l = sheet.lower()
    k = f"{stem} {sheet_l}"

    if any(x in stem for x in ("all_ru_equipment", "equipment_master", "master_equipment",
                                "daftar_equipment", "daftar_alat", "master_alat",
                                "equipment_list", "tag_list", "tag_register")):    return "equipment"
    if "all_ru_equipment" in stem or "equipment_data" in stem:                     return "equipment"
    if stem.startswith(("pt02_", "pt03_")):                                        return "maintenance"
    if any(x in k for x in ("vw_reportirkapplanactual", "cost_program")) \
            or (any(x in k for x in ("rkap", "irkap")) and "alias_map" not in stem):
        return "rkap"
    if stem.startswith(("running_hours_", "n_0_")):                   return "reliability"
    if "inspection_plan" in k:                                         return "inspection"
    if any(x in k for x in ("apr_", "readiness_atg", "power_steam", "weekly_monitoring")):
        return "readiness"
    if any(x in k for x in ("issue_list", "paf_issue")):             return "org_issue"
    if any(x in k for x in ("icu_database", "icu")):                 return "icu_issue"
    if "rcps" in stem:
        return "rcps_recommendation" if any(x in sheet_l for x in ("rekomendasi", "recommendation")) else "rcps"
    if "oa_data" in stem or "oa_" in stem:
        if any(x in sheet_l for x in ("allowance", "unplanned")):    return "oa_allowance"
        if any(x in sheet_l for x in ("issue", "permasalahan")):     return "oa_issue"
        return "oa_availability"
    if stem.startswith("plo") or "plo_" in stem:                      return "plo"
    return None


# ---------------------------------------------------------------------------
# Excel → DuckDB loader
# ---------------------------------------------------------------------------

def _read_sheet_rows(path: Path, sheet_name: str) -> list[tuple]:
    """Baca baris sheet. Coba read_only=True dulu; fallback ke normal jika hasilnya <= 1 baris."""
    for read_only in (True, False):
        wb = openpyxl.load_workbook(path, read_only=read_only, data_only=True)
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        wb.close()
        if len(rows) > 1:
            return rows
        if not read_only:
            break
    return rows


def _load_excel_to_duckdb(con: duckdb.DuckDBPyConnection, path: Path) -> list[tuple[str, str, str, list[str]]]:
    """Baca semua sheet dari Excel, load ke DuckDB. Return list (table_name, filename, sheet, raw_headers)."""
    import pandas as pd
    loaded: list[tuple[str, str, str, list[str]]] = []

    wb_meta = openpyxl.load_workbook(path, read_only=True, data_only=True)
    sheet_names = wb_meta.sheetnames
    wb_meta.close()

    for sheet_name in sheet_names:
        rows = _read_sheet_rows(path, sheet_name)
        if not rows:
            continue
        raw_headers = [str(c).strip() if c is not None else f"col_{i}" for i, c in enumerate(rows[0])]

        # deduplikasi header untuk DuckDB
        seen: dict[str, int] = {}
        clean_headers: list[str] = []
        for h in raw_headers:
            h_clean = re.sub(r'[^a-zA-Z0-9_]', '_', h).lower() or "col"
            if h_clean in seen:
                seen[h_clean] += 1
                h_clean = f"{h_clean}_{seen[h_clean]}"
            else:
                seen[h_clean] = 0
            clean_headers.append(h_clean)

        data_rows = [
            tuple(str(v).strip() if v is not None else None for v in row)
            for row in rows[1:]
        ]
        if not data_rows:
            continue

        df = pd.DataFrame(data_rows, columns=clean_headers)
        df["_input_source_file"] = path.name
        df["_input_source_sheet"] = sheet_name
        df["_source_row"] = range(2, len(df) + 2)

        tname = f"src_{uuid.uuid4().hex[:8]}"
        con.register(tname, df)
        loaded.append((tname, path.name, sheet_name, raw_headers))

    return loaded


# ---------------------------------------------------------------------------
# DuckDB macros & reference tables
# ---------------------------------------------------------------------------

_MACROS_SQL = """
CREATE OR REPLACE MACRO norm_text(x) AS
    nullif(trim(regexp_replace(upper(coalesce(cast(x AS VARCHAR), '')),
                               '[^A-Z0-9]+', ' ', 'g')), '');

CREATE OR REPLACE MACRO norm_code(x) AS
    nullif(regexp_replace(upper(coalesce(cast(x AS VARCHAR), '')),
                          '[^A-Z0-9]+', '', 'g'), '');

CREATE OR REPLACE MACRO norm_equipment(x) AS norm_text(x);

CREATE OR REPLACE MACRO ru_normalize(x) AS (
    CASE
        WHEN norm_text(x) IN ('R201','R202','K201','K202','6201','6202')
          OR regexp_matches(norm_text(x), '(^| )RU ?(II|2)( |$)') THEN 'RU II'
        WHEN norm_text(x) IN ('R301','K301','6301')
          OR regexp_matches(norm_text(x), '(^| )RU ?(III|3)( |$)') THEN 'RU III'
        WHEN norm_text(x) IN ('R401','R402','R403','K401','6401')
          OR regexp_matches(norm_text(x), '(^| )RU ?(IV|4)( |$)') THEN 'RU IV'
        WHEN norm_text(x) IN ('R501','K501','6501')
          OR regexp_matches(norm_text(x), '(^| )RU ?(V|5)( |$)') THEN 'RU V'
        WHEN norm_text(x) IN ('R601','K601','6601')
          OR regexp_matches(norm_text(x), '(^| )RU ?(VI|6)( |$)') THEN 'RU VI'
        WHEN norm_text(x) IN ('R701','K701','6701')
          OR regexp_matches(norm_text(x), '(^| )RU ?(VII|7)( |$)') THEN 'RU VII'
        ELSE NULL
    END
);

CREATE OR REPLACE MACRO ru_from_filename(x) AS (
    CASE
        WHEN regexp_matches(lower(coalesce(x,'')), 'ru[_ -]?ii([^iv]|$)') THEN 'RU II'
        WHEN regexp_matches(lower(coalesce(x,'')), 'ru[_ -]?iii') THEN 'RU III'
        WHEN regexp_matches(lower(coalesce(x,'')), 'ru[_ -]?iv([^i]|$)') THEN 'RU IV'
        WHEN regexp_matches(lower(coalesce(x,'')), 'ru[_ -]?v([^i]|$)') THEN 'RU V'
        WHEN regexp_matches(lower(coalesce(x,'')), 'ru[_ -]?vi([^i]|$)') THEN 'RU VI'
        WHEN regexp_matches(lower(coalesce(x,'')), 'ru[_ -]?vii') THEN 'RU VII'
        ELSE NULL
    END
);
"""

_REFERENCE_SQL = """
CREATE TABLE IF NOT EXISTS ru_reference (
    refinery_unit VARCHAR, refinery_unit_id VARCHAR,
    display_order INTEGER, site_name VARCHAR
);
INSERT INTO ru_reference VALUES
    ('RU II',  'node_ru_' || md5('RU II'),  2, 'Dumai'),
    ('RU III', 'node_ru_' || md5('RU III'), 3, 'Plaju'),
    ('RU IV',  'node_ru_' || md5('RU IV'),  4, 'Cilacap'),
    ('RU V',   'node_ru_' || md5('RU V'),   5, 'Balikpapan'),
    ('RU VI',  'node_ru_' || md5('RU VI'),  6, 'Balongan'),
    ('RU VII', 'node_ru_' || md5('RU VII'), 7, 'Kasim');

CREATE TABLE IF NOT EXISTS plant_ru_map (plant VARCHAR, refinery_unit VARCHAR);
INSERT INTO plant_ru_map VALUES
    ('R201','RU II'),('R202','RU II'),('K201','RU II'),('K202','RU II'),
    ('6201','RU II'),('6202','RU II'),
    ('R301','RU III'),('K301','RU III'),('6301','RU III'),
    ('R401','RU IV'),('R402','RU IV'),('R403','RU IV'),('K401','RU IV'),('6401','RU IV'),
    ('R501','RU V'),('K501','RU V'),('6501','RU V'),
    ('R601','RU VI'),('K601','RU VI'),('6601','RU VI'),
    ('R701','RU VII'),('K701','RU VII'),('6701','RU VII');

CREATE TABLE node_raw (
    node_id VARCHAR, node_type VARCHAR, business_key VARCHAR,
    label VARCHAR, domain VARCHAR, properties_json VARCHAR,
    source_file VARCHAR, source_sheet VARCHAR,
    source_row INTEGER, source_record_id VARCHAR
);

CREATE TABLE relationship_raw (
    source_node_id VARCHAR, target_node_id VARCHAR,
    relationship_type VARCHAR, domain VARCHAR,
    confidence DOUBLE, match_rule VARCHAR, is_candidate BOOLEAN,
    properties_json VARCHAR,
    source_file VARCHAR, source_sheet VARCHAR,
    source_row INTEGER, source_record_id VARCHAR
);

CREATE TABLE unmatched_raw (
    identifier VARCHAR, identifier_type VARCHAR,
    domain VARCHAR, source_file VARCHAR,
    source_sheet VARCHAR, source_row INTEGER, reason VARCHAR
);
"""


# ---------------------------------------------------------------------------
# Helper: safe column reference (hindari "column not found" di DuckDB)
# ---------------------------------------------------------------------------

def _union_cols(con: duckdb.DuckDBPyConnection, views: list[str]) -> set[str]:
    """Kumpulkan semua nama kolom yang ada di sekumpulan views."""
    cols: set[str] = set()
    for v in views:
        cols |= {r[0].lower() for r in con.execute(f'DESCRIBE {v}').fetchall()}
    return cols


def _cs(cols: set[str], *names: str, cast: bool = False, default: str = "''") -> str:
    """Build COALESCE hanya dari kolom yang benar-benar ada. Kolom tidak ada → default."""
    found = [f'cast("{n}" AS VARCHAR)' if cast else f'"{n}"' for n in names if n in cols]
    if not found:
        return default
    return f"coalesce({', '.join(found + [default])})"


# ---------------------------------------------------------------------------
# Node builders
# ---------------------------------------------------------------------------

def _build_ru_plant_nodes(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        INSERT INTO node_raw (node_id, node_type, business_key, label, domain, properties_json)
        SELECT refinery_unit_id, 'refinery_unit', refinery_unit, refinery_unit, 'asset',
               json_object('refinery_unit', refinery_unit, 'site_name', site_name,
                            'display_order', display_order)
        FROM ru_reference
    """)


def _build_equipment_nodes(con: duckdb.DuckDBPyConnection, views: list[str]) -> None:
    if not views:
        return
    c = _union_cols(con, views)
    union_sql = " UNION ALL BY NAME ".join(f"SELECT * FROM {v}" for v in views)
    eq_raw = _cs(c, 'equipment', 'tag_number', 'tag_no', default='NULL')
    plant_expr = f"norm_text({_cs(c, 'maintplant','planning_plant','maint_plant','plant', default='NULL')})"
    ru_expr = f"ru_normalize({_cs(c, 'refinery_unit','ru','refineryunit', default='NULL')})"
    con.execute(f"""
        CREATE TABLE equipment_master AS
        WITH base AS (
            SELECT
                norm_equipment({eq_raw}) AS equipment_code_normalized,
                {eq_raw} AS equipment_code_raw,
                {plant_expr} AS plant,
                coalesce(
                    {ru_expr},
                    (SELECT refinery_unit FROM plant_ru_map
                     WHERE plant = {plant_expr} LIMIT 1),
                    ru_from_filename(_input_source_file)
                ) AS refinery_unit,
                nullif(trim({_cs(c, 'functional_loc','functional_location','floc')}), '') AS functional_location,
                nullif(trim({_cs(c, 'catalog_profile','equipment_group','object_type')}), '') AS equipment_group,
                nullif(trim({_cs(c, 'description_of_technical_object','description','equipment_name')}), '') AS description,
                nullif(trim({_cs(c, 'criticallity','criticality','critical_rank')}), '') AS criticallity,
                nullif(trim({_cs(c, 'location','plant_area','plant_section')}), '') AS plant_area,
                nullif(trim({_cs(c, 'date_update_data','last_update','update_date')}), '') AS date_update_data,
                _input_source_file AS source_file,
                _input_source_sheet AS source_sheet,
                _source_row AS source_row,
                'record_' || md5(_input_source_file || '|' || cast(_source_row AS VARCHAR)) AS source_record_id
            FROM ({union_sql})
            WHERE norm_equipment({eq_raw}) IS NOT NULL
        ),
        ranked AS (
            SELECT *, row_number() OVER (
                PARTITION BY coalesce(refinery_unit, 'UNKNOWN'), equipment_code_normalized
                ORDER BY try_cast(date_update_data AS DATE) DESC NULLS LAST,
                         ((functional_location IS NOT NULL)::INTEGER +
                          (description IS NOT NULL)::INTEGER +
                          (equipment_group IS NOT NULL)::INTEGER) DESC,
                         source_file, source_row DESC
            ) AS rn
            FROM base
        )
        SELECT 'node_equipment_' || md5(coalesce(refinery_unit, 'UNKNOWN') || '|' || equipment_code_normalized) AS equipment_id, *
        FROM ranked WHERE rn = 1
    """)

    con.execute("""
        INSERT INTO node_raw
        SELECT equipment_id, 'equipment',
               coalesce(refinery_unit, 'UNKNOWN') || '|' || equipment_code_normalized,
               coalesce(description, equipment_code_raw), 'asset',
               json_object(
                   'refinery_unit', refinery_unit,
                   'equipment_code_raw', equipment_code_raw,
                   'equipment_code_normalized', equipment_code_normalized,
                   'plant', plant,
                   'functional_location', functional_location,
                   'equipment_group', equipment_group,
                   'description', description,
                   'criticallity', criticallity,
                   'plant_area', plant_area,
                   'derived_ru_normalized', refinery_unit,
                   'derived_equipment_code_compact', norm_code(equipment_code_raw)
               ),
               source_file, source_sheet, source_row, source_record_id
        FROM equipment_master
    """)

    # RU → equipment edges
    con.execute("""
        INSERT INTO relationship_raw (source_node_id, target_node_id, relationship_type,
            domain, confidence, match_rule, is_candidate, properties_json, source_file,
            source_sheet, source_row, source_record_id)
        SELECT r.refinery_unit_id, e.equipment_id, 'REFINERY_UNIT_HAS_EQUIPMENT',
               'asset', 1.0, 'plant_mapping', false,
               json_object('refinery_unit', e.refinery_unit),
               e.source_file, e.source_sheet, e.source_row, e.source_record_id
        FROM equipment_master e
        JOIN ru_reference r ON e.refinery_unit = r.refinery_unit
    """)


def _build_maintenance_nodes(con: duckdb.DuckDBPyConnection, views: list[str]) -> None:
    if not views:
        return
    c = _union_cols(con, views)
    union_sql = " UNION ALL BY NAME ".join(f"SELECT * FROM {v}" for v in views)
    ord_expr = _cs(c, 'order','order_no','order_number','aufnr','maint_order', cast=True, default='NULL')
    ru_expr = f"ru_normalize({_cs(c, 'refinery_unit','ru','plant','maintplant','refineryunit', default='NULL')})"
    con.execute(f"""
        CREATE TABLE maintenance_stage AS
        SELECT
            {ord_expr} AS order_raw,
            {_cs(c, 'notification','notif','qmnum', cast=True)} AS notification_raw,
            norm_code({ord_expr}) AS order_code,
            {_cs(c, 'description','kurztext','short_text','order_description')} AS order_desc,
            {_cs(c, 'order_type','auart','order_category')} AS order_type,
            {_cs(c, 'priority','priok')} AS priority,
            {_cs(c, 'user_status','txt04','ustatus')} AS user_status,
            {_cs(c, 'system_status','sttxt')} AS system_status,
            {_cs(c, 'reference_date','gstrp','basic_start', cast=True)} AS reference_date,
            {_cs(c, 'total_planned_costs','geplk','planned_cost', cast=True, default="'0'")} AS planned_cost,
            {_cs(c, 'total_actual_costs','istko','actual_cost', cast=True, default="'0'")} AS actual_cost,
            {_cs(c, 'main_work_center','main_workcenter','arbpl','work_center','workcenter')} AS work_center,
            {_cs(c, 'equipment','tag_number','tag_no','equnr')} AS equipment_raw,
            coalesce({ru_expr}, ru_from_filename(_input_source_file)) AS refinery_unit,
            _input_source_file AS source_file,
            _input_source_sheet AS source_sheet,
            _source_row AS source_row,
            'record_' || md5(_input_source_file || '|' || cast(_source_row AS VARCHAR)) AS source_record_id
        FROM ({union_sql})
        WHERE {ord_expr} IS NOT NULL
    """)

    # Tambah derived columns
    con.execute("""
        ALTER TABLE maintenance_stage ADD COLUMN order_id VARCHAR;
        UPDATE maintenance_stage
        SET order_id = 'node_order_' || md5(refinery_unit || '|' || order_code)
        WHERE order_code IS NOT NULL AND refinery_unit IS NOT NULL;

        ALTER TABLE maintenance_stage ADD COLUMN equipment_id VARCHAR;
        UPDATE maintenance_stage SET equipment_id = e.equipment_id
        FROM equipment_master e
        WHERE maintenance_stage.refinery_unit = e.refinery_unit
          AND norm_code(maintenance_stage.equipment_raw) = norm_code(e.equipment_code_raw)
          AND maintenance_stage.equipment_raw IS NOT NULL;
    """)

    con.execute("""
        INSERT INTO node_raw
        SELECT DISTINCT order_id, 'maintenance_order',
               refinery_unit || '|' || order_code,
               coalesce(nullif(order_desc,''), order_raw), 'maintenance',
               json_object(
                   'refinery_unit', refinery_unit,
                   'order_type', order_type,
                   'priority', priority,
                   'user_status', user_status,
                   'system_status', system_status,
                   'reference_date', reference_date,
                   'derived_planned_cost', planned_cost,
                   'derived_actual_cost', actual_cost,
                   'work_center', work_center,
                   'derived_is_open_order',
                       CASE WHEN user_status ILIKE '%WAMA%' OR user_status ILIKE '%WAMA%'
                            THEN 'true' ELSE 'false' END,
                   'derived_status_bucket',
                       CASE WHEN user_status ILIKE '%WAMA%' THEN 'WAMA'
                            WHEN user_status ILIKE '%WASR%' THEN 'WASR'
                            WHEN system_status ILIKE '%TECO%' THEN 'TECO'
                            WHEN system_status ILIKE '%CLSD%' THEN 'CLSD'
                            ELSE 'OPEN' END
               ),
               source_file, source_sheet, source_row, source_record_id
        FROM maintenance_stage WHERE order_id IS NOT NULL
    """)

    # EQUIPMENT_HAS_MAINTENANCE_ORDER
    con.execute("""
        INSERT INTO relationship_raw (source_node_id, target_node_id, relationship_type,
            domain, confidence, match_rule, is_candidate, properties_json,
            source_file, source_sheet, source_row, source_record_id)
        SELECT equipment_id, order_id, 'EQUIPMENT_HAS_MAINTENANCE_ORDER',
               'maintenance', 1.0, 'ru_and_equipment_exact', false,
               json_object('match_token', equipment_raw, 'source_ru', refinery_unit),
               source_file, source_sheet, source_row, source_record_id
        FROM maintenance_stage WHERE equipment_id IS NOT NULL AND order_id IS NOT NULL
    """)

    # Unmatched equipment di maintenance
    con.execute("""
        INSERT INTO unmatched_raw
        SELECT equipment_raw, 'equipment', 'maintenance', source_file, source_sheet, source_row,
               'Equipment tidak ditemukan di master'
        FROM maintenance_stage
        WHERE equipment_id IS NULL AND nullif(trim(equipment_raw),'') IS NOT NULL
    """)


def _build_rkap_nodes(con: duckdb.DuckDBPyConnection, views: list[str]) -> None:
    if not views:
        return
    c = _union_cols(con, views)
    union_sql = " UNION ALL BY NAME ".join(f"SELECT * FROM {v}" for v in views)
    ru_expr = f"ru_normalize({_cs(c, 'refineryunit','refinery_unit','ru','plant', default='NULL')})"
    con.execute(f"""
        CREATE TABLE rkap_stage AS
        SELECT
            {_cs(c, 'noprogram','no_program','program_no','programkerja','program_kerja','nama_program','program_name')} AS program_no,
            {_cs(c, 'programkerja','program_kerja','nama_program','program_name','noprogram','no_program')} AS program_name,
            {_cs(c, 'equipment','tag_number','tag_no')} AS equipment_raw,
            {_cs(c, 'katergorirkap','kategori_rkap','kategori')} AS kategori,
            {_cs(c, 'kelompokbiaya','kelompok_biaya')} AS kelompok_biaya,
            {_cs(c, 'disiplin','discipline')} AS disiplin,
            {_cs(c, 'fiscalyear','fiscal_year','tahun', cast=True)} AS fiscal_year,
            {_cs(c, 'planstart','plan_start', cast=True)} AS plan_start,
            {_cs(c, 'planfinish','plan_finish', cast=True)} AS plan_finish,
            {_cs(c, 'actualstart','actual_start', cast=True)} AS actual_start,
            {_cs(c, 'actualfinish','actual_finish', cast=True)} AS actual_finish,
            {_cs(c, 'statusactual','status_actual','status')} AS status_actual,
            {_cs(c, 'totalequivalentidr','total_equivalent_idr', cast=True, default="'0'")} AS total_idr,
            {_cs(c, 'toprisk','top_risk', cast=True)} AS top_risk,
            coalesce({ru_expr}, ru_from_filename(_input_source_file)) AS refinery_unit,
            _input_source_file AS source_file,
            _input_source_sheet AS source_sheet,
            _source_row AS source_row,
            'record_' || md5(_input_source_file || '|' || cast(_source_row AS VARCHAR)) AS source_record_id
        FROM ({union_sql})
        WHERE nullif(trim({_cs(c, 'noprogram','no_program','program_no','programkerja','program_kerja','nama_program','program_name')}), '') IS NOT NULL
    """)

    con.execute("""
        ALTER TABLE rkap_stage ADD COLUMN program_id VARCHAR;
        UPDATE rkap_stage
        SET program_id = 'node_rkap_program_' || md5(refinery_unit || '|' ||
                          norm_code(program_no) || '|' || norm_code(program_name))
        WHERE refinery_unit IS NOT NULL;

        ALTER TABLE rkap_stage ADD COLUMN equipment_id VARCHAR;
        UPDATE rkap_stage SET equipment_id = e.equipment_id
        FROM equipment_master e
        WHERE rkap_stage.refinery_unit = e.refinery_unit
          AND norm_code(rkap_stage.equipment_raw) = norm_code(e.equipment_code_raw)
          AND nullif(trim(rkap_stage.equipment_raw),'') IS NOT NULL;
    """)

    con.execute("""
        INSERT INTO node_raw
        SELECT DISTINCT program_id, 'rkap_program',
               refinery_unit || '|' || program_no,
               coalesce(nullif(program_name,''), program_no), 'cost_program',
               json_object(
                   'refinery_unit', refinery_unit,
                   'program_no', program_no,
                   'kategori', kategori,
                   'kelompok_biaya', kelompok_biaya,
                   'disiplin', disiplin,
                   'fiscal_year', fiscal_year,
                   'plan_start', plan_start,
                   'plan_finish', plan_finish,
                   'status_actual', status_actual,
                   'derived_total_equivalent_idr_num', total_idr,
                   'derived_is_top_risk', top_risk,
                   'derived_is_delayed',
                       CASE WHEN status_actual ILIKE '%delay%' OR status_actual ILIKE '%terlambat%'
                            THEN 'true' ELSE 'false' END
               ),
               source_file, source_sheet, source_row, source_record_id
        FROM rkap_stage WHERE program_id IS NOT NULL
    """)

    # RU → RKAP
    con.execute("""
        INSERT INTO relationship_raw (source_node_id, target_node_id, relationship_type,
            domain, confidence, match_rule, is_candidate, properties_json,
            source_file, source_sheet, source_row, source_record_id)
        SELECT r.refinery_unit_id, s.program_id, 'REFINERY_UNIT_HAS_RKAP_PROGRAM',
               'cost_program', 1.0, 'refinery_unit_direct', false,
               json_object('refinery_unit', s.refinery_unit),
               s.source_file, s.source_sheet, s.source_row, s.source_record_id
        FROM rkap_stage s
        JOIN ru_reference r ON s.refinery_unit = r.refinery_unit
        WHERE s.program_id IS NOT NULL
    """)

    # EQUIPMENT → RKAP (exact match)
    con.execute("""
        INSERT INTO relationship_raw (source_node_id, target_node_id, relationship_type,
            domain, confidence, match_rule, is_candidate, properties_json,
            source_file, source_sheet, source_row, source_record_id)
        SELECT equipment_id, program_id, 'EQUIPMENT_HAS_RKAP_PROGRAM',
               'cost_program', 1.0, 'ru_and_equipment_exact', false,
               json_object('match_token', equipment_raw, 'source_ru', refinery_unit),
               source_file, source_sheet, source_row, source_record_id
        FROM rkap_stage WHERE equipment_id IS NOT NULL AND program_id IS NOT NULL
    """)

    # EQUIPMENT → RKAP (token fallback)
    con.execute("""
        INSERT INTO relationship_raw (source_node_id, target_node_id, relationship_type,
            domain, confidence, match_rule, is_candidate, properties_json,
            source_file, source_sheet, source_row, source_record_id)
        SELECT DISTINCT e.equipment_id, s.program_id, 'EQUIPMENT_HAS_RKAP_PROGRAM',
               'cost_program', 0.80, 'rkap_equipment_token_match', true,
               json_object('match_token', trim(token), 'source_ru', s.refinery_unit),
               s.source_file, s.source_sheet, s.source_row, s.source_record_id
        FROM rkap_stage s,
             unnest(string_split(
                 regexp_replace(upper(coalesce(s.equipment_raw,'')), '[,;&]+', ';', 'g'), ';'
             )) t(token)
        JOIN equipment_master e
          ON s.refinery_unit = e.refinery_unit
         AND norm_code(token) = norm_code(e.equipment_code_raw)
        WHERE s.equipment_id IS NULL AND length(norm_code(token)) >= 4
          AND s.program_id IS NOT NULL
    """)


def _build_reliability_nodes(con: duckdb.DuckDBPyConnection, views: list[str]) -> None:
    if not views:
        return
    c = _union_cols(con, views)
    union_sql = " UNION ALL BY NAME ".join(f"SELECT * FROM {v}" for v in views)
    eq_expr = _cs(c, 'equipment','tag_number','tag_no')
    ru_expr = f"ru_normalize({_cs(c, 'refinery_unit','ru','plant','refineryunit', default='NULL')})"
    con.execute(f"""
        CREATE TABLE reliability_stage AS
        SELECT
            {eq_expr} AS equipment_raw,
            {_cs(c, 'hasil','status','result')} AS hasil,
            {_cs(c, 'running_hours','run_hours','operating_hours','jam_operasi', cast=True, default="'0'")} AS running_hours,
            {_cs(c, 'mtbf', cast=True, default="'0'")} AS mtbf,
            {_cs(c, 'mttr', cast=True, default="'0'")} AS mttr,
            {_cs(c, 'minggu','week', cast=True)} AS minggu,
            {_cs(c, 'bulan','month_update','month','period', cast=True)} AS bulan,
            {_cs(c, 'tahun','year','fiscal_year', cast=True)} AS tahun,
            coalesce({ru_expr}, ru_from_filename(_input_source_file)) AS refinery_unit,
            _input_source_file AS source_file,
            _input_source_sheet AS source_sheet,
            _source_row AS source_row,
            'record_' || md5(_input_source_file || '|' || cast(_source_row AS VARCHAR)) AS source_record_id
        FROM ({union_sql})
        WHERE nullif(trim({eq_expr}), '') IS NOT NULL
    """)

    con.execute("""
        ALTER TABLE reliability_stage ADD COLUMN obs_id VARCHAR;
        UPDATE reliability_stage
        SET obs_id = 'node_reliability_' || md5(source_record_id);

        ALTER TABLE reliability_stage ADD COLUMN equipment_id VARCHAR;
        UPDATE reliability_stage SET equipment_id = e.equipment_id
        FROM equipment_master e
        WHERE reliability_stage.refinery_unit = e.refinery_unit
          AND norm_code(reliability_stage.equipment_raw) = norm_code(e.equipment_code_raw);
    """)

    con.execute("""
        INSERT INTO node_raw
        SELECT obs_id, 'reliability_observation',
               source_record_id, coalesce(nullif(hasil,''), 'Reliability observation'),
               'reliability',
               json_object(
                   'refinery_unit', refinery_unit,
                   'equipment_raw', equipment_raw,
                   'hasil', hasil,
                   'running_hours', running_hours,
                   'mtbf', mtbf,
                   'mttr', mttr,
                   'minggu', minggu,
                   'bulan', bulan,
                   'tahun', tahun
               ),
               source_file, source_sheet, source_row, source_record_id
        FROM reliability_stage WHERE obs_id IS NOT NULL
    """)

    con.execute("""
        INSERT INTO relationship_raw (source_node_id, target_node_id, relationship_type,
            domain, confidence, match_rule, is_candidate, properties_json,
            source_file, source_sheet, source_row, source_record_id)
        SELECT equipment_id, obs_id, 'EQUIPMENT_HAS_RELIABILITY_OBSERVATION',
               'reliability', 1.0, 'ru_and_equipment_exact', false,
               json_object('match_token', equipment_raw),
               source_file, source_sheet, source_row, source_record_id
        FROM reliability_stage WHERE equipment_id IS NOT NULL AND obs_id IS NOT NULL
    """)


def _build_inspection_nodes(con: duckdb.DuckDBPyConnection, views: list[str]) -> None:
    if not views:
        return
    c = _union_cols(con, views)
    union_sql = " UNION ALL BY NAME ".join(f"SELECT * FROM {v}" for v in views)
    eq_expr = _cs(c, 'tag_no_ln','tag_number','tag_no','equipment')
    ru_expr = f"ru_normalize({_cs(c, 'refinery_unit','ru','plant', default='NULL')})"
    con.execute(f"""
        CREATE TABLE inspection_stage AS
        SELECT
            {eq_expr} AS equipment_raw,
            {_cs(c, 'type_inspection','jenis_inspeksi','inspection_type')} AS type_inspection,
            {_cs(c, 'type_pekerjaan','jenis_pekerjaan','work_type')} AS type_pekerjaan,
            {_cs(c, 'plan_date','tanggal_rencana','inspection_date','next_inspection', cast=True)} AS plan_date,
            {_cs(c, 'actual_date','tanggal_aktual','last_inspection', cast=True)} AS actual_date,
            {_cs(c, 'grand_result','hasil','result','inspection_result')} AS grand_result,
            coalesce({ru_expr}, ru_from_filename(_input_source_file)) AS refinery_unit,
            _input_source_file AS source_file,
            _input_source_sheet AS source_sheet,
            _source_row AS source_row,
            'record_' || md5(_input_source_file || '|' || cast(_source_row AS VARCHAR)) AS source_record_id
        FROM ({union_sql})
        WHERE nullif(trim({eq_expr}), '') IS NOT NULL
    """)

    con.execute("""
        ALTER TABLE inspection_stage ADD COLUMN inspection_id VARCHAR;
        UPDATE inspection_stage
        SET inspection_id = 'node_inspection_' || md5(source_record_id);

        ALTER TABLE inspection_stage ADD COLUMN equipment_id VARCHAR;
        UPDATE inspection_stage SET equipment_id = e.equipment_id
        FROM equipment_master e
        WHERE inspection_stage.refinery_unit = e.refinery_unit
          AND norm_code(inspection_stage.equipment_raw) = norm_code(e.equipment_code_raw);
    """)

    con.execute("""
        INSERT INTO node_raw
        SELECT inspection_id, 'inspection', source_record_id,
               coalesce(nullif(type_inspection,''), 'Inspection'), 'inspection_issue',
               json_object(
                   'refinery_unit', refinery_unit,
                   'equipment_raw', equipment_raw,
                   'type_inspection', type_inspection,
                   'type_pekerjaan', type_pekerjaan,
                   'plan_date', plan_date,
                   'actual_date', actual_date,
                   'grand_result', grand_result
               ),
               source_file, source_sheet, source_row, source_record_id
        FROM inspection_stage WHERE inspection_id IS NOT NULL
    """)

    con.execute("""
        INSERT INTO relationship_raw (source_node_id, target_node_id, relationship_type,
            domain, confidence, match_rule, is_candidate, properties_json,
            source_file, source_sheet, source_row, source_record_id)
        SELECT equipment_id, inspection_id, 'EQUIPMENT_HAS_INSPECTION',
               'inspection_issue', 1.0, 'ru_and_equipment_exact', false,
               json_object('match_token', equipment_raw),
               source_file, source_sheet, source_row, source_record_id
        FROM inspection_stage WHERE equipment_id IS NOT NULL AND inspection_id IS NOT NULL
    """)


def _build_icu_issue_nodes(con: duckdb.DuckDBPyConnection, views: list[str]) -> None:
    if not views:
        return
    c = _union_cols(con, views)
    union_sql = " UNION ALL BY NAME ".join(f"SELECT * FROM {v}" for v in views)
    issue_expr = _cs(c, 'issue', 'deskripsi_issue', 'description')
    ru_expr = f"ru_normalize({_cs(c, 'refinery_unit','ru','kilang','plant', default='NULL')})"
    con.execute(f"""
        CREATE TABLE icu_stage AS
        SELECT
            {_cs(c, 'tag_no','tag_number','equipment')} AS equipment_raw,
            {issue_expr} AS issue_text,
            {_cs(c, 'icu_status','status')} AS icu_status,
            {_cs(c, 'report_date','tanggal', cast=True)} AS report_date,
            {_cs(c, 'target_closed', cast=True)} AS target_closed,
            {_cs(c, 'mitigation_temporary_solution','mitigation','solusi_sementara')} AS mitigation,
            {_cs(c, 'permanent_solution','solusi_permanen')} AS permanent_solution,
            coalesce(
                {ru_expr},
                ru_from_filename(_input_source_file)
            ) AS refinery_unit,
            _input_source_file AS source_file,
            _input_source_sheet AS source_sheet,
            _source_row AS source_row,
            'record_' || md5(_input_source_file || '|' || cast(_source_row AS VARCHAR)) AS source_record_id
        FROM ({union_sql})
        WHERE nullif(trim({issue_expr}), '') IS NOT NULL
    """)

    con.execute("""
        ALTER TABLE icu_stage ADD COLUMN issue_id VARCHAR;
        UPDATE icu_stage SET issue_id = 'node_issue_' || md5(source_record_id);

        ALTER TABLE icu_stage ADD COLUMN equipment_id VARCHAR;
        UPDATE icu_stage SET equipment_id = e.equipment_id
        FROM equipment_master e
        WHERE icu_stage.refinery_unit = e.refinery_unit
          AND norm_code(icu_stage.equipment_raw) = norm_code(e.equipment_code_raw)
          AND nullif(trim(icu_stage.equipment_raw),'') IS NOT NULL;
    """)

    con.execute("""
        INSERT INTO node_raw
        SELECT issue_id, 'equipment_issue', source_record_id,
               coalesce(nullif(left(issue_text, 80), ''), 'Issue'), 'inspection_issue',
               json_object(
                   'refinery_unit', refinery_unit,
                   'equipment_raw', equipment_raw,
                   'issue', issue_text,
                   'icu_status', icu_status,
                   'report_date', report_date,
                   'target_closed', target_closed,
                   'mitigation', mitigation,
                   'permanent_solution', permanent_solution
               ),
               source_file, source_sheet, source_row, source_record_id
        FROM icu_stage WHERE issue_id IS NOT NULL
    """)

    con.execute("""
        INSERT INTO relationship_raw (source_node_id, target_node_id, relationship_type,
            domain, confidence, match_rule, is_candidate, properties_json,
            source_file, source_sheet, source_row, source_record_id)
        SELECT equipment_id, issue_id, 'EQUIPMENT_HAS_ISSUE',
               'inspection_issue', 1.0, 'ru_and_equipment_exact', false,
               json_object('match_token', equipment_raw),
               source_file, source_sheet, source_row, source_record_id
        FROM icu_stage WHERE equipment_id IS NOT NULL AND issue_id IS NOT NULL
    """)

    # RU → Issue shortcut
    con.execute("""
        INSERT INTO relationship_raw (source_node_id, target_node_id, relationship_type,
            domain, confidence, match_rule, is_candidate, properties_json,
            source_file, source_sheet, source_row, source_record_id)
        SELECT r.refinery_unit_id, s.issue_id, 'REFINERY_UNIT_HAS_ISSUE',
               'inspection_issue', 1.0, 'refinery_unit_direct', false,
               json_object('refinery_unit', s.refinery_unit),
               s.source_file, s.source_sheet, s.source_row, s.source_record_id
        FROM icu_stage s
        JOIN ru_reference r ON s.refinery_unit = r.refinery_unit
        WHERE s.issue_id IS NOT NULL
    """)


def _build_readiness_nodes(con: duckdb.DuckDBPyConnection, views: list[str]) -> None:
    if not views:
        return
    c = _union_cols(con, views)
    union_sql = " UNION ALL BY NAME ".join(f"SELECT * FROM {v}" for v in views)
    ru_expr = f"ru_normalize({_cs(c, 'refinery_unit','ru','kilang','plant', default='NULL')})"
    con.execute(f"""
        CREATE TABLE readiness_stage AS
        SELECT
            {_cs(c, 'equipment','tag_number','tag_no','process_equipment','nama_tangki','no_tangki')} AS equipment_raw,
            {_cs(c, 'period_date','month_update', cast=True)} AS period_date,
            {_cs(c, 'status_operation','status_operasi')} AS status_operation,
            {_cs(c, 'status_item')} AS status_item,
            {_cs(c, 'remark','keterangan')} AS remark,
            {_cs(c, 'rtl')} AS rtl,
            coalesce(
                {ru_expr},
                ru_from_filename(_input_source_file)
            ) AS refinery_unit,
            _input_source_file AS source_file,
            _input_source_sheet AS source_sheet,
            _source_row AS source_row,
            'record_' || md5(_input_source_file || '|' || cast(_source_row AS VARCHAR)) AS source_record_id
        FROM ({union_sql})
    """)

    con.execute("""
        ALTER TABLE readiness_stage ADD COLUMN readiness_id VARCHAR;
        UPDATE readiness_stage SET readiness_id = 'node_readiness_' || md5(source_record_id);

        ALTER TABLE readiness_stage ADD COLUMN equipment_id VARCHAR;
        UPDATE readiness_stage SET equipment_id = e.equipment_id
        FROM equipment_master e
        WHERE readiness_stage.refinery_unit = e.refinery_unit
          AND norm_code(readiness_stage.equipment_raw) = norm_code(e.equipment_code_raw)
          AND nullif(trim(readiness_stage.equipment_raw),'') IS NOT NULL;
    """)

    con.execute("""
        INSERT INTO node_raw
        SELECT readiness_id, 'readiness_record', source_record_id,
               coalesce(nullif(status_operation,''), 'Readiness record'), 'readiness_operation',
               json_object(
                   'refinery_unit', refinery_unit,
                   'equipment_raw', equipment_raw,
                   'period_date', period_date,
                   'status_operation', status_operation,
                   'status_item', status_item,
                   'remark', remark,
                   'rtl', rtl
               ),
               source_file, source_sheet, source_row, source_record_id
        FROM readiness_stage WHERE readiness_id IS NOT NULL
    """)

    con.execute("""
        INSERT INTO relationship_raw (source_node_id, target_node_id, relationship_type,
            domain, confidence, match_rule, is_candidate, properties_json,
            source_file, source_sheet, source_row, source_record_id)
        SELECT equipment_id, readiness_id, 'EQUIPMENT_HAS_READINESS_RECORD',
               'readiness_operation', 1.0, 'ru_and_equipment_exact', false,
               json_object('match_token', equipment_raw),
               source_file, source_sheet, source_row, source_record_id
        FROM readiness_stage WHERE equipment_id IS NOT NULL AND readiness_id IS NOT NULL
    """)

    con.execute("""
        INSERT INTO relationship_raw (source_node_id, target_node_id, relationship_type,
            domain, confidence, match_rule, is_candidate, properties_json,
            source_file, source_sheet, source_row, source_record_id)
        SELECT r.refinery_unit_id, s.readiness_id, 'REFINERY_UNIT_HAS_READINESS_RECORD',
               'readiness_operation', 1.0, 'refinery_unit_direct', false,
               json_object('refinery_unit', s.refinery_unit),
               s.source_file, s.source_sheet, s.source_row, s.source_record_id
        FROM readiness_stage s
        JOIN ru_reference r ON s.refinery_unit = r.refinery_unit
        WHERE s.readiness_id IS NOT NULL
    """)


# ---------------------------------------------------------------------------
# OA Data: Operational Availability, Allowance Unplanned, Issue List
# ---------------------------------------------------------------------------

def _build_oa_nodes(con: duckdb.DuckDBPyConnection,
                    allowance_views: list[str],
                    availability_views: list[str],
                    issue_views: list[str]) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS oa_allowance_stage (
            refinery_unit VARCHAR, mi_pi VARCHAR, allowance_day DOUBLE, unplanned_day DOUBLE, month_update VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS oa_availability_stage (
            refinery_unit VARCHAR, actual_target VARCHAR, value_perc DOUBLE, month_update VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS oa_issue_stage (
            refinery_unit VARCHAR, mi_pi VARCHAR, issue VARCHAR, month_update VARCHAR, date_issue VARCHAR
        )
    """)

    for tname in allowance_views:
        cols = [r[0].lower() for r in con.execute(f"DESCRIBE {tname}").fetchall()]
        ru_col  = next((c for c in cols if 'refinery' in c), cols[0])
        mip_col = next((c for c in cols if 'mi' in c or 'pi' in c), cols[1] if len(cols) > 1 else cols[0])
        alw_col = next((c for c in cols if 'allowance' in c), cols[2] if len(cols) > 2 else cols[0])
        unp_col = next((c for c in cols if 'unplanned' in c), cols[3] if len(cols) > 3 else cols[0])
        mon_col = next((c for c in cols if 'month' in c or 'update' in c), cols[4] if len(cols) > 4 else cols[0])
        con.execute(f"""
            INSERT INTO oa_allowance_stage
            SELECT TRIM(CAST("{ru_col}" AS VARCHAR)), TRIM(CAST("{mip_col}" AS VARCHAR)),
                   TRY_CAST("{alw_col}" AS DOUBLE), TRY_CAST("{unp_col}" AS DOUBLE),
                   CAST("{mon_col}" AS VARCHAR)
            FROM {tname} WHERE "{ru_col}" IS NOT NULL
        """)

    for tname in availability_views:
        cols = [r[0].lower() for r in con.execute(f"DESCRIBE {tname}").fetchall()]
        ru_col  = next((c for c in cols if 'refinery' in c), cols[0])
        at_col  = next((c for c in cols if 'actual' in c or 'target' in c), cols[1] if len(cols) > 1 else cols[0])
        val_col = next((c for c in cols if 'value' in c or 'perc' in c), cols[2] if len(cols) > 2 else cols[0])
        mon_col = next((c for c in cols if 'month' in c or 'update' in c), cols[3] if len(cols) > 3 else cols[0])
        con.execute(f"""
            INSERT INTO oa_availability_stage
            SELECT TRIM(CAST("{ru_col}" AS VARCHAR)), TRIM(CAST("{at_col}" AS VARCHAR)),
                   TRY_CAST("{val_col}" AS DOUBLE), CAST("{mon_col}" AS VARCHAR)
            FROM {tname} WHERE "{ru_col}" IS NOT NULL
        """)

    for tname in issue_views:
        cols = [r[0].lower() for r in con.execute(f"DESCRIBE {tname}").fetchall()]
        ru_col  = next((c for c in cols if 'refinery' in c), cols[0])
        mip_col = next((c for c in cols if 'mi' in c or 'pi' in c), cols[1] if len(cols) > 1 else cols[0])
        iss_col = next((c for c in cols if 'issue' in c), cols[2] if len(cols) > 2 else cols[0])
        mon_col = next((c for c in cols if 'month' in c), cols[3] if len(cols) > 3 else cols[0])
        dat_col = next((c for c in cols if 'date' in c), cols[4] if len(cols) > 4 else cols[0])
        con.execute(f"""
            INSERT INTO oa_issue_stage
            SELECT TRIM(CAST("{ru_col}" AS VARCHAR)), TRIM(CAST("{mip_col}" AS VARCHAR)),
                   TRIM(CAST("{iss_col}" AS VARCHAR)), CAST("{mon_col}" AS VARCHAR),
                   CAST("{dat_col}" AS VARCHAR)
            FROM {tname} WHERE "{ru_col}" IS NOT NULL AND "{iss_col}" IS NOT NULL
        """)

    # OA availability nodes
    con.execute("""
        INSERT INTO node_raw (node_id, node_type, business_key, label, domain,
                              properties_json, source_file, source_sheet, source_row, source_record_id)
        SELECT
            'oa_avail_' || norm_code(a.refinery_unit || '_' || a.actual_target || '_' || a.month_update),
            'oa_availability', a.refinery_unit || '|' || a.actual_target,
            a.refinery_unit || ' OA ' || a.actual_target || ' (' || LEFT(a.month_update, 7) || ')',
            'oa',
            json_object('refinery_unit', a.refinery_unit, 'actual_target', a.actual_target,
                        'value_perc', COALESCE(CAST(a.value_perc AS VARCHAR), ''),
                        'month_update', a.month_update),
            'oa_data', 'Operational Availability', row_number() OVER (), ''
        FROM oa_availability_stage a WHERE a.refinery_unit IS NOT NULL
    """)

    # OA issue nodes
    con.execute("""
        INSERT INTO node_raw (node_id, node_type, business_key, label, domain,
                              properties_json, source_file, source_sheet, source_row, source_record_id)
        SELECT
            'oa_issue_' || norm_code(oi.refinery_unit || '_' || oi.issue || '_' || oi.month_update),
            'oa_issue', oi.refinery_unit || '|' || LEFT(oi.issue, 40),
            LEFT(oi.issue, 80), 'oa',
            json_object('refinery_unit', oi.refinery_unit, 'mi_pi', oi.mi_pi,
                        'issue', oi.issue, 'month_update', oi.month_update,
                        'date_issue', oi.date_issue),
            'oa_data', 'Issue List', row_number() OVER (), ''
        FROM oa_issue_stage oi
    """)

    # Edges: RU → OA availability
    con.execute("""
        INSERT INTO relationship_raw (source_node_id, target_node_id, relationship_type,
            domain, confidence, match_rule, is_candidate, properties_json,
            source_file, source_sheet, source_row, source_record_id)
        SELECT DISTINCT r.refinery_unit_id, n.node_id, 'REFINERY_UNIT_HAS_OA_AVAILABILITY',
               'oa', 1.0, 'refinery_unit_direct', false,
               json_object('refinery_unit', r.refinery_unit),
               'oa_data', 'Operational Availability', 0, ''
        FROM node_raw n
        JOIN oa_availability_stage a
          ON n.node_type = 'oa_availability'
         AND json_extract_string(n.properties_json, '$.refinery_unit') = a.refinery_unit
        JOIN ru_reference r ON norm_text(a.refinery_unit) = norm_text(r.refinery_unit)
    """)

    # Edges: RU → OA issue
    con.execute("""
        INSERT INTO relationship_raw (source_node_id, target_node_id, relationship_type,
            domain, confidence, match_rule, is_candidate, properties_json,
            source_file, source_sheet, source_row, source_record_id)
        SELECT DISTINCT r.refinery_unit_id, n.node_id, 'REFINERY_UNIT_HAS_OA_ISSUE',
               'oa', 1.0, 'refinery_unit_direct', false,
               json_object('refinery_unit', r.refinery_unit),
               'oa_data', 'Issue List', 0, ''
        FROM node_raw n
        JOIN oa_issue_stage oi
          ON n.node_type = 'oa_issue'
         AND json_extract_string(n.properties_json, '$.refinery_unit') = oi.refinery_unit
         AND json_extract_string(n.properties_json, '$.issue') = oi.issue
        JOIN ru_reference r ON norm_text(oi.refinery_unit) = norm_text(r.refinery_unit)
    """)


# ---------------------------------------------------------------------------
# PLO: Perizinan Layak Operasi
# ---------------------------------------------------------------------------

def _build_plo_nodes(con: duckdb.DuckDBPyConnection, views: list[str]) -> None:
    if not views:
        return

    con.execute("""
        CREATE TABLE IF NOT EXISTS plo_stage (
            refinery_unit VARCHAR, nomor_ijin VARCHAR, nama_plo VARCHAR,
            kapasitas VARCHAR, date_expired VARCHAR, days_expired INTEGER, status_plo VARCHAR
        )
    """)

    for tname in views:
        cols = [r[0].lower() for r in con.execute(f"DESCRIBE {tname}").fetchall()]
        ru_col   = next((c for c in cols if 'refinery' in c), cols[0])
        no_col   = next((c for c in cols if 'nomor' in c or 'ijin' in c or 'number' in c), cols[1] if len(cols) > 1 else cols[0])
        nm_col   = next((c for c in cols if 'nama' in c or 'name' in c), cols[2] if len(cols) > 2 else cols[0])
        kap_col  = next((c for c in cols if 'cakupan' in c or 'kapasitas' in c or 'capacity' in c), cols[3] if len(cols) > 3 else cols[0])
        exp_col  = next((c for c in cols if 'expired' in c and 'sum' not in c and 'days' not in c and 'day' not in c), cols[4] if len(cols) > 4 else cols[0])
        days_col = next((c for c in cols if 'days' in c or ('sum' in c and 'expired' in c)), cols[5] if len(cols) > 5 else cols[0])
        st_col   = next((c for c in cols if 'status' in c), cols[6] if len(cols) > 6 else cols[0])
        con.execute(f"""
            INSERT INTO plo_stage
            SELECT TRIM(CAST("{ru_col}" AS VARCHAR)), TRIM(CAST("{no_col}" AS VARCHAR)),
                   TRIM(CAST("{nm_col}" AS VARCHAR)), TRIM(CAST("{kap_col}" AS VARCHAR)),
                   CAST("{exp_col}" AS VARCHAR), TRY_CAST("{days_col}" AS INTEGER),
                   TRIM(CAST("{st_col}" AS VARCHAR))
            FROM {tname}
            WHERE "{ru_col}" ILIKE '%RU%' AND "{no_col}" IS NOT NULL AND "{no_col}" != ''
        """)

    con.execute("""
        INSERT INTO node_raw (node_id, node_type, business_key, label, domain,
                              properties_json, source_file, source_sheet, source_row, source_record_id)
        SELECT
            'plo_' || norm_code(p.nomor_ijin), 'plo_permit', p.nomor_ijin,
            LEFT(p.nama_plo, 80), 'plo',
            json_object('refinery_unit', p.refinery_unit, 'nomor_ijin', p.nomor_ijin,
                        'nama_plo', p.nama_plo, 'kapasitas', p.kapasitas,
                        'date_expired', p.date_expired,
                        'days_expired', COALESCE(CAST(p.days_expired AS VARCHAR), ''),
                        'status_plo', COALESCE(p.status_plo, '')),
            'plo', 'Sheet1', row_number() OVER (), p.nomor_ijin
        FROM plo_stage p WHERE p.nomor_ijin IS NOT NULL AND p.nomor_ijin != ''
    """)

    # Edges: RU → PLO permit (match "RU II Dumai" → "RU II")
    con.execute("""
        INSERT INTO relationship_raw (source_node_id, target_node_id, relationship_type,
            domain, confidence, match_rule, is_candidate, properties_json,
            source_file, source_sheet, source_row, source_record_id)
        SELECT DISTINCT r.refinery_unit_id, n.node_id, 'REFINERY_UNIT_HAS_PLO_PERMIT',
               'plo', 1.0, 'refinery_unit_direct', false,
               json_object('refinery_unit', r.refinery_unit, 'status_plo', p.status_plo),
               'plo', 'Sheet1', 0, ''
        FROM node_raw n
        JOIN plo_stage p ON n.business_key = p.nomor_ijin AND n.node_type = 'plo_permit'
        JOIN ru_reference r
          ON norm_text(SPLIT_PART(p.refinery_unit, ' ', 1) || ' ' || SPLIT_PART(p.refinery_unit, ' ', 2))
           = norm_text(r.refinery_unit)
    """)


# ---------------------------------------------------------------------------
# Output writer
# ---------------------------------------------------------------------------

def _write_outputs(con: duckdb.DuckDBPyConnection, out_dir: Path) -> dict[str, int]:
    out_dir.mkdir(parents=True, exist_ok=True)

    # Deduplikasi node
    con.execute("""
        CREATE TABLE nodes_final AS
        SELECT DISTINCT ON (node_id) node_id, node_type, business_key, label, properties_json
        FROM node_raw ORDER BY node_id, source_row
    """)

    # Deduplikasi relationship (ambil confidence tertinggi)
    con.execute("""
        CREATE TABLE relationships_final AS
        SELECT source_node_id,
               'rel_' || md5(source_node_id || '|' || relationship_type || '|' || target_node_id) AS relationship_id,
               target_node_id, relationship_type, domain, confidence, match_rule, is_candidate, properties_json
        FROM (
            SELECT *, row_number() OVER (
                PARTITION BY source_node_id, relationship_type, target_node_id, is_candidate
                ORDER BY confidence DESC
            ) AS rn
            FROM relationship_raw
            WHERE source_node_id IN (SELECT node_id FROM nodes_final)
              AND target_node_id IN (SELECT node_id FROM nodes_final)
        ) WHERE rn = 1
    """)

    # nodes.csv — format sesuai importer KGRRE
    con.execute(f"""
        COPY (
            SELECT node_id, node_type, label, properties_json
            FROM nodes_final
        ) TO '{out_dir}/nodes.csv' (HEADER, DELIMITER ',')
    """)

    # relationships.csv — verified
    con.execute(f"""
        COPY (
            SELECT relationship_id, source_node_id, target_node_id, relationship_type
            FROM relationships_final WHERE NOT is_candidate
        ) TO '{out_dir}/relationships.csv' (HEADER, DELIMITER ',')
    """)

    # relationship_candidates.csv
    con.execute(f"""
        COPY (
            SELECT relationship_id, source_node_id, target_node_id, relationship_type,
                   confidence, match_rule
            FROM relationships_final WHERE is_candidate
        ) TO '{out_dir}/relationship_candidates.csv' (HEADER, DELIMITER ',')
    """)

    # domain CSVs — dari node_raw dengan properties_json diexpand
    for domain, node_type, fname in [
        ('asset', 'equipment', 'domain_equipment.csv'),
        ('maintenance', 'maintenance_order', 'domain_maintenance.csv'),
        ('reliability', 'reliability_observation', 'domain_reliability.csv'),
        ('inspection_issue', 'inspection', 'domain_inspection_issue.csv'),
        ('readiness_operation', 'readiness_record', 'domain_readiness_operation.csv'),
        ('cost_program', 'rkap_program', 'domain_cost_program.csv'),
        ('oa', 'oa_availability', 'domain_oa_availability.csv'),
        ('oa', 'oa_issue', 'domain_oa_issue.csv'),
        ('plo', 'plo_permit', 'domain_plo_permit.csv'),
    ]:
        con.execute(f"""
            COPY (
                SELECT node_id, node_type, label,
                       source_file, source_sheet, source_row, source_record_id,
                       properties_json
                FROM node_raw WHERE node_type = '{node_type}'
            ) TO '{out_dir}/{fname}' (HEADER, DELIMITER ',')
        """)

    # audit files
    con.execute(f"""
        COPY (SELECT * FROM unmatched_raw) TO '{out_dir}/unmatched_identifier.csv' (HEADER, DELIMITER ',')
    """)

    counts = con.execute("""
        SELECT
            (SELECT count(*) FROM nodes_final),
            (SELECT count(*) FROM relationships_final WHERE NOT is_candidate),
            (SELECT count(*) FROM relationships_final WHERE is_candidate),
            (SELECT count(*) FROM unmatched_raw)
    """).fetchone()

    return {
        "nodes": counts[0], "relationships": counts[1],
        "candidates": counts[2], "unmatched": counts[3]
    }


# ---------------------------------------------------------------------------
# Main ETL runner
# ---------------------------------------------------------------------------

def _run_etl(job: ImportJob, excel_paths: list[Path], out_dir: Path) -> None:
    try:
        job.status = "running"
        job.phase = "Memuat file Excel"
        job.progress = 5

        con = duckdb.connect()
        con.execute(_MACROS_SQL)
        con.execute(_REFERENCE_SQL)

        # Classify sheets by domain
        domain_views: dict[str, list[str]] = {
            "equipment": [], "maintenance": [], "rkap": [],
            "reliability": [], "inspection": [], "readiness": [],
            "icu_issue": [], "org_issue": [], "rcps": [], "rcps_recommendation": [],
            "oa_allowance": [], "oa_availability": [], "oa_issue": [], "plo": [],
        }

        detection_log: list[str] = []
        job.phase = "Membaca sheet Excel"
        for path in excel_paths:
            job.message = f"Membaca {path.name}…"
            loaded = _load_excel_to_duckdb(con, path)
            for tname, filename, sheet, headers in loaded:
                domain = _detect_domain(filename, sheet, headers)
                if domain and domain in domain_views:
                    domain_views[domain].append(tname)
                    detection_log.append(f"{Path(filename).name}/{sheet} → {domain}")
                else:
                    detection_log.append(f"{Path(filename).name}/{sheet} → (tidak dikenali)")
        job.detection_log = detection_log

        build_warnings: list[str] = []

        def _safe(phase: str, fn, *args):
            try:
                fn(*args)
            except Exception as exc:
                build_warnings.append(f"{phase}: {exc}")

        job.progress = 20
        job.phase = "Membangun node RU & Plant"
        _build_ru_plant_nodes(con)

        job.progress = 25
        job.phase = "Membangun node Equipment"
        _safe("Equipment", _build_equipment_nodes, con, domain_views["equipment"])
        # Pastikan equipment_master ada agar builder lain tidak crash jika equipment builder gagal
        con.execute("""
            CREATE TABLE IF NOT EXISTS equipment_master (
                equipment_id VARCHAR, equipment_code_normalized VARCHAR,
                equipment_code_raw VARCHAR, refinery_unit VARCHAR,
                plant VARCHAR, functional_location VARCHAR,
                equipment_group VARCHAR, description VARCHAR,
                criticallity VARCHAR, plant_area VARCHAR,
                date_update_data VARCHAR, source_file VARCHAR,
                source_sheet VARCHAR, source_row INTEGER,
                source_record_id VARCHAR, rn INTEGER
            )
        """)

        job.progress = 35
        job.phase = "Membangun node Maintenance Order"
        _safe("Maintenance", _build_maintenance_nodes, con, domain_views["maintenance"])

        job.progress = 45
        job.phase = "Membangun node RKAP Program"
        _safe("RKAP", _build_rkap_nodes, con, domain_views["rkap"])

        job.progress = 55
        job.phase = "Membangun node Reliability"
        _safe("Reliability", _build_reliability_nodes, con, domain_views["reliability"])

        job.progress = 62
        job.phase = "Membangun node Inspection"
        _safe("Inspection", _build_inspection_nodes, con, domain_views["inspection"])

        job.progress = 68
        job.phase = "Membangun node ICU Issue"
        _safe("ICU Issue", _build_icu_issue_nodes, con, domain_views["icu_issue"])

        job.progress = 74
        job.phase = "Membangun node Readiness"
        _safe("Readiness", _build_readiness_nodes, con, domain_views["readiness"])

        job.progress = 76
        job.phase = "Membangun node OA Data"
        _safe("OA Data", _build_oa_nodes, con, domain_views["oa_allowance"], domain_views["oa_availability"], domain_views["oa_issue"])

        job.progress = 79
        job.phase = "Membangun node PLO"
        _safe("PLO", _build_plo_nodes, con, domain_views["plo"])

        job.progress = 82
        job.phase = "Menulis output CSV"
        counts = _write_outputs(con, out_dir)
        con.close()

        job.progress = 88
        job.phase = "Import ke database"
        warn_str = (" | Peringatan: " + "; ".join(build_warnings)) if build_warnings else ""
        job.message = (
            f"ETL selesai: {counts['nodes']:,} node, {counts['relationships']:,} relasi, "
            f"{counts['candidates']:,} kandidat, {counts['unmatched']:,} tidak cocok{warn_str}"
        )

        # Trigger existing import pipeline
        from .scanner import scan_package
        from .importer import _select_ready_files, _run_import
        scan = scan_package(out_dir, validate=True)
        files = _select_ready_files(scan, allow_partial=True)
        _run_import(job, files, out_dir, True, existing_dataset_id=job.dataset_id)

    except Exception as exc:
        job.status = "failed"
        job.error = str(exc)
        job.message = str(exc)
        job.finished_at = time.time()


def start_etl_import(name: str, excel_paths: list[Path], existing_dataset_id: str | None = None) -> ImportJob:
    out_dir = UPLOADS_DIR / uuid.uuid4().hex
    out_dir.mkdir(parents=True, exist_ok=True)
    job = _create_job(name)
    job.dataset_id = existing_dataset_id  # pre-set agar _run_import tahu ini sync
    with ETL_JOBS_LOCK:
        ETL_JOBS[job.id] = job
    threading.Thread(
        target=_run_etl, args=(job, excel_paths, out_dir), daemon=True
    ).start()
    return job
