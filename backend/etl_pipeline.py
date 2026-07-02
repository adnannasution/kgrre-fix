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
# Sheet domain detection — sesuai pola dari Colab notebook
# ---------------------------------------------------------------------------

def _key(filename: str, sheet: str) -> str:
    return f"{Path(filename).stem} {sheet}".lower()


def _detect_domain(filename: str, sheet: str) -> str | None:
    stem = Path(filename).stem.lower()
    sheet_l = sheet.lower()
    k = _key(filename, sheet)

    if "all_ru_equipment" in stem and "sheet4" in sheet_l:
        return "equipment"
    if stem.startswith(("pt02_", "pt03_")):
        return "maintenance"
    if any(x in k for x in ("vw_reportirkapplanactual", "reportirkapplanactual", "cost_program")) \
            or (any(x in k for x in ("rkap", "irkap")) and "alias_map" not in stem):
        return "rkap"
    if stem.startswith("running_hours_") or (stem.startswith("n_0_") and sheet_l == "sheet1"):
        return "reliability"
    if "inspection_plan" in k:
        return "inspection"
    if any(x in k for x in ("apr_", "readiness_atg", "power_steam", "weekly_monitoring")):
        return "readiness"
    if any(x in k for x in ("issue_list", "paf_issue")):
        return "org_issue"
    if any(x in k for x in ("icu_database", "icu")):
        return "icu_issue"
    if "rcps" in stem and sheet_l == "rcps":
        return "rcps"
    if "rcps" in stem and sheet_l == "rekomendasi":
        return "rcps_recommendation"
    return None


# ---------------------------------------------------------------------------
# Excel → DuckDB loader
# ---------------------------------------------------------------------------

def _load_excel_to_duckdb(con: duckdb.DuckDBPyConnection, path: Path) -> list[tuple[str, str, str]]:
    """Baca semua sheet dari Excel, load ke DuckDB. Return list (table_name, filename, sheet)."""
    loaded: list[tuple[str, str, str]] = []
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            continue
        headers = [str(c).strip() if c is not None else f"col_{i}" for i, c in enumerate(rows[0])]
        # deduplikasi header
        seen: dict[str, int] = {}
        clean_headers: list[str] = []
        for h in headers:
            h_clean = re.sub(r'[^a-zA-Z0-9_]', '_', h).lower() or "col"
            if h_clean in seen:
                seen[h_clean] += 1
                h_clean = f"{h_clean}_{seen[h_clean]}"
            else:
                seen[h_clean] = 0
            clean_headers.append(h_clean)

        data_rows = []
        for row in rows[1:]:
            data_rows.append(tuple(
                str(v).strip() if v is not None else None for v in row
            ))

        if not data_rows:
            continue

        import pandas as pd
        df = pd.DataFrame(data_rows, columns=clean_headers)
        # tambahkan kolom sumber
        df["_input_source_file"] = path.name
        df["_input_source_sheet"] = sheet_name
        df["_source_row"] = range(2, len(df) + 2)

        tname = f"src_{uuid.uuid4().hex[:8]}"
        con.register(tname, df)
        loaded.append((tname, path.name, sheet_name))

    wb.close()
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
    union_sql = " UNION ALL BY NAME ".join(f"SELECT * FROM {v}" for v in views)
    con.execute(f"""
        CREATE TABLE equipment_master AS
        WITH base AS (
            SELECT
                norm_equipment(coalesce(equipment, tag_number, tag_no)) AS equipment_code_normalized,
                coalesce(equipment, tag_number, tag_no) AS equipment_code_raw,
                norm_text(coalesce(maintplant, planning_plant)) AS plant,
                coalesce(
                    ru_normalize(coalesce(refinery_unit, ru)),
                    (SELECT refinery_unit FROM plant_ru_map
                     WHERE plant = norm_text(coalesce(maintplant, planning_plant)) LIMIT 1),
                    ru_from_filename(_input_source_file)
                ) AS refinery_unit,
                nullif(trim(coalesce(functional_loc, functional_location, '')), '') AS functional_location,
                nullif(trim(coalesce(catalog_profile, equipment_group, '')), '') AS equipment_group,
                nullif(trim(coalesce(description_of_technical_object, description, '')), '') AS description,
                nullif(trim(coalesce(criticallity, criticality, '')), '') AS criticallity,
                nullif(trim(coalesce(location, plant_area, '')), '') AS plant_area,
                nullif(trim(coalesce(date_update_data, '')), '') AS date_update_data,
                _input_source_file AS source_file,
                _input_source_sheet AS source_sheet,
                _source_row AS source_row,
                'record_' || md5(_input_source_file || '|' || cast(_source_row AS VARCHAR)) AS source_record_id
            FROM ({union_sql})
            WHERE norm_equipment(coalesce(equipment, tag_number, tag_no)) IS NOT NULL
        ),
        ranked AS (
            SELECT *, row_number() OVER (
                PARTITION BY refinery_unit, equipment_code_normalized
                ORDER BY try_cast(date_update_data AS DATE) DESC NULLS LAST,
                         ((functional_location IS NOT NULL)::INTEGER +
                          (description IS NOT NULL)::INTEGER +
                          (equipment_group IS NOT NULL)::INTEGER) DESC,
                         source_file, source_row DESC
            ) AS rn
            FROM base WHERE refinery_unit IS NOT NULL
        )
        SELECT 'node_equipment_' || md5(refinery_unit || '|' || equipment_code_normalized) AS equipment_id, *
        FROM ranked WHERE rn = 1
    """)

    con.execute("""
        INSERT INTO node_raw
        SELECT equipment_id, 'equipment',
               refinery_unit || '|' || equipment_code_normalized,
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
    union_sql = " UNION ALL BY NAME ".join(f"SELECT * FROM {v}" for v in views)
    con.execute(f"""
        CREATE TABLE maintenance_stage AS
        SELECT
            coalesce(cast("order" AS VARCHAR), cast(order_no AS VARCHAR),
                     cast(aufnr AS VARCHAR)) AS order_raw,
            coalesce(cast(notification AS VARCHAR), cast(notif AS VARCHAR),
                     cast(qmnum AS VARCHAR)) AS notification_raw,
            norm_code(coalesce(cast("order" AS VARCHAR), cast(order_no AS VARCHAR),
                               cast(aufnr AS VARCHAR))) AS order_code,
            coalesce(description, kurztext, short_text, '') AS order_desc,
            coalesce(order_type, auart, '') AS order_type,
            coalesce(priority, priok, '') AS priority,
            coalesce(user_status, txt04, '') AS user_status,
            coalesce(system_status, sttxt, '') AS system_status,
            coalesce(cast(reference_date AS VARCHAR),
                     cast(gstrp AS VARCHAR), '') AS reference_date,
            coalesce(cast(total_planned_costs AS VARCHAR),
                     cast(geplk AS VARCHAR), '0') AS planned_cost,
            coalesce(cast(total_actual_costs AS VARCHAR),
                     cast(istko AS VARCHAR), '0') AS actual_cost,
            coalesce(main_work_center, arbpl, '') AS work_center,
            coalesce(equipment, tag_number, tag_no, equnr, '') AS equipment_raw,
            coalesce(
                ru_normalize(coalesce(refinery_unit, ru, plant, maintplant)),
                ru_from_filename(_input_source_file)
            ) AS refinery_unit,
            _input_source_file AS source_file,
            _input_source_sheet AS source_sheet,
            _source_row AS source_row,
            'record_' || md5(_input_source_file || '|' || cast(_source_row AS VARCHAR)) AS source_record_id
        FROM ({union_sql})
        WHERE coalesce(cast("order" AS VARCHAR), cast(order_no AS VARCHAR),
                       cast(aufnr AS VARCHAR)) IS NOT NULL
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
    union_sql = " UNION ALL BY NAME ".join(f"SELECT * FROM {v}" for v in views)
    con.execute(f"""
        CREATE TABLE rkap_stage AS
        SELECT
            coalesce(noprogram, no_program, program_no, '') AS program_no,
            coalesce(programkerja, program_kerja, nama_program, program_name, '') AS program_name,
            coalesce(equipment, tag_number, tag_no, '') AS equipment_raw,
            coalesce(cast(plant AS VARCHAR), cast(refineryunit AS VARCHAR),
                     cast(refinery_unit AS VARCHAR), cast(ru AS VARCHAR), '') AS ru_raw,
            coalesce(katergorirkap, kategori_rkap, kategori, '') AS kategori,
            coalesce(kelompokbiaya, kelompok_biaya, '') AS kelompok_biaya,
            coalesce(disiplin, discipline, '') AS disiplin,
            coalesce(cast(fiscalyear AS VARCHAR), cast(fiscal_year AS VARCHAR),
                     cast(tahun AS VARCHAR), '') AS fiscal_year,
            coalesce(cast(planstart AS VARCHAR), cast(plan_start AS VARCHAR), '') AS plan_start,
            coalesce(cast(planfinish AS VARCHAR), cast(plan_finish AS VARCHAR), '') AS plan_finish,
            coalesce(cast(actualstart AS VARCHAR), cast(actual_start AS VARCHAR), '') AS actual_start,
            coalesce(cast(actualfinish AS VARCHAR), cast(actual_finish AS VARCHAR), '') AS actual_finish,
            coalesce(statusactual, status_actual, status, '') AS status_actual,
            coalesce(cast(totalequivalentidr AS VARCHAR),
                     cast(total_equivalent_idr AS VARCHAR), '0') AS total_idr,
            coalesce(cast(toprisk AS VARCHAR), '') AS top_risk,
            coalesce(
                ru_normalize(coalesce(refineryunit, refinery_unit, ru, plant)),
                ru_from_filename(_input_source_file)
            ) AS refinery_unit,
            _input_source_file AS source_file,
            _input_source_sheet AS source_sheet,
            _source_row AS source_row,
            'record_' || md5(_input_source_file || '|' || cast(_source_row AS VARCHAR)) AS source_record_id
        FROM ({union_sql})
        WHERE nullif(trim(coalesce(noprogram, no_program, program_no, programkerja,
                                   program_kerja, '')), '') IS NOT NULL
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
    union_sql = " UNION ALL BY NAME ".join(f"SELECT * FROM {v}" for v in views)
    con.execute(f"""
        CREATE TABLE reliability_stage AS
        SELECT
            coalesce(equipment, tag_number, tag_no, '') AS equipment_raw,
            coalesce(cast(hasil AS VARCHAR), cast(status AS VARCHAR), '') AS hasil,
            coalesce(cast(running_hours AS VARCHAR), '0') AS running_hours,
            coalesce(cast(mtbf AS VARCHAR), '0') AS mtbf,
            coalesce(cast(mttr AS VARCHAR), '0') AS mttr,
            coalesce(cast(minggu AS VARCHAR), '') AS minggu,
            coalesce(cast(bulan AS VARCHAR), cast(month_update AS VARCHAR), '') AS bulan,
            coalesce(cast(tahun AS VARCHAR), '') AS tahun,
            coalesce(
                ru_normalize(coalesce(refinery_unit, ru, plant)),
                ru_from_filename(_input_source_file)
            ) AS refinery_unit,
            _input_source_file AS source_file,
            _input_source_sheet AS source_sheet,
            _source_row AS source_row,
            'record_' || md5(_input_source_file || '|' || cast(_source_row AS VARCHAR)) AS source_record_id
        FROM ({union_sql})
        WHERE nullif(trim(coalesce(equipment, tag_number, tag_no, '')), '') IS NOT NULL
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
    union_sql = " UNION ALL BY NAME ".join(f"SELECT * FROM {v}" for v in views)
    con.execute(f"""
        CREATE TABLE inspection_stage AS
        SELECT
            coalesce(tag_no_ln, tag_number, tag_no, equipment, '') AS equipment_raw,
            coalesce(type_inspection, jenis_inspeksi, '') AS type_inspection,
            coalesce(type_pekerjaan, jenis_pekerjaan, '') AS type_pekerjaan,
            coalesce(cast(plan_date AS VARCHAR), cast(tanggal_rencana AS VARCHAR), '') AS plan_date,
            coalesce(cast(actual_date AS VARCHAR), cast(tanggal_aktual AS VARCHAR), '') AS actual_date,
            coalesce(grand_result, hasil, '') AS grand_result,
            coalesce(
                ru_normalize(coalesce(refinery_unit, ru, plant)),
                ru_from_filename(_input_source_file)
            ) AS refinery_unit,
            _input_source_file AS source_file,
            _input_source_sheet AS source_sheet,
            _source_row AS source_row,
            'record_' || md5(_input_source_file || '|' || cast(_source_row AS VARCHAR)) AS source_record_id
        FROM ({union_sql})
        WHERE nullif(trim(coalesce(tag_no_ln, tag_number, tag_no, equipment, '')), '') IS NOT NULL
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
    union_sql = " UNION ALL BY NAME ".join(f"SELECT * FROM {v}" for v in views)
    con.execute(f"""
        CREATE TABLE icu_stage AS
        SELECT
            coalesce(tag_no, tag_number, equipment, '') AS equipment_raw,
            coalesce(issue, deskripsi_issue, '') AS issue_text,
            coalesce(icu_status, status, '') AS icu_status,
            coalesce(cast(report_date AS VARCHAR), cast(tanggal AS VARCHAR), '') AS report_date,
            coalesce(cast(target_closed AS VARCHAR), '') AS target_closed,
            coalesce(mitigation_temporary_solution, solusi_sementara, '') AS mitigation,
            coalesce(permanent_solution, solusi_permanen, '') AS permanent_solution,
            coalesce(
                ru_normalize(coalesce(refinery_unit, ru, kilang, plant)),
                ru_from_filename(_input_source_file)
            ) AS refinery_unit,
            _input_source_file AS source_file,
            _input_source_sheet AS source_sheet,
            _source_row AS source_row,
            'record_' || md5(_input_source_file || '|' || cast(_source_row AS VARCHAR)) AS source_record_id
        FROM ({union_sql})
        WHERE nullif(trim(coalesce(issue, deskripsi_issue, '')), '') IS NOT NULL
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
    union_sql = " UNION ALL BY NAME ".join(f"SELECT * FROM {v}" for v in views)
    con.execute(f"""
        CREATE TABLE readiness_stage AS
        SELECT
            coalesce(equipment, tag_number, tag_no, process_equipment,
                     nama_tangki, no_tangki, '') AS equipment_raw,
            coalesce(cast(period_date AS VARCHAR), cast(month_update AS VARCHAR), '') AS period_date,
            coalesce(status_operation, status_operasi, '') AS status_operation,
            coalesce(status_item, '') AS status_item,
            coalesce(remark, keterangan, '') AS remark,
            coalesce(rtl, '') AS rtl,
            coalesce(
                ru_normalize(coalesce(refinery_unit, ru, kilang, plant)),
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
    ]:
        con.execute(f"""
            COPY (
                SELECT node_id, node_type, label,
                       source_file, source_sheet, source_row, source_record_id,
                       properties_json
                FROM node_raw WHERE domain = '{domain}'
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
        }

        job.phase = "Membaca sheet Excel"
        for path in excel_paths:
            job.message = f"Membaca {path.name}…"
            loaded = _load_excel_to_duckdb(con, path)
            for tname, filename, sheet in loaded:
                domain = _detect_domain(filename, sheet)
                if domain and domain in domain_views:
                    domain_views[domain].append(tname)

        job.progress = 20
        job.phase = "Membangun node RU & Plant"
        _build_ru_plant_nodes(con)

        job.progress = 25
        job.phase = "Membangun node Equipment"
        _build_equipment_nodes(con, domain_views["equipment"])

        job.progress = 35
        job.phase = "Membangun node Maintenance Order"
        _build_maintenance_nodes(con, domain_views["maintenance"])

        job.progress = 45
        job.phase = "Membangun node RKAP Program"
        _build_rkap_nodes(con, domain_views["rkap"])

        job.progress = 55
        job.phase = "Membangun node Reliability"
        _build_reliability_nodes(con, domain_views["reliability"])

        job.progress = 62
        job.phase = "Membangun node Inspection"
        _build_inspection_nodes(con, domain_views["inspection"])

        job.progress = 68
        job.phase = "Membangun node ICU Issue"
        _build_icu_issue_nodes(con, domain_views["icu_issue"])

        job.progress = 74
        job.phase = "Membangun node Readiness"
        _build_readiness_nodes(con, domain_views["readiness"])

        job.progress = 82
        job.phase = "Menulis output CSV"
        counts = _write_outputs(con, out_dir)
        con.close()

        job.progress = 88
        job.phase = "Import ke database"
        job.message = (
            f"ETL selesai: {counts['nodes']:,} node, {counts['relationships']:,} relasi, "
            f"{counts['candidates']:,} kandidat, {counts['unmatched']:,} tidak cocok"
        )

        # Trigger existing import pipeline
        from .scanner import scan_package
        from .importer import _select_ready_files, _run_import
        scan = scan_package(out_dir, validate=True)
        files = _select_ready_files(scan, allow_partial=True)
        _run_import(job, files, out_dir, True)

    except Exception as exc:
        job.status = "failed"
        job.error = str(exc)
        job.message = str(exc)
        job.finished_at = time.time()


def start_etl_import(name: str, excel_paths: list[Path]) -> ImportJob:
    out_dir = UPLOADS_DIR / uuid.uuid4().hex
    out_dir.mkdir(parents=True, exist_ok=True)
    job = _create_job(name)
    with ETL_JOBS_LOCK:
        ETL_JOBS[job.id] = job
    threading.Thread(
        target=_run_etl, args=(job, excel_paths, out_dir), daemon=True
    ).start()
    return job
