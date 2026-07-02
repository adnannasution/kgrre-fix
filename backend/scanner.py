from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

from .config import get_config

REQUIRED_FILES = {
    "nodes.csv": "node_master",
    "relationships.csv": "verified_edges",
}

RECOMMENDED_FILES = {
    "relationship_candidates.csv": "candidate_edges",
    "etl_summary.json": "etl_summary",
    "refinery_units.csv": "ru_master",
    "ru_equipment_summary.csv": "ru_equipment_summary",
    "ru_data_coverage.csv": "ru_data_coverage",
    "ru_relationship_quality.csv": "ru_relationship_quality",
    "graph_schema.csv": "graph_schema",
    "ontology_depth.csv": "ontology_depth",
    "deepest_paths.csv": "deepest_paths",
    "unmatched_identifier.csv": "audit_unmatched",
    "ambiguous_match.csv": "audit_ambiguous",
    "invalid_value.csv": "audit_invalid",
    "output_manifest.csv": "output_manifest",
}

DOMAIN_PREFIX = "domain_"
METADATA_DIR = "input_metadata"
ANALYSIS_READY_DIR = "analysis_ready"


def package_file_type(path: Path) -> str | None:
    name = path.name
    if name in REQUIRED_FILES:
        return REQUIRED_FILES[name]
    if name in RECOMMENDED_FILES:
        return RECOMMENDED_FILES[name]
    if path.suffix.lower() == ".csv" and name.startswith(DOMAIN_PREFIX):
        return f"domain_{path.stem.removeprefix(DOMAIN_PREFIX)}"
    if path.parent.name == METADATA_DIR and path.suffix.lower() in {".csv", ".json"}:
        return f"metadata_{path.stem}"
    if path.parent.name == ANALYSIS_READY_DIR and path.suffix.lower() == ".csv":
        return f"analysis_ready_{path.stem}"
    return None


def sha256(path: Path, cancel=None) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(4 * 1024 * 1024):
            if cancel and cancel():
                raise RuntimeError("Import dibatalkan.")
            digest.update(chunk)
    return digest.hexdigest()


def imported_fingerprints() -> dict[str, dict]:
    # Tracking incremental import (catalog imports) tidak lagi dipertahankan setelah
    # migrasi ke PostgreSQL; alur utama adalah upload via frontend. Setiap scan
    # memperlakukan file sebagai baru.
    return {}


def scan_package(folder: Path, validate: bool = False) -> dict:
    imported = imported_fingerprints()
    config = get_config()
    now = time.time()
    files: list[dict] = []

    if folder.exists() and folder.is_dir():
        candidates = [p for p in folder.iterdir() if p.is_file()]
        metadata = folder / METADATA_DIR
        if metadata.exists() and metadata.is_dir():
            candidates.extend([p for p in metadata.iterdir() if p.is_file()])
        analysis_ready = folder / ANALYSIS_READY_DIR
        if analysis_ready.exists() and analysis_ready.is_dir():
            candidates.extend([p for p in analysis_ready.iterdir() if p.is_file()])
        for path in sorted(candidates, key=lambda item: str(item.relative_to(folder))):
            if path.name.startswith(".") or path.name.startswith("~$"):
                continue
            kind = package_file_type(path)
            if not kind:
                continue
            stat = path.stat()
            stable = now - stat.st_mtime >= config["stability_seconds"]
            previous = imported.get(str(path))
            status = "Copying" if not stable else "Ready"
            if previous and previous.get("size") == stat.st_size and previous.get("mtime_ns") == stat.st_mtime_ns:
                status = "Already imported"
            elif previous:
                status = "Changed"
            entry = {
                "name": str(path.relative_to(folder)),
                "path": str(path),
                "size": stat.st_size,
                "modified_at": stat.st_mtime,
                "mtime_ns": stat.st_mtime_ns,
                "file_type": kind,
                "workbook_type": kind,
                "required": path.name in REQUIRED_FILES,
                "status": status,
                "stable": stable,
                "sheets": [],
                "warnings": [],
                "row_count": None,
            }
            if validate:
                inspect_package_file(entry, path)
            files.append(entry)

    present_names = {Path(item["name"]).name for item in files}
    for name, kind in {**REQUIRED_FILES, **RECOMMENDED_FILES}.items():
        if name in present_names:
            continue
        files.append({
            "name": name,
            "path": None,
            "size": 0,
            "modified_at": None,
            "mtime_ns": None,
            "file_type": kind,
            "workbook_type": kind,
            "required": name in REQUIRED_FILES,
            "status": "Missing" if name in REQUIRED_FILES else "Optional",
            "stable": False,
            "sheets": [],
            "warnings": [],
            "row_count": None,
        })

    required_ready = all(
        any(Path(item["name"]).name == required and item["status"] in {"Ready", "Already imported", "Changed"} for item in files)
        for required in REQUIRED_FILES
    )
    return {
        "folder": str(folder),
        "exists": folder.exists(),
        "readable": folder.exists() and folder.is_dir(),
        "package_type": "csv_graph_export",
        "scan_interval_seconds": config["scan_interval_seconds"],
        "stability_seconds": config["stability_seconds"],
        "ready": required_ready,
        "files": files,
    }


def scan_folder(validate_sheets: bool = False) -> dict:
    return scan_package(Path(get_config()["upload_folder"]), validate_sheets)


def inspect_package_file(entry: dict, path: Path) -> None:
    try:
        if path.suffix.lower() == ".json":
            json.loads(path.read_text("utf-8"))
            return
        if path.suffix.lower() != ".csv":
            entry["status"] = "Invalid"
            entry["warnings"].append("Hanya CSV/JSON yang didukung dalam paket ETL.")
            return
        with path.open("rb") as handle:
            header = handle.readline().decode("utf-8-sig", "replace").strip()
        columns = [item.strip() for item in header.split(",") if item.strip()]
        entry["columns"] = columns
        if path.name == "nodes.csv":
            missing = {"node_id", "node_type", "label", "properties_json"} - set(columns)
            if missing:
                entry["warnings"].append(f"Kolom node wajib hilang: {', '.join(sorted(missing))}")
        elif path.name in {"relationships.csv", "relationship_candidates.csv"}:
            missing = {"relationship_id", "source_node_id", "target_node_id", "relationship_type"} - set(columns)
            if missing:
                entry["warnings"].append(f"Kolom relationship wajib hilang: {', '.join(sorted(missing))}")
        if entry["warnings"] and entry["status"] == "Ready":
            entry["status"] = "Invalid"
    except Exception as exc:
        entry["status"] = "Invalid"
        entry["warnings"].append(f"File tidak dapat dibaca: {exc}")
