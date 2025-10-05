"""Utilities to load race data files into the database."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from .database import DB_PATH_DEFAULT, bulk_insert, get_connection, initialize_database

STRUCTURED_BLOCK_START = "### structured-data:start"
STRUCTURED_BLOCK_END = "### structured-data:end"

REQUIRED_COLUMNS = {
    "race_id",
    "date",
    "racecourse",
    "distance",
    "track_condition",
    "num_runners",
    "track_direction",
    "weather",
    "horse_number",
    "horse_name",
    "popularity",
    "finish_position",
    "odds_win",
    "odds_place",
    "return_win",
    "return_place",
}


class DataValidationError(Exception):
    """Raised when the ingested data does not match expectations."""


def _validate_columns(columns: Iterable[str]) -> None:
    missing = REQUIRED_COLUMNS.difference(columns)
    if missing:
        joined = ", ".join(sorted(missing))
        raise DataValidationError(f"CSV is missing required columns: {joined}")


def _cast_int(record: Dict[str, str], key: str) -> int:
    try:
        return int(record[key])
    except (KeyError, ValueError) as exc:
        raise DataValidationError(f"Column '{key}' must contain integers") from exc


def _cast_float(record: Dict[str, str], key: str) -> float:
    try:
        return float(record[key])
    except (KeyError, ValueError) as exc:
        raise DataValidationError(f"Column '{key}' must contain numbers") from exc


def _parse_structured_block(text: str) -> Dict[str, Any]:
    """Extract and decode the structured JSON block embedded in a text file."""

    start_index = text.find(STRUCTURED_BLOCK_START)
    if start_index == -1:
        raise DataValidationError(
            "Text file does not contain the structured data block marker"
        )
    start_index += len(STRUCTURED_BLOCK_START)

    end_index = text.find(STRUCTURED_BLOCK_END, start_index)
    if end_index == -1:
        raise DataValidationError(
            "Text file does not contain the structured data block terminator"
        )

    block = text[start_index:end_index].strip()
    if not block:
        raise DataValidationError("Structured data block is empty")

    try:
        return json.loads(block)
    except json.JSONDecodeError as exc:
        raise DataValidationError("Structured data block is not valid JSON") from exc


def ingest_csv(csv_path: Path | str, db_path: Path | str | None = None) -> Tuple[int, int]:
    """Load a CSV file into the SQLite database."""

    csv_path = Path(csv_path)
    db_path = Path(db_path) if db_path else DB_PATH_DEFAULT

    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    initialize_database(db_path)

    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise DataValidationError("CSV file does not contain headers")
        _validate_columns(reader.fieldnames)

        race_records: Dict[str, Dict[str, object]] = {}
        entry_records: List[Dict[str, object]] = []

        for row in reader:
            race_id = row["race_id"].strip()
            race_records[race_id] = {
                "race_id": race_id,
                "date": row["date"].strip(),
                "racecourse": row["racecourse"].strip(),
                "distance": _cast_int(row, "distance"),
                "track_condition": row["track_condition"].strip(),
                "num_runners": _cast_int(row, "num_runners"),
                "track_direction": row["track_direction"].strip(),
                "weather": row["weather"].strip(),
            }
            entry_records.append(
                {
                    "race_id": race_id,
                    "horse_number": _cast_int(row, "horse_number"),
                    "horse_name": row["horse_name"].strip(),
                    "popularity": _cast_int(row, "popularity"),
                    "finish_position": _cast_int(row, "finish_position"),
                    "odds_win": _cast_float(row, "odds_win"),
                    "odds_place": _cast_float(row, "odds_place"),
                    "return_win": _cast_float(row, "return_win"),
                    "return_place": _cast_float(row, "return_place"),
                }
            )

    with get_connection(db_path) as conn:
        bulk_insert(
            conn,
            """
            INSERT OR REPLACE INTO races (
                race_id, date, racecourse, distance, track_condition,
                num_runners, track_direction, weather
            ) VALUES (
                :race_id, :date, :racecourse, :distance, :track_condition,
                :num_runners, :track_direction, :weather
            );
            """,
            race_records.values(),
        )
        bulk_insert(
            conn,
            """
            INSERT OR REPLACE INTO race_entries (
                race_id, horse_number, horse_name, popularity, finish_position,
                odds_win, odds_place, return_win, return_place
            ) VALUES (
                :race_id, :horse_number, :horse_name, :popularity, :finish_position,
                :odds_win, :odds_place, :return_win, :return_place
            );
            """,
            entry_records,
        )

    return len(race_records), len(entry_records)


def ingest_txt(txt_path: Path | str, db_path: Path | str | None = None) -> Tuple[int, int]:
    """Load a structured text file into the SQLite database."""

    txt_path = Path(txt_path)
    db_path = Path(db_path) if db_path else DB_PATH_DEFAULT

    if not txt_path.exists():
        raise FileNotFoundError(txt_path)

    initialize_database(db_path)

    data = _parse_structured_block(txt_path.read_text(encoding="utf-8"))
    races = data.get("races")
    if not isinstance(races, list):
        raise DataValidationError("Structured data must contain a 'races' list")

    race_records: Dict[str, Dict[str, object]] = {}
    entry_records: List[Dict[str, object]] = []

    for race in races:
        if not isinstance(race, dict):
            raise DataValidationError("Each race entry must be a mapping")

        try:
            race_id = str(race["race_id"]).strip()
            race_records[race_id] = {
                "race_id": race_id,
                "date": str(race["date"]).strip(),
                "racecourse": str(race["racecourse"]).strip(),
                "distance": int(race["distance"]),
                "track_condition": str(race["track_condition"]).strip(),
                "num_runners": int(race["num_runners"]),
                "track_direction": str(race["track_direction"]).strip(),
                "weather": str(race["weather"]).strip(),
            }
        except KeyError as exc:
            raise DataValidationError(
                f"Structured race entry missing field: {exc.args[0]}"
            ) from exc
        except (TypeError, ValueError) as exc:
            raise DataValidationError("Race metadata contains invalid values") from exc

        entries = race.get("entries")
        if not isinstance(entries, list):
            raise DataValidationError(
                f"Race '{race_id}' must contain an 'entries' list"
            )

        if len(entries) != race_records[race_id]["num_runners"]:
            raise DataValidationError(
                f"Race '{race_id}' expects {race_records[race_id]['num_runners']} entries"
            )

        for entry in entries:
            if not isinstance(entry, dict):
                raise DataValidationError("Race entry must be a mapping")
            try:
                entry_records.append(
                    {
                        "race_id": race_id,
                        "horse_number": int(entry["horse_number"]),
                        "horse_name": str(entry["horse_name"]).strip(),
                        "popularity": int(entry["popularity"]),
                        "finish_position": int(entry["finish_position"]),
                        "odds_win": float(entry["odds_win"]),
                        "odds_place": float(entry["odds_place"]),
                        "return_win": float(entry["return_win"]),
                        "return_place": float(entry["return_place"]),
                    }
                )
            except KeyError as exc:
                raise DataValidationError(
                    f"Race entry missing field: {exc.args[0]}"
                ) from exc
            except (TypeError, ValueError) as exc:
                raise DataValidationError("Race entry contains invalid values") from exc

    with get_connection(db_path) as conn:
        bulk_insert(
            conn,
            """
            INSERT OR REPLACE INTO races (
                race_id, date, racecourse, distance, track_condition,
                num_runners, track_direction, weather
            ) VALUES (
                :race_id, :date, :racecourse, :distance, :track_condition,
                :num_runners, :track_direction, :weather
            );
            """,
            race_records.values(),
        )
        bulk_insert(
            conn,
            """
            INSERT OR REPLACE INTO race_entries (
                race_id, horse_number, horse_name, popularity, finish_position,
                odds_win, odds_place, return_win, return_place
            ) VALUES (
                :race_id, :horse_number, :horse_name, :popularity, :finish_position,
                :odds_win, :odds_place, :return_win, :return_place
            );
            """,
            entry_records,
        )

    return len(race_records), len(entry_records)


def ingest_file(path: Path | str, db_path: Path | str | None = None) -> Tuple[int, int]:
    """Load a race data file (CSV or structured text) into the database."""

    path = Path(path)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return ingest_csv(path, db_path)
    if suffix == ".txt":
        return ingest_txt(path, db_path)
    raise DataValidationError(
        f"Unsupported data file extension: '{suffix or '<none>'}'"
    )

