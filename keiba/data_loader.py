"""Utilities to load raw CSV files into the database."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from .database import DB_PATH_DEFAULT, bulk_insert, get_connection, initialize_database

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

