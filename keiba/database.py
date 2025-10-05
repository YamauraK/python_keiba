"""Database utilities for the keiba analytics package."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Iterable, Mapping, Sequence

DB_PATH_DEFAULT = Path("keiba.db")


@contextmanager
def get_connection(db_path: Path | str | None = None) -> Generator[sqlite3.Connection, None, None]:
    """Context manager that yields an SQLite connection.

    Parameters
    ----------
    db_path:
        Optional path to the SQLite database. When omitted, ``keiba.db`` in the
        current working directory is used.
    """

    path = Path(db_path) if db_path else DB_PATH_DEFAULT
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.row_factory = sqlite3.Row
        yield conn
    finally:
        conn.commit()
        conn.close()


def initialize_database(db_path: Path | str | None = None) -> None:
    """Create database tables if they do not already exist."""

    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.executescript(
            """
            CREATE TABLE IF NOT EXISTS races (
                race_id TEXT PRIMARY KEY,
                date TEXT,
                racecourse TEXT,
                distance INTEGER,
                track_condition TEXT,
                num_runners INTEGER,
                track_direction TEXT,
                weather TEXT
            );

            CREATE TABLE IF NOT EXISTS race_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                race_id TEXT NOT NULL,
                horse_number INTEGER,
                horse_name TEXT,
                popularity INTEGER,
                finish_position INTEGER,
                odds_win REAL,
                odds_place REAL,
                return_win REAL,
                return_place REAL,
                FOREIGN KEY (race_id) REFERENCES races(race_id)
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_entries_race_horse
            ON race_entries (race_id, horse_number);
            """
        )


def bulk_insert(
    conn: sqlite3.Connection,
    query: str,
    rows: Iterable[Sequence | Mapping],
) -> None:
    """Execute a parameterized insert for many rows."""

    cursor = conn.cursor()
    cursor.executemany(query, rows)

