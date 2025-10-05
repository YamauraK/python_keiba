"""Command line interface for the keiba analytics tools."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Optional

from .analysis import recommend_bets
from .data_loader import DataValidationError, ingest_file
from .database import DB_PATH_DEFAULT, initialize_database


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Utilities to manage keiba race data")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init-db", help="Initialise the SQLite database")
    init_parser.add_argument("--db-path", type=Path, default=None, help="Path to the SQLite database")

    ingest_parser = subparsers.add_parser("ingest", help="Load a race data file into the database")
    ingest_parser.add_argument("data_path", type=Path, help="Path to the CSV or structured text file")
    ingest_parser.add_argument("--db-path", type=Path, default=None, help="Path to the SQLite database")

    suggest_parser = subparsers.add_parser("suggest", help="Generate bet recommendations")
    suggest_parser.add_argument("--racecourse", type=str, default=None)
    suggest_parser.add_argument("--distance", type=int, default=None)
    suggest_parser.add_argument("--track-condition", dest="track_condition", type=str, default=None)
    suggest_parser.add_argument("--num-runners", dest="num_runners", type=int, default=None)
    suggest_parser.add_argument("--track-direction", dest="track_direction", type=str, default=None)
    suggest_parser.add_argument("--weather", type=str, default=None)
    suggest_parser.add_argument(
        "--horse-popularities",
        dest="horse_popularities",
        type=int,
        nargs="+",
        required=True,
        help="Popularity ranks for the upcoming race",
    )
    suggest_parser.add_argument("--budget", type=int, default=10_000, help="Total budget in yen")
    suggest_parser.add_argument("--num-tickets", dest="num_tickets", type=int, default=10, help="Number of tickets to buy")
    suggest_parser.add_argument("--db-path", type=Path, default=None, help="Path to the SQLite database")

    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)

    if args.command == "init-db":
        initialize_database(args.db_path)
        print(f"Database initialised at {args.db_path or DB_PATH_DEFAULT}")
        return 0

    if args.command == "ingest":
        try:
            races, entries = ingest_file(args.data_path, db_path=args.db_path)
        except DataValidationError as exc:
            print(f"Validation failed: {exc}")
            return 1
        print(f"Imported {races} races and {entries} race entries.")
        return 0

    if args.command == "suggest":
        recommendations = recommend_bets(
            db_path=args.db_path,
            racecourse=args.racecourse,
            distance=args.distance,
            track_condition=args.track_condition,
            num_runners=args.num_runners,
            track_direction=args.track_direction,
            weather=args.weather,
            horse_popularities=args.horse_popularities,
            budget=args.budget,
            num_tickets=args.num_tickets,
        )
        for rec in recommendations:
            print(
                f"{rec.horse_label} (popularity {rec.popularity_rank}): "
                f"bet {rec.suggested_bet_amount:.0f}円 -> expected return {rec.expected_return_per_ticket:.0f}円"
            )
        return 0

    raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())

