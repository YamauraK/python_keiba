"""Analytical helpers for horse racing wagers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from statistics import fmean
from typing import Dict, List, Optional, Sequence

from .database import DB_PATH_DEFAULT, get_connection


@dataclass
class BetRecommendation:
    """Structured representation of a suggested bet."""

    horse_label: str
    popularity_rank: int
    expected_return_per_ticket: float
    suggested_bet_amount: float


def _build_filters(
    *,
    racecourse: Optional[str],
    distance: Optional[int],
    track_condition: Optional[str],
    num_runners: Optional[int],
    track_direction: Optional[str],
    weather: Optional[str],
) -> tuple[str, List[object]]:
    clauses = ["WHERE 1=1"]
    params: List[object] = []

    def add(column: str, value: Optional[object]) -> None:
        if value is None:
            return
        clauses.append(f"AND r.{column} = ?")
        params.append(value)

    add("racecourse", racecourse)
    add("distance", distance)
    add("track_condition", track_condition)
    add("num_runners", num_runners)
    add("track_direction", track_direction)
    add("weather", weather)

    return "\n".join(clauses), params


def _fetch_popularity_metrics(
    db_path: Path | str | None,
    *,
    racecourse: Optional[str] = None,
    distance: Optional[int] = None,
    track_condition: Optional[str] = None,
    num_runners: Optional[int] = None,
    track_direction: Optional[str] = None,
    weather: Optional[str] = None,
) -> List[Dict[str, float]]:
    where_clause, params = _build_filters(
        racecourse=racecourse,
        distance=distance,
        track_condition=track_condition,
        num_runners=num_runners,
        track_direction=track_direction,
        weather=weather,
    )

    query = f"""
        SELECT
            e.popularity AS popularity,
            COUNT(*) AS bets,
            COUNT(DISTINCT e.race_id) AS races,
            SUM(CASE WHEN e.finish_position = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS win_rate,
            AVG(e.return_win) AS avg_return
        FROM race_entries e
        JOIN races r ON r.race_id = e.race_id
        {where_clause}
        GROUP BY e.popularity
        ORDER BY avg_return DESC
    """

    with get_connection(db_path) as conn:
        rows = conn.execute(query, params).fetchall()

    if not rows:
        raise ValueError(
            "No historical entries match the provided race filters. "
            "Ingest more data or relax the conditions."
        )

    metrics = []
    for row in rows:
        avg_return = float(row["avg_return"] or 0.0)
        metrics.append(
            {
                "popularity": int(row["popularity"]),
                "bets": float(row["bets"]),
                "races": float(row["races"]),
                "win_rate": float(row["win_rate"] or 0.0),
                "avg_return": avg_return,
                "expected_return_per_100yen": avg_return - 100.0,
            }
        )

    return metrics


def recommend_bets(
    *,
    db_path: Path | str | None = None,
    racecourse: Optional[str] = None,
    distance: Optional[int] = None,
    track_condition: Optional[str] = None,
    num_runners: Optional[int] = None,
    track_direction: Optional[str] = None,
    weather: Optional[str] = None,
    horse_popularities: Sequence[int],
    budget: int = 10_000,
    num_tickets: int = 10,
) -> List[BetRecommendation]:
    """Recommend wagers for an upcoming race."""

    if num_tickets <= 0:
        raise ValueError("num_tickets must be positive")
    if budget <= 0:
        raise ValueError("budget must be positive")
    if len(horse_popularities) < num_tickets:
        raise ValueError("Provide at least as many horses as tickets you plan to buy")

    metrics = _fetch_popularity_metrics(
        db_path,
        racecourse=racecourse,
        distance=distance,
        track_condition=track_condition,
        num_runners=num_runners,
        track_direction=track_direction,
        weather=weather,
    )

    mean_expected = fmean(m["expected_return_per_100yen"] for m in metrics)
    ticket_value = budget / num_tickets

    popularity_to_metric = {m["popularity"]: m for m in metrics}

    upcoming = []
    for idx, popularity in enumerate(horse_popularities, start=1):
        metric = popularity_to_metric.get(popularity)
        if metric is None:
            expected_bonus = mean_expected
            avg_return = 100.0 + expected_bonus
        else:
            expected_bonus = metric["expected_return_per_100yen"]
            avg_return = metric["avg_return"]
        expected_total = (avg_return / 100.0) * ticket_value
        upcoming.append(
            {
                "horse_label": f"Horse #{idx}",
                "popularity": popularity,
                "expected": expected_bonus,
                "expected_total": expected_total,
            }
        )

    ranked = sorted(upcoming, key=lambda item: item["expected"], reverse=True)

    recommendations = [
        BetRecommendation(
            horse_label=item["horse_label"],
            popularity_rank=item["popularity"],
            expected_return_per_ticket=item["expected_total"],
            suggested_bet_amount=ticket_value,
        )
        for item in ranked[:num_tickets]
    ]

    return recommendations

