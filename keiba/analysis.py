"""Analytical helpers for horse racing wagers."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from statistics import fmean
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .database import DB_PATH_DEFAULT, get_connection


@dataclass
class BetRecommendation:
    """Structured representation of a suggested trio (三連複) bet."""

    combination: Tuple[int, int, int]
    hit_rate: float
    estimated_average_payout: float
    expected_return_per_ticket: float
    suggested_bet_amount: float

    @property
    def combination_label(self) -> str:
        """Return a human readable label such as ``1-2-4``."""

        return "-".join(str(popularity) for popularity in self.combination)


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


def _fetch_trifecta_statistics(
    db_path: Path | str | None,
    *,
    racecourse: Optional[str] = None,
    distance: Optional[int] = None,
    track_condition: Optional[str] = None,
    num_runners: Optional[int] = None,
    track_direction: Optional[str] = None,
    weather: Optional[str] = None,
) -> Tuple[int, Dict[Tuple[int, int, int], Dict[str, float]]]:
    """Return hit counts and payout aggregates for 三連複 combinations.

    The SQLite schema does not store actual 三連複払戻金. 代わりに、1 着馬の
    単勝払戻しと上位 3 頭の複勝払戻しを合算したものを近似的な平均配当と
    して扱う。実際の三連複配当はこれより高くなることが多いため、安全側の
    推定として利用できる。
    """

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
            r.race_id,
            e.popularity,
            e.finish_position,
            e.return_win,
            e.return_place
        FROM race_entries e
        JOIN races r ON r.race_id = e.race_id
        {where_clause}
        AND e.finish_position <= 3
        ORDER BY r.race_id, e.finish_position
    """

    with get_connection(db_path) as conn:
        rows = conn.execute(query, params).fetchall()

    if not rows:
        raise ValueError(
            "No historical entries match the provided race filters. "
            "Ingest more data or relax the conditions."
        )

    combo_stats: Dict[Tuple[int, int, int], Dict[str, float]] = {}
    total_races = 0

    current_race: Optional[str] = None
    current_entries: List[Dict[str, float]] = []

    def _flush_race(entries: Iterable[Dict[str, float]]) -> None:
        nonlocal total_races
        entries = list(entries)
        if len(entries) != 3:
            return
        popularities = [int(item["popularity"]) for item in entries]
        if any(pop <= 0 for pop in popularities):
            return
        combination = tuple(sorted(popularities))
        total_races += 1

        total_return = 0.0
        for item in entries:
            total_return += float(item["return_place"] or 0.0)
            if int(item["finish_position"]) == 1:
                total_return += float(item["return_win"] or 0.0)

        stats = combo_stats.setdefault(
            combination,
            {"count": 0.0, "total_return": 0.0, "total_popularity_sum": 0.0},
        )
        stats["count"] += 1
        stats["total_return"] += total_return
        stats["total_popularity_sum"] += sum(popularities)

    for row in rows:
        race_id = str(row["race_id"])
        if race_id != current_race:
            if current_race is not None:
                _flush_race(current_entries)
            current_race = race_id
            current_entries = []
        current_entries.append(
            {
                "popularity": row["popularity"],
                "finish_position": row["finish_position"],
                "return_win": row["return_win"],
                "return_place": row["return_place"],
            }
        )

    if current_race is not None:
        _flush_race(current_entries)

    if total_races == 0:
        raise ValueError(
            "Historical data does not contain complete trifecta results for the "
            "given filters."
        )

    return total_races, combo_stats


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
    """Recommend 三連複 wagers for an upcoming race."""

    if num_tickets <= 0:
        raise ValueError("num_tickets must be positive")
    if budget <= 0:
        raise ValueError("budget must be positive")
    if len(horse_popularities) < 3:
        raise ValueError("三連複を検討するには最低でも 3 頭の人気順が必要です")

    total_races, combo_stats = _fetch_trifecta_statistics(
        db_path,
        racecourse=racecourse,
        distance=distance,
        track_condition=track_condition,
        num_runners=num_runners,
        track_direction=track_direction,
        weather=weather,
    )

    candidate_combos = list(combinations(sorted(set(horse_popularities)), 3))
    if not candidate_combos:
        raise ValueError("指定された人気順では三連複の組み合わせを作成できません")

    ticket_value = budget / num_tickets

    if combo_stats:
        avg_returns = [stats["total_return"] / stats["count"] for stats in combo_stats.values() if stats["count"] > 0]
        global_avg_return = fmean(avg_returns)
    else:
        global_avg_return = 100.0

    alpha = 1.0
    smoothing_denominator = len(candidate_combos)

    recommendations: List[BetRecommendation] = []
    for combo in candidate_combos:
        stats = combo_stats.get(combo)
        hit_count = stats["count"] if stats else 0.0
        smoothed_hit_rate = (hit_count + alpha) / (total_races + alpha * smoothing_denominator)

        if stats and stats["count"] > 0:
            avg_payout = stats["total_return"] / stats["count"]
        else:
            avg_payout = global_avg_return

        expected_return_multiplier = (avg_payout / 100.0) * smoothed_hit_rate
        expected_return_per_ticket = ticket_value * expected_return_multiplier

        recommendations.append(
            BetRecommendation(
                combination=combo,
                hit_rate=smoothed_hit_rate,
                estimated_average_payout=avg_payout,
                expected_return_per_ticket=expected_return_per_ticket,
                suggested_bet_amount=ticket_value,
            )
        )

    recommendations.sort(key=lambda rec: rec.expected_return_per_ticket, reverse=True)

    return recommendations[: min(num_tickets, len(recommendations))]

