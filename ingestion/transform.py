"""Transformación MÍNIMA (EL antes que T): solo darle forma tabular a lo que llega.
No se calcula ninguna métrica aquí; eso se hace después, en DuckDB."""
from __future__ import annotations

from typing import Iterator


def timeline_rows(raw: dict) -> Iterator[tuple[str, dict]]:
    """Un timeline -> filas de dos tablas: timeline_frames y timeline_events."""
    mid = raw["match_id"]
    for frame in raw["frames"]:
        for p in frame["participants"]:
            yield "timeline_frames", {"match_id": mid, "minute": frame["minute"], "puuid": p["puuid"],
                                      "gold": p["gold"], "xp": p["xp"], "cs": p["cs"]}
    for e in raw["events"]:
        yield "timeline_events", {"match_id": mid, **e}
