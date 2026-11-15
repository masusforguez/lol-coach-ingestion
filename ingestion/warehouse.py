"""Capa analítica con DuckDB sobre el lago Parquet.

El lago (bronze) conserva todo el histórico, incluidas las versiones antiguas de
una partida revisada. Aquí se crean vistas:

  silver_*  -> última versión de cada registro (dedupe por clave primaria)
  gold_*    -> métricas listas para el coach de IA (benchmarks por rango y rol)

DuckDB lee Parquet directamente: no hay que cargar nada en una base de datos.
"""
from __future__ import annotations

import glob
from pathlib import Path

import duckdb


def connect(db_path: str = ":memory:") -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path)


def _pattern(lake, dataset: str, table: str) -> str:
    return f"{Path(lake).resolve()}/{dataset}/{table}/*.parquet"


def has_table(lake, dataset: str, table: str) -> bool:
    return bool(glob.glob(_pattern(lake, dataset, table)))


def _latest(lake, dataset, table, key: str, order: str) -> str:
    """SELECT con solo la última versión de cada clave."""
    return f"""
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, row_number() OVER (PARTITION BY {key} ORDER BY {order}, _dlt_load_id DESC) AS rn
            FROM read_parquet('{_pattern(lake, dataset, table)}', union_by_name=true)
        ) WHERE rn = 1"""


def build_views(con, lake) -> list[str]:
    """Crea las vistas para las tablas que existan en el lago. Devuelve sus nombres."""
    made = []

    def view(name, sql):
        con.execute(f"CREATE OR REPLACE VIEW {name} AS {sql}")
        made.append(name)

    if has_table(lake, "riot", "matches"):
        view("silver_matches", _latest(lake, "riot", "matches", "match_id", "updated_at DESC"))
        if has_table(lake, "riot", "matches__participants"):
            # Los participantes de la ÚLTIMA versión de cada partida (enlace por _dlt_parent_id)
            view("silver_participants", f"""
                SELECT m.match_id, m.rank_tier, m.patch, m.game_duration, m.data_revision,
                       p.puuid, p.riot_id, p.champion, p.role, p.team_id, p.win, p.kills, p.deaths,
                       p.assists, p.total_cs, p.gold_earned, p.vision_score, p.first_item_minute,
                       p.total_cs / (m.game_duration / 60.0)    AS cs_per_min,
                       p.vision_score / (m.game_duration / 60.0) AS vision_per_min
                FROM read_parquet('{_pattern(lake, 'riot', 'matches__participants')}', union_by_name=true) p
                JOIN silver_matches m ON p._dlt_parent_id = m._dlt_id""")
    if has_table(lake, "riot", "timeline_frames"):
        view("silver_timeline_frames", _latest(lake, "riot", "timeline_frames", "match_id, minute, puuid", "minute"))
    if has_table(lake, "riot", "timeline_events"):
        view("silver_timeline_events", _latest(lake, "riot", "timeline_events", "event_id", "minute"))
    if has_table(lake, "provider", "pro_matches"):
        view("silver_pro_matches", _latest(lake, "provider", "pro_matches", "provider_match_id", "ingested_at DESC"))
    if has_table(lake, "video", "videos"):
        view("silver_videos", _latest(lake, "video", "videos", "video_id", "published_ms DESC"))

    if "silver_participants" in made:
        view("gold_benchmarks", """
            SELECT rank_tier, role, count(*) AS n,
                   quantile_cont(cs_per_min, 0.25)     AS cs_p25,
                   quantile_cont(cs_per_min, 0.50)     AS cs_p50,
                   quantile_cont(cs_per_min, 0.75)     AS cs_p75,
                   quantile_cont(vision_per_min, 0.50) AS vision_p50,
                   avg(deaths)                         AS deaths_avg
            FROM silver_participants GROUP BY rank_tier, role""")
    if "silver_participants" in made and "silver_timeline_events" in made:
        view("gold_early_deaths", """
            SELECT e.match_id, e.puuid, count(*) AS deaths_before_8
            FROM silver_timeline_events e
            WHERE e.type = 'CHAMPION_KILL' AND e.minute < 8
            GROUP BY e.match_id, e.puuid""")
    return made


def player_vs_benchmark(con, puuid: str, last: int = 20):
    """Compara las últimas partidas de un jugador con la mediana de su rango y rol.
    Es el tipo de dato que luego recibiría el modelo de lenguaje para redactar consejos."""
    return con.execute("""
        WITH mine AS (
            SELECT p.*, row_number() OVER (ORDER BY m.game_creation DESC) AS rk
            FROM silver_participants p JOIN silver_matches m USING (match_id)
            WHERE p.puuid = ?
        )
        SELECT mine.rank_tier, mine.role, count(*) AS games,
               avg(mine.cs_per_min)   AS my_cs_per_min,   any_value(b.cs_p50)     AS bench_cs_p50,
               avg(mine.vision_per_min) AS my_vision_per_min, any_value(b.vision_p50) AS bench_vision_p50
        FROM mine JOIN gold_benchmarks b USING (rank_tier, role)
        WHERE mine.rk <= ? GROUP BY mine.rank_tier, mine.role""", [puuid, last]).fetchall()
