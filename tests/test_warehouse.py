"""PASO 4 - DuckDB sobre Parquet: dedupe por clave primaria y métricas gold."""
import pytest

from ingestion import warehouse as wh
from ingestion.pipeline import Endpoints, run_direct
from synthetic_api import generator as g


@pytest.fixture
def loaded(api, tmp_path):
    lake, state = tmp_path / "lake", str(tmp_path / "pipelines")
    ep = Endpoints(base_url=api.url)
    run_direct(ep, lake, state, max_timelines=30)
    api.advance(180)                                   # salen partidas nuevas y revisiones
    run_direct(ep, lake, state, max_timelines=30)
    con = wh.connect()
    wh.build_views(con, lake)
    return con, api, lake


def test_silver_matches_has_one_row_per_match_with_latest_version(loaded):
    con, api, _ = loaded
    n, nd = con.execute("select count(*), count(distinct match_id) from silver_matches").fetchone()
    assert n == nd == api.state()["visible_matches"]
    revised = con.execute("select count(*) from silver_matches where data_revision = 2").fetchone()[0]
    assert revised > 0


def test_revised_match_participants_come_from_latest_version(loaded):
    con, api, _ = loaded
    now = api.state()["sim_now_ms"]
    row = con.execute("select match_id from silver_matches where data_revision = 2 limit 1").fetchone()
    mid = row[0]
    expected = g.generate_match(g.match_index(mid), now)["participants"]
    got = con.execute("select puuid, total_cs from silver_participants where match_id = ? order by puuid",
                      [mid]).fetchall()
    assert len(got) == 10                              # 10, no 20: no se mezclan versiones
    assert sorted((p["puuid"], p["total_cs"]) for p in expected) == got


def test_participants_derived_metrics(loaded):
    con, *_ = loaded
    bad = con.execute("""select count(*) from silver_participants
                         where cs_per_min < 0 or vision_per_min < 0 or game_duration <= 0""").fetchone()[0]
    assert bad == 0


def test_benchmarks_are_ordered_percentiles(loaded):
    con, *_ = loaded
    rows = con.execute("select cs_p25, cs_p50, cs_p75 from gold_benchmarks").fetchall()
    assert rows and all(a <= b <= c for a, b, c in rows)


def test_early_deaths_match_timeline_events(loaded):
    con, *_ = loaded
    total_view = con.execute("select coalesce(sum(deaths_before_8),0) from gold_early_deaths").fetchone()[0]
    total_raw = con.execute("""select count(*) from silver_timeline_events
                               where type='CHAMPION_KILL' and minute < 8""").fetchone()[0]
    assert total_view == total_raw


def test_player_vs_benchmark_returns_comparison(loaded):
    con, *_ = loaded
    puuid = con.execute("select puuid from silver_participants limit 1").fetchone()[0]
    rows = wh.player_vs_benchmark(con, puuid)
    assert rows and rows[0][2] >= 1


def test_views_built_only_for_existing_tables(tmp_path):
    lake = tmp_path / "empty_lake"
    lake.mkdir()
    con = wh.connect()
    assert wh.build_views(con, lake) == []
