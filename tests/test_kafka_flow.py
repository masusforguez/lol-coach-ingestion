"""PASO 5 - Bus de mensajes: sincronización entre fuentes e ingesta.
Con InMemoryBus (sin Docker). El mismo código funciona con KafkaBus."""
import pytest

from ingestion import warehouse as wh
from ingestion.bus import InMemoryBus
from ingestion.consumer import LakeConsumer
from ingestion.pipeline import Endpoints, run_direct
from ingestion.producer import ALL_TOPICS, TOPIC_MATCHES, TOPIC_TIMELINES, Fetcher


@pytest.fixture
def env(tmp_path):
    return {"lake": tmp_path / "lake", "state": str(tmp_path / "pipelines"),
            "fetch_state": tmp_path / "fetcher.json"}


def build(api, env, **fetcher_kw):
    bus = InMemoryBus()
    ep = Endpoints(base_url=api.url)
    f = Fetcher(ep, bus, env["fetch_state"], **fetcher_kw)
    c = LakeConsumer(bus, env["lake"], env["state"])
    return bus, f, c


def silver_counts(lake):
    con = wh.connect()
    wh.build_views(con, lake)
    return con


def test_bus_contract_uncommitted_messages_are_redelivered():
    bus = InMemoryBus()
    for i in range(5):
        bus.publish("t", str(i), {"i": i})
    first = bus.poll("t", "g", max_messages=3)
    assert [m.value["i"] for m in first] == [0, 1, 2]
    again = bus.poll("t", "g", max_messages=3)            # sin commit -> vuelve a llegar lo mismo
    assert [m.value["i"] for m in again] == [0, 1, 2]
    bus.commit("g", first)
    nxt = bus.poll("t", "g", max_messages=10)
    assert [m.value["i"] for m in nxt] == [3, 4]
    assert [m.value["i"] for m in bus.poll("t", "other-group")] == [0, 1, 2, 3, 4]   # grupos independientes


def test_producer_to_consumer_loads_everything(api, env):
    bus, f, c = build(api, env)
    sent = f.run_once()
    got = c.drain()
    assert got[TOPIC_MATCHES] == sent["matches"] == api.state()["visible_matches"]
    con = silver_counts(env["lake"])
    assert con.execute("select count(*) from silver_matches").fetchone()[0] == sent["matches"]
    assert bus.lag("lake-loader", TOPIC_MATCHES) == 0


def test_kafka_path_gives_same_result_as_direct_path(make_api, tmp_path):
    """Dos caminos, mismos datos: el resultado analítico debe coincidir."""
    a1, a2 = make_api(), make_api()
    d = {"lake": tmp_path / "d_lake", "state": str(tmp_path / "d_state")}
    run_direct(Endpoints(base_url=a1.url), d["lake"], d["state"], with_timelines=False)
    k = {"lake": tmp_path / "k_lake", "state": str(tmp_path / "k_state"), "fetch_state": tmp_path / "f.json"}
    bus, f, c = build(a2, k, max_timelines=0)
    f.run_once(); c.drain()
    q = "select count(*), sum(total_cs), sum(kills) from silver_participants"
    cd, ck = silver_counts(d["lake"]), silver_counts(k["lake"])
    assert cd.execute(q).fetchone() == ck.execute(q).fetchone()
    q2 = "select count(*) from silver_videos"
    assert cd.execute(q2).fetchone() == ck.execute(q2).fetchone()


def test_failed_load_is_not_committed_and_is_retried(api, env, monkeypatch):
    bus, f, c = build(api, env)
    f.run_once()
    original = c._load
    calls = {"n": 0}

    def flaky(topic, msgs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("fallo simulado del destino")
        return original(topic, msgs)

    monkeypatch.setattr(c, "_load", flaky)
    with pytest.raises(RuntimeError):
        c.consume_once()
    assert bus.lag("lake-loader", TOPIC_MATCHES) > 0       # nada confirmado
    c.drain()                                              # reintento: ahora sí
    assert bus.lag("lake-loader", TOPIC_MATCHES) == 0
    con = silver_counts(env["lake"])
    n, nd = con.execute("select count(*), count(distinct match_id) from silver_matches").fetchone()
    assert n == nd == api.state()["visible_matches"]


def test_duplicate_messages_do_not_duplicate_silver(api, env):
    """Al menos una vez: si el productor repite mensajes, el dato analítico no se duplica."""
    bus, f, c = build(api, env)
    f.run_once()
    for m in list(bus.topics[TOPIC_MATCHES][:20]):          # simula una repetición del productor
        bus.publish(TOPIC_MATCHES, m[0], m[1])
    c.drain()
    con = silver_counts(env["lake"])
    n, nd = con.execute("select count(*), count(distinct match_id) from silver_matches").fetchone()
    assert n == nd
    assert con.execute("select count(*) from silver_participants").fetchone()[0] == 10 * n


def test_slow_consumer_fast_producer_nothing_lost(api, env):
    """El productor corre varias veces seguidas (pasa el tiempo) y el consumidor va por detrás."""
    bus, f, c = build(api, env)
    f.run_once()
    api.advance(60); f.run_once()
    api.advance(60); f.run_once()
    assert bus.lag("lake-loader", TOPIC_MATCHES) > 0         # hay retraso acumulado
    c.drain()
    con = silver_counts(env["lake"])
    assert con.execute("select count(distinct match_id) from silver_matches").fetchone()[0] \
        == api.state()["visible_matches"]


def test_timelines_lag_behind_matches_and_catch_up(api, env):
    """Sincronización: los timelines cuestan 1 petición cada uno y van más lentos que las partidas."""
    bus, f, c = build(api, env, max_timelines=10)
    f.run_once(); c.drain()
    con = silver_counts(env["lake"])
    assert con.execute("select count(distinct match_id) from silver_timeline_frames").fetchone()[0] == 10
    assert len(f.state["pending_timelines"]) > 0             # pendientes, no perdidos
    pend_before = len(f.state["pending_timelines"])
    f.max_timelines = None                                   # ahora hay presupuesto
    f.run_once(); c.drain()
    con = silver_counts(env["lake"])
    assert len(f.state["pending_timelines"]) == 0
    assert con.execute("select count(distinct match_id) from silver_timeline_frames").fetchone()[0] \
        == api.state()["visible_matches"]
    assert pend_before > 0


def test_fetcher_state_survives_restart_and_does_not_refetch(api, env):
    bus, f, c = build(api, env)
    f.run_once()
    published = len(bus.topics[TOPIC_MATCHES])
    f2 = Fetcher(Endpoints(base_url=api.url), bus, env["fetch_state"])   # "reinicio" del proceso
    f2.run_once()
    assert len(bus.topics[TOPIC_MATCHES]) == published       # no vuelve a pedir lo ya publicado


def test_producer_survives_rate_limit(make_api, env):
    api = make_api(rate_limit=6, rate_window_s=2.0)
    bus, f, c = build(api, env, page_size=20, max_timelines=0)
    f.run_once()
    assert len(bus.topics[TOPIC_MATCHES]) == api.state()["visible_matches"]
