"""PASO 2 - La API sintética debe comportarse como una fuente real limitada.
Contrato: paginación, autenticación, límite (429), créditos (402), cuota (403)."""
from fastapi.testclient import TestClient

from synthetic_api.main import Settings, create_app

RIOT = {"X-Riot-Token": "dev-riot-key"}


def client(**kw):
    base = dict(rate_limit=100_000, failure_rate=0.0, total_matches=300, start_matches=100,
                provider_credits=1000, video_daily_quota=10_000)
    base.update(kw)
    return TestClient(create_app(Settings(**base)))


def test_requires_token():
    c = client()
    assert c.get("/riot/matches").status_code == 401
    assert c.get("/riot/matches", headers={"X-Riot-Token": "mal"}).status_code == 401
    assert c.get("/health").status_code == 200


def test_matches_pagination_and_ordering():
    c = client()
    p1 = c.get("/riot/matches", params={"limit": 30, "offset": 0}, headers=RIOT).json()
    p2 = c.get("/riot/matches", params={"limit": 30, "offset": 30}, headers=RIOT).json()
    ups = [m["updated_at"] for m in p1["items"] + p2["items"]]
    assert ups == sorted(ups)
    assert len({m["match_id"] for m in p1["items"] + p2["items"]}) == 60
    assert p1["total"] > 60


def test_updated_after_filters_incrementally():
    c = client()
    first = c.get("/riot/matches", params={"limit": 100}, headers=RIOT).json()["items"]
    cut = first[10]["updated_at"]
    later = c.get("/riot/matches", params={"limit": 100, "updated_after": cut}, headers=RIOT).json()["items"]
    assert all(m["updated_at"] > cut for m in later)


def test_advancing_clock_releases_new_and_revised_matches():
    c = client()
    adm = {"X-Admin-Token": "dev-admin"}
    n0 = c.get("/riot/matches", params={"limit": 1}, headers=RIOT).json()["total"]
    c.post("/admin/clock/advance", params={"minutes": 120}, headers=adm)
    n1 = c.get("/riot/matches", params={"limit": 1}, headers=RIOT).json()["total"]
    assert n1 > n0


def test_match_cursor_breaks_updated_at_ties(monkeypatch):
    from synthetic_api import generator

    c = client(total_matches=4, start_matches=4)
    timestamp = generator.BASE_TIME_MS + 10_000_000
    original_generate = generator.generate_match

    monkeypatch.setattr(generator, "updated_at", lambda i, now_ms: timestamp)

    def generate_with_tied_timestamp(i, now_ms):
        match = original_generate(i, now_ms)
        match["updated_at"] = timestamp
        return match

    monkeypatch.setattr(generator, "generate_match", generate_with_tied_timestamp)
    first = c.get("/riot/matches", params={"limit": 2}, headers=RIOT).json()["items"]
    next_page = c.get("/riot/matches", params={
        "updated_after": timestamp,
        "match_id_after": first[-1]["match_id"],
        "limit": 2,
    }, headers=RIOT).json()["items"]

    assert [m["match_id"] for m in first + next_page] == sorted(
        m["match_id"] for m in first + next_page
    )
    assert len({m["match_id"] for m in first + next_page}) == 4


def test_timeline_and_unknown_match():
    c = client()
    first = c.get("/riot/matches", params={"limit": 1}, headers=RIOT).json()["items"][0]
    t = c.get(f"/riot/matches/{first['match_id']}/timeline", headers=RIOT)
    assert t.status_code == 200 and t.json()["match_id"] == first["match_id"]
    assert c.get("/riot/matches/EUW1_9999999999/timeline", headers=RIOT).status_code == 404


def test_rate_limit_returns_429_with_retry_after():
    t = {"now": 0.0}
    c = client(rate_limit=3, rate_window_s=60, clock=lambda: t["now"])
    for _ in range(3):
        assert c.get("/riot/matches", params={"limit": 1}, headers=RIOT).status_code == 200
    r = c.get("/riot/matches", params={"limit": 1}, headers=RIOT)
    assert r.status_code == 429 and int(r.headers["Retry-After"]) >= 1
    t["now"] = 61.0   # la ventana se vacía
    assert c.get("/riot/matches", params={"limit": 1}, headers=RIOT).status_code == 200


def test_provider_cursor_pagination_and_credits_exhausted():
    c = client(provider_credits=3)
    h = {"Authorization": "Bearer dev-provider-key"}
    r = c.get("/provider/pro-matches", params={"page_size": 5}, headers=h).json()
    assert len(r["items"]) == 5 and r["next_cursor"]
    r2 = c.get("/provider/pro-matches", params={"page_size": 5, "cursor": r["next_cursor"]}, headers=h).json()
    assert r2["items"][0]["provider_match_id"] != r["items"][0]["provider_match_id"]
    c.get("/provider/pro-matches", headers=h)
    assert c.get("/provider/pro-matches", headers=h).status_code == 402


def test_video_quota_exceeded_and_resets_next_sim_day():
    c = client(video_daily_quota=2)
    p = {"key": "dev-video-key"}
    assert c.get("/youtube/videos", params=p).status_code == 200
    assert c.get("/youtube/videos", params=p).status_code == 200
    r = c.get("/youtube/videos", params=p)
    assert r.status_code == 403 and r.json()["error"] == "quotaExceeded"
    c.post("/admin/clock/advance", params={"minutes": 24 * 60}, headers={"X-Admin-Token": "dev-admin"})
    assert c.get("/youtube/videos", params=p).status_code == 200


def test_random_failures_are_503():
    c = client(failure_rate=1.0)
    assert c.get("/riot/matches", headers=RIOT).status_code == 503
