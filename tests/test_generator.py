"""PASO 1 - El generador debe ser determinista y coherente.
Estos tests son el CONTRATO del generador: si fallan, se arregla el código,
no el test."""
from synthetic_api import generator as g

NOW = g.BASE_TIME_MS + 10 * 24 * 3600 * 1000  # mucho después de todo


def test_match_is_deterministic():
    assert g.generate_match(5, NOW) == g.generate_match(5, NOW)
    assert g.generate_match(5, NOW) != g.generate_match(6, NOW)


def test_match_has_ten_participants_two_teams_one_winner():
    m = g.generate_match(3, NOW)
    assert len(m["participants"]) == 10
    teams = {p["team_id"] for p in m["participants"]}
    assert teams == {100, 200}
    winners = {p["team_id"] for p in m["participants"] if p["win"]}
    assert winners == {m["winning_team"]}
    assert len({p["puuid"] for p in m["participants"]}) == 10


def test_match_id_roundtrip():
    for i in (0, 1, 999):
        assert g.match_index(g.match_id(i)) == i


def test_revised_match_changes_after_revision_time():
    i = 7  # i % 20 == 7  -> partida que se revisa
    assert g.is_revised(i)
    before = g.generate_match(i, g.match_ended_at(i) + 1)
    after = g.generate_match(i, g.revision_time(i) + 1)
    assert before["data_revision"] == 1 and after["data_revision"] == 2
    assert after["updated_at"] > before["updated_at"]
    assert after["participants"][0]["total_cs"] == before["participants"][0]["total_cs"] + 12


def test_non_revised_match_never_changes():
    i = 8
    assert not g.is_revised(i)
    assert g.generate_match(i, g.match_ended_at(i) + 1) == g.generate_match(i, NOW)


def test_timeline_consistent_with_match():
    m = g.generate_match(4, NOW)
    t = g.generate_timeline(4, NOW)
    assert t["match_id"] == m["match_id"]
    assert len(t["frames"]) == m["game_duration"] // 60 + 1
    deaths = [e for e in t["events"] if e["type"] == "CHAMPION_KILL"]
    assert len(deaths) == sum(p["deaths"] for p in m["participants"])
    assert len({e["event_id"] for e in t["events"]}) == len(t["events"])


def test_pro_and_video_deterministic_and_monotonic():
    assert g.generate_pro_match(2) == g.generate_pro_match(2)
    assert g.pro_ingested_at(3) > g.pro_ingested_at(2)
    v = g.generate_video(1, 100)
    assert v == g.generate_video(1, 100)
    assert g.video_published_ms(2) > g.video_published_ms(1)


def test_tokens_roundtrip():
    assert g.decode_token(g.encode_token(42)) == 42
