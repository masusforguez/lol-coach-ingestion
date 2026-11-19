"""PASO 3 - Ingesta directa con dlt: incremental, idempotente, tolerante a límites.
Contrato de la ingesta. Si un test falla, se arregla el código, no el test."""
import pytest

from ingestion.pipeline import Endpoints, run_direct
from tests.helpers import count_rows, distinct


@pytest.fixture
def dirs(tmp_path):
    return {"lake": tmp_path / "lake", "state": str(tmp_path / "pipelines")}


def ep(api):
    return Endpoints(base_url=api.url)


def test_first_run_loads_matches_and_nested_participants(api, dirs):
    run_direct(ep(api), dirs["lake"], dirs["state"], sources=("riot",), with_timelines=False)
    n = distinct(dirs["lake"], "riot", "matches", "match_id")
    assert n == api.state()["visible_matches"]          # exactamente las visibles al arrancar
    # dlt normaliza la lista anidada 'participants' en una tabla hija: 10 por partida
    assert count_rows(dirs["lake"], "riot", "matches__participants") == 10 * count_rows(dirs["lake"], "riot", "matches")


def test_rerun_is_idempotent_no_new_rows(api, dirs):
    run_direct(ep(api), dirs["lake"], dirs["state"], sources=("riot",), with_timelines=False)
    before = count_rows(dirs["lake"], "riot", "matches")
    run_direct(ep(api), dirs["lake"], dirs["state"], sources=("riot",), with_timelines=False)
    assert count_rows(dirs["lake"], "riot", "matches") == before


def test_incremental_does_not_redownload_everything(api, dirs, monkeypatch):
    """No basta con que las filas no se dupliquen: la segunda pasada no debe
    volver a pedir todo. dlt filtraría por su cuenta y el resultado sería
    correcto con el doble de tráfico, que es justo lo que el cursor evita."""
    from ingestion import clients

    run_direct(ep(api), dirs["lake"], dirs["state"], sources=("riot",), with_timelines=False)

    seen = []
    original = clients.riot_matches_pages

    def spy(base_url, token, updated_after, page_size=50, match_id_after=None):
        seen.append(updated_after)
        return original(base_url, token, updated_after, page_size, match_id_after)

    monkeypatch.setattr(clients, "riot_matches_pages", spy)
    run_direct(ep(api), dirs["lake"], dirs["state"], sources=("riot",), with_timelines=False)

    assert seen, "la segunda pasada no pidió nada"
    assert all(u > 0 for u in seen), f"se pidió desde cero: {seen}"


def test_incremental_loads_only_new_and_revised(api, dirs):
    from synthetic_api import generator as g
    total = api.settings.total_matches
    run_direct(ep(api), dirs["lake"], dirs["state"], sources=("riot",), with_timelines=False)
    before_rows = count_rows(dirs["lake"], "riot", "matches")
    now0 = api.state()["sim_now_ms"]
    cursor0 = max(g.updated_at(i, now0) for i in range(total) if g.match_ended_at(i) <= now0)

    api.advance(180)  # pasan 3 h: salen partidas nuevas y se revisan algunas antiguas
    now1 = api.state()["sim_now_ms"]
    # Oráculo independiente: lo que ha cambiado desde el cursor según el generador
    expected = [i for i in range(total)
                if g.match_ended_at(i) <= now1 and g.updated_at(i, now1) > cursor0]
    revised_old = [i for i in expected if g.match_ended_at(i) <= now0]
    assert revised_old, "el escenario debe incluir revisiones de partidas ya cargadas"

    run_direct(ep(api), dirs["lake"], dirs["state"], sources=("riot",), with_timelines=False)
    after_rows = count_rows(dirs["lake"], "riot", "matches")
    assert after_rows - before_rows == len(expected)    # ni una fila de más ni de menos
    assert distinct(dirs["lake"], "riot", "matches", "match_id") == api.state()["visible_matches"]


def test_timelines_loaded_for_new_matches(api, dirs):
    run_direct(ep(api), dirs["lake"], dirs["state"], sources=("riot",), max_timelines=5)
    assert distinct(dirs["lake"], "riot", "timeline_frames", "match_id") == 5
    assert count_rows(dirs["lake"], "riot", "timeline_events") > 0


def test_survives_rate_limit_with_retries(make_api, dirs):
    # límite muy bajo: 4 peticiones cada 2 s -> habrá 429 y dlt debe esperar y reintentar
    api = make_api(rate_limit=4, rate_window_s=2.0)
    run_direct(ep(api), dirs["lake"], dirs["state"], sources=("riot",), with_timelines=False, page_size=20)
    assert distinct(dirs["lake"], "riot", "matches", "match_id") > 100


def test_provider_credits_exhausted_loads_partial_then_continues(make_api, dirs):
    api = make_api(provider_credits=2, start_matches=300)
    run_direct(ep(api), dirs["lake"], dirs["state"], sources=("provider",), page_size=5)
    first = distinct(dirs["lake"], "provider", "pro_matches", "provider_match_id")
    assert first == 10                                   # 2 créditos x 5 por página: carga parcial, sin fallo
    api.app.state.st.credits = 1000                      # se recargan créditos
    run_direct(ep(api), dirs["lake"], dirs["state"], sources=("provider",), page_size=5)
    assert distinct(dirs["lake"], "provider", "pro_matches", "provider_match_id") == 20  # continúa donde lo dejó
    assert count_rows(dirs["lake"], "provider", "pro_matches") == 20                      # sin duplicados


def test_video_quota_exceeded_loads_partial_then_continues(make_api, dirs):
    api = make_api(video_daily_quota=1, start_matches=380)
    run_direct(ep(api), dirs["lake"], dirs["state"], sources=("video",), page_size=3)
    first = distinct(dirs["lake"], "video", "videos", "video_id")
    assert first == 3                                     # cuota 1 -> una sola página de 3
    api.advance(24 * 60)                                  # día nuevo: la cuota se renueva
    run_direct(ep(api), dirs["lake"], dirs["state"], sources=("video",), page_size=3)
    assert distinct(dirs["lake"], "video", "videos", "video_id") > first
