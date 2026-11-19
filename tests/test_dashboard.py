import subprocess
from pathlib import Path

from fastapi.testclient import TestClient

from synthetic_api.main import Settings, create_app


def local_client():
    return TestClient(
        create_app(Settings(dev_dashboard_enabled=True)),
        base_url="http://127.0.0.1",
        client=("127.0.0.1", 50000),
    )


def test_dashboard_is_disabled_by_default():
    client = TestClient(create_app(Settings()))
    assert client.get("/dev").status_code == 404
    assert client.post("/dev/run-tests").status_code == 404


def test_dashboard_is_served_when_enabled():
    response = local_client().get("/dev")
    assert response.status_code == 200
    assert "LoL Coach · Consola de pruebas" in response.text


def test_dashboard_rejects_non_loopback_clients():
    client = TestClient(create_app(Settings(dev_dashboard_enabled=True)), base_url="http://example.test")
    assert client.get("/dev").status_code == 403
    assert client.post("/dev/run-tests").status_code == 403


def test_request_history_omits_query_secrets_and_dashboard_polling():
    client = local_client()
    response = client.get("/youtube/videos", params={"key": "dev-video-key", "maxResults": 1})
    assert response.status_code == 200

    history = client.get("/dev/api-requests").json()
    assert len(history) == 1
    assert history[0]["path"] == "/youtube/videos"
    assert history[0]["status_code"] == 200
    assert "dev-video-key" not in str(history)
    assert client.post("/dev/api-requests/clear").json() == {"status": "cleared"}
    assert client.get("/dev/api-requests").json() == []


def test_test_runner_uses_fixed_local_pytest_command(monkeypatch):
    calls = {}

    def fake_run(command, **kwargs):
        calls["command"] = command
        calls["kwargs"] = kwargs
        return subprocess.CompletedProcess(command, 0, stdout="4 passed")

    monkeypatch.setattr("synthetic_api.main.subprocess.run", fake_run)
    response = local_client().post("/dev/run-tests")

    assert response.status_code == 200
    assert response.json()["passed"] is True
    assert response.json()["output"] == "4 passed"
    assert calls["command"][-4:] == ["-m", "pytest", "tests", "-q"]
    assert calls["kwargs"]["cwd"] == Path(__file__).resolve().parents[1]
    assert calls["kwargs"]["timeout"] == 300
