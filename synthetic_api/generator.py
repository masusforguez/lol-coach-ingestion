"""Generador determinista de datos sintéticos para el coach de IA de LoL.

Simula tres fuentes distintas:
  * "riot"     -> partidas y timelines (como Match-V5, capa gratuita con límite)
  * "provider" -> partidas profesionales (proveedor de pago, con créditos)
  * "video"    -> metadatos de vídeos de rangos altos (como YouTube/Twitch, con cuota)

Todo es DETERMINISTA: el mismo índice produce siempre el mismo dato. Así los tests
son reproducibles y una "revisión" posterior de una partida es reconstruible.
Los datos son inventados: no vienen de Riot ni de ninguna plataforma real.
"""
from __future__ import annotations

import base64
import hashlib
import random
from datetime import datetime, timezone

from faker import Faker

# ---------------------------------------------------------------- constantes
CHAMPIONS = [
    "Ahri", "Garen", "Lee Sin", "Jinx", "Thresh", "Morgana", "Camille", "Caitlyn",
    "Talon", "Yasuo", "Lux", "Darius", "Ezreal", "Leona", "Zed", "Vi", "Ashe",
    "Katarina", "Malphite", "Kai'Sa", "Nautilus", "Sett", "Viego", "Orianna",
    "Jhin", "Renekton", "Elise", "Syndra", "Lucian", "Braum",
]
ROLES = ["TOP", "JUNGLE", "MID", "BOTTOM", "UTILITY"]
TIERS = ["IRON", "BRONZE", "SILVER", "GOLD", "PLATINUM", "EMERALD", "DIAMOND", "MASTER", "CHALLENGER"]
TIER_WEIGHTS = [8, 14, 22, 22, 16, 9, 5, 3, 1]
PATCHES = ["26.21", "26.22"]
PLATFORM = "EUW1"
LEAGUES = ["LEC", "LCK", "LPL", "LCS"]

BASE_TIME_MS = int(datetime(2026, 11, 1, tzinfo=timezone.utc).timestamp() * 1000)
MATCH_INTERVAL_MS = 30_000          # una partida nueva cada 30 s (simulado)
PRO_INTERVAL_MS = 10 * 60_000       # una partida pro cada 10 min
VIDEO_INTERVAL_MS = 20 * 60_000     # un vídeo nuevo cada 20 min
REVISION_DELAY_MS = 2 * 3_600_000   # las partidas "revisadas" se corrigen 2 h después
N_PLAYERS = 400

_fake = Faker("es_ES")


# ---------------------------------------------------------------- jugadores
def player(idx: int) -> dict:
    """Jugador sintético estable (mismo idx -> mismos datos)."""
    _fake.seed_instance(idx)
    return {
        "puuid": f"puuid-{idx:05d}",
        "riot_id": f"{_fake.user_name()}#{_fake.pystr(min_chars=3, max_chars=4).upper()}",
    }


# ---------------------------------------------------------------- partidas Riot
def match_id(i: int) -> str:
    return f"{PLATFORM}_{7_000_000_000 + i}"


def match_index(mid: str) -> int:
    return int(mid.split("_")[1]) - 7_000_000_000


def _rng(*parts) -> random.Random:
    return random.Random("|".join(str(p) for p in parts))


def match_duration_s(i: int) -> int:
    return _rng("dur", i).randint(1200, 2400)  # 20-40 min


def match_ended_at(i: int) -> int:
    return BASE_TIME_MS + i * MATCH_INTERVAL_MS + match_duration_s(i) * 1000


def is_revised(i: int) -> bool:
    """Un 5 % de las partidas se corrige más tarde (p. ej. se recalcula el CS)."""
    return i % 20 == 7


def revision_time(i: int) -> int:
    return match_ended_at(i) + REVISION_DELAY_MS


def updated_at(i: int, now_ms: int) -> int:
    """Instante de la última versión visible en 'now_ms' (cursor incremental)."""
    if is_revised(i) and now_ms >= revision_time(i):
        return revision_time(i)
    return match_ended_at(i)


def generate_match(i: int, now_ms: int) -> dict:
    """Partida con 10 participantes. Se parece a Match-V5, simplificada."""
    r = _rng("match", i)
    duration = match_duration_s(i)
    tier = r.choices(TIERS, TIER_WEIGHTS)[0]
    tier_idx = TIERS.index(tier)
    patch = PATCHES[(i // 1500) % len(PATCHES)]
    winner = r.choice([100, 200])
    players_idx = r.sample(range(N_PLAYERS), 10)
    revised_now = is_revised(i) and now_ms >= revision_time(i)

    participants = []
    for slot, pidx in enumerate(players_idx):
        pr = _rng("part", i, slot)
        role = ROLES[slot % 5]
        team = 100 if slot < 5 else 200
        minutes = duration / 60
        cs_rate = {"TOP": 6.5, "JUNGLE": 5.5, "MID": 7.0, "BOTTOM": 7.5, "UTILITY": 1.2}[role]
        cs_rate *= 0.8 + 0.04 * tier_idx            # mejores rangos farmean más
        cs = max(0, int(minutes * cs_rate * pr.uniform(0.8, 1.2)))
        if revised_now and slot == 0:
            cs += 12                                 # corrección posterior
        win = team == winner
        deaths = max(0, int(pr.gauss(5 if not win else 3.5, 2)))
        p = player(pidx)
        participants.append({
            "puuid": p["puuid"],
            "riot_id": p["riot_id"],
            "champion": pr.choice(CHAMPIONS),
            "role": role,
            "team_id": team,
            "win": win,
            "kills": max(0, int(pr.gauss(6 if win else 4, 3))),
            "deaths": deaths,
            "assists": max(0, int(pr.gauss(7, 4))),
            "total_cs": cs,
            "gold_earned": int(cs * 21 + pr.randint(4000, 9000)),
            "vision_score": int(minutes * (2.5 if role == "UTILITY" else 0.8) * pr.uniform(0.7, 1.3)),
            "first_item_minute": round(pr.uniform(1.0, 4.5), 1),
        })

    return {
        "match_id": match_id(i),
        "platform": PLATFORM,
        "queue_id": 420,
        "rank_tier": tier,
        "patch": patch,
        "game_creation": BASE_TIME_MS + i * MATCH_INTERVAL_MS,
        "game_duration": duration,
        "winning_team": winner,
        "updated_at": updated_at(i, now_ms),
        "data_revision": 2 if revised_now else 1,
        "participants": participants,
    }


def generate_timeline(i: int, now_ms: int) -> dict:
    """Timeline minuto a minuto: oro/xp/cs por jugador y eventos (muertes, objetos)."""
    m = generate_match(i, now_ms)
    r = _rng("tl", i)
    minutes = m["game_duration"] // 60
    frames = []
    for minute in range(minutes + 1):
        frames.append({
            "minute": minute,
            "participants": [
                {
                    "puuid": p["puuid"],
                    "gold": int(500 + p["gold_earned"] * minute / max(minutes, 1)),
                    "xp": int(minute * 450 * (0.9 + (k % 5) * 0.05)),
                    "cs": int(p["total_cs"] * minute / max(minutes, 1)),
                }
                for k, p in enumerate(m["participants"])
            ],
        })
    events = []
    n = 0
    for p in m["participants"]:
        events.append({"event_id": f"{m['match_id']}-{n}", "minute": p["first_item_minute"],
                       "type": "ITEM_PURCHASED", "puuid": p["puuid"], "detail": "first_item"})
        n += 1
        for _ in range(p["deaths"]):
            # las muertes se concentran en la parte media de la partida
            minute = round(min(minutes, max(1.0, r.gauss(minutes * 0.45, minutes * 0.25))), 1)
            events.append({"event_id": f"{m['match_id']}-{n}", "minute": minute,
                           "type": "CHAMPION_KILL", "puuid": p["puuid"], "detail": "death"})
            n += 1
    events.sort(key=lambda e: e["minute"])
    return {"match_id": m["match_id"], "frames": frames, "events": events}


# ---------------------------------------------------------------- proveedor de pago
def pro_ingested_at(k: int) -> int:
    return BASE_TIME_MS + k * PRO_INTERVAL_MS


def generate_pro_match(k: int) -> dict:
    r = _rng("pro", k)
    picks = []
    for side in ("blue", "red"):
        for role in ROLES:
            picks.append({"side": side, "role": role, "champion": r.choice(CHAMPIONS),
                          "player_handle": f"{side[:1].upper()}-{role[:3]}-{r.randint(1, 60)}"})
    return {
        "provider_match_id": f"PRO-{k:06d}",
        "league": r.choice(LEAGUES),
        "patch": PATCHES[(k // 400) % len(PATCHES)],
        "played_at": datetime.fromtimestamp(pro_ingested_at(k) / 1000 - 3600, timezone.utc).isoformat(),
        "winner": r.choice(["blue", "red"]),
        "game_length_s": r.randint(1500, 2700),
        "ingested_at": pro_ingested_at(k),
        "picks": picks,
    }


# ---------------------------------------------------------------- vídeos
def video_published_ms(k: int) -> int:
    return BASE_TIME_MS + k * VIDEO_INTERVAL_MS


def generate_video(k: int, total_matches_visible: int) -> dict:
    r = _rng("vid", k)
    champ, opp = r.sample(CHAMPIONS, 2)
    role = r.choice(ROLES)
    rank = r.choice(["CHALLENGER", "CHALLENGER", "GRANDMASTER"])
    patch = PATCHES[(k // 300) % len(PATCHES)]
    linked = None
    if total_matches_visible > 0 and r.random() < 0.7:
        linked = match_id(r.randrange(total_matches_visible))
    vid = hashlib.sha1(f"video-{k}".encode()).hexdigest()[:11]
    return {
        "video_id": vid,
        "channel": f"Replays {rank.title()} {champ}",
        "title": f"{rank.title()} {champ} {role} vs {opp} - Parche {patch}",
        "published_at": datetime.fromtimestamp(video_published_ms(k) / 1000, timezone.utc)
                        .strftime("%Y-%m-%dT%H:%M:%SZ"),
        "published_ms": video_published_ms(k),
        "champion": champ,
        "opponent": opp,
        "role": role,
        "rank": rank,
        "patch": patch,
        "duration_s": r.randint(1500, 2600),
        "riot_match_id": linked,
    }


# ---------------------------------------------------------------- tokens de página
def encode_token(n: int) -> str:
    return base64.urlsafe_b64encode(f"o:{n}".encode()).decode()


def decode_token(tok: str) -> int:
    try:
        return int(base64.urlsafe_b64decode(tok.encode()).decode().split(":")[1])
    except Exception as exc:  # token manipulado
        raise ValueError("token inválido") from exc
