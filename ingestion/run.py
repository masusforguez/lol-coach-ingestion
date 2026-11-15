"""Línea de comandos del prototipo.

  python -m ingestion.run direct     dlt consulta las APIs (incremental) y escribe Parquet
  python -m ingestion.run produce    productor: APIs -> Kafka (topics lol.raw.*)
  python -m ingestion.run consume    consumidor: Kafka -> dlt -> Parquet
  python -m ingestion.run pipeline   produce + consume seguidos (sirve con bus en memoria)
  python -m ingestion.run report     DuckDB: benchmarks y comparación de un jugador
"""
from __future__ import annotations

import argparse
import logging
import os

from . import warehouse as wh
from .bus import InMemoryBus, KafkaBus
from .consumer import LakeConsumer
from .pipeline import Endpoints, run_direct
from .producer import Fetcher


def make_bus(kind: str):
    if kind == "kafka":
        return KafkaBus(os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    return InMemoryBus()


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["direct", "produce", "consume", "pipeline", "report"])
    ap.add_argument("--lake", default="data/lake", help="carpeta del lago Parquet")
    ap.add_argument("--state", default="data/state", help="estado de dlt y del productor")
    ap.add_argument("--bus", choices=["memory", "kafka"], default="memory")
    ap.add_argument("--page-size", type=int, default=50)
    ap.add_argument("--max-timelines", type=int, default=None, help="presupuesto de timelines por pasada")
    ap.add_argument("--puuid", default=None, help="jugador para el informe")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    os.makedirs(args.state, exist_ok=True)
    ep = Endpoints.from_dlt_config()

    if args.command == "direct":
        infos = run_direct(ep, args.lake, f"{args.state}/dlt", page_size=args.page_size,
                           max_timelines=args.max_timelines)
        for k, v in infos.items():
            print(f"[{k}]", v)
        return

    if args.command in ("produce", "pipeline"):
        bus = make_bus(args.bus)
        if args.command == "produce" and args.bus == "memory":
            print("AVISO: con --bus memory los mensajes se pierden al terminar; usa --bus kafka o 'pipeline'.")
        f = Fetcher(ep, bus, f"{args.state}/fetcher.json", args.page_size, args.max_timelines)
        print("publicado:", f.run_once())
    if args.command in ("consume", "pipeline"):
        if args.command == "consume":
            bus = make_bus(args.bus)
        c = LakeConsumer(bus, args.lake, f"{args.state}/dlt")
        print("cargado:", c.drain())
    if args.command == "report":
        con = wh.connect()
        views = wh.build_views(con, args.lake)
        print("vistas:", ", ".join(views))
        if "gold_benchmarks" in views:
            print("\nBenchmarks por rango y rol (mediana de CS/min):")
            for r in con.execute("""select rank_tier, role, n, round(cs_p25,2), round(cs_p50,2), round(cs_p75,2)
                                    from gold_benchmarks where n >= 20 order by rank_tier, role limit 12""").fetchall():
                print("  ", r)
            puuid = args.puuid or con.execute("select puuid from silver_participants limit 1").fetchone()[0]
            print(f"\nJugador {puuid} frente a la mediana de su rango y rol:")
            for r in wh.player_vs_benchmark(con, puuid):
                print("  ", tuple(round(x, 2) if isinstance(x, float) else x for x in r))


if __name__ == "__main__":
    main()
