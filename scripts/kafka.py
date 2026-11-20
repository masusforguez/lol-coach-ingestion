#!/usr/bin/env python3
"""Arranca un broker Kafka local en modo KRaft, sin Docker.

Subcomandos:
    python scripts/kafka.py install   descarga Kafka 3.7.1 (una sola vez)
    python scripts/kafka.py start     arranca el broker en localhost:9092
    python scripts/kafka.py stop      lo para; los datos de data/kafka se conservan
    python scripts/kafka.py status    dice si está escuchando

Funciona en Windows, Linux y macOS con el mismo comando. Necesita Java 8 o
superior en el PATH (Kafka 3.7 es la última que funciona con Java 8).

Los lanzadores oficiales de Kafka (.bat, .sh) fallan o estorban aquí, así que se
invoca la clase kafka.Kafka directamente con classpath wildcard: Java expande
`libs/*` en cualquier sistema, y así no hay que pelear con la longitud máxima de
comando de Windows (Kafka trae ~120 JARs).
"""
from __future__ import annotations

import argparse
import os
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import time
import urllib.request
from pathlib import Path

VERSION = "3.7.1"
SCALA = "2.13"
PORT = 9092
# Kafka mapea los ficheros de log en memoria. Con un JVM de 32 bits el espacio
# de direcciones es de ~1,5 GB en total, así que un heap grande deja sin sitio
# al mapeo y aparece "OutOfMemoryError: Map failed". 512M funciona en ambos.
HEAP = "512M"

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = Path(__file__).resolve().parent
CONFIG = SCRIPTS / "kafka.properties"
LOGGING = SCRIPTS / "kafka-log4j.properties"

DATA_DIR = ROOT / "data" / "kafka"
LOG_FILE = ROOT / "data" / "kafka.log"
PID_FILE = DATA_DIR / "kafka.pid"

IS_WINDOWS = os.name == "nt"


# --------------------------------------------------------------------- rutas
def default_home() -> Path:
    """Carpeta de usuario para la distribución de Kafka, por sistema."""
    if IS_WINDOWS:
        return Path(os.environ.get("LOCALAPPDATA", Path.home())) / "kafka"
    return Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share")) / "kafka"


def kafka_dir(home: Path, version: str = VERSION, scala: str = SCALA) -> Path:
    return Path(home) / f"kafka_{scala}-{version}"


# ------------------------------------------------------------------ utilidades
def require_java() -> None:
    if shutil.which("java") is None:
        sys.exit("No encuentro java en el PATH. Kafka 3.7 necesita Java 8 o superior.")
    # Un JVM de 32 bits solo puede mapear ~1,5 GB en total. Kafka mapea sus
    # ficheros de log, así que con data/kafka grande falla con "Map failed" y el
    # mensaje no dice por qué. Se avisa antes de arrancar.
    try:
        out = subprocess.run(["java", "-XshowSettings:properties", "-version"],
                             capture_output=True, text=True, timeout=30)
        info = (out.stderr or "") + (out.stdout or "")
        if "sun.arch.data.model = 32" in info:
            print("AVISO: el java del PATH es de 32 bits. Kafka mapea sus ficheros de "
                  "log y se queda sin espacio de direcciones en cuanto data/kafka "
                  "crece ('OutOfMemoryError: Map failed'). Instala un JDK de 64 bits "
                  "si vas a acumular datos.", file=sys.stderr)
    except (OSError, subprocess.SubprocessError):
        pass


def port_open(port: int, host: str = "127.0.0.1") -> bool:
    with socket.socket() as s:
        s.settimeout(0.3)
        return s.connect_ex((host, port)) == 0


def log_tail(lines: int = 20) -> str:
    if not LOG_FILE.exists():
        return "(sin log)"
    return "\n".join(LOG_FILE.read_text(encoding="utf-8", errors="replace").splitlines()[-lines:])


def pids_listening_on(port: int) -> list[int]:
    """Procesos que escuchan en el puerto, con las herramientas del sistema."""
    pids: list[int] = []
    if IS_WINDOWS:
        out = subprocess.run(["netstat", "-ano", "-p", "TCP"], capture_output=True, text=True).stdout
        for line in out.splitlines():
            parts = line.split()
            if len(parts) >= 5 and parts[0] == "TCP" and parts[3] == "LISTENING":
                if parts[1].endswith(f":{port}"):
                    try:
                        pids.append(int(parts[4]))
                    except ValueError:
                        pass
    else:
        if shutil.which("lsof"):
            out = subprocess.run(["lsof", "-ti", f"tcp:{port}"], capture_output=True, text=True).stdout
            pids = [int(p) for p in out.split() if p.isdigit()]
        if not pids and shutil.which("ss"):
            out = subprocess.run(["ss", "-lptn", f"sport = :{port}"],
                                 capture_output=True, text=True).stdout
            for token in out.replace(",", " ").split():
                if token.startswith("pid="):
                    rest = token[4:].rstrip(")")
                    if rest.isdigit():
                        pids.append(int(rest))
    return sorted(set(pids))


def terminate(pid: int, force: bool = False) -> None:
    """Para el proceso del broker.

    En Windows se usa taskkill /T /F: las señales de consola (CTRL_BREAK_EVENT)
    se propagan también a la consola del usuario y son frágiles. En POSIX se
    pide un SIGTERM y se fuerza con SIGKILL si no responde, que es donde el
    cierre ordenado sí importa (Kafka lo atiende con su shutdown hook).
    """
    try:
        if IS_WINDOWS:
            subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True)
        else:
            os.kill(pid, signal.SIGKILL if force else signal.SIGTERM)
    except (ProcessLookupError, PermissionError, OSError):
        pass


def form_cluster_if_needed(libs: Path) -> None:
    """KRaft necesita un id de clúster la primera vez, antes de formatear el log."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if (DATA_DIR / "meta.properties").exists():
        return
    print("Formateando el almacenamiento KRaft en data/kafka...")
    java = ["java", "-cp", str(libs / "*"), "kafka.tools.StorageTool"]
    uid = subprocess.run(java + ["random-uuid"], cwd=ROOT, capture_output=True, text=True)
    cluster_id = (uid.stdout or "").strip()
    if not cluster_id:
        sys.exit(f"No se pudo generar el id de clúster.\n{(uid.stderr or '')[-800:]}")
    fmt = subprocess.run(java + ["format", "-t", cluster_id, "-c", str(CONFIG)],
                         cwd=ROOT, capture_output=True, text=True)
    if not (DATA_DIR / "meta.properties").exists():
        sys.exit(f"El formateo KRaft falló.\n{(fmt.stderr or fmt.stdout or '')[-800:]}")


# ------------------------------------------------------------------ comandos
def cmd_install(args) -> None:
    require_java()
    home = Path(args.home)
    target = kafka_dir(home, args.version, args.scala)
    if (target / "libs").is_dir():
        print(f"Kafka {args.version} ya está en {target}")
        return

    home.mkdir(parents=True, exist_ok=True)
    url = f"https://archive.apache.org/dist/kafka/{args.version}/{target.name}.tgz"
    archive = home / f"{target.name}.tgz"
    print(f"Descargando {url}")
    try:
        with urllib.request.urlopen(url) as response, archive.open("wb") as out:
            shutil.copyfileobj(response, out)
    except OSError as exc:
        sys.exit(f"No se pudo descargar: {exc}")

    print(f"Descomprimiendo en {home}")
    with tarfile.open(archive, "r:gz") as tar:
        try:
            tar.extractall(home, filter="data")      # Python 3.12+
        except TypeError:
            tar.extractall(home)
    archive.unlink(missing_ok=True)

    if not (target / "libs").is_dir():
        sys.exit(f"La extracción no produjo {target / 'libs'}")
    print(f"Kafka {args.version} instalado en {target}")
    print("Arranca el broker con: python scripts/kafka.py start")


def cmd_start(args) -> None:
    require_java()
    khome = kafka_dir(args.home)
    libs = khome / "libs"
    if not libs.is_dir():
        sys.exit(f"No encuentro Kafka en {khome}.\nEjecuta primero: python scripts/kafka.py install")
    if port_open(args.port):
        sys.exit(f"El puerto {args.port} ya está ocupado. Kafka parece estar arrancado.")

    form_cluster_if_needed(libs)
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    command = [
        "java",
        f"-Dlog4j.configuration=file:{LOGGING}",
        f"-Dkafka.logs.dir={ROOT / 'data'}",
        f"-Xmx{args.heap}",
        "-cp", str(libs / "*"),
        "kafka.Kafka", str(CONFIG),
    ]
    # La salida va a un fichero y el proceso se desacopla de esta consola: si se
    # heredara el pipe, el script no terminaría nunca (nos pasó con PowerShell).
    with LOG_FILE.open("w", encoding="utf-8") as log:
        extra = {} if IS_WINDOWS else {"start_new_session": True}
        proc = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                stdin=subprocess.DEVNULL, **extra)
    PID_FILE.write_text(str(proc.pid), encoding="utf-8")

    print("Arrancando Kafka...")
    for _ in range(120):
        if port_open(args.port):
            print(f"Kafka escuchando en localhost:{args.port}")
            print("  datos: data/kafka")
            print("  log:   data/kafka.log")
            print("  parar: python scripts/kafka.py stop")
            return
        if proc.poll() is not None:
            sys.exit(f"Kafka terminó con código {proc.returncode}.\n--- data/kafka.log ---\n{log_tail()}")
        time.sleep(0.5)
    sys.exit(f"Kafka no escuchó en {args.port} tras 60 s.\n--- data/kafka.log ---\n{log_tail()}")


def cmd_stop(args) -> None:
    # Se prefiere quien escucha en el puerto: el pid guardado puede ser viejo si
    # hubo un arranque fallido o un reinicio, y matar ese PID pararía otro proceso.
    pids = pids_listening_on(args.port)
    if not pids and PID_FILE.exists():
        try:
            pids = [int(PID_FILE.read_text(encoding="utf-8").strip())]
        except ValueError:
            pids = []
    if not pids:
        print(f"No hay nada escuchando en el puerto {args.port}: Kafka no está arrancado.")
        PID_FILE.unlink(missing_ok=True)
        return

    print(f"Parando Kafka (PID {', '.join(map(str, pids))})...")
    for pid in pids:
        terminate(pid)

    for _ in range(60):
        if not port_open(args.port):
            PID_FILE.unlink(missing_ok=True)
            print("Kafka parado. Los datos de data/kafka se conservan.")
            return
        time.sleep(0.25)

    sys.exit(f"El puerto {args.port} sigue ocupado (PID {', '.join(map(str, pids))}).")


def cmd_status(args) -> None:
    if port_open(args.port):
        pids = pids_listening_on(args.port)
        detail = f" (PID {', '.join(map(str, pids))})" if pids else ""
        print(f"Kafka escuchando en localhost:{args.port}{detail}")
    else:
        print(f"Kafka no está arrancado (nada escucha en {args.port}).")


# ----------------------------------------------------------------------- CLI
def main(argv=None) -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, default=PORT, help="puerto del broker (9092)")
    ap.add_argument("--home", default=str(default_home()),
                    help="carpeta donde vive la distribución de Kafka")
    ap.add_argument("--heap", default=HEAP,
                    help=f"memoria máxima de la JVM, p. ej. 1G (por defecto {HEAP})")
    sub = ap.add_subparsers(dest="command", required=True)

    p_install = sub.add_parser("install", help="descarga y descomprime Kafka (una sola vez)")
    p_install.add_argument("--version", default=VERSION)
    p_install.add_argument("--scala", default=SCALA)
    p_install.set_defaults(func=cmd_install)

    sub.add_parser("start", help="arranca el broker").set_defaults(func=cmd_start)
    sub.add_parser("stop", help="para el broker y conserva los datos").set_defaults(func=cmd_stop)
    sub.add_parser("status", help="indica si el broker está escuchando").set_defaults(func=cmd_status)

    args = ap.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
