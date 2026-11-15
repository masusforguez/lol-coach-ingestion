"""Construcción de pipelines dlt y ejecución del modo directo.

Destino: sistema de ficheros local con Parquet (el "lago"). En producción bastaría
cambiar bucket_url por s3://... (MinIO/S3) sin tocar nada más (ver apuntes dlt).
OJO: con destino filesystem+parquet, dlt degrada 'merge' a 'append' (lo avisa en el
log). El lago conserva TODO el histórico; la deduplicación por clave primaria se
hace al leer, en DuckDB (ver warehouse.py).
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import dlt
from dlt.destinations import filesystem

from .sources import provider_source, riot_source, video_source

DATASETS = {"riot": "riot", "provider": "provider", "video": "video"}


@dataclass
class Endpoints:
    base_url: str = "http://127.0.0.1:8000"
    riot_token: str = "dev-riot-key"
    provider_token: str = "dev-provider-key"
    video_key: str = "dev-video-key"

    @classmethod
    def from_dlt_config(cls) -> "Endpoints":
        """Lee .dlt/config.toml y .dlt/secrets.toml (o variables de entorno)."""
        get = dlt.config.get
        sget = dlt.secrets.get
        return cls(
            base_url=get("sources.api.base_url") or cls.base_url,
            riot_token=sget("sources.api.riot_token") or cls.riot_token,
            provider_token=sget("sources.api.provider_token") or cls.provider_token,
            video_key=sget("sources.api.video_key") or cls.video_key,
        )


def make_pipeline(name: str, lake_dir, dataset: str, pipelines_dir: str | None = None):
    dest = filesystem(bucket_url=Path(lake_dir).resolve().as_uri())
    return dlt.pipeline(pipeline_name=name, destination=dest, dataset_name=dataset,
                        pipelines_dir=pipelines_dir)


def run_direct(ep: Endpoints, lake_dir, pipelines_dir: str | None = None,
               sources=("riot", "provider", "video"), page_size: int = 50, **riot_kw) -> dict:
    """Una pasada de ingesta directa (dlt -> API -> Parquet)."""
    infos = {}
    if "riot" in sources:
        p = make_pipeline("lol_riot", lake_dir, "riot", pipelines_dir)
        infos["riot"] = p.run(riot_source(ep.base_url, ep.riot_token, page_size=page_size, **riot_kw), loader_file_format="parquet")
    if "provider" in sources:
        p = make_pipeline("lol_provider", lake_dir, "provider", pipelines_dir)
        infos["provider"] = p.run(provider_source(ep.base_url, ep.provider_token, page_size=page_size), loader_file_format="parquet")
    if "video" in sources:
        p = make_pipeline("lol_video", lake_dir, "video", pipelines_dir)
        infos["video"] = p.run(video_source(ep.base_url, ep.video_key, page_size=page_size), loader_file_format="parquet")
    return infos
