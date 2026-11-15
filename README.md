# Prototipo de ingesta de datos: coach de IA para League of Legends

Prototipo funcional de la **capa de ingesta** del proyecto. Usa solo las herramientas propuestas en los apuntes: **dlt** para extraer y cargar, **Apache Kafka** para desacoplar las fuentes de la carga, y **Parquet + DuckDB** para almacenar y consultar. Las fuentes con capa gratuita limitada o de pago se simulan con **APIs sintéticas** desplegables en render.com.

Los datos son inventados (generados con Faker): no vienen de Riot ni de ninguna plataforma real.

## Qué hace

```
 APIs sintéticas (render.com)              Kafka (opcional)                 Lago Parquet            DuckDB
 ┌────────────────────────────┐                                                                 
 │ /riot      429 + Retry-After│   MODO DIRECTO:  dlt (incremental + merge) ──────────────►  data/lake/*  ──► vistas silver_*
 │ /provider  402 sin créditos │                                                                   (Parquet)      (dedupe por clave)
 │ /youtube   403 cuota diaria │   MODO BUS:  Fetcher ─► topics lol.raw.* ─► LakeConsumer(dlt) ─►                  vistas gold_*
 └────────────────────────────┘              (respeta límites)   (commit tras cargar)                              (benchmarks)
```

- **Riot (simulada):** partidas con 10 participantes (lista anidada) y su timeline minuto a minuto. Límite de peticiones por ventana, con revisiones posteriores de algunas partidas.
- **Proveedor de pago (simulado):** partidas profesionales con paginación por cursor y créditos que se agotan.
- **Vídeo (simulada, estilo YouTube/Twitch):** metadatos de vídeos de rangos altos con cuota diaria.
- **Reloj simulado:** `POST /admin/clock/advance?minutes=N` hace "pasar el tiempo" para que aparezcan datos nuevos y revisados sin esperar.

## Estructura

```
synthetic_api/   generator.py (datos deterministas) · main.py (FastAPI con límites)
ingestion/       clients.py · transform.py · sources.py (dlt) · pipeline.py
                 bus.py · producer.py · consumer.py (Kafka) · warehouse.py (DuckDB) · run.py (CLI)
tests/           41 pruebas (40 automáticas + 1 que requiere un broker Kafka real)
.dlt/            config.toml (se sube) · secrets.toml.example (copiar a secrets.toml, no se sube)
render.yaml · docker-compose.yml · requirements*.txt
```

## Puesta en marcha

```bash
pip install -r requirements.txt
python -m pytest tests -q                       # 40 passed, 1 skipped

# 1) API sintética en local
uvicorn synthetic_api.main:app --port 8000

# 2) Ingesta directa con dlt (otra terminal)
python -m ingestion.run direct --max-timelines 20
curl -X POST "localhost:8000/admin/clock/advance?minutes=360" -H "X-Admin-Token: dev-admin"
python -m ingestion.run direct --max-timelines 20     # solo baja lo nuevo y lo revisado

# 3) Modo bus (productor + consumidor) y análisis
python -m ingestion.run pipeline --max-timelines 20   # bus en memoria, sin Docker
python -m ingestion.run report                        # benchmarks y comparación de un jugador
```

**Con Kafka real:** `docker compose up -d`, y después `KAFKA_BOOTSTRAP=localhost:9092 python -m ingestion.run produce --bus kafka` y lo mismo con `consume`.

**En Render:** subir el repositorio a GitHub, en Render elegir *New > Blueprint* y apuntar a `render.yaml`. Las claves se generan solas; cópialas del panel a `.dlt/secrets.toml` y pon la URL en `.dlt/config.toml` (o en `SOURCES__API__BASE_URL`).

## Cómo se construyó (incremental, un paso por test)

| Paso | Qué se añadió | Tests (el contrato) |
| --- | --- | --- |
| 1 | Generador sintético determinista | `test_generator.py` |
| 2 | API con 429, créditos, cuota y reloj | `test_api.py` |
| 3 | Ingesta directa con dlt (incremental, merge, paginación) | `test_direct_ingestion.py` |
| 4 | Vistas DuckDB sobre Parquet (silver/gold) | `test_warehouse.py` |
| 5 | Bus Kafka: productor, consumidor, commit tras carga | `test_kafka_flow.py` · `test_kafka_integration.py` |

## Decisiones de diseño

- **EL antes que T.** Se carga el dato en bruto y la transformación (dedupe, métricas) se hace después en DuckDB.
- **Cursor `updated_at`, no `game_creation`.** Así se capturan también las partidas que la fuente revisa más tarde (el test lo comprueba con un oráculo independiente).
- **El lago conserva el histórico y DuckDB deduplica.** Comprobado: con destino Parquet en disco, dlt degrada `merge` a `append` (lo avisa en el log). Por eso las vistas `silver_*` se quedan con la última versión de cada clave primaria.
- **Kafka resuelve la sincronización.** El productor respeta los límites de cada API; el consumidor confirma el offset solo si dlt cargó el lote. Si falla, se reentrega (al menos una vez) y los duplicados se absorben por clave primaria.
- **Timelines con presupuesto.** Cuestan una petición por partida y van más lentos que las partidas; lo pendiente se conserva y se completa en la siguiente pasada.
- **Cuota agotada no es un fallo.** Se carga lo que se pudo y se continúa en la siguiente ejecución sin perder el cursor.

## Limitaciones (lo que NO está comprobado)

- **`KafkaBus` no se ha probado contra un broker real.** El resto de la lógica sí, con un bus en memoria que cumple la misma interfaz. Hay un test de integración preparado (`KAFKA_BOOTSTRAP=localhost:9092 pytest tests/test_kafka_integration.py`).
- **El despliegue en Render no se ha ejecutado** desde mi entorno; `render.yaml` sigue el formato estándar, pero hay que probarlo. En el plan gratuito el servicio suele dormirse tras un rato sin tráfico, y el estado en memoria (reloj, créditos, cuota) se pierde al reiniciar: tras un reinicio, borra `data/` para empezar de cero.
- **El destino es el disco local.** Para MinIO/S3 basta cambiar `bucket_url` y las credenciales en `secrets.toml`, como en los apuntes, pero no se ha probado aquí.
- **Los datos sintéticos no tienen valor estadístico real.** Sirven para probar el sistema, no para sacar conclusiones sobre el juego.

## Revisar el código generado con IA

- Léelo en el orden de los pasos: cada uno se puede ejecutar y entender antes de seguir.
- **Los tests son el contrato.** Si un test falla, se arregla el código, no el test. Durante el desarrollo se modificaron cuatro tests, siempre con motivo: dos tenían una comprobación débil o equivocada y se sustituyó por otra más estricta (con un oráculo independiente, lo que además destapó un bug real en la API sintética), y otros dos planteaban un escenario que no llegaba a agotar la cuota o los créditos.
- Se hicieron pruebas de mutación: romper el código a propósito (confirmar antes de cargar, no deduplicar, no guardar el cursor) hace fallar los tests, que es lo que se espera.
