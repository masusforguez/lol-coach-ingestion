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
- **Reloj simulado:** `POST /admin/clock/advance?minutes=N` adelanta solo el tiempo interno de la API. Al avanzar, se completan partidas nuevas (su duración simulada es de 20-40 minutos), aparecen partidas profesionales cada 10 minutos y vídeos cada 20; el 5 % de las partidas se revisa dos horas después y la cuota de vídeo se renueva al cambiar el día simulado. No cambia la hora del ordenador.

## Estructura

```
synthetic_api/   generator.py · main.py (FastAPI con límites) · dashboard.html (panel local)
ingestion/       clients.py · transform.py · sources.py (dlt) · pipeline.py
                 bus.py · producer.py · consumer.py (Kafka) · warehouse.py (DuckDB) · run.py (CLI)
consume_kafka_retry.py  consumidor Kafka con grupo persistente y lago reutilizable
tests/           pruebas de API, ingesta, almacén y Kafka (una requiere un broker real)
.dlt/            config.toml (se sube) · secrets.toml (local, ignorado por Git)
render.yaml · docker-compose.yml · requirements*.txt
```

## Puesta en marcha

```bash
pip install -r requirements.txt
python -m pytest tests -q                       # la integración Kafka se omite si no hay broker

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

### Panel visual local y consultas a Render

En PowerShell, habilita el panel antes de arrancar la API:

```powershell
$env:ENABLE_DEV_DASHBOARD = "1"
uvicorn synthetic_api.main:app --host 127.0.0.1 --port 8000
```

Abre `http://127.0.0.1:8000/dev` (o `http://127.0.0.1:8000`, que redirige al panel cuando está habilitado). El selector permite probar la API local o consultar la URL de `.dlt/config.toml` (Render). Para Render, el proceso local lee `sources.api.riot_token`, `sources.api.provider_token` y `sources.api.video_key` de `.dlt/secrets.toml` o del entorno y reenvía solo las rutas de lectura permitidas. Las claves no se envían al navegador ni se guardan en el historial. El botón de estado remoto requiere además `sources.api.admin_token`; avanzar o reiniciar el reloj de Render no está permitido desde el panel.

La sección **Datos de la API en tablas** carga bajo demanda partidas Riot y sus participantes, partidas profesionales y picks, vídeos, o los eventos y la evolución minuto a minuto de un timeline. La tabla Riot muestra el total de partidas visibles, permite elegir entre 10 y 100 filas por página y navegar hasta las más recientes; avanzar o reiniciar el reloj local actualiza la tabla si ya se había cargado. Para facilitar la lectura, muestra `UTILITY` como `SUPPORT` en los valores de rol; la respuesta JSON de la API no se altera. El panel también muestra las respuestas y el historial, y permite ejecutar `pytest`. El historial se guarda solo en memoria y no registra query strings ni credenciales. El panel requiere una conexión local y está desactivado por defecto; no habilites `ENABLE_DEV_DASHBOARD` en Render.

**Con Kafka real (PowerShell):**

```powershell
docker compose up -d kafka
$env:KAFKA_BOOTSTRAP = "localhost:9092"
python -m ingestion.run produce --bus kafka --state data/state_kafka --max-timelines 5
python .\consume_kafka_retry.py
```

El consumidor reutiliza siempre `data/lake_kafka` y `data/state_kafka`; el grupo `lake-loader-kafka` conserva sus offsets entre ejecuciones. Un grupo nuevo empieza a leer desde el principio de los mensajes retenidos en Kafka, por lo que su primera ejecución puede volver a añadir filas al lago Parquet. DuckDB deduplica las vistas `silver_*`; después, el mismo grupo continúa desde los offsets confirmados.

**En Render:** subir el repositorio a GitHub y en Render elegir *New > Blueprint* apuntando a `render.yaml`. Copia las claves que genera el panel al archivo local `.dlt/secrets.toml` (no se sube a GitHub) y configura la URL en `.dlt/config.toml` o en `SOURCES__API__BASE_URL`.

**Credenciales:** no hacen falta para ejecutar las pruebas ni la API sintética local; esta usa las claves de desarrollo (`dev-riot-key`, `dev-provider-key` y `dev-video-key`). Para usar la API desplegada en Render sí necesitas sus claves generadas. Si ejecutas la API local, apunta también la ingesta a `http://127.0.0.1:8000` (por ejemplo, con `SOURCES__API__BASE_URL`) en vez de la URL desplegada que viene en `.dlt/config.toml`.

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
- **Cursor `(updated_at, match_id)`, no `game_creation`.** El ID desempata marcas iguales al reanudar una página y `updated_at` también captura partidas revisadas más tarde.
- **El lago conserva el histórico y DuckDB deduplica.** Comprobado: con destino Parquet en disco, dlt degrada `merge` a `append` (lo avisa en el log). Por eso las vistas `silver_*` se quedan con la última versión de cada clave primaria.
- **Kafka resuelve la sincronización.** El productor respeta los límites de cada API; el consumidor confirma el offset solo si dlt cargó el lote. Si falla, se reentrega (al menos una vez) y los duplicados se absorben por clave primaria.
- **Timelines con presupuesto.** Cuestan una petición por partida y van más lentos que las partidas; lo pendiente se conserva y se completa en la siguiente pasada.
- **Cuota agotada no es un fallo.** Se carga lo que se pudo y se continúa en la siguiente ejecución sin perder el cursor.

## Pruebas y limitaciones

- **Kafka real y el flujo productor-consumidor se probaron localmente.** El test de integración verifica publicación, confirmación y reentrega (`$env:KAFKA_BOOTSTRAP="localhost:9092"; python -m pytest -q tests/test_kafka_integration.py`).
- **El API sintético desplegado en Render también se probó.** En el plan gratuito puede dormirse y, al reiniciarse, pierde su estado en memoria. El cursor local puede quedar por delante de los datos reiniciados; comprueba el estado antes de reiniciar la ingesta y no borres `data/` sin decidir primero qué lago o cursor quieres conservar.
- **El destino es el disco local.** Para MinIO/S3 basta cambiar `bucket_url` y las credenciales en `secrets.toml`, como en los apuntes, pero no se ha probado aquí.
- **Los datos sintéticos no tienen valor estadístico real.** Sirven para probar el sistema, no para sacar conclusiones sobre el juego.

## Revisar el código generado con IA

- Léelo en el orden de los pasos: cada uno se puede ejecutar y entender antes de seguir.
- **Los tests son el contrato.** Si un test falla, se arregla el código, no el test. Durante el desarrollo se modificaron cuatro tests, siempre con motivo: dos tenían una comprobación débil o equivocada y se sustituyó por otra más estricta (con un oráculo independiente, lo que además destapó un bug real en la API sintética), y otros dos planteaban un escenario que no llegaba a agotar la cuota o los créditos.
- Se hicieron pruebas de mutación: romper el código a propósito (confirmar antes de cargar, no deduplicar, no guardar el cursor) hace fallar los tests, que es lo que se espera.
