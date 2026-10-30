# Streaming with Kafka / Streaming con Kafka

## ES — Objetivo

RetailPulse v2.0 añade una primera vertical de eventos web sin modificar el pipeline batch estable de v1.0:

```text
synthetic web-event producer -> Kafka -> consumer -> raw -> bronze -> quality -> silver -> warehouse -> dbt
```

Kafka desacopla al productor del consumidor. El productor publica eventos cuando ocurren y el consumidor puede procesarlos a su ritmo usando las posiciones conservadas por Kafka.

## ES — Conceptos del flujo

- **Producer:** lee de PostgreSQL los IDs existentes de clientes y productos, crea el topic con el número de particiones configurado —o amplía uno existente—, genera un lote finito de eventos `product_view` y `add_to_cart`, los serializa como JSON UTF-8 y los publica.
- **Broker:** el laboratorio usa tres nodos Kafka locales en modo combinado; cada contenedor actúa como broker y controller KRaft, sin ZooKeeper.
- **Topic:** `retailpulse.web_events` es el canal lógico dedicado a los eventos web.
- **Message key:** se usa `session_id`. Los eventos de una misma sesión llegan a la misma partición mientras no cambie el número de particiones, y Kafka solo garantiza orden dentro de una partición.
- **Partition:** subdivisión ordenada del topic. Esta iteración usa tres particiones para observar distribución y consumo paralelo.
- **Offset:** posición creciente de cada mensaje dentro de su partición. Permite conocer y reanudar el progreso del consumer.
- **Consumer:** lee un número finito de mensajes, escribe raw y confirma los offsets solo después de guardar el fichero.

## ES — Evento, raw y bronze

Cada payload contiene:

- `event_id`
- `event_type`
- `occurred_at` en UTC
- `session_id`
- `customer_id`, que puede ser nulo
- `product_id`

El generador parte de una fecha UTC fija para que una misma combinación de `count` y `seed` reproduzca también los timestamps, no solo el resto de campos.

Los eventos solo referencian `customer_id` y `product_id` existentes en la fuente PostgreSQL. Un `customer_id` nulo representa navegación anónima. Cada sesión aparece al menos una vez en el lote y conserva el mismo cliente —o permanece anónima— en todos sus eventos. El `session_id` incluye la seed de la simulación para que dos lotes generados con seeds distintas no reutilicen la misma sesión.

### Fechas del flujo

El proyecto distingue tres tiempos diferentes:

- `occurred_at` representa cuándo ocurrió el evento en el tiempo simulado del negocio. Actualmente parte de `2026-01-01`, la misma fecha de referencia que usa el generador relacional, por lo que clientes y productos ya existen cuando son referenciados.
- El `timestamp` de Kafka usa el modo predeterminado `CreateTime`: representa el timestamp asignado al record por el producer al publicarlo. No es la hora de escritura del broker; para eso Kafka tendría que usar `LogAppendTime`.
- `load_date` pertenece al pipeline batch y representa cuándo se extrajo un snapshot de PostgreSQL; no es la fecha de creación de sus registros ni tiene que coincidir con `event_date`.

La partición raw `event_date=YYYY-MM-DD` se obtiene de `occurred_at`. Por tanto, ejecutar hoy eventos con `occurred_at=2026-01-01` representa un replay histórico reproducible, no una simulación estrictamente en tiempo real.

Los mensajes se escriben en:

```text
data/raw/kafka/web_events/event_date=YYYY-MM-DD/web_events_<run_id>.jsonl
```

Cada línea usa un sobre con dos objetos:

```json
{
  "kafka": {
    "topic": "retailpulse.web_events",
    "partition": 0,
    "offset": 17,
    "timestamp": "2026-10-27T12:00:00Z",
    "key": "session-seed-42-000001"
  },
  "payload": {
    "event_id": "...",
    "event_type": "product_view",
    "occurred_at": "2026-01-01T00:00:00Z",
    "session_id": "session-seed-42-000001",
    "customer_id": null,
    "product_id": 10
  }
}
```

El sobre mantiene el payload de negocio separado de los metadatos de transporte. El nombre contiene un `run_id` único y la escritura usa creación exclusiva para no sobrescribir silenciosamente una ejecución anterior.

La conversión a bronze lee esos sobres, aplana el payload y los metadatos Kafka en columnas y escribe Parquet:

```text
data/bronze/kafka/web_events/event_date=YYYY-MM-DD/web_events_<run_id>.parquet
```

Cada fichero raw se corresponde con un fichero bronze del mismo `run_id`. Además de las columnas del evento, bronze conserva `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp` y `kafka_key`, y añade `ingestion_id`, `ingested_at`, `source_system`, `source_table` y `source_file`. Esta transformación es estructural: todavía no aprueba ni rechaza eventos por reglas de negocio.

## ES — Calidad y publicación de eventos

La validación procesa conjuntamente los ficheros bronze seleccionados para poder detectar duplicados incluso entre `run_id` diferentes. Comprueba:

- `event_id` obligatorio, UUID y único;
- `event_type` dentro del dominio permitido;
- `occurred_at` UTC y coherente con la partición `event_date`;
- IDs enteros positivos, permitiendo `customer_id` nulo;
- `kafka_key` igual a `session_id`;
- topic esperado, partición y offset no negativos;
- unicidad de `(kafka_topic, kafka_partition, kafka_offset)` dentro de cada `run_id`;
- procedencia técnica de cada fichero bronze.

Las filas válidas e inválidas se escriben por separado conservando el mismo `run_id`:

```text
data/silver/kafka/web_events/event_date=YYYY-MM-DD/web_events_<run_id>.parquet
data/rejected/kafka/web_events/event_date=YYYY-MM-DD/web_events_<run_id>_rejected.parquet
data/audit/streaming_quality_runs.parquet
```

Silver añade `quality_run_id` y `quality_checked_at`. Rejected añade además `rejection_reason`. La auditoría registra conteos y estado por fichero; un contrato técnico bronze inválido bloquea la publicación de ese fichero y queda registrado como `failed`.

## ES — Warehouse y dbt

`load_streaming_silver_to_warehouse` exige la última auditoría aprobada, comprueba `quality_run_id`, conteos e `ingestion_id`, y añade únicamente silver a `warehouse_source.web_events`. La tabla conserva ejecuciones anteriores; `event_id` es la clave primaria y `ON CONFLICT DO NOTHING` hace idempotente la recarga del mismo evento.

dbt expone `stg_web_events` y construye:

- `fact_web_events`: una fila por `event_id`;
- `fact_web_sessions`: una fila por `session_id`, con conteos de vistas, carritos y productos distintos.

Los tests comprueban claves, dominios, relaciones con clientes/productos, reconciliación staging-fact y un único cliente identificado por sesión.

### Demo controlada de rejected

Una ejecución normal puede producir cero rechazados. Para enseñar el circuito de calidad sin contaminar el lote normal, la demo copia un bronze existente a un `run_id` separado, crea su raw correspondiente e introduce cinco anomalías deterministas: `event_type` inválido, `product_id` negativo, fecha fuera de la partición y una posición Kafka duplicada que afecta a dos filas.

```bash
make quality-events-demo EVENT_DATE=2026-01-01 EVENT_RUN_ID=<source_run_id>
```

Si la partición solo contiene un bronze, `EVENT_RUN_ID` se puede omitir. La demo usa `quality-demo` como `run_id`, necesita al menos cinco filas y reemplaza únicamente sus propios archivos al repetirse. Es material de demostración y no debe utilizarse como fuente normal.

## ES — Ejecución local

La configuración del host vive en `.env`:

```dotenv
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094
KAFKA_TOPIC_WEB_EVENTS=retailpulse.web_events
KAFKA_TOPIC_PARTITIONS=3
KAFKA_TOPIC_REPLICATION_FACTOR=3
```

Dentro de la red Docker los brokers se anuncian como `kafka-1:19092`, `kafka-2:19092` y `kafka-3:19092`. El topic replica cada partición en los tres brokers y exige al menos dos réplicas sincronizadas para las escrituras con `acks="all"`.

```bash
make up
make init-db       # solo si el esquema todavía no existe
make seed-db       # prepara los clientes y productos de la demo
make kafka-up
make produce-events EVENT_COUNT=20 EVENT_SEED=42
make consume-events EVENT_COUNT=20
make bronze-events EVENT_DATE=2026-01-01
make quality-events EVENT_DATE=2026-01-01
make load-events-warehouse EVENT_DATE=2026-01-01 EVENT_RUN_ID=<run_id>
make dbt-run
make dbt-test
make kafka-down
```

También se pueden ejecutar los módulos directamente:

```bash
python -m src.streaming.produce_web_events --count 20 --seed 42
python -m src.streaming.consume_web_events --count 20
python -m src.streaming.raw_to_bronze --event-date 2026-01-01
python -m src.streaming.validate_web_events --event-date 2026-01-01
python -m src.warehouse.load_streaming_silver_to_warehouse --event-date 2026-01-01 --run-id <run_id>
```

`--run-id` permite convertir un único fichero raw. Si se omite, se procesan todos los ficheros de la partición. Repetir la conversión mantiene la misma ruta bronze para cada `run_id`, evitando crear copias adicionales del mismo fichero de origen.

### Demo de consumer group

Tres consumers con el mismo `group-id` se reparten las tres particiones. Cada instancia usa un `consumer-id` distinto para poder identificarla. `--allow-partial` permite que cada consumer termine al vencer el tiempo de espera aunque haya recibido menos que el total publicado.

Primero se inicia un consumer en cada una de tres terminales:

```bash
make consume-events-partial EVENT_COUNT=100 KAFKA_CONSUMER_GROUP=partition-demo KAFKA_CONSUMER_ID=consumer-1
make consume-events-partial EVENT_COUNT=100 KAFKA_CONSUMER_GROUP=partition-demo KAFKA_CONSUMER_ID=consumer-2
make consume-events-partial EVENT_COUNT=100 KAFKA_CONSUMER_GROUP=partition-demo KAFKA_CONSUMER_ID=consumer-3
```

Después se publican los eventos desde una cuarta terminal:

```bash
make produce-events EVENT_COUNT=100 EVENT_SEED=42
```

Kafka asigna cada partición a un solo consumer dentro del grupo. La distribución de eventos no tiene por qué ser uniforme porque se calcula a partir de `session_id`. Si se añaden más consumers que particiones, alguno quedará sin asignación.

### Demo de failover

Con los tres brokers healthy, se puede detener el primero y continuar publicando mediante los otros bootstrap servers:

```bash
make produce-events EVENT_COUNT=20 EVENT_SEED=42
make consume-events EVENT_COUNT=20 KAFKA_CONSUMER_GROUP=failover-demo
make kafka-stop-broker-1
make kafka-ps
make produce-events EVENT_COUNT=20 EVENT_SEED=43
make consume-events EVENT_COUNT=20 KAFKA_CONSUMER_GROUP=failover-demo
make kafka-start-broker-1
```

El primer consumo inicializa el grupo y permite que Kafka cree `__consumer_offsets` con los tres brokers disponibles. El factor de replicación 3 conserva una copia de cada partición en todos los brokers. `min.insync.replicas=2` junto con `acks="all"` permite perder un broker sin aceptar escrituras que solo existan en una única copia. Perder dos brokers deja al cluster sin suficientes réplicas sincronizadas para publicar de forma segura.

La demo de failover se valida ejecutando los clientes dentro del entorno Linux/Docker. La ejecución directa desde Windows puede presentar timeouts de bootstrap al apagar uno de los brokers.

En las pruebas locales con Windows y `kafka-python==3.0.11`, un cliente nuevo puede agotar el timeout al intentar conectar con el broker detenido antes de probar otro bootstrap server. El mismo diagnóstico en Linux sí continúa con el servidor disponible. Para evitar esa limitación en Windows, los dos comandos ejecutados con `kafka-1` detenido pueden indicar únicamente los endpoints disponibles:

```bash
make produce-events EVENT_COUNT=20 EVENT_SEED=43 KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9093,127.0.0.1:9094
make consume-events EVENT_COUNT=20 KAFKA_CONSUMER_GROUP=failover-demo KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9093,127.0.0.1:9094
```

Esta variable se aplica solo a esos comandos; no modifica `.env` ni el factor de replicación del topic. El producer y el consumer usan directamente los clientes estándar de la librería.

## ES — Batch frente a streaming

El batch obtiene snapshots relacionales completos desde PostgreSQL y continúa hasta silver, warehouse y dbt. El flujo streaming captura eventos web, valida silver y lo añade de forma incremental al mismo esquema `warehouse_source` para construir marts dbt. Kafka añade un segundo flujo; no sustituye PostgreSQL ni está orquestado por Airflow.

## ES — Limitaciones

- Tres brokers combinados KRaft, tres particiones y factor de replicación tres.
- El failover es una demostración local; no incluye persistencia de Kafka, seguridad de red, monitorización ni operación productiva.
- Sin Schema Registry, Avro, Kafka Connect ni procesamiento continuo.
- El consumer procesa lotes finitos para demos controladas.
- La tabla de eventos es incremental, pero los modelos dbt se reconstruyen como tablas completas.
- La calidad aún no contrasta `customer_id` y `product_id` contra una snapshot relacional de referencia.
- La unicidad se comprueba dentro de los ficheros y la `event_date` seleccionados, no mediante un índice global de todo el histórico.
- CI prueba generación, serialización, raw, bronze y calidad; la prueba real broker-cliente se ejecuta localmente y podrá convertirse en integración opt-in si el flujo crece.

---

## EN — Objective

RetailPulse v2.0 adds a first web-event vertical without changing the stable v1.0 batch pipeline:

```text
synthetic web-event producer -> Kafka -> consumer -> raw -> bronze -> quality -> silver -> warehouse -> dbt
```

Kafka decouples the producer from the consumer. The producer publishes events as they occur, while the consumer can process them at its own pace using positions retained by Kafka.

## EN — Flow concepts

- **Producer:** reads existing customer and product IDs from PostgreSQL, creates the topic with the configured partition count —or expands an existing topic—, generates a finite batch of `product_view` and `add_to_cart` events, serializes them as UTF-8 JSON and publishes them.
- **Broker:** the lab uses three local Kafka nodes in combined mode; each container acts as a KRaft broker and controller without ZooKeeper.
- **Topic:** `retailpulse.web_events` is the dedicated logical channel for web events.
- **Message key:** `session_id` is used. Events from the same session reach the same partition while the partition count remains unchanged, and Kafka only guarantees ordering within a partition.
- **Partition:** an ordered subdivision of the topic. This iteration uses three partitions to demonstrate distribution and parallel consumption.
- **Offset:** the increasing position of each message within its partition. It tracks consumer progress and allows processing to resume.
- **Consumer:** reads a finite number of messages, writes raw data and commits offsets only after the file is stored.

## EN — Event, raw and bronze storage

Each payload contains:

- `event_id`
- `event_type`
- UTC `occurred_at`
- `session_id`
- nullable `customer_id`
- `product_id`

The generator uses a fixed UTC start time so the same `count` and `seed` reproduce timestamps as well as the other fields.

Events only reference `customer_id` and `product_id` values that exist in the PostgreSQL source. A null `customer_id` represents anonymous browsing. Every generated session appears at least once in the batch and keeps the same customer —or remains anonymous— across all its events. The `session_id` includes the simulation seed so batches generated with different seeds do not reuse the same session.

### Flow dates

The project distinguishes three different times:

- `occurred_at` represents when the event happened in simulated business time. It currently starts on `2026-01-01`, the same reference date used by the relational generator, so customers and products already exist when referenced.
- The Kafka `timestamp` uses the default `CreateTime` mode: it represents the record timestamp assigned by the producer when publishing. It is not the broker append time; that would require Kafka to use `LogAppendTime`.
- `load_date` belongs to the batch pipeline and represents when a PostgreSQL snapshot was extracted; it is not the creation date of its records and does not have to match `event_date`.

The raw `event_date=YYYY-MM-DD` partition is derived from `occurred_at`. Therefore, publishing events today with `occurred_at=2026-01-01` represents a reproducible historical replay rather than a strict real-time simulation.

Messages are written to:

```text
data/raw/kafka/web_events/event_date=YYYY-MM-DD/web_events_<run_id>.jsonl
```

Each line uses an envelope with separate `kafka` and `payload` objects, as shown in the Spanish example above. This keeps business data separate from transport metadata. The filename contains a unique `run_id`, and exclusive file creation prevents a previous run from being overwritten silently.

Bronze conversion reads those envelopes, flattens the payload and Kafka metadata into columns, and writes Parquet:

```text
data/bronze/kafka/web_events/event_date=YYYY-MM-DD/web_events_<run_id>.parquet
```

Each raw file maps to one bronze file with the same `run_id`. In addition to event columns, bronze keeps `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp` and `kafka_key`, and adds `ingestion_id`, `ingested_at`, `source_system`, `source_table` and `source_file`. This is a structural transformation: it does not yet approve or reject events through business rules.

## EN — Event quality and publication

Validation processes the selected bronze files together so duplicates can be detected even across different `run_id` values. It checks:

- required, UUID and unique `event_id` values;
- the accepted `event_type` domain;
- UTC `occurred_at` values matching the `event_date` partition;
- positive integer IDs, while allowing a null `customer_id`;
- `kafka_key` matching `session_id`;
- expected topic and non-negative partition and offset;
- uniqueness of `(kafka_topic, kafka_partition, kafka_offset)` within each `run_id`;
- technical provenance for every bronze file.

Valid and invalid rows are written separately while preserving the same `run_id`:

```text
data/silver/kafka/web_events/event_date=YYYY-MM-DD/web_events_<run_id>.parquet
data/rejected/kafka/web_events/event_date=YYYY-MM-DD/web_events_<run_id>_rejected.parquet
data/audit/streaming_quality_runs.parquet
```

Silver adds `quality_run_id` and `quality_checked_at`. Rejected also adds `rejection_reason`. Audit records counts and status per file; an invalid bronze technical contract blocks publication for that file and is recorded as `failed`.

## EN — Warehouse and dbt

`load_streaming_silver_to_warehouse` requires the latest approved audit record, checks `quality_run_id`, counts and `ingestion_id`, and appends only silver to `warehouse_source.web_events`. The table preserves earlier runs; `event_id` is its primary key and `ON CONFLICT DO NOTHING` makes reloading an event idempotent.

dbt exposes `stg_web_events` and builds:

- `fact_web_events`: one row per `event_id`;
- `fact_web_sessions`: one row per `session_id`, with view, cart and distinct-product counts.

Tests cover keys, domains, customer/product relationships, staging-to-fact reconciliation and one identified customer per session.

### Controlled rejected-record demo

A normal execution may produce zero rejected rows. To demonstrate the quality path without contaminating the normal run, the demo copies an existing bronze file to a separate `run_id`, creates its matching raw file and introduces five deterministic anomalies: an invalid `event_type`, a negative `product_id`, a date outside the partition and one duplicated Kafka position affecting two rows.

```bash
make quality-events-demo EVENT_DATE=2026-01-01 EVENT_RUN_ID=<source_run_id>
```

When the partition contains only one bronze file, `EVENT_RUN_ID` can be omitted. The demo uses `quality-demo` as its `run_id`, requires at least five rows and replaces only its own files when rerun. It is demonstration material and must not be used as a normal source.

## EN — Local execution

Host-side configuration lives in `.env`:

```dotenv
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094
KAFKA_TOPIC_WEB_EVENTS=retailpulse.web_events
KAFKA_TOPIC_PARTITIONS=3
KAFKA_TOPIC_REPLICATION_FACTOR=3
```

Inside the Docker network, the brokers advertise `kafka-1:19092`, `kafka-2:19092` and `kafka-3:19092`. The topic replicates each partition across all three brokers and requires at least two in-sync replicas for writes using `acks="all"`.

```bash
make up
make init-db       # only if the schema does not exist yet
make seed-db       # prepares demo customers and products
make kafka-up
make produce-events EVENT_COUNT=20 EVENT_SEED=42
make consume-events EVENT_COUNT=20
make bronze-events EVENT_DATE=2026-01-01
make quality-events EVENT_DATE=2026-01-01
make load-events-warehouse EVENT_DATE=2026-01-01 EVENT_RUN_ID=<run_id>
make dbt-run
make dbt-test
make kafka-down
```

The modules can also be run directly:

```bash
python -m src.streaming.produce_web_events --count 20 --seed 42
python -m src.streaming.consume_web_events --count 20
python -m src.streaming.raw_to_bronze --event-date 2026-01-01
python -m src.streaming.validate_web_events --event-date 2026-01-01
python -m src.warehouse.load_streaming_silver_to_warehouse --event-date 2026-01-01 --run-id <run_id>
```

`--run-id` converts a single raw file. When omitted, every file in the partition is processed. Repeating the conversion keeps the same bronze path for each `run_id`, avoiding additional copies of the same source file.

### Consumer-group demo

Three consumers sharing the same `group-id` divide the three partitions among themselves. Every instance uses a different `consumer-id` so it can be identified. `--allow-partial` lets each consumer finish on idle timeout even when it received fewer messages than the total published count.

Start one consumer in each of three terminals first:

```bash
make consume-events-partial EVENT_COUNT=100 KAFKA_CONSUMER_GROUP=partition-demo KAFKA_CONSUMER_ID=consumer-1
make consume-events-partial EVENT_COUNT=100 KAFKA_CONSUMER_GROUP=partition-demo KAFKA_CONSUMER_ID=consumer-2
make consume-events-partial EVENT_COUNT=100 KAFKA_CONSUMER_GROUP=partition-demo KAFKA_CONSUMER_ID=consumer-3
```

Then publish the events from a fourth terminal:

```bash
make produce-events EVENT_COUNT=100 EVENT_SEED=42
```

Kafka assigns each partition to only one consumer within the group. Event distribution need not be even because it is derived from `session_id`. If there are more consumers than partitions, one of them receives no assignment.

### Failover demo

With all three brokers healthy, the first one can be stopped while publishing continues through the remaining bootstrap servers:

```bash
make produce-events EVENT_COUNT=20 EVENT_SEED=42
make consume-events EVENT_COUNT=20 KAFKA_CONSUMER_GROUP=failover-demo
make kafka-stop-broker-1
make kafka-ps
make produce-events EVENT_COUNT=20 EVENT_SEED=43
make consume-events EVENT_COUNT=20 KAFKA_CONSUMER_GROUP=failover-demo
make kafka-start-broker-1
```

The initial consumption initializes the group and lets Kafka create `__consumer_offsets` while all three brokers are available. Replication factor 3 keeps one copy of every partition on each broker. `min.insync.replicas=2` together with `acks="all"` allows one broker to fail without accepting writes stored on only one copy. Losing two brokers leaves the cluster without enough in-sync replicas for safe publishing.

The failover demo is validated with the clients running inside the Linux/Docker environment. Running them directly from Windows may produce bootstrap timeouts after one broker is stopped.

In local tests on Windows with `kafka-python==3.0.11`, a new client may exhaust its timeout connecting to the stopped broker before trying another bootstrap server. The same diagnostic on Linux does continue to the available server. To avoid this limitation on Windows, the two commands run while `kafka-1` is stopped can specify only the available endpoints:

```bash
make produce-events EVENT_COUNT=20 EVENT_SEED=43 KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9093,127.0.0.1:9094
make consume-events EVENT_COUNT=20 KAFKA_CONSUMER_GROUP=failover-demo KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9093,127.0.0.1:9094
```

This variable applies only to those commands; it changes neither `.env` nor the topic replication factor. The producer and consumer use the library's standard clients directly.

## EN — Batch versus streaming

The batch pipeline extracts complete relational snapshots from PostgreSQL and continues through silver, warehouse and dbt. The streaming flow captures web events, validates silver and appends it incrementally to the same `warehouse_source` schema for dbt marts. Kafka adds a second flow; it does not replace PostgreSQL and is not orchestrated by Airflow.

## EN — Limitations

- Three combined KRaft brokers, three partitions and replication factor three.
- Failover is a local demonstration; it does not include Kafka persistence, network security, monitoring or production operations.
- No Schema Registry, Avro, Kafka Connect or continuous processing.
- The consumer processes finite batches for controlled demos.
- The event table is incremental, but dbt models are rebuilt as full tables.
- Quality does not yet compare `customer_id` and `product_id` against a relational reference snapshot.
- Uniqueness is checked within the selected files and `event_date`, not through a global index over the full history.
- CI tests generation, serialization, raw, bronze and quality; the real broker-client check is local and may become an opt-in integration test if the flow grows.
