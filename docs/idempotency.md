# Idempotency / Idempotencia

## ES — Objetivo

Este documento explica cómo RetailPulse v1.0 permite reejecutar etapas sin duplicar datos de forma accidental.

La idempotencia aquí es local y limitada. No pretende ser una estrategia productiva completa.

## ES — Principio general

Para una misma entrada y una misma `load_date`, una reejecución debe dejar un resultado equivalente, sin acumular duplicados innecesarios.

La equivalencia se refiere a los datos de negocio con la misma entrada. Los IDs y timestamps técnicos de ingesta/calidad cambian entre ejecuciones y audit acumula registros, por lo que no hay identidad byte a byte ni idempotencia estricta de extremo a extremo.

## ES — Fuente PostgreSQL

`make seed-db` reemplaza el contenido de las tablas fuente en una transacción.

Objetivo:

- evitar duplicados en reejecuciones.
- mantener datos reproducibles usando la misma seed.
- dejar la fuente en un estado conocido.

Limitación:

- en un sistema real, la fuente operacional no se regeneraría dentro del pipeline.

## ES — Raw y bronze

Las rutas dependen de `load_date`:

```text
data/raw/postgres/<table>/load_date=YYYY-MM-DD/<table>.csv
data/bronze/postgres/<table>/load_date=YYYY-MM-DD/<table>.parquet
```

Para una misma tabla y `load_date`, el archivo se reemplaza.

Objetivo:

- evitar múltiples copias del mismo snapshot lógico.
- permitir reejecución manual.

Limitación:

- la escritura de todas las tablas no es transacción atómica de filesystem.
- no hay versionado histórico por reintento dentro de una misma `load_date`.

## ES — Silver y rejected

La validación lee una partición bronze y escribe:

```text
data/silver/postgres/<table>/load_date=YYYY-MM-DD/<table>.parquet
data/rejected/postgres/<table>/load_date=YYYY-MM-DD/<table>_rejected.parquet
```

Para la misma `load_date`, las salidas por tabla se reemplazan.

Objetivo:

- mantener una sola versión aprobada y rechazada por partición.
- evitar duplicados por reejecución.

Limitación:

- los archivos silver/rejected no conservan versiones anteriores de la misma partición.

## ES — Audit

`data/audit/quality_runs.parquet` acumula una fila por tabla y ejecución de calidad.

Esto no es estrictamente idempotente, porque una reejecución añade nuevas filas de auditoría. Es intencional: la auditoría debe conservar evidencia de cada ejecución.

Objetivo:

- preservar historial de validaciones.
- comparar conteos entre ejecuciones.
- ver si una reejecución cambió resultados.

Limitación:

- no hay control de concurrencia sobre el archivo local de audit.

## ES — Warehouse

La carga a `warehouse_source` usa full refresh / replace.

Objetivo:

- evitar duplicados en tablas warehouse.
- hacer que el estado final dependa de la última carga silver.
- simplificar la v1.0.

Limitación:

- no conserva histórico de snapshots.
- no demuestra incrementalidad.

## ES — dbt

dbt reconstruye staging y marts desde `warehouse_source`.

Objetivo:

- que los marts reflejen el estado actual del warehouse.
- permitir reejecutar `dbt run` y `dbt test`.

Limitación:

- no se usan modelos incrementales en v1.0.

## ES — Airflow

El DAG coordina los mismos comandos manuales y comparte `load_date` entre ingesta, calidad y warehouse.

Objetivo:

- evitar mezclar particiones de distintas fechas.
- mantener trazabilidad por task.

Limitación:

- Airflow local no añade locking, SLA ni gestión productiva de reintentos.

## ES — Reglas prácticas

- Usar la misma `load_date` desde ingesta hasta warehouse.
- No cargar warehouse desde particiones demo salvo para pruebas controladas.
- Ejecutar `dbt run` después de `load-warehouse`.
- Revisar audit si se repite una validación.
- No interpretar audit acumulado como tabla deduplicada de estado final.

---

## EN — Objective

This document explains how RetailPulse v1.0 allows stages to be rerun without accidentally duplicating data.

Idempotency here is local and limited. It is not a full production strategy.

## EN — General principle

For the same input and the same `load_date`, a rerun should leave an equivalent result without accumulating unnecessary duplicates.

Equivalence refers to business data for the same input. Ingestion/quality IDs and timestamps change between runs and audit accumulates records, so there is no byte-for-byte identity or strict end-to-end idempotency.

## EN — PostgreSQL source

`make seed-db` replaces the contents of source tables in a transaction.

Goal:

- avoid duplicates on reruns.
- keep data reproducible with the same seed.
- leave the source in a known state.

Limitation:

- in a real system, the operational source would not be regenerated inside the pipeline.

## EN — Raw and bronze

Paths depend on `load_date`:

```text
data/raw/postgres/<table>/load_date=YYYY-MM-DD/<table>.csv
data/bronze/postgres/<table>/load_date=YYYY-MM-DD/<table>.parquet
```

For the same table and `load_date`, the file is replaced.

Goal:

- avoid multiple copies of the same logical snapshot.
- allow manual reruns.

Limitation:

- writing all tables is not an atomic filesystem transaction.
- there is no historical versioning by retry inside the same `load_date`.

## EN — Silver and rejected

Validation reads a bronze partition and writes:

```text
data/silver/postgres/<table>/load_date=YYYY-MM-DD/<table>.parquet
data/rejected/postgres/<table>/load_date=YYYY-MM-DD/<table>_rejected.parquet
```

For the same `load_date`, table outputs are replaced.

Goal:

- keep one approved and rejected version per partition.
- avoid duplicates caused by reruns.

Limitation:

- silver/rejected files do not keep previous versions of the same partition.

## EN — Audit

`data/audit/quality_runs.parquet` appends one row per table and quality execution.

This is not strictly idempotent because a rerun adds new audit rows. That is intentional: audit should preserve evidence of every execution.

Goal:

- preserve validation history.
- compare row counts between runs.
- see whether a rerun changed results.

Limitation:

- no concurrency control over the local audit file.

## EN — Warehouse

Loading into `warehouse_source` uses full refresh / replace.

Goal:

- avoid duplicates in warehouse tables.
- make final state depend on the latest silver load.
- simplify v1.0.

Limitation:

- no snapshot history.
- no incremental loading.

## EN — dbt

dbt rebuilds staging and marts from `warehouse_source`.

Goal:

- keep marts aligned with the current warehouse state.
- allow rerunning `dbt run` and `dbt test`.

Limitation:

- no incremental dbt models in v1.0.

## EN — Airflow

The DAG coordinates the same manual commands and shares `load_date` across ingestion, quality and warehouse.

Goal:

- avoid mixing partitions from different dates.
- keep task-level traceability.

Limitation:

- local Airflow does not add locking, SLA or production retry management.

## EN — Practical rules

- Use the same `load_date` from ingestion to warehouse.
- Do not load warehouse from demo partitions except for controlled tests.
- Run `dbt run` after `load-warehouse`.
- Review audit if validation is repeated.
- Do not interpret appended audit as a deduplicated final-state table.
