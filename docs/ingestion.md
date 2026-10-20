# Ingestion / Ingesta

## ES — Objetivo

La ingesta extrae snapshots completos de las seis tablas operacionales de PostgreSQL y los persiste en el data lake local.

El objetivo de esta capa es separar la extracción de la fuente del resto del pipeline y dejar evidencia reproducible de cada carga.

## ES — Entrada

Fuente PostgreSQL:

- `customers`
- `products`
- `inventory`
- `orders`
- `order_items`
- `payments`

## ES — Snapshot coherente de lectura

Las seis tablas se leen con una única conexión PostgreSQL dentro de una
transacción de solo lectura con aislamiento `REPEATABLE READ`. PostgreSQL fija
una vista estable de la base de datos al comenzar las consultas, por lo que un
cambio confirmado por otra conexión durante la extracción no aparece a mitad de
la misma ingesta.

Los DataFrames se extraen dentro de la transacción y los archivos raw/bronze se
escriben después de cerrarla. Así se reduce el tiempo durante el que PostgreSQL
debe conservar el snapshot. El `ingestion_id` compartido identifica por tanto
tablas obtenidas desde la misma vista de la fuente.

## ES — Salidas

Raw:

```text
data/raw/postgres/<table>/load_date=YYYY-MM-DD/<table>.csv
```

Bronze:

```text
data/bronze/postgres/<table>/load_date=YYYY-MM-DD/<table>.parquet
```

## ES — Raw

Raw guarda una copia fiel de cada tabla fuente en CSV.

Características:

- sin índice de pandas.
- sin columnas técnicas añadidas.
- fácil de inspeccionar manualmente.
- útil como evidencia de lo extraído.

Raw no debe contener transformaciones de negocio.

## ES — Bronze

Bronze guarda la misma información en Parquet y añade metadatos técnicos:

| Columna | Descripción |
|---|---|
| `ingestion_id` | UUID común a las tablas de una misma ejecución |
| `ingested_at` | timestamp UTC de ingesta |
| `source_system` | sistema fuente, `postgres` |
| `source_table` | tabla fuente original |

Bronze es la entrada de la validación de calidad, pero todavía no se considera dato aprobado para analítica.

## ES — Fecha de carga

La partición `load_date` representa la fecha lógica de la carga.

Puede indicarse explícitamente:

```bash
python -m src.ingest.postgres_to_lake --load-date YYYY-MM-DD
```

Si se omite, se usa la fecha UTC actual.

## ES — Ejecución

Con Make:

```bash
make ingest-lake
```

Con Python:

```bash
python -m src.ingest.postgres_to_lake
python -m src.ingest.postgres_to_lake --load-date YYYY-MM-DD
```

## ES — Decisiones

- CSV se usa en raw por legibilidad.
- Parquet se usa en bronze por eficiencia, compresión y preservación de tipos.
- La extracción es full snapshot en v1.0.
- Las tablas comparten una transacción `REPEATABLE READ` de solo lectura.
- No se aplica limpieza fuerte en esta capa.
- La calidad se aplica después, en la transición bronze → silver/rejected.

## ES — Limitaciones

- No hay ingesta incremental.
- Reejecutar la misma `load_date` reemplaza archivos.
- La escritura de las seis tablas no es una transacción atómica de filesystem.
- El data lake es local, sin catálogo ni almacenamiento cloud.
- No hay locking entre dos ejecuciones que escriban simultáneamente la misma
  `load_date`.

---

## EN — Objective

Ingestion extracts full snapshots from the six PostgreSQL operational tables and stores them in the local data lake.

The goal of this layer is to separate source extraction from the rest of the pipeline and keep reproducible evidence of each load.

## EN — Input

PostgreSQL source:

- `customers`
- `products`
- `inventory`
- `orders`
- `order_items`
- `payments`

## EN — Consistent read snapshot

All six tables are read through one PostgreSQL connection inside a read-only
`REPEATABLE READ` transaction. PostgreSQL establishes a stable database view
when queries begin, so a change committed by another connection during
extraction cannot appear halfway through the same ingestion.

DataFrames are extracted inside the transaction and raw/bronze files are
written after it closes. This reduces how long PostgreSQL must retain the
snapshot. The shared `ingestion_id` therefore identifies tables obtained from
the same source view.

## EN — Outputs

Raw:

```text
data/raw/postgres/<table>/load_date=YYYY-MM-DD/<table>.csv
```

Bronze:

```text
data/bronze/postgres/<table>/load_date=YYYY-MM-DD/<table>.parquet
```

## EN — Raw

Raw stores a faithful CSV copy of each source table.

Characteristics:

- no pandas index.
- no added technical columns.
- easy to inspect manually.
- useful as evidence of what was extracted.

Raw should not contain business transformations.

## EN — Bronze

Bronze stores the same information in Parquet and adds technical metadata:

| Column | Description |
|---|---|
| `ingestion_id` | UUID shared by tables in the same run |
| `ingested_at` | UTC ingestion timestamp |
| `source_system` | source system, `postgres` |
| `source_table` | original source table |

Bronze is the input to quality validation, but it is still not approved for analytics.

## EN — Load date

The `load_date` partition represents the logical load date.

It can be passed explicitly:

```bash
python -m src.ingest.postgres_to_lake --load-date YYYY-MM-DD
```

If omitted, the current UTC date is used.

## EN — Execution

With Make:

```bash
make ingest-lake
```

With Python:

```bash
python -m src.ingest.postgres_to_lake
python -m src.ingest.postgres_to_lake --load-date YYYY-MM-DD
```

## EN — Decisions

- CSV is used in raw for readability.
- Parquet is used in bronze for efficiency, compression and type preservation.
- Ingestion is a full snapshot in v1.0.
- Tables share one read-only `REPEATABLE READ` transaction.
- No heavy cleaning is applied in this layer.
- Quality is applied later, in the bronze → silver/rejected transition.

## EN — Limitations

- No incremental ingestion.
- Rerunning the same `load_date` replaces files.
- Writing the six tables is not an atomic filesystem transaction.
- The data lake is local, without catalog or cloud storage.
- There is no locking between two runs writing the same `load_date`
  simultaneously.
