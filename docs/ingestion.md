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
- No se aplica limpieza fuerte en esta capa.
- La calidad se aplica después, en la transición bronze → silver/rejected.

## ES — Limitaciones

- No hay ingesta incremental.
- Reejecutar la misma `load_date` reemplaza archivos.
- La escritura de las seis tablas no es una transacción atómica de filesystem.
- El data lake es local, sin catálogo ni almacenamiento cloud.
- No hay control de concurrencia.

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
- No heavy cleaning is applied in this layer.
- Quality is applied later, in the bronze → silver/rejected transition.

## EN — Limitations

- No incremental ingestion.
- Rerunning the same `load_date` replaces files.
- Writing the six tables is not an atomic filesystem transaction.
- The data lake is local, without catalog or cloud storage.
- No concurrency control.
