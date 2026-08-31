# Ingesta PostgreSQL a data lake local

## Objetivo de la Fase 2

La Fase 2 extrae snapshots completos de las seis tablas operacionales de PostgreSQL y los persiste en las capas locales `raw` y `bronze`. El proceso se ejecuta con Python, pandas y SQLAlchemy, sin orquestador externo.

## Capas

- **Raw:** copia fiel de la tabla fuente en CSV, sin índice de pandas ni columnas técnicas añadidas. Sirve como evidencia legible de lo recibido.
- **Bronze:** la misma información en Parquet, enriquecida con metadatos de ingesta. Es una representación eficiente y tipada para procesamiento posterior.

CSV se utiliza en raw por su transparencia y facilidad de inspección. Parquet se utiliza en bronze por su almacenamiento columnar, compresión, preservación de tipos y lectura selectiva de columnas.

## Estructura de rutas

```text
data/raw/postgres/<table>/load_date=YYYY-MM-DD/<table>.csv
data/bronze/postgres/<table>/load_date=YYYY-MM-DD/<table>.parquet
```

La partición `load_date` representa la fecha UTC de ejecución. Puede indicarse mediante `--load-date`; si se omite, se usa la fecha UTC actual.

## Metadatos bronze

| Columna | Descripción |
|---|---|
| `ingestion_id` | UUID común a todas las tablas de una misma ejecución |
| `ingested_at` | Timestamp UTC de la ejecución |
| `source_system` | Valor constante `postgres` |
| `source_table` | Nombre de la tabla operacional original |

## Ejecución

```bash
python -m src.ingest.postgres_to_lake
python -m src.ingest.postgres_to_lake --load-date 2026-08-21
```

## Limitaciones actuales

- Cada extracción lee la tabla completa; todavía no existe carga incremental.
- Reejecutar la misma tabla para una misma `load_date` reemplaza el archivo diario.
- La escritura de las seis tablas no es una transacción atómica de filesystem.
- Bronze solo añade metadatos; no aplica limpieza fuerte ni reglas avanzadas de calidad.
- El data lake es local y no incorpora catálogo, almacenamiento cloud ni control de concurrencia.

## Próxima fase

La Fase 3 podrá incorporar validaciones de calidad, tipado explícito, deduplicación, idempotencia más estricta y transformación hacia silver. No se incluyen todavía warehouse, dbt, Airflow ni procesamiento distribuido.
