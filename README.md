# RetailPulse — E-commerce Data Engineering Platform

RetailPulse es un proyecto de portfolio de Data Engineering para construir una plataforma reproducible de procesamiento y modelado de datos de comercio electrónico.

## Objetivo

Diseñar un flujo por capas (`raw`, `bronze`, `silver` y `gold`) con trazabilidad, calidad, auditoría y documentación técnica.

## Estado actual

**Fase 4 — warehouse PostgreSQL y modelo analítico dbt.** Los datos silver se publican en `warehouse_source` y dbt construye staging, dimensiones y hechos iniciales.

## Stack objetivo v1.0

- Python, pandas, Parquet, SQLAlchemy y PostgreSQL.
- Docker Compose, pytest y GitHub Actions.
- Logging, auditoría y documentación técnica.
- dbt para el modelado analítico; Airflow queda reservado para una fase posterior.

## Fase 1

La fuente contiene las tablas `customers`, `products`, `inventory`, `orders`, `order_items` y `payments`. Para crear el esquema y generar una carga sintética:

```bash
cp .env.example .env
make up
make init-db
make seed-db
```

El generador también puede ejecutarse directamente:

```bash
python -m src.synthetic_data.generate_retail_data \
  --customers 500 \
  --products 100 \
  --orders 1000 \
  --seed 42
```

Los parámetros son opcionales. Por defecto se generan 2.000 clientes, 300 productos y 10.000 pedidos con seed `42`. Una misma combinación de volúmenes y seed produce el mismo dataset.

## Fase 2

Las seis tablas fuente se extraen completas desde PostgreSQL. Raw conserva snapshots CSV sin modificar y bronze escribe Parquet con `ingestion_id`, `ingested_at`, `source_system` y `source_table`.

```bash
make ingest-lake
python -m src.ingest.postgres_to_lake --load-date 2026-08-21
```

Los archivos se organizan como `data/<layer>/postgres/<table>/load_date=YYYY-MM-DD/` y están excluidos de Git.

## Fase 3

La validación aplica reglas de completitud, unicidad, dominio, importes y claves foráneas. Los registros válidos se escriben en silver, los inválidos en rejected y cada ejecución se resume en `data/audit/quality_runs.parquet`.

```bash
make quality
python -m src.quality.validate_bronze --load-date 2026-08-21
```

La partición bronze de la fecha solicitada debe existir antes de ejecutar la validación.

Una ejecución normal puede producir cero rejected records cuando la fuente cumple todas las reglas. Para demostrar el circuito de rechazo sin alterar la fuente ni el generador principal, existe un modo demo con anomalías controladas:

```bash
make quality-demo SOURCE_LOAD_DATE=2026-09-01
```

El comando crea y valida por defecto la partición demo `2099-01-01`. Puede cambiarse con `DEMO_LOAD_DATE=YYYY-MM-DD`.

## Fase 4

Las seis tablas silver se cargan mediante estrategia replace en el esquema PostgreSQL `warehouse_source`. dbt crea seis vistas staging y los marts `dim_customer`, `dim_product`, `dim_date`, `fact_sales` y `fact_inventory`.

Antes del primer uso, crea el perfil dbt local:

```powershell
Copy-Item dbt/profiles.yml.example dbt/profiles.yml
```

Después ejecuta:

```bash
make load-warehouse LOAD_DATE=2026-09-02
make dbt-run
make dbt-test
make dbt-docs-generate
```

`LOAD_DATE` es opcional; sin ese valor se usa la fecha UTC actual. El perfil local utiliza las variables PostgreSQL y está excluido de Git.

## Fases futuras

1. Orquestación con Airflow.
2. Capa gold, visualización y evolución operativa.

## Primeros comandos

```bash
make up
make ps
make logs
make init-db
make seed-db
make ingest-lake
make quality
make quality-demo SOURCE_LOAD_DATE=2026-09-01
make load-warehouse LOAD_DATE=2026-09-02
make dbt-run
make dbt-test
make test
make down
```

> La Fase 4 cubre carga completa al warehouse y modelado dbt inicial. Todavía no hay incrementalidad, Airflow ni dashboards.
