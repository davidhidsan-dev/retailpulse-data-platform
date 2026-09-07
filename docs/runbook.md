# Runbook / Guía de ejecución

## ES — Objetivo

Este documento resume cómo ejecutar RetailPulse en local, validar resultados y resolver errores comunes.

## ES — Requisitos previos

- Python 3.11+
- Docker Desktop o Docker Engine
- Docker Compose v2
- Make
- Git

Recomendado:

- entorno virtual de Python.
- PowerShell en Windows o Bash en Linux/macOS.

## ES — Preparación inicial

Desde la raíz del repo:

```bash
cp .env.example .env
pip install -r requirements.txt
cp dbt/profiles.yml.example dbt/profiles.yml
```

En PowerShell:

```powershell
Copy-Item .env.example .env
Copy-Item dbt/profiles.yml.example dbt/profiles.yml
```

`.env` y `dbt/profiles.yml` no deben subirse a Git.

## ES — Levantar PostgreSQL

```bash
make up
make ps
```

Logs:

```bash
make logs
```

## ES — Pipeline manual completo

```bash
make init-db
make seed-db
make ingest-lake
make quality
make load-warehouse LOAD_DATE=YYYY-MM-DD
make dbt-run
make dbt-test
```

Ejemplo:

```bash
make load-warehouse LOAD_DATE=2026-09-07
```

## ES — Ejecución con fechas explícitas

```bash
python -m src.ingest.postgres_to_lake --load-date YYYY-MM-DD
python -m src.quality.validate_bronze --load-date YYYY-MM-DD
python -m src.warehouse.load_silver_to_warehouse --load-date YYYY-MM-DD
```

Usar la misma fecha evita errores de partición.

## ES — Tests

```bash
make test
```

Los tests son ligeros y no necesitan levantar Airflow completo.

## ES — Documentación dbt

```bash
make dbt-docs-generate
```

Para servir la documentación:

```bash
cd dbt
dbt docs serve --profiles-dir .
```

## ES — Demo de calidad

```bash
make quality-demo SOURCE_LOAD_DATE=YYYY-MM-DD
```

Fecha demo personalizada:

```bash
make quality-demo SOURCE_LOAD_DATE=YYYY-MM-DD DEMO_LOAD_DATE=2099-01-02
```

Resultado esperado:

- se crea una partición bronze demo.
- aparecen rejected records.
- `rejection_reason` explica el motivo.
- audit muestra `warning`.

## ES — Airflow local

```bash
make airflow-up
make airflow-ps
make airflow-logs
```

Abrir:

```text
http://localhost:8080
```

Ejecutar DAG:

```text
retailpulse_batch_pipeline
```

Configuración opcional:

```json
{
  "load_date": "YYYY-MM-DD"
}
```

Detener:

```bash
make airflow-down
```

## ES — Salidas esperadas

```text
data/raw/postgres/<table>/load_date=YYYY-MM-DD/<table>.csv
data/bronze/postgres/<table>/load_date=YYYY-MM-DD/<table>.parquet
data/silver/postgres/<table>/load_date=YYYY-MM-DD/<table>.parquet
data/rejected/postgres/<table>/load_date=YYYY-MM-DD/<table>_rejected.parquet
data/audit/quality_runs.parquet
```

Tablas warehouse:

```text
warehouse_source.customers
warehouse_source.products
warehouse_source.inventory
warehouse_source.orders
warehouse_source.order_items
warehouse_source.payments
```

Modelos dbt:

```text
analytics.dim_customer
analytics.dim_product
analytics.dim_date
analytics.fact_sales
analytics.fact_inventory
```

## ES — Errores comunes

### No existe partición bronze

Causa probable: se ejecutó calidad antes de ingesta o con fecha incorrecta.

Solución:

```bash
python -m src.ingest.postgres_to_lake --load-date YYYY-MM-DD
python -m src.quality.validate_bronze --load-date YYYY-MM-DD
```

### No existe partición silver

Causa probable: se ejecutó warehouse antes de calidad.

Solución:

```bash
python -m src.quality.validate_bronze --load-date YYYY-MM-DD
make load-warehouse LOAD_DATE=YYYY-MM-DD
```

### dbt no encuentra `profiles.yml`

Solución:

```bash
cp dbt/profiles.yml.example dbt/profiles.yml
```

En PowerShell:

```powershell
Copy-Item dbt/profiles.yml.example dbt/profiles.yml
```

### `POSTGRES_HOST` incorrecto

Fuera de Docker suele ser:

```text
localhost
```

Dentro de Airflow/Docker debe ser:

```text
postgres
```

### Airflow tarda en arrancar

El primer arranque construye imagen e instala dependencias. Revisar logs:

```bash
make airflow-logs
```

### dbt test falla por relationships

Reejecutar carga y modelos con la misma fecha:

```bash
make load-warehouse LOAD_DATE=YYYY-MM-DD
make dbt-run
make dbt-test
```

### dbt test falla por accepted_values

Puede haberse cargado una partición corrupta o demo. Cargar silver normal:

```bash
make quality
make load-warehouse LOAD_DATE=YYYY-MM-DD
make dbt-run
make dbt-test
```

---

## EN — Objective

This document summarizes how to run RetailPulse locally, validate outputs and solve common errors.

## EN — Prerequisites

- Python 3.11+
- Docker Desktop or Docker Engine
- Docker Compose v2
- Make
- Git

Recommended:

- Python virtual environment.
- PowerShell on Windows or Bash on Linux/macOS.

## EN — Initial setup

From the repository root:

```bash
cp .env.example .env
pip install -r requirements.txt
cp dbt/profiles.yml.example dbt/profiles.yml
```

PowerShell:

```powershell
Copy-Item .env.example .env
Copy-Item dbt/profiles.yml.example dbt/profiles.yml
```

`.env` and `dbt/profiles.yml` must not be committed.

## EN — Start PostgreSQL

```bash
make up
make ps
```

Logs:

```bash
make logs
```

## EN — Full manual pipeline

```bash
make init-db
make seed-db
make ingest-lake
make quality
make load-warehouse LOAD_DATE=YYYY-MM-DD
make dbt-run
make dbt-test
```

Example:

```bash
make load-warehouse LOAD_DATE=2026-09-07
```

## EN — Explicit-date execution

```bash
python -m src.ingest.postgres_to_lake --load-date YYYY-MM-DD
python -m src.quality.validate_bronze --load-date YYYY-MM-DD
python -m src.warehouse.load_silver_to_warehouse --load-date YYYY-MM-DD
```

Use the same date to avoid partition errors.

## EN — Tests

```bash
make test
```

The tests are lightweight and do not require a full Airflow deployment.

## EN — dbt documentation

```bash
make dbt-docs-generate
```

To serve docs:

```bash
cd dbt
dbt docs serve --profiles-dir .
```

## EN — Quality demo

```bash
make quality-demo SOURCE_LOAD_DATE=YYYY-MM-DD
```

Custom demo date:

```bash
make quality-demo SOURCE_LOAD_DATE=YYYY-MM-DD DEMO_LOAD_DATE=2099-01-02
```

Expected result:

- a demo bronze partition is created.
- rejected records are produced.
- `rejection_reason` explains why.
- audit shows `warning`.

## EN — Local Airflow

```bash
make airflow-up
make airflow-ps
make airflow-logs
```

Open:

```text
http://localhost:8080
```

Run DAG:

```text
retailpulse_batch_pipeline
```

Optional config:

```json
{
  "load_date": "YYYY-MM-DD"
}
```

Stop:

```bash
make airflow-down
```

## EN — Expected outputs

```text
data/raw/postgres/<table>/load_date=YYYY-MM-DD/<table>.csv
data/bronze/postgres/<table>/load_date=YYYY-MM-DD/<table>.parquet
data/silver/postgres/<table>/load_date=YYYY-MM-DD/<table>.parquet
data/rejected/postgres/<table>/load_date=YYYY-MM-DD/<table>_rejected.parquet
data/audit/quality_runs.parquet
```

Warehouse tables:

```text
warehouse_source.customers
warehouse_source.products
warehouse_source.inventory
warehouse_source.orders
warehouse_source.order_items
warehouse_source.payments
```

dbt models:

```text
analytics.dim_customer
analytics.dim_product
analytics.dim_date
analytics.fact_sales
analytics.fact_inventory
```

## EN — Common errors

### Bronze partition does not exist

Likely cause: quality was run before ingestion or with the wrong date.

Fix:

```bash
python -m src.ingest.postgres_to_lake --load-date YYYY-MM-DD
python -m src.quality.validate_bronze --load-date YYYY-MM-DD
```

### Silver partition does not exist

Likely cause: warehouse load was run before quality.

Fix:

```bash
python -m src.quality.validate_bronze --load-date YYYY-MM-DD
make load-warehouse LOAD_DATE=YYYY-MM-DD
```

### dbt cannot find `profiles.yml`

Fix:

```bash
cp dbt/profiles.yml.example dbt/profiles.yml
```

PowerShell:

```powershell
Copy-Item dbt/profiles.yml.example dbt/profiles.yml
```

### Wrong `POSTGRES_HOST`

Outside Docker it is usually:

```text
localhost
```

Inside Airflow/Docker it must be:

```text
postgres
```

### Airflow takes a long time to start

The first startup builds the image and installs dependencies. Check logs:

```bash
make airflow-logs
```

### dbt test fails on relationships

Reload and rebuild using the same date:

```bash
make load-warehouse LOAD_DATE=YYYY-MM-DD
make dbt-run
make dbt-test
```

### dbt test fails on accepted_values

A corrupted or demo partition may have been loaded. Load normal silver:

```bash
make quality
make load-warehouse LOAD_DATE=YYYY-MM-DD
make dbt-run
make dbt-test
```
