# Runbook / Guía de ejecución

## ES — Objetivo

Este documento resume cómo ejecutar RetailPulse en local, validar resultados y resolver errores comunes.

## ES — Requisitos previos

- Python 3.11
- Docker Desktop o Docker Engine
- Docker Compose v2
- Make
- Git

Recomendado:

- entorno virtual de Python.
- PowerShell en Windows o Bash en Linux/macOS.

## ES — Preparación inicial

Desde la raíz del repo:

Los ejemplos de creación son para la primera instalación. Si `.venv`, `.env` o `dbt/profiles.yml` ya existen, activa el entorno y conserva su configuración. `YYYY-MM-DD` es un marcador que debes sustituir por una fecha real.

```bash
python -m venv .venv
source .venv/bin/activate
cp .env.example .env
python -m pip install -r requirements.txt
cp dbt/profiles.yml.example dbt/profiles.yml
```

En PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
Copy-Item .env.example .env
python -m pip install -r requirements.txt
Copy-Item dbt/profiles.yml.example dbt/profiles.yml
```

`.env` y `dbt/profiles.yml` no deben subirse a Git.

Python y Compose leen `.env`. Los comandos dbt del Makefile leen variables del proceso o los valores de fallback del perfil, pero no cargan `.env`. Si personalizas credenciales, puerto o esquema, exporta las mismas variables `POSTGRES_*` y `DBT_SCHEMA` en la terminal o adapta el perfil local. Las variables ya exportadas prevalecen sobre `.env`.

## ES — Levantar PostgreSQL

```bash
make up
make ps
```

Espera a que `make ps` muestre PostgreSQL `healthy` antes de ejecutar `make init-db`.

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

`make seed-db` reemplaza el contenido de las seis tablas fuente. `make ingest-lake` y `make quality` usan la fecha UTC actual; utiliza esa fecha para `LOAD_DATE`. Estos dos targets no propagan una variable Make `LOAD_DATE`.

La carga warehouse elimina las vistas staging dependientes mediante `CASCADE`; reconstruye con `make dbt-run` y valida con `make dbt-test` después de cada carga.

## ES — Ejecución con fechas explícitas

```bash
python -m src.ingest.postgres_to_lake --load-date YYYY-MM-DD
python -m src.quality.validate_bronze --load-date YYYY-MM-DD
python -m src.warehouse.load_silver_to_warehouse --load-date YYYY-MM-DD
```

Usar la misma fecha evita errores de partición. Debe existir bronze antes de validar y silver antes de cargar warehouse. `load_date` etiqueta la extracción completa actual: no filtra pedidos ni recupera un estado histórico de PostgreSQL.

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
dbt docs serve --profiles-dir . --port 8081
```

Abre `http://localhost:8081`. Ese puerto evita el conflicto con Airflow en `8080`. Usa `Ctrl+C` para detener el servidor y vuelve a la raíz antes de continuar con Make.

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

La fecha de origen debe contener las seis tablas bronze. La demo usa `2099-01-01` por defecto y no debe cargarse como dataset analítico normal.

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

Espera a que Airflow esté saludable, activa el DAG (nace pausado) y pulsa **Trigger DAG**:

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

Este comando detiene Airflow y PostgreSQL del Compose combinado, conservando volúmenes. Airflow es local/dev con acceso administrativo sin autenticación. El DAG regenera la fuente; no lo ejecutes a la vez que el flujo manual, porque comparten datos.

## ES — Limpieza del entorno

`make clean` limpia cachés Python/pytest y conserva `data/` y los volúmenes Docker. `make down` detiene el entorno básico; si levantaste Airflow, usa `make airflow-down`. No se eliminan automáticamente datasets ni bases de datos.

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

Primero identifica el test fallido y consulta su SQL compilado bajo `dbt/target/compiled/`. Las FK de calidad se validan contra bronze: un padre rechazado por otra regla no rechaza sus hijos en cascada. Revisa rejected y los conteos de los marts antes de volver a cargar una partición silver normal y coherente:

```bash
make load-warehouse LOAD_DATE=YYYY-MM-DD
make dbt-run
make dbt-test
```

### dbt test falla por accepted_values

Puede haberse cargado una partición corrupta o demo. Cargar silver normal:

```bash
python -m src.quality.validate_bronze --load-date YYYY-MM-DD
make load-warehouse LOAD_DATE=YYYY-MM-DD
make dbt-run
make dbt-test
```

---

## EN — Objective

This document summarizes how to run RetailPulse locally, validate outputs and solve common errors.

## EN — Prerequisites

- Python 3.11
- Docker Desktop or Docker Engine
- Docker Compose v2
- Make
- Git

Recommended:

- Python virtual environment.
- PowerShell on Windows or Bash on Linux/macOS.

## EN — Initial setup

From the repository root:

Creation examples are for the first installation. If `.venv`, `.env` or `dbt/profiles.yml` already exist, activate the environment and preserve their configuration. Replace the `YYYY-MM-DD` placeholder with a real date.

```bash
python -m venv .venv
source .venv/bin/activate
cp .env.example .env
python -m pip install -r requirements.txt
cp dbt/profiles.yml.example dbt/profiles.yml
```

PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
Copy-Item .env.example .env
python -m pip install -r requirements.txt
Copy-Item dbt/profiles.yml.example dbt/profiles.yml
```

`.env` and `dbt/profiles.yml` must not be committed.

Python and Compose read `.env`. The Makefile dbt targets read process environment variables or profile fallbacks, but do not load `.env`. For custom credentials, ports or schemas, export matching `POSTGRES_*` and `DBT_SCHEMA` variables in the terminal or adjust the local profile. Existing exported variables take precedence over `.env`.

## EN — Start PostgreSQL

```bash
make up
make ps
```

Wait until `make ps` shows PostgreSQL as `healthy` before running `make init-db`.

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

`make seed-db` replaces the contents of the six source tables. `make ingest-lake` and `make quality` use the current UTC date; use that date for `LOAD_DATE`. These two targets do not forward a Make `LOAD_DATE` variable.

The warehouse load drops dependent staging views through `CASCADE`; rebuild with `make dbt-run` and validate with `make dbt-test` after every load.

## EN — Explicit-date execution

```bash
python -m src.ingest.postgres_to_lake --load-date YYYY-MM-DD
python -m src.quality.validate_bronze --load-date YYYY-MM-DD
python -m src.warehouse.load_silver_to_warehouse --load-date YYYY-MM-DD
```

Use the same date to avoid partition errors. Bronze must exist before validation and silver before warehouse loading. `load_date` labels the current full extraction; it does not filter orders or retrieve a historical PostgreSQL state.

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
dbt docs serve --profiles-dir . --port 8081
```

Open `http://localhost:8081`. This port avoids a conflict with Airflow on `8080`. Stop the server with `Ctrl+C` and return to the repository root before continuing with Make.

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

The source date must contain all six bronze tables. The demo defaults to `2099-01-01` and must not be loaded as the normal analytical dataset.

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

Wait until Airflow is healthy, enable the DAG (initially paused) and select **Trigger DAG**:

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

This command stops both Airflow and PostgreSQL in the combined Compose project, preserving volumes. Airflow is local/dev with administrative access without authentication. The DAG regenerates the source; do not run it concurrently with the manual flow, as they share data.

## EN — Environment cleanup

`make clean` removes Python/pytest caches while preserving `data/` and Docker volumes. `make down` stops the basic environment; use `make airflow-down` if Airflow is running. Datasets and databases are not automatically deleted.

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

First identify the failing test and inspect its compiled SQL under `dbt/target/compiled/`. Quality checks FK references against bronze: rejecting a parent for another rule does not cascade to its children. Review rejected records and mart counts before reloading a consistent normal silver partition:

```bash
make load-warehouse LOAD_DATE=YYYY-MM-DD
make dbt-run
make dbt-test
```

### dbt test fails on accepted_values

A corrupted or demo partition may have been loaded. Load normal silver:

```bash
python -m src.quality.validate_bronze --load-date YYYY-MM-DD
make load-warehouse LOAD_DATE=YYYY-MM-DD
make dbt-run
make dbt-test
```
