# v1.0 Validation Checklist / Checklist de Validación v1.0

## ES — Objetivo

Checklist para validar RetailPulse antes de presentarlo como v1.0 técnica.

Las casillas son una plantilla, no evidencia de una ejecución previa. Activa `.venv`, prepara `.env` y el perfil dbt según el [runbook](runbook.md). Sustituye `YYYY-MM-DD` por la misma fecha UTC de ingesta en todo el recorrido; espera PostgreSQL `healthy` con `make ps` antes de `make init-db`.

## ES — Checks estáticos

- [ ] `make test` pasa.
- [ ] No hay datos generados subidos al repo.
- [ ] `.env` no está subido.
- [ ] `dbt/profiles.yml` no está subido.
- [ ] README presenta v1.0 técnica.
- [ ] `docs/architecture.md` refleja la arquitectura actual.
- [ ] `docs/runbook.md` existe.
- [ ] `docs/technical_decisions.md` contiene decisiones reales.
- [ ] `docs/v1_validation_checklist.md` existe.

## ES — Pipeline manual

Ejecutar:

```bash
make up
make init-db
make seed-db
make ingest-lake
make quality
make load-warehouse LOAD_DATE=YYYY-MM-DD
make dbt-run
make dbt-test
make dbt-docs-generate
```

Validar:

- [ ] PostgreSQL levanta correctamente.
- [ ] Se crea el esquema fuente.
- [ ] Se cargan datos sintéticos.
- [ ] Se crean raw y bronze.
- [ ] Se crean silver, rejected y audit.
- [ ] Se cargan tablas en `warehouse_source`.
- [ ] dbt construye staging y marts.
- [ ] dbt test pasa.
- [ ] dbt docs se generan.

## ES — Evidencia esperada

Capas del lake:

- [ ] `data/raw/postgres/...`
- [ ] `data/bronze/postgres/...`
- [ ] `data/silver/postgres/...`
- [ ] `data/rejected/postgres/...`
- [ ] `data/audit/quality_runs.parquet`

Warehouse:

- [ ] `warehouse_source.customers`
- [ ] `warehouse_source.products`
- [ ] `warehouse_source.inventory`
- [ ] `warehouse_source.orders`
- [ ] `warehouse_source.order_items`
- [ ] `warehouse_source.payments`

dbt marts:

- [ ] `analytics.dim_customer`
- [ ] `analytics.dim_product`
- [ ] `analytics.dim_date`
- [ ] `analytics.fact_sales`
- [ ] `analytics.fact_inventory`

## ES — Demo de calidad

Ejecutar:

```bash
make quality-demo SOURCE_LOAD_DATE=YYYY-MM-DD
```

Validar:

- [ ] Se crea partición bronze demo.
- [ ] Se producen rejected records.
- [ ] Los rejected contienen `rejection_reason`.
- [ ] Audit muestra `warning`.

## ES — Airflow

Ejecutar:

```bash
make airflow-up
make airflow-ps
make airflow-logs
```

Abrir `http://localhost:8080` y lanzar:

```text
retailpulse_batch_pipeline
```

Con configuración opcional:

```json
{
  "load_date": "YYYY-MM-DD"
}
```

Validar:

- [ ] Airflow levanta.
- [ ] El DAG aparece.
- [ ] El DAG se ejecuta manualmente.
- [ ] Todas las tasks terminan correctamente.
- [ ] `load_date` es consistente.
- [ ] El DAG solo orquesta comandos existentes.

- [ ] Se revisaron los logs de cada task en la interfaz y se anotó el run ID exitoso.

Detener:

```bash
make airflow-down
```

## ES — Preparación para entrevista

- [ ] Puedo explicar por qué los datos son sintéticos.
- [ ] Puedo explicar raw vs bronze vs silver vs rejected.
- [ ] Puedo explicar por qué rejected no alimenta marts.
- [ ] Puedo explicar por qué dbt empieza después de warehouse.
- [ ] Puedo explicar el grano de `fact_sales`.
- [ ] Puedo explicar el grano de `fact_inventory`.
- [ ] Puedo explicar por qué Airflow solo orquesta.
- [ ] Puedo explicar limitaciones sin vender humo.

---

## EN — Objective

Checklist to validate RetailPulse before presenting it as a technical v1.0.

These checkboxes are a template, not evidence of a previous execution. Activate `.venv` and prepare `.env` and the dbt profile using the [runbook](runbook.md). Replace `YYYY-MM-DD` with the same UTC ingestion date throughout; wait for PostgreSQL to be `healthy` in `make ps` before running `make init-db`.

## EN — Static checks

- [ ] `make test` passes.
- [ ] No generated data is committed.
- [ ] `.env` is not committed.
- [ ] `dbt/profiles.yml` is not committed.
- [ ] README presents technical v1.0.
- [ ] `docs/architecture.md` reflects the current architecture.
- [ ] `docs/runbook.md` exists.
- [ ] `docs/technical_decisions.md` contains real decisions.
- [ ] `docs/v1_validation_checklist.md` exists.

## EN — Manual pipeline

Run:

```bash
make up
make init-db
make seed-db
make ingest-lake
make quality
make load-warehouse LOAD_DATE=YYYY-MM-DD
make dbt-run
make dbt-test
make dbt-docs-generate
```

Validate:

- [ ] PostgreSQL starts correctly.
- [ ] Source schema is created.
- [ ] Synthetic data is loaded.
- [ ] Raw and bronze are created.
- [ ] Silver, rejected and audit are created.
- [ ] Tables are loaded into `warehouse_source`.
- [ ] dbt builds staging and marts.
- [ ] dbt test passes.
- [ ] dbt docs are generated.

## EN — Expected evidence

Lake layers:

- [ ] `data/raw/postgres/...`
- [ ] `data/bronze/postgres/...`
- [ ] `data/silver/postgres/...`
- [ ] `data/rejected/postgres/...`
- [ ] `data/audit/quality_runs.parquet`

Warehouse:

- [ ] `warehouse_source.customers`
- [ ] `warehouse_source.products`
- [ ] `warehouse_source.inventory`
- [ ] `warehouse_source.orders`
- [ ] `warehouse_source.order_items`
- [ ] `warehouse_source.payments`

dbt marts:

- [ ] `analytics.dim_customer`
- [ ] `analytics.dim_product`
- [ ] `analytics.dim_date`
- [ ] `analytics.fact_sales`
- [ ] `analytics.fact_inventory`

## EN — Quality demo

Run:

```bash
make quality-demo SOURCE_LOAD_DATE=YYYY-MM-DD
```

Validate:

- [ ] Demo bronze partition is created.
- [ ] Rejected records are produced.
- [ ] Rejected records contain `rejection_reason`.
- [ ] Audit shows `warning`.

## EN — Airflow

Run:

```bash
make airflow-up
make airflow-ps
make airflow-logs
```

Open `http://localhost:8080` and trigger:

```text
retailpulse_batch_pipeline
```

Optional config:

```json
{
  "load_date": "YYYY-MM-DD"
}
```

Validate:

- [ ] Airflow starts.
- [ ] The DAG appears.
- [ ] The DAG runs manually.
- [ ] All tasks finish successfully.
- [ ] `load_date` is consistent.
- [ ] The DAG only orchestrates existing commands.

- [ ] Each task's logs were reviewed in the UI and the successful run ID was recorded.

Stop:

```bash
make airflow-down
```

## EN — Interview readiness

- [ ] I can explain why the data is synthetic.
- [ ] I can explain raw vs bronze vs silver vs rejected.
- [ ] I can explain why rejected does not feed marts.
- [ ] I can explain why dbt starts after warehouse.
- [ ] I can explain the grain of `fact_sales`.
- [ ] I can explain the grain of `fact_inventory`.
- [ ] I can explain why Airflow only orchestrates.
- [ ] I can explain limitations without overselling.
