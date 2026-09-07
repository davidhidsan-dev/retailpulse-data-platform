# Orchestration / Orquestación

## ES — Objetivo

Airflow coordina el pipeline batch completo de RetailPulse y permite observar el estado, duración y logs de cada paso desde una interfaz local.

La implementación está pensada para desarrollo y demostración, no para producción.

## ES — Principio clave

Airflow no contiene transformaciones ni reglas de negocio.

Cada task llama a una interfaz CLI existente. La lógica sigue viviendo en:

- Python para generación, ingesta, calidad y carga.
- dbt para modelado analítico y tests.
- PostgreSQL como fuente y warehouse local.

## ES — DAG

DAG:

```text
retailpulse_batch_pipeline
```

Orden:

```text
init_source_schema
  >> seed_source_data
  >> ingest_postgres_to_lake
  >> validate_bronze_quality
  >> load_silver_to_warehouse
  >> dbt_run
  >> dbt_test
```

## ES — Tasks

| Task | Responsabilidad |
|---|---|
| `init_source_schema` | Crea el esquema fuente operacional. |
| `seed_source_data` | Genera y carga datos sintéticos. |
| `ingest_postgres_to_lake` | Extrae PostgreSQL hacia raw y bronze. |
| `validate_bronze_quality` | Valida bronze y escribe silver, rejected y audit. |
| `load_silver_to_warehouse` | Carga silver en `warehouse_source`. |
| `dbt_run` | Construye staging y marts. |
| `dbt_test` | Ejecuta tests dbt. |

## ES — Fecha de carga

Las tasks particionadas comparten la misma `load_date`.

Puede pasarse al lanzar el DAG:

```json
{
  "load_date": "YYYY-MM-DD"
}
```

Si no se indica, se usa la fecha lógica del DAG; si el run manual no tiene fecha lógica, se utiliza `dag_run.run_after` en UTC.

## ES — Ejecución local

```bash
make airflow-up
make airflow-ps
make airflow-logs
```

Abrir:

```text
http://localhost:8080
```

El DAG nace pausado: actívalo y pulsa **Trigger DAG** con una fecha válida. Esta configuración local concede acceso administrativo sin autenticación. El DAG regenera la fuente sintética y comparte archivos y tablas con el flujo manual; no ejecutes ambos a la vez.

Detener:

```bash
make airflow-down
```

Este comando detiene también PostgreSQL en el Compose combinado y conserva los volúmenes.

## ES — Docker

Airflow se define en `docker-compose.airflow.yml`, separado del `docker-compose.yml` principal.

Motivos:

- mantener PostgreSQL como flujo básico.
- evitar que Airflow pese sobre el uso manual.
- permitir orquestación opcional.
- aislar dependencias de Airflow.

## ES — Ejecución sin Airflow

El flujo manual sigue disponible:

```bash
make up
make init-db
make seed-db
make ingest-lake
make quality
make load-warehouse LOAD_DATE=YYYY-MM-DD
make dbt-run
make dbt-test
```

## ES — Limitaciones

- Airflow es local/dev.
- DAG manual y lineal.
- Sin backfills avanzados.
- Sin alertas externas.
- Sin SLA.
- Sin gestión productiva de secretos.
- El seeding forma parte de la demo; en una fuente real viviría fuera del DAG.

---

## EN — Objective

Airflow coordinates the full RetailPulse batch pipeline and makes step status, duration and logs observable from a local interface.

The implementation is intended for development and demonstration, not production.

## EN — Key principle

Airflow does not contain transformations or business rules.

Each task calls an existing CLI interface. Logic remains in:

- Python for generation, ingestion, quality and loading.
- dbt for analytical modeling and tests.
- PostgreSQL as source and local warehouse.

## EN — DAG

DAG:

```text
retailpulse_batch_pipeline
```

Order:

```text
init_source_schema
  >> seed_source_data
  >> ingest_postgres_to_lake
  >> validate_bronze_quality
  >> load_silver_to_warehouse
  >> dbt_run
  >> dbt_test
```

## EN — Tasks

| Task | Responsibility |
|---|---|
| `init_source_schema` | Creates the operational source schema. |
| `seed_source_data` | Generates and loads synthetic data. |
| `ingest_postgres_to_lake` | Extracts PostgreSQL into raw and bronze. |
| `validate_bronze_quality` | Validates bronze and writes silver, rejected and audit. |
| `load_silver_to_warehouse` | Loads silver into `warehouse_source`. |
| `dbt_run` | Builds staging and marts. |
| `dbt_test` | Runs dbt tests. |

## EN — Load date

Partitioned tasks share the same `load_date`.

It can be passed when triggering the DAG:

```json
{
  "load_date": "YYYY-MM-DD"
}
```

If omitted, the DAG logical date is used; when a manual run has no logical date, `dag_run.run_after` in UTC is used instead.

## EN — Local execution

```bash
make airflow-up
make airflow-ps
make airflow-logs
```

Open:

```text
http://localhost:8080
```

The DAG starts paused: enable it and select **Trigger DAG** with a valid date. This local configuration grants administrative access without authentication. The DAG regenerates the synthetic source and shares files and tables with the manual flow; do not run both at the same time.

Stop:

```bash
make airflow-down
```

This command also stops PostgreSQL in the combined Compose project and preserves volumes.

## EN — Docker

Airflow is defined in `docker-compose.airflow.yml`, separated from the main `docker-compose.yml`.

Reasons:

- keep PostgreSQL as the basic flow.
- avoid making manual usage heavier.
- make orchestration optional.
- isolate Airflow dependencies.

## EN — Execution without Airflow

The manual flow remains available:

```bash
make up
make init-db
make seed-db
make ingest-lake
make quality
make load-warehouse LOAD_DATE=YYYY-MM-DD
make dbt-run
make dbt-test
```

## EN — Limitations

- Airflow is local/dev.
- Manual and linear DAG.
- No advanced backfills.
- No external alerts.
- No SLA.
- No production secret management.
- Seeding is part of the demo; with a real source it would live outside the DAG.
