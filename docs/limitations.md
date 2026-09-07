# Limitations / Limitaciones

## ES — Objetivo

Este documento resume las limitaciones conocidas de RetailPulse v1.0 para evitar vender el proyecto como algo que todavía no es.

## ES — Datos sintéticos

Los datos son sintéticos y generados con reglas controladas.

Implicaciones:

- no representan una empresa real.
- no sirven para conclusiones comerciales reales.
- sí sirven para demostrar diseño de pipeline, calidad, modelado y orquestación.

## ES — Pipeline batch y local

El pipeline es batch y se ejecuta en local.

No incluye:

- despliegue cloud.
- alta disponibilidad.
- escalado distribuido.
- gestión productiva de secretos.
- monitorización externa.

## ES — Data lake local

Las capas `raw`, `bronze`, `silver`, `rejected` y `audit` viven en filesystem local.

Limitaciones:

- sin catálogo de datos.
- sin control de permisos por capa.
- sin versionado de objetos.
- sin almacenamiento tipo S3/MinIO en v1.0.
- sin control robusto de concurrencia.

## ES — Ingesta full snapshot

La ingesta extrae tablas completas.

Limitaciones:

- no hay CDC.
- no hay incrementalidad.
- no hay watermark.
- reejecutar una misma `load_date` reemplaza outputs.

## ES — Calidad de datos

Las reglas son deterministas y locales.

Limitaciones:

- no hay reglas estadísticas.
- no hay detección avanzada de anomalías.
- no hay SLA de frescura.
- no hay circuito automático de corrección de rejected.
- audit es archivo local, no observabilidad productiva.

## ES — Warehouse

La carga a `warehouse_source` usa full refresh / replace.

Limitaciones:

- no conserva histórico de snapshots.
- no hay merge incremental.
- no hay SCD.
- no hay particionado físico de warehouse.

## ES — dbt

El modelo dbt es inicial.

Limitaciones:

- marts pequeños.
- sin capa semántica de métricas.
- sin métricas avanzadas.
- sin modelos incrementales.
- sin snapshots dbt.
- tests básicos, aunque útiles.

## ES — Airflow

Airflow es local/dev.

Limitaciones:

- modo standalone.
- sin Celery/Kubernetes.
- sin alertas externas.
- sin SLA.
- sin backfills avanzados.
- sin gestión productiva de usuarios y permisos.

## ES — Herramientas fuera de v1.0

No forman parte de v1.0:

- dashboard.
- Spark.
- Kafka.
- MongoDB.
- ML.
- GenAI.
- HDFS.
- MinIO.

Estas tecnologías solo deberían añadirse después si resuelven un problema concreto y documentado.

## ES — Riesgo principal

El mayor riesgo del proyecto no es que falten herramientas, sino añadir demasiadas antes de cerrar bien el flujo actual.

La prioridad de v1.0 es que el pipeline sea claro, reproducible y explicable.

---

## EN — Objective

This document summarizes the known limitations of RetailPulse v1.0 to avoid presenting the project as something it is not yet.

## EN — Synthetic data

The data is synthetic and generated with controlled rules.

Implications:

- it does not represent a real company.
- it should not be used for real business conclusions.
- it is useful to demonstrate pipeline design, quality, modeling and orchestration.

## EN — Local batch pipeline

The pipeline is batch and runs locally.

It does not include:

- cloud deployment.
- high availability.
- distributed scaling.
- production secret management.
- external monitoring.

## EN — Local data lake

The `raw`, `bronze`, `silver`, `rejected` and `audit` layers live on the local filesystem.

Limitations:

- no data catalog.
- no layer-level permission model.
- no object versioning.
- no S3/MinIO storage in v1.0.
- no robust concurrency control.

## EN — Full snapshot ingestion

Ingestion extracts full tables.

Limitations:

- no CDC.
- no incremental loading.
- no watermark.
- rerunning the same `load_date` replaces outputs.

## EN — Data quality

Rules are deterministic and local.

Limitations:

- no statistical rules.
- no advanced anomaly detection.
- no freshness SLA.
- no automatic rejected-record correction flow.
- audit is a local file, not production observability.

## EN — Warehouse

Loading into `warehouse_source` uses full refresh / replace.

Limitations:

- no snapshot history.
- no incremental merge.
- no SCD.
- no physical warehouse partitioning.

## EN — dbt

The dbt model is initial.

Limitations:

- small marts.
- no semantic metrics layer.
- no advanced metrics.
- no incremental models.
- no dbt snapshots.
- basic but useful tests.

## EN — Airflow

Airflow is local/dev.

Limitations:

- standalone mode.
- no Celery/Kubernetes.
- no external alerts.
- no SLA.
- no advanced backfills.
- no production user and permission management.

## EN — Tools outside v1.0

The following are not part of v1.0:

- dashboard.
- Spark.
- Kafka.
- MongoDB.
- ML.
- GenAI.
- HDFS.
- MinIO.

These technologies should only be added later if they solve a specific documented problem.

## EN — Main risk

The main project risk is not missing tools, but adding too many before closing the current flow properly.

The v1.0 priority is for the pipeline to be clear, reproducible and explainable.
