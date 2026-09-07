# Technical Decisions / Decisiones Técnicas

## ES — Objetivo

Este documento resume las decisiones principales de RetailPulse v1.0. Cada decisión incluye contexto, decisión, alternativa simple y limitación.

## ES — TD-001 Datos sintéticos

### Contexto

El proyecto necesita una fuente repetible con clientes, productos, inventario, pedidos, líneas de pedido y pagos.

### Decisión

Generar datos sintéticos con Python manteniendo relaciones consistentes.

### Alternativa simple

Usar un CSV público.

### Limitación

Los datos no representan un negocio real. Sirven para demostrar ingeniería de datos, no conclusiones comerciales reales.

## ES — TD-002 PostgreSQL como fuente y warehouse local

### Contexto

Se necesita una fuente relacional y un destino SQL ejecutable en local.

### Decisión

Usar PostgreSQL como sistema operacional simulado y como warehouse local en esquemas separados.

### Alternativa simple

Usar solo CSV/Parquet.

### Limitación

Usar la misma tecnología para fuente y warehouse es una simplificación local, no una arquitectura productiva.

## ES — TD-003 Raw CSV

### Contexto

Conviene conservar una copia legible de lo extraído.

### Decisión

Guardar raw como CSV.

### Alternativa simple

Escribir directamente a Parquet.

### Limitación

CSV es menos robusto para tipos, pero es fácil de inspeccionar.

## ES — TD-004 Bronze y silver en Parquet

### Contexto

Las capas procesables necesitan formato eficiente y más estable que CSV.

### Decisión

Usar Parquet para bronze y silver.

### Alternativa simple

Usar CSV en todas las capas.

### Limitación

Parquet requiere dependencias adicionales y no es tan legible manualmente.

## ES — TD-005 Separar silver y rejected

### Contexto

Los registros inválidos no deben contaminar métricas, pero tampoco deben perderse.

### Decisión

Escribir válidos en silver e inválidos en rejected.

### Alternativa simple

Fallar todo el pipeline ante el primer error.

### Limitación

Añade rutas y documentación, pero mejora diagnóstico y auditoría.

## ES — TD-006 Audit de calidad

### Contexto

Hay que conocer filas leídas, válidas, rechazadas y estado por tabla.

### Decisión

Guardar `data/audit/quality_runs.parquet`.

### Alternativa simple

Mostrar solo logs por consola.

### Limitación

Es auditoría local, no observabilidad productiva ni alertas.

## ES — TD-007 dbt para modelado analítico

### Contexto

Las transformaciones analíticas deben vivir en SQL versionado con dependencias y tests.

### Decisión

Usar dbt para staging y marts.

### Alternativa simple

Escribir SQL suelto o transformar todo con Python.

### Limitación

Añade herramienta, pero mejora contratos, lineage y documentación.

## ES — TD-008 Staging tipado

### Contexto

Aunque pandas cargue tipos razonables, dbt debe definir contrato explícito.

### Decisión

Los `stg_*` seleccionan columnas y aplican casts.

### Alternativa simple

Usar `select *`.

### Limitación

Algunos casts son redundantes, pero hacen el modelo más revisable.

## ES — TD-009 Airflow solo orquesta

### Contexto

El pipeline tiene varias etapas independientes.

### Decisión

Airflow ejecuta comandos existentes y muestra logs por tarea.

### Alternativa simple

Usar solo Makefile.

### Limitación

Airflow añade complejidad local, por eso no debe contener lógica de negocio.

## ES — TD-010 Docker Compose separado para Airflow

### Contexto

Airflow es pesado y no debe complicar el flujo básico.

### Decisión

Mantener `docker-compose.yml` para PostgreSQL y `docker-compose.airflow.yml` como extensión opcional.

### Alternativa simple

Un único compose con todo.

### Limitación

Hay más archivos, pero el uso básico queda más limpio.

## ES — TD-011 Full refresh en v1.0

### Contexto

La prioridad es cerrar un flujo end-to-end claro.

### Decisión

Cargar `warehouse_source` con replace completo.

### Alternativa simple

Append sin control.

### Limitación

No conserva histórico ni demuestra incrementalidad.

## ES — TD-012 No Spark, Kafka, MongoDB, ML ni GenAI en v1.0

### Contexto

Añadir demasiadas herramientas puede hacer el proyecto menos claro.

### Decisión

v1.0 se centra en batch, calidad, warehouse, dbt y Airflow.

### Alternativa simple

Añadir todo el roadmap ya.

### Limitación

Es menos vistoso, pero más coherente y defendible.

## ES — TD-013 Dashboard fuera de v1.0

### Contexto

Primero debe cerrarse la plataforma técnica.

### Decisión

Dejar dashboard para la siguiente fase.

### Alternativa simple

Crear visualización rápida ya.

### Limitación

El repo es menos visual, pero evita construir una capa de consumo sobre una base no cerrada.

---

## EN — Objective

This document summarizes the main decisions behind RetailPulse v1.0. Each decision includes context, decision, simple alternative and limitation.

## EN — TD-001 Synthetic data

### Context

The project needs a repeatable source with customers, products, inventory, orders, order items and payments.

### Decision

Generate synthetic data with Python while keeping consistent relationships.

### Simple alternative

Use a public CSV dataset.

### Limitation

The data does not represent a real business. It demonstrates data engineering, not real commercial insights.

## EN — TD-002 PostgreSQL as source and local warehouse

### Context

The project needs a relational source and a local SQL destination.

### Decision

Use PostgreSQL as both the simulated operational system and local warehouse, separated by schemas.

### Simple alternative

Use only CSV/Parquet.

### Limitation

Using the same technology for source and warehouse is a local simplification, not a production architecture.

## EN — TD-003 Raw CSV

### Context

It is useful to keep a readable copy of what was extracted.

### Decision

Store raw as CSV.

### Simple alternative

Write directly to Parquet.

### Limitation

CSV is weaker for types, but easy to inspect.

## EN — TD-004 Bronze and silver in Parquet

### Context

Processing layers need an efficient and more stable format than CSV.

### Decision

Use Parquet for bronze and silver.

### Simple alternative

Use CSV in every layer.

### Limitation

Parquet requires extra dependencies and is less manually readable.

## EN — TD-005 Separate silver and rejected

### Context

Invalid records should not contaminate metrics, but they should not disappear.

### Decision

Write valid records to silver and invalid records to rejected.

### Simple alternative

Fail the whole pipeline on the first error.

### Limitation

It adds paths and documentation, but improves diagnosis and auditability.

## EN — TD-006 Quality audit

### Context

The pipeline should expose input, valid, rejected and status counts by table.

### Decision

Store `data/audit/quality_runs.parquet`.

### Simple alternative

Print only console logs.

### Limitation

It is local audit, not production observability or alerting.

## EN — TD-007 dbt for analytical modeling

### Context

Analytical transformations should live in versioned SQL with dependencies and tests.

### Decision

Use dbt for staging and marts.

### Simple alternative

Write standalone SQL or transform everything with Python.

### Limitation

It adds a tool, but improves contracts, lineage and documentation.

## EN — TD-008 Typed staging

### Context

Even if pandas loads reasonable types, dbt should define an explicit contract.

### Decision

`stg_*` models select columns and apply casts.

### Simple alternative

Use `select *`.

### Limitation

Some casts are redundant, but they make the model easier to review.

## EN — TD-009 Airflow only orchestrates

### Context

The pipeline has several independent stages.

### Decision

Airflow runs existing commands and exposes task logs.

### Simple alternative

Use only Makefile.

### Limitation

Airflow adds local complexity, so it must not contain business logic.

## EN — TD-010 Separate Docker Compose for Airflow

### Context

Airflow is heavy and should not complicate the basic flow.

### Decision

Keep `docker-compose.yml` for PostgreSQL and `docker-compose.airflow.yml` as an optional extension.

### Simple alternative

Use one compose file for everything.

### Limitation

There are more files, but the basic path stays cleaner.

## EN — TD-011 Full refresh in v1.0

### Context

The priority is to close a clear end-to-end flow.

### Decision

Load `warehouse_source` with full replace.

### Simple alternative

Append without control.

### Limitation

It does not preserve history or demonstrate incremental loading.

## EN — TD-012 No Spark, Kafka, MongoDB, ML or GenAI in v1.0

### Context

Adding too many tools can make the project less clear.

### Decision

v1.0 focuses on batch, quality, warehouse, dbt and Airflow.

### Simple alternative

Add the whole roadmap now.

### Limitation

It is less flashy, but more coherent and defensible.

## EN — TD-013 Dashboard outside v1.0

### Context

The technical platform should be closed first.

### Decision

Leave the dashboard for the next phase.

### Simple alternative

Create a quick visualization now.

### Limitation

The repo is less visual, but avoids building a consumption layer on an unfinished base.
