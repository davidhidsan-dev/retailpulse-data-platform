# RetailPulse — E-commerce Data Engineering Platform

RetailPulse es un proyecto de portfolio de Data Engineering para construir una plataforma reproducible de procesamiento y modelado de datos de comercio electrónico.

## Objetivo

Diseñar un flujo por capas (`raw`, `bronze`, `silver` y `gold`) con trazabilidad, calidad, auditoría y documentación técnica.

## Estado actual

**Fase 0 — estructura y configuración inicial.** El repositorio está preparado para comenzar la Fase 1.

## Stack objetivo v1.0

- Python, pandas, Parquet, SQLAlchemy y PostgreSQL.
- Docker Compose, pytest y GitHub Actions.
- Logging, auditoría y documentación técnica.
- dbt y Airflow en fases posteriores, cuando exista un pipeline que modelar y orquestar.

## Fases futuras

1. Fuentes, contratos e ingestión inicial.
2. Limpieza, calidad, auditoría e idempotencia.
3. Modelado analítico con dbt.
4. Orquestación con Airflow y visualización.

## Primeros comandos

```bash
cp .env.example .env
make up
make ps
make logs
make test
make down
```

> En la Fase 0 todavía no hay generación de datos, ETL ni pipeline implementado.
