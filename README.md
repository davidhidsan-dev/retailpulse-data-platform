# RetailPulse — E-commerce Data Engineering Platform

RetailPulse es un proyecto de portfolio de Data Engineering para construir una plataforma reproducible de procesamiento y modelado de datos de comercio electrónico.

## Objetivo

Diseñar un flujo por capas (`raw`, `bronze`, `silver` y `gold`) con trazabilidad, calidad, auditoría y documentación técnica.

## Estado actual

**Fase 1 — datos sintéticos y PostgreSQL.** El modelo fuente, el generador y la carga inicial están implementados.

## Stack objetivo v1.0

- Python, pandas, Parquet, SQLAlchemy y PostgreSQL.
- Docker Compose, pytest y GitHub Actions.
- Logging, auditoría y documentación técnica.
- dbt y Airflow en fases posteriores, cuando exista un pipeline que modelar y orquestar.

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

## Fases futuras

1. Fuentes, contratos e ingestión inicial.
2. Limpieza, calidad, auditoría e idempotencia.
3. Modelado analítico con dbt.
4. Orquestación con Airflow y visualización.

## Primeros comandos

```bash
make up
make ps
make logs
make init-db
make seed-db
make test
make down
```

> La Fase 1 solo cubre datos sintéticos y PostgreSQL. Todavía no hay ETL, data lake ni warehouse.
