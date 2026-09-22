# RetailPulse — E-commerce Data Engineering Platform

![CI](https://github.com/davidhidsan-dev/retailpulse-data-platform/actions/workflows/ci.yml/badge.svg)

[English version](README_EN.md)

Proyecto de portfolio de Data Engineering que simula una plataforma batch de e-commerce de extremo a extremo: fuente operacional en PostgreSQL, ingesta a un data lake local, validación de calidad, separación de registros rechazados, carga a warehouse, modelado con dbt y orquestación local opcional con Airflow.

El foco está en construir un flujo batch reproducible, trazable y técnicamente sólido en un entorno local, no en replicar una plataforma productiva a escala.

## Qué problema simula

Una empresa de e-commerce tiene datos operacionales repartidos en clientes, productos, inventario, pedidos, líneas de pedido y pagos. Antes de poder analizarlos, esos datos deben extraerse, almacenarse por capas, validarse, cargarse en un warehouse y transformarse en modelos analíticos útiles.

RetailPulse reproduce ese proceso con datos sintéticos relacionales y controlados.

## Estado actual

Versión técnica v1.0.

Incluye:

- generación sintética de datos e-commerce.
- PostgreSQL como fuente operacional.
- data lake local con capas `raw`, `bronze`, `silver`, `rejected` y `audit`.
- ingesta Python desde PostgreSQL.
- validación de calidad con rejected records.
- modo demo de anomalías controladas.
- carga de `silver` a `warehouse_source` en PostgreSQL.
- modelos dbt de staging y marts.
- tests dbt sobre claves, dominios y relaciones.
- orquestación local opcional con Airflow.
- tests ligeros con pytest y GitHub Actions.

La v1.0 cubre el pipeline completo hasta los marts de dbt e incluye orquestación local con Airflow. El dashboard y otras extensiones quedan para fases posteriores.

## Arquitectura

```text
PostgreSQL source
    ↓
Python ingestion
    ↓
raw CSV
    ↓
bronze Parquet + ingestion metadata
    ↓
Python quality validation
    ├── silver Parquet → warehouse_source PostgreSQL → dbt staging → dbt marts
    ├── rejected Parquet
    └── audit quality_runs.parquet

Airflow orchestrates the end-to-end batch flow above.
```

Airflow no transforma datos. Solo coordina comandos Python y dbt ya existentes.

Más detalle: [`docs/architecture.md`](docs/architecture.md).

## Stack

- Python
- pandas
- PostgreSQL
- SQLAlchemy
- Parquet / pyarrow
- dbt-postgres
- Apache Airflow local
- Docker Compose
- pytest
- GitHub Actions
- SQL

## Qué demuestra

- Diseño de un pipeline batch por capas con trazabilidad de ejecución.
- Separación clara entre ingesta, calidad, modelado y orquestación.
- Gestión explícita de registros válidos, rechazados y auditoría.
- Modelado dimensional con hechos y dimensiones construidos a partir de datos previamente validados.
- Reejecución controlada e idempotencia práctica mediante particiones y cargas replace.
- Uso de tests y documentación para definir contratos técnicos y analíticos.

## Ejecución rápida

Requiere Python 3.11. Preparar entorno:

```bash
cp .env.example .env
pip install -r requirements.txt
cp dbt/profiles.yml.example dbt/profiles.yml
```

En PowerShell:

```powershell
Copy-Item .env.example .env
python -m pip install -r requirements.txt
Copy-Item dbt/profiles.yml.example dbt/profiles.yml
```

Levantar PostgreSQL y ejecutar el flujo manual:

```bash
make up
make init-db
make seed-db
make ingest-lake LOAD_DATE=YYYY-MM-DD
make quality LOAD_DATE=YYYY-MM-DD
make load-warehouse LOAD_DATE=YYYY-MM-DD
make dbt-run
make dbt-test
```

Los tres targets aceptan `LOAD_DATE`; si se omite, usan la fecha UTC actual. Los módulos Python también aceptan `--load-date`.

## Ejecución con Airflow

Airflow es opcional y está pensado para desarrollo local.

```bash
make airflow-up
make airflow-ps
make airflow-logs
```

Abrir:

```text
http://localhost:8080
```

Ejecutar manualmente el DAG:

```text
retailpulse_batch_pipeline
```

Configuración opcional:

```json
{
  "load_date": "YYYY-MM-DD"
}
```

Detener Airflow:

```bash
make airflow-down
```

Más detalle: [`docs/orchestration.md`](docs/orchestration.md).

## Calidad de datos y rejected records

La validación lee `bronze`, aplica reglas de completitud, unicidad, dominios, importes y claves foráneas, y separa:

```text
bronze
    ├── silver   → warehouse → dbt → marts
    ├── rejected → diagnóstico / auditoría / posible reproceso
    └── audit    → resumen por tabla y ejecución
```

Los registros rechazados conservan las columnas originales y añaden:

- `quality_run_id`
- `quality_checked_at`
- `rejection_reason`

Rejected no alimenta los marts para evitar contaminar las métricas de negocio.

Más detalle: [`docs/data_quality.md`](docs/data_quality.md).

## Modo demo de calidad

Como la fuente sintética normal es consistente, una ejecución puede producir cero rejected records. Eso es correcto.

Para demostrar de forma controlada el circuito de rechazo:

```bash
make quality-demo SOURCE_LOAD_DATE=YYYY-MM-DD
```

Por defecto crea y valida una partición demo con fecha `2099-01-01`.

## Modelo analítico

dbt construye:

Staging:

- `stg_customers`
- `stg_products`
- `stg_inventory`
- `stg_orders`
- `stg_order_items`
- `stg_payments`

Marts:

- `dim_customer`
- `dim_product`
- `dim_date`
- `fact_sales`
- `fact_inventory`

`fact_sales` tiene grano de línea de pedido (`order_item_id`). `fact_inventory` tiene grano de producto (`product_id`).

Más detalle: [`docs/warehouse_model.md`](docs/warehouse_model.md).

## Estructura del repositorio

```text
.
├── dags/                         # DAG de Airflow
├── data/                         # Capas locales del lake, datos ignorados por Git
├── dbt/                          # Proyecto dbt
├── docs/                         # Documentación técnica
├── sql/                          # SQL de esquemas fuente y warehouse
├── src/
│   ├── ingest/                   # Ingesta PostgreSQL → lake
│   ├── quality/                  # Calidad, rejected y demo
│   ├── synthetic_data/           # Generación sintética
│   ├── utils/                    # Configuración, rutas y base de datos
│   └── warehouse/                # Carga silver → warehouse
├── tests/                        # Tests unitarios y estáticos
├── docker-compose.yml            # PostgreSQL
├── docker-compose.airflow.yml    # Airflow opcional
├── Dockerfile.airflow
├── constraints.txt               # Resolución exacta probada en Python 3.11
├── Makefile
└── requirements.txt
```

## Documentación técnica

- [`docs/architecture.md`](docs/architecture.md)
- [`docs/source_model.md`](docs/source_model.md)
- [`docs/ingestion.md`](docs/ingestion.md)
- [`docs/data_quality.md`](docs/data_quality.md)
- [`docs/data_dictionary.md`](docs/data_dictionary.md)
- [`docs/idempotency.md`](docs/idempotency.md)
- [`docs/warehouse_model.md`](docs/warehouse_model.md)
- [`docs/limitations.md`](docs/limitations.md)
- [`docs/orchestration.md`](docs/orchestration.md)
- [`docs/runbook.md`](docs/runbook.md)
- [`docs/technical_decisions.md`](docs/technical_decisions.md)
- [`docs/v1_validation_checklist.md`](docs/v1_validation_checklist.md)

## Nota sobre el desarrollo

Este proyecto se ha desarrollado con apoyo de Codex como asistente de programación y aprendizaje. Se ha utilizado para contrastar decisiones técnicas, depurar errores, revisar alternativas y acelerar tareas de implementación y documentación.

La arquitectura del pipeline, el alcance del proyecto, la validación de resultados, las decisiones finales y la revisión del código se han trabajado de forma consciente durante el desarrollo.

## Limitaciones

- Los datos son sintéticos.
- El pipeline es batch y local.
- La carga a warehouse usa full refresh / replace.
- No hay incrementalidad.
- No hay dashboard en v1.0.
- Airflow es local/dev.
- No hay SCD ni capa semántica de métricas.
- CI no ejecuta el pipeline completo con servicios reales.
- Rejected se conserva en el lake, no en marts analíticos.

## Próximos pasos razonables

1. Crear un dashboard sencillo sobre los marts dbt.
2. Añadir métricas de negocio orientadas a consumo analítico.
3. Explorar una estrategia incremental para las cargas.
4. Añadir checks operativos de frescura y volumen.
5. Evaluar extensiones adicionales solo cuando resuelvan un problema concreto del proyecto.
