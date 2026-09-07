# RetailPulse — E-commerce Data Engineering Platform

[English version](README_EN.md)

Proyecto de portfolio de Data Engineering que simula una plataforma batch de e-commerce de extremo a extremo: fuente operacional en PostgreSQL, ingesta a un data lake local, validación de calidad, separación de registros rechazados, carga a warehouse, modelado con dbt y orquestación local opcional con Airflow.

El objetivo no es simular un despliegue productivo real, sino construir un flujo reproducible, entendible y defendible técnicamente.

## Qué problema simula

Una empresa de e-commerce tiene datos operacionales repartidos en clientes, productos, inventario, pedidos, líneas de pedido y pagos. Antes de analizarlos, esos datos deben extraerse, almacenarse por capas, validarse, cargarse en un warehouse y modelarse en tablas analíticas.

RetailPulse reproduce ese flujo con datos sintéticos relacionales y controlados.

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

No incluye todavía dashboard, incrementalidad, Spark, Kafka, MongoDB, ML, GenAI ni despliegue productivo.

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
    ↓
Airflow optional orchestration
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

- Diseño de una fuente relacional sintética.
- Pipeline batch reproducible.
- Separación entre raw, bronze, silver, rejected y audit.
- Validación determinista de calidad.
- Conservación de registros rechazados con motivo de rechazo.
- Carga de datos aprobados a una capa warehouse.
- Modelado analítico con dbt.
- Tests dbt de contratos analíticos.
- Orquestación con Airflow sin mover lógica de negocio al DAG.
- Documentación de decisiones, limitaciones y ejecución.

## Ejecución rápida

Preparar entorno:

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

Levantar PostgreSQL y ejecutar flujo manual:

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

Si `make ingest-lake` y `make quality` se ejecutan sin fecha explícita, usan la fecha UTC actual. Para evitar dudas, los módulos Python aceptan `--load-date`.

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

Rejected no alimenta los marts para no contaminar métricas de negocio.

Más detalle: [`docs/data_quality.md`](docs/data_quality.md).

## Modo demo de calidad

Como la fuente sintética normal es consistente, una ejecución puede producir cero rejected records. Eso es correcto.

Para demostrar el circuito de rechazo:

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
2. Añadir métricas de negocio más claras.
3. Mejorar la carga warehouse con incrementalidad o snapshots.
4. Añadir checks operativos de frescura y conteos.
5. Incorporar Spark, Kafka, MongoDB o ML solo como extensiones separadas y justificadas.
