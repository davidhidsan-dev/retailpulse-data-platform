# Warehouse Model / Modelo de Warehouse

## ES — Objetivo

La capa warehouse lleva los datos aprobados desde `silver` a PostgreSQL y usa dbt para construir un modelo analítico inicial.

Python mueve datos validados. dbt modela tablas analíticas, dependencias, documentación y tests.

## ES — Silver y `warehouse_source`

`silver` es la salida de calidad en Parquet. `warehouse_source` es el esquema PostgreSQL que expone esas tablas a dbt.

```text
data/silver/postgres/<table>/load_date=YYYY-MM-DD/<table>.parquet
                              │
                              ▼
warehouse_source.<table>
```

La carga usa replace completo en v1.0. No carga rejected ni audit al modelo analítico.

Antes de iniciar la transacción, la carga comprueba que la última ejecución de calidad para esa `load_date` aprobó todas las tablas. Verifica también el número de filas y el `quality_run_id` de cada archivo silver. Si falta la auditoría, la ejecución falló o un archivo pertenece a otra ejecución, no modifica el warehouse.

La carga compara también el `ingestion_id` de silver con el de la partición bronze actual. Si bronze se ha vuelto a ingerir para la misma fecha, hay que repetir la validación antes de cargar. Un silver vacío no permite comprobar esta procedencia y tampoco se carga al warehouse.

El reemplazo elimina las vistas staging dependientes mediante `CASCADE`. Los marts existentes pueden seguir mostrando la carga anterior hasta ejecutar `make dbt-run`; después de cada carga ejecuta también `make dbt-test`.

## ES — Staging

Modelos staging:

- `stg_customers`
- `stg_products`
- `stg_inventory`
- `stg_orders`
- `stg_order_items`
- `stg_payments`

Responsabilidades:

- leer desde `source('warehouse_source', ...)`.
- seleccionar columnas explícitas.
- aplicar casts.
- conservar metadatos de ingesta y calidad.
- no hacer joins.
- no agregar métricas.

## ES — Marts

Modelos marts:

- `dim_customer`
- `dim_product`
- `dim_date`
- `fact_sales`
- `fact_inventory`

`dim_customer` tiene una fila por `customer_id`.

`dim_product` tiene una fila por `product_id`.

`dim_date` contiene días entre la fecha mínima y máxima de pedidos.

`fact_sales` une líneas de pedido, pedidos y pagos. Su grano es:

```text
una fila por order_item_id
```

`fact_inventory` une inventario con productos. Su grano es:

```text
una fila por product_id
```

## ES — `synthetic_behavior_segment`

`synthetic_behavior_segment` es una etiqueta sintética usada por el generador para simular patrones de clientes. No representa un segmento real de negocio ni un resultado analítico.

En v1.0 se conserva como campo auxiliar de trazabilidad del dataset sintético. No debe usarse como target de ML ni como conclusión de negocio.

Tampoco debe utilizarse como feature en RFM/K-means: es una etiqueta de generación reservada para auditoría o validación sintética.

## ES — Tests dbt

Los tests validan:

- claves únicas.
- campos no nulos.
- dominios permitidos.
- relaciones entre hechos y dimensiones.

Estos tests protegen el contrato analítico. La separación de rejected records pertenece a la fase de calidad en Python.

## ES — Limitaciones

- carga full refresh / replace.
- sin histórico de snapshots en warehouse.
- sin SCD.
- sin claves sustitutas.
- sin capa semántica de métricas.
- modelo analítico inicial y deliberadamente pequeño.

---

## EN — Objective

The warehouse layer moves approved data from `silver` to PostgreSQL and uses dbt to build an initial analytical model.

Python moves validated data. dbt models analytical tables, dependencies, documentation and tests.

## EN — Silver and `warehouse_source`

`silver` is the quality-approved Parquet output. `warehouse_source` is the PostgreSQL schema that exposes those tables to dbt.

```text
data/silver/postgres/<table>/load_date=YYYY-MM-DD/<table>.parquet
                              │
                              ▼
warehouse_source.<table>
```

The v1.0 load uses full replace. It does not load rejected or audit data into the analytical model.

Before starting the transaction, the loader checks that the latest quality run for the `load_date` approved every table. It also verifies each silver file's row count and `quality_run_id`. If the audit is missing, the run failed, or a file belongs to another run, it leaves the warehouse unchanged.

The loader also compares the `ingestion_id` in silver with the current bronze partition. If bronze has been ingested again for the same date, quality validation must be rerun before loading. An empty silver file cannot prove this lineage and is not loaded into the warehouse.

Replacement drops dependent staging views through `CASCADE`. Existing marts may still show the previous load until `make dbt-run` runs; follow every load with `make dbt-test` as well.

## EN — Staging

Staging models:

- `stg_customers`
- `stg_products`
- `stg_inventory`
- `stg_orders`
- `stg_order_items`
- `stg_payments`

Responsibilities:

- read from `source('warehouse_source', ...)`.
- select explicit columns.
- apply casts.
- preserve ingestion and quality metadata.
- avoid joins.
- avoid metric aggregation.

## EN — Marts

Mart models:

- `dim_customer`
- `dim_product`
- `dim_date`
- `fact_sales`
- `fact_inventory`

`dim_customer` has one row per `customer_id`.

`dim_product` has one row per `product_id`.

`dim_date` contains days between the minimum and maximum order dates.

`fact_sales` joins order items, orders and payments. Its grain is:

```text
one row per order_item_id
```

`fact_inventory` joins inventory and products. Its grain is:

```text
one row per product_id
```

## EN — `synthetic_behavior_segment`

`synthetic_behavior_segment` is a synthetic label used by the generator to simulate customer patterns. It is not a real business segment or an analytical result.

In v1.0 it is kept as an auxiliary traceability field for the synthetic dataset. It should not be used as an ML target or business conclusion.

It must not be used as an RFM/K-means feature either: it is a generation label reserved for synthetic auditing or validation.

## EN — dbt tests

Tests validate:

- unique keys.
- non-null fields.
- accepted domains.
- relationships between facts and dimensions.

These tests protect the analytical contract. Rejected-record separation belongs to the Python quality layer.

## EN — Limitations

- full refresh / replace load.
- no warehouse snapshot history.
- no SCD.
- no surrogate keys.
- no semantic metrics layer.
- initial and deliberately small analytical model.
