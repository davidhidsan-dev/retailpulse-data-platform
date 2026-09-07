# Data Dictionary / Diccionario de Datos

## ES — Objetivo

Este diccionario resume las entidades, campos principales y columnas técnicas usadas en RetailPulse v1.0.

El proyecto usa datos sintéticos, por lo que las descripciones explican el contrato técnico y analítico del pipeline, no una fuente comercial real.

## ES — Tablas fuente

### `customers`

| Campo | Descripción | Capa |
|---|---|---|
| `customer_id` | Identificador técnico del cliente. | source / lake / warehouse / dbt |
| `first_name` | Nombre sintético del cliente. | source / lake / warehouse / dbt |
| `last_name` | Apellido sintético del cliente. | source / lake / warehouse / dbt |
| `email` | Email sintético único del cliente. | source / lake / warehouse / dbt |
| `country` | País asociado al cliente. | source / lake / warehouse / dbt |
| `city` | Ciudad asociada al cliente. | source / lake / warehouse / dbt |
| `synthetic_behavior_segment` | Etiqueta sintética usada por el generador para simular patrones. No es un segmento real de negocio. | source / lake / warehouse / dbt |
| `created_at` | Fecha de creación del cliente. | source / lake / warehouse / dbt |

### `products`

| Campo | Descripción | Capa |
|---|---|---|
| `product_id` | Identificador técnico del producto. | source / lake / warehouse / dbt |
| `sku` | Identificador comercial único del producto. | source / lake / warehouse / dbt |
| `product_name` | Nombre sintético del producto. | source / lake / warehouse / dbt |
| `category` | Categoría del producto. | source / lake / warehouse / dbt |
| `unit_price` | Precio vigente del producto en el catálogo sintético. | source / lake / warehouse / dbt |
| `created_at` | Fecha de creación del producto. | source / lake / warehouse / dbt |

### `inventory`

| Campo | Descripción | Capa |
|---|---|---|
| `product_id` | Producto al que pertenece el registro de inventario. | source / lake / warehouse / dbt |
| `stock_quantity` | Unidades disponibles. | source / lake / warehouse / dbt |
| `reorder_level` | Umbral de reposición. | source / lake / warehouse / dbt |
| `updated_at` | Fecha de actualización del inventario. | source / lake / warehouse / dbt |

### `orders`

| Campo | Descripción | Capa |
|---|---|---|
| `order_id` | Identificador técnico del pedido. | source / lake / warehouse / dbt |
| `customer_id` | Cliente asociado al pedido. | source / lake / warehouse / dbt |
| `order_date` | Fecha del pedido. | source / lake / warehouse / dbt |
| `order_status` | Estado del pedido. Valores esperados: `completed`, `cancelled`, `refunded`, `pending`. | source / lake / warehouse / dbt |
| `country` | País asociado al pedido. | source / lake / warehouse / dbt |

### `order_items`

| Campo | Descripción | Capa |
|---|---|---|
| `order_item_id` | Identificador técnico de la línea de pedido. | source / lake / warehouse / dbt |
| `order_id` | Pedido al que pertenece la línea. | source / lake / warehouse / dbt |
| `product_id` | Producto vendido en la línea. | source / lake / warehouse / dbt |
| `quantity` | Número de unidades vendidas. | source / lake / warehouse / dbt |
| `unit_price` | Precio unitario aplicado a la línea. | source / lake / warehouse / dbt |
| `line_total` | Total de línea, calculado como `quantity * unit_price`. | source / lake / warehouse / dbt |

### `payments`

| Campo | Descripción | Capa |
|---|---|---|
| `payment_id` | Identificador técnico del pago. | source / lake / warehouse / dbt |
| `order_id` | Pedido asociado al pago. | source / lake / warehouse / dbt |
| `payment_method` | Método de pago. Valores esperados: `credit_card`, `paypal`, `bank_transfer`, `gift_card`. | source / lake / warehouse / dbt |
| `payment_status` | Estado del pago. Valores esperados: `paid`, `failed`, `refunded`, `pending`. | source / lake / warehouse / dbt |
| `payment_amount` | Importe del pago. | source / lake / warehouse / dbt |
| `payment_date` | Fecha del pago. | source / lake / warehouse / dbt |

## ES — Columnas técnicas de bronze

| Campo | Descripción |
|---|---|
| `ingestion_id` | UUID común a las tablas ingeridas en una misma ejecución. |
| `ingested_at` | Timestamp UTC de ingesta. |
| `source_system` | Sistema origen. En v1.0: `postgres`. |
| `source_table` | Nombre de la tabla fuente original. |

## ES — Columnas técnicas de silver

| Campo | Descripción |
|---|---|
| `quality_run_id` | UUID de la validación de calidad. |
| `quality_checked_at` | Timestamp UTC de validación. |

Silver conserva también las columnas técnicas de bronze.

## ES — Columnas técnicas de rejected

| Campo | Descripción |
|---|---|
| `quality_run_id` | UUID de la validación de calidad. |
| `quality_checked_at` | Timestamp UTC de validación. |
| `rejection_reason` | Motivo o motivos por los que la fila fue rechazada. |

Rejected conserva las columnas originales y las columnas técnicas necesarias para trazabilidad.

## ES — Audit

Archivo:

```text
data/audit/quality_runs.parquet
```

| Campo | Descripción |
|---|---|
| `quality_run_id` | UUID de la ejecución de calidad. |
| `quality_checked_at` | Timestamp UTC de validación. |
| `load_date` | Fecha lógica de carga validada. |
| `source_system` | Sistema origen. |
| `table_name` | Tabla validada. |
| `input_rows` | Filas de entrada. |
| `valid_rows` | Filas válidas. |
| `rejected_rows` | Filas rechazadas. |
| `status` | `passed`, `warning` o `failed`. |

## ES — Marts dbt

| Modelo | Grano | Descripción |
|---|---|---|
| `dim_customer` | una fila por `customer_id` | Dimensión inicial de clientes. |
| `dim_product` | una fila por `product_id` | Dimensión inicial de productos. |
| `dim_date` | una fila por día | Dimensión calendario basada en fechas de pedido. |
| `fact_sales` | una fila por `order_item_id` | Hecho de ventas a nivel línea de pedido. |
| `fact_inventory` | una fila por `product_id` | Snapshot analítico de inventario por producto. |

## ES — Nota sobre tipos

Los modelos `stg_*` de dbt aplican casts explícitos para definir el contrato analítico. Los tipos concretos pueden depender de PostgreSQL y dbt, pero el objetivo es mantener IDs numéricos, importes numéricos, timestamps tipados y campos descriptivos como texto.

---

## EN — Objective

This dictionary summarizes the main entities, fields and technical columns used in RetailPulse v1.0.

The project uses synthetic data, so descriptions explain the technical and analytical contract of the pipeline, not a real commercial source.

## EN — Source tables

### `customers`

| Field | Description | Layer |
|---|---|---|
| `customer_id` | Technical customer identifier. | source / lake / warehouse / dbt |
| `first_name` | Synthetic customer first name. | source / lake / warehouse / dbt |
| `last_name` | Synthetic customer last name. | source / lake / warehouse / dbt |
| `email` | Unique synthetic customer email. | source / lake / warehouse / dbt |
| `country` | Customer country. | source / lake / warehouse / dbt |
| `city` | Customer city. | source / lake / warehouse / dbt |
| `synthetic_behavior_segment` | Synthetic generator label used to simulate patterns. It is not a real business segment. | source / lake / warehouse / dbt |
| `created_at` | Customer creation date. | source / lake / warehouse / dbt |

### `products`

| Field | Description | Layer |
|---|---|---|
| `product_id` | Technical product identifier. | source / lake / warehouse / dbt |
| `sku` | Unique commercial product identifier. | source / lake / warehouse / dbt |
| `product_name` | Synthetic product name. | source / lake / warehouse / dbt |
| `category` | Product category. | source / lake / warehouse / dbt |
| `unit_price` | Current product price in the synthetic catalog. | source / lake / warehouse / dbt |
| `created_at` | Product creation date. | source / lake / warehouse / dbt |

### `inventory`

| Field | Description | Layer |
|---|---|---|
| `product_id` | Product linked to the inventory record. | source / lake / warehouse / dbt |
| `stock_quantity` | Available units. | source / lake / warehouse / dbt |
| `reorder_level` | Reorder threshold. | source / lake / warehouse / dbt |
| `updated_at` | Inventory update date. | source / lake / warehouse / dbt |

### `orders`

| Field | Description | Layer |
|---|---|---|
| `order_id` | Technical order identifier. | source / lake / warehouse / dbt |
| `customer_id` | Customer linked to the order. | source / lake / warehouse / dbt |
| `order_date` | Order date. | source / lake / warehouse / dbt |
| `order_status` | Order status. Expected values: `completed`, `cancelled`, `refunded`, `pending`. | source / lake / warehouse / dbt |
| `country` | Order country. | source / lake / warehouse / dbt |

### `order_items`

| Field | Description | Layer |
|---|---|---|
| `order_item_id` | Technical order-line identifier. | source / lake / warehouse / dbt |
| `order_id` | Parent order. | source / lake / warehouse / dbt |
| `product_id` | Product sold in the line. | source / lake / warehouse / dbt |
| `quantity` | Number of units sold. | source / lake / warehouse / dbt |
| `unit_price` | Unit price applied to the line. | source / lake / warehouse / dbt |
| `line_total` | Line total, calculated as `quantity * unit_price`. | source / lake / warehouse / dbt |

### `payments`

| Field | Description | Layer |
|---|---|---|
| `payment_id` | Technical payment identifier. | source / lake / warehouse / dbt |
| `order_id` | Order linked to the payment. | source / lake / warehouse / dbt |
| `payment_method` | Payment method. Expected values: `credit_card`, `paypal`, `bank_transfer`, `gift_card`. | source / lake / warehouse / dbt |
| `payment_status` | Payment status. Expected values: `paid`, `failed`, `refunded`, `pending`. | source / lake / warehouse / dbt |
| `payment_amount` | Payment amount. | source / lake / warehouse / dbt |
| `payment_date` | Payment date. | source / lake / warehouse / dbt |

## EN — Bronze technical columns

| Field | Description |
|---|---|
| `ingestion_id` | UUID shared by tables ingested in the same run. |
| `ingested_at` | UTC ingestion timestamp. |
| `source_system` | Source system. In v1.0: `postgres`. |
| `source_table` | Original source table name. |

## EN — Silver technical columns

| Field | Description |
|---|---|
| `quality_run_id` | Quality validation UUID. |
| `quality_checked_at` | UTC validation timestamp. |

Silver also keeps bronze technical columns.

## EN — Rejected technical columns

| Field | Description |
|---|---|
| `quality_run_id` | Quality validation UUID. |
| `quality_checked_at` | UTC validation timestamp. |
| `rejection_reason` | Reason or reasons why the row was rejected. |

Rejected keeps original columns and the technical fields needed for traceability.

## EN — Audit

File:

```text
data/audit/quality_runs.parquet
```

| Field | Description |
|---|---|
| `quality_run_id` | Quality run UUID. |
| `quality_checked_at` | UTC validation timestamp. |
| `load_date` | Validated logical load date. |
| `source_system` | Source system. |
| `table_name` | Validated table. |
| `input_rows` | Input rows. |
| `valid_rows` | Valid rows. |
| `rejected_rows` | Rejected rows. |
| `status` | `passed`, `warning` or `failed`. |

## EN — dbt marts

| Model | Grain | Description |
|---|---|---|
| `dim_customer` | one row per `customer_id` | Initial customer dimension. |
| `dim_product` | one row per `product_id` | Initial product dimension. |
| `dim_date` | one row per day | Calendar dimension based on order dates. |
| `fact_sales` | one row per `order_item_id` | Sales fact at order-line level. |
| `fact_inventory` | one row per `product_id` | Analytical inventory snapshot by product. |

## EN — Type note

dbt `stg_*` models apply explicit casts to define the analytical contract. Concrete types depend on PostgreSQL and dbt, but the goal is to keep numeric IDs, numeric amounts, typed timestamps and descriptive fields as text.
