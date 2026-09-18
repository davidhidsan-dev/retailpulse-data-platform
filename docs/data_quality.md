# Data Quality / Calidad de Datos

## ES — Objetivo

La capa de calidad valida snapshots bronze antes de que puedan alimentar modelos analíticos.

El proceso separa registros válidos e inválidos:

```text
bronze
  ├── silver
  ├── rejected
  └── audit
```

## ES — Bronze, silver y rejected

- `bronze`: datos ingeridos desde PostgreSQL con metadatos de ingesta.
- `silver`: registros que superan todas las reglas de calidad.
- `rejected`: registros inválidos con motivo de rechazo.
- `audit`: resumen de ejecución por tabla.

Silver y rejected no modifican los archivos bronze originales.

## ES — Reglas por tabla

| Tabla | Controles principales |
|---|---|
| `customers` | `customer_id` presente y único; email presente y válido; país y fecha presentes; segmento sintético permitido. |
| `products` | `product_id` y `sku` presentes y únicos; nombre, categoría y fecha presentes; precio positivo. |
| `inventory` | `product_id` presente y único; stock y reorder level no negativos; fecha de actualización presente. |
| `orders` | `order_id` presente y único; cliente, fecha y país presentes; estado permitido. |
| `order_items` | `order_item_id` presente y único; pedido y producto presentes; cantidad y precio positivos; total de línea consistente. |
| `payments` | `payment_id` presente y único; un pago por pedido; método y estado permitidos; importe no negativo; fecha presente. |

## ES — Validaciones referenciales

El proceso comprueba:

- `orders.customer_id` contra `customers.customer_id`.
- `inventory.product_id` contra `products.product_id`.
- `order_items.order_id` contra `orders.order_id`.
- `order_items.product_id` contra `products.product_id`.
- `payments.order_id` contra `orders.order_id`.

Las referencias se comparan contra las tablas bronze completas de la misma partición. Una fila padre rechazada por otra regla no provoca rechazo en cascada de las hijas.

## ES — Salidas

Silver:

```text
data/silver/postgres/<table>/load_date=YYYY-MM-DD/<table>.parquet
```

Rejected:

```text
data/rejected/postgres/<table>/load_date=YYYY-MM-DD/<table>_rejected.parquet
```

Audit:

```text
data/audit/quality_runs.parquet
```

## ES — Columnas técnicas

Silver añade:

- `quality_run_id`
- `quality_checked_at`

Rejected añade además:

- `rejection_reason`

Si una fila incumple varias reglas, los motivos se concatenan con `; `.

## ES — Auditoría

`quality_runs.parquet` contiene una fila por tabla y ejecución con:

- `quality_run_id`
- `quality_checked_at`
- `load_date`
- `source_system`
- `table_name`
- `input_rows`
- `valid_rows`
- `rejected_rows`
- `status`

Estados:

- `passed`: no hay rejected.
- `warning`: hay rejected.
- `failed`: error de validación o una tabla sin filas válidas.

## ES — Publicación de silver

La validación se detiene si alguna tabla no produce filas válidas, incluso si bronze
estaba vacía. Comprueba todas las tablas antes de publicar archivos silver y
registra `failed` en audit para la tabla que bloqueó la ejecución. Conserva sus
registros rechazados para diagnóstico.

La opción `--allow-empty` permite omitir este control en demostraciones
controladas. `make quality-demo` la utiliza porque un dataset pequeño puede
tener una única fila por tabla y rechazarla deliberadamente.

## ES — Ejecución

```bash
make quality
python -m src.quality.validate_bronze --load-date YYYY-MM-DD
```

## ES — Modo demo

La fuente sintética normal suele producir cero rejected porque está generada de forma consistente. Eso significa que la fuente pasó las reglas.

Para demostrar rejected records:

```bash
make quality-demo SOURCE_LOAD_DATE=YYYY-MM-DD
```

La demo copia una partición bronze existente y altera de forma determinista una fila por tabla.

Anomalías demo:

| Tabla | Anomalía |
|---|---|
| `customers` | email inválido |
| `products` | precio negativo |
| `inventory` | stock negativo |
| `orders` | FK de cliente inexistente |
| `order_items` | total de línea inconsistente |
| `payments` | estado de pago no permitido |

## ES — Por qué rejected no entra a marts

Rejected conserva datos conocidos como inválidos. Si se cargaran al modelo analítico, podrían contaminar ventas, inventario, relaciones y métricas.

Por eso:

```text
silver   -> warehouse_source -> dbt -> marts
rejected -> diagnóstico / auditoría / reproceso
```

## ES — Limitaciones

- Reglas deterministas y locales.
- No mide frescura con SLA.
- No compara todavía `payment_amount` contra suma de líneas.
- Audit es archivo local, no sistema productivo de observabilidad.
- No hay flujo automático de corrección/reproceso de rejected.

---

## EN — Objective

The quality layer validates bronze snapshots before they can feed analytical models.

The process separates valid and invalid records:

```text
bronze
  ├── silver
  ├── rejected
  └── audit
```

## EN — Bronze, silver and rejected

- `bronze`: data ingested from PostgreSQL with ingestion metadata.
- `silver`: records that pass all quality rules.
- `rejected`: invalid records with rejection reasons.
- `audit`: run summary by table.

Silver and rejected do not modify the original bronze files.

## EN — Rules by table

| Table | Main checks |
|---|---|
| `customers` | `customer_id` present and unique; email present and valid; country and date present; allowed synthetic segment. |
| `products` | `product_id` and `sku` present and unique; name, category and date present; positive price. |
| `inventory` | `product_id` present and unique; non-negative stock and reorder level; update date present. |
| `orders` | `order_id` present and unique; customer, date and country present; accepted status. |
| `order_items` | `order_item_id` present and unique; order and product present; positive quantity and price; consistent line total. |
| `payments` | `payment_id` present and unique; one payment per order; accepted method and status; non-negative amount; date present. |

## EN — Referential checks

The process checks:

- `orders.customer_id` against `customers.customer_id`.
- `inventory.product_id` against `products.product_id`.
- `order_items.order_id` against `orders.order_id`.
- `order_items.product_id` against `products.product_id`.
- `payments.order_id` against `orders.order_id`.

References are checked against the full bronze tables for the same partition. A parent row rejected by another rule does not trigger cascading rejection of child rows.

## EN — Outputs

Silver:

```text
data/silver/postgres/<table>/load_date=YYYY-MM-DD/<table>.parquet
```

Rejected:

```text
data/rejected/postgres/<table>/load_date=YYYY-MM-DD/<table>_rejected.parquet
```

Audit:

```text
data/audit/quality_runs.parquet
```

## EN — Technical columns

Silver adds:

- `quality_run_id`
- `quality_checked_at`

Rejected also adds:

- `rejection_reason`

If a row fails several rules, reasons are joined with `; `.

## EN — Audit

`quality_runs.parquet` contains one row per table and run with:

- `quality_run_id`
- `quality_checked_at`
- `load_date`
- `source_system`
- `table_name`
- `input_rows`
- `valid_rows`
- `rejected_rows`
- `status`

Statuses:

- `passed`: no rejected rows.
- `warning`: rejected rows exist.
- `failed`: validation error or a table with no valid rows.

## EN — Silver publication

Validation stops when any table has no valid rows, including an empty bronze
input. It checks every table before publishing silver files and records `failed`
for the table that blocked the run. Its rejected records remain available for
diagnosis.

The `--allow-empty` option bypasses this check for controlled demonstrations.
`make quality-demo` uses it because a small dataset can have just one row per
table and deliberately reject it.

## EN — Execution

```bash
make quality
python -m src.quality.validate_bronze --load-date YYYY-MM-DD
```

## EN — Demo mode

The normal synthetic source usually produces zero rejected records because it is generated consistently. That means the source passed the rules.

To demonstrate rejected records:

```bash
make quality-demo SOURCE_LOAD_DATE=YYYY-MM-DD
```

The demo copies an existing bronze partition and deterministically modifies one row per table.

Demo anomalies:

| Table | Anomaly |
|---|---|
| `customers` | invalid email |
| `products` | negative price |
| `inventory` | negative stock |
| `orders` | nonexistent customer FK |
| `order_items` | inconsistent line total |
| `payments` | unsupported payment status |

## EN — Why rejected does not enter marts

Rejected contains data known to be invalid. Loading it into the analytical model could contaminate sales, inventory, relationships and metrics.

Therefore:

```text
silver   -> warehouse_source -> dbt -> marts
rejected -> diagnosis / audit / reprocessing
```

## EN — Limitations

- Deterministic local rules.
- No freshness SLA.
- `payment_amount` is not yet compared against order-line totals.
- Audit is a local file, not production observability.
- No automatic correction/reprocessing flow for rejected records.
