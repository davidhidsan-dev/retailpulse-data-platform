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

## ES — Contrato técnico de bronze

Cada tabla bronze debe incluir `ingestion_id`, `ingested_at`, `source_system` y
`source_table`. La validación exige un único `ingestion_id` de texto no vacío,
un `ingested_at` válido, `source_system="postgres"` y un `source_table` que
coincida con la tabla procesada. Las seis tablas no vacías de una partición
deben compartir el mismo `ingestion_id`.

La ausencia o incoherencia de estos metadatos bloquea toda la publicación de
silver porque impide demostrar la procedencia del snapshot. En cambio, los
errores propios de un registro se conservan en rejected: los IDs deben poder
representarse como enteros positivos y las fechas no admiten números
arbitrarios interpretados como timestamps.

Los IDs técnicos deben ser enteros reales entre `1` y `2147483647`. Valores
textuales como `"01"` o `"1.0"` se rechazan; los códigos que necesitan conservar
formato se almacenan como texto, como ocurre con `sku`.

## ES — Reglas por tabla

| Tabla | Controles principales |
|---|---|
| `customers` | `customer_id` entero positivo, presente y único; nombre, apellido y ciudad presentes; email presente, válido y único; país no vacío; fecha válida; segmento sintético permitido. |
| `products` | `product_id` entero positivo, presente y único; `sku` presente y único; SKU, nombre y categoría no vacíos; fecha válida; precio positivo y finito. |
| `inventory` | `product_id` entero positivo, presente y único; stock y reorder level enteros, no negativos y finitos; fecha de actualización válida. |
| `orders` | `order_id` y `customer_id` enteros positivos y presentes; `order_id` único; fecha válida; país no vacío; estado permitido. |
| `order_items` | IDs enteros positivos y presentes; `order_item_id` único; cantidad entera positiva; precio positivo y finito; total de línea consistente y finito. |
| `payments` | `payment_id` y `order_id` enteros positivos y presentes; `payment_id` único; un pago por pedido; método y estado permitidos; importe no negativo y finito; fecha válida; importe y estado coherentes con el pedido. |

## ES — Validaciones referenciales

El proceso comprueba:

- `orders.customer_id` contra `customers.customer_id`.
- `inventory.product_id` contra `products.product_id`.
- `order_items.order_id` contra `orders.order_id`.
- `order_items.product_id` contra `products.product_id`.
- `payments.order_id` contra `orders.order_id`.

Las referencias se comparan contra las filas padre aceptadas para silver, en orden de dependencias. Si una fila padre se rechaza, las hijas que la referencian también se rechazan.

## ES — Coherencia completa del pedido

`orders`, `order_items` y `payments` se publican como una unidad. Cada pedido debe tener al menos una línea válida y exactamente un pago válido. El importe del pago debe coincidir con la suma de `line_total`, y los estados deben respetar `completed → paid`, `cancelled → failed`, `refunded → refunded` y `pending → pending`.

Si una línea o el pago se rechazan, la cabecera, todas las líneas y el pago de ese pedido se trasladan a rejected. Los productos y clientes válidos no se rechazan por esta cascada. Así, silver nunca contiene un pedido parcial que pueda infravalorar `fact_sales`.

La API de validación exige seleccionar juntas las tres tablas del pedido. Una selección parcial se detiene antes de leer o publicar archivos para impedir que se omita esta política.

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
- `rejection_rate`
- `status`

Estados:

- `passed`: no hay rejected.
- `warning`: hay rejected.
- `failed`: error de validación, una tabla sin filas válidas o demasiados rechazos.

## ES — Publicación de silver

Antes de validar filas se comprueba el contrato técnico completo de bronze. Una
tabla con metadatos ausentes, nulos o incoherentes se marca como `failed` y no
publica silver. Cada tabla no vacía debe tener un solo `ingestion_id`, compartido
por todas las tablas de la partición. Así, una ingesta interrumpida que mezcle
snapshots de dos intentos también bloquea la publicación.

La validación se detiene si alguna tabla no produce filas válidas, incluso si bronze
estaba vacía. Comprueba todas las tablas antes de publicar archivos silver y
registra `failed` en audit para la tabla que bloqueó la ejecución. Conserva sus
registros rechazados para diagnóstico.

También bloquea la publicación cuando más del 10 % de una tabla se rechaza. El
límite se puede ajustar entre 0 y 1 con `--max-rejection-rate`; debe responder a
una decisión de negocio antes de cambiarse.

Los controles de tabla vacía y tasa de rechazo se calculan después de aplicar la
política de pedido completo, antes de escribir ningún archivo silver.

Las opciones `--allow-empty` y `--max-rejection-rate 1` permiten omitir ambos
controles en demostraciones controladas. `make quality-demo` las utiliza porque
un dataset pequeño puede tener una única fila por tabla y rechazarla
deliberadamente.

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
El rechazo en cascada puede producir más de una fila rechazada por tabla.

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

## EN — Bronze technical contract

Every bronze table must include `ingestion_id`, `ingested_at`, `source_system`
and `source_table`. Validation requires one non-empty string `ingestion_id`, a
valid `ingested_at`, `source_system="postgres"`, and a `source_table` matching
the table being processed. All non-empty tables in a partition must share the
same `ingestion_id`.

Missing or inconsistent metadata blocks the complete silver publication because
the snapshot provenance cannot be trusted. Record-level errors are instead
preserved in rejected: IDs must represent positive integers, and date fields do
not accept arbitrary numbers interpreted as timestamps.

Technical IDs must be actual integers between `1` and `2147483647`. Text values
such as `"01"` or `"1.0"` are rejected; codes that must preserve formatting are
stored as text, as with `sku`.

## EN — Rules by table

| Table | Main checks |
|---|---|
| `customers` | Positive integer, present and unique `customer_id`; first name, last name and city present; email present, valid and unique; non-blank country; valid date; allowed synthetic segment. |
| `products` | Positive integer, present and unique `product_id`; present and unique `sku`; non-blank SKU, name and category; valid date; positive finite price. |
| `inventory` | Positive integer, present and unique `product_id`; finite, non-negative integer stock and reorder level; valid update date. |
| `orders` | Positive integer, present `order_id` and `customer_id`; unique `order_id`; valid date; non-blank country; accepted status. |
| `order_items` | Positive integer, present IDs; unique `order_item_id`; positive integer quantity; positive finite price; consistent finite line total. |
| `payments` | Positive integer, present `payment_id` and `order_id`; unique `payment_id`; one payment per order; accepted method and status; finite non-negative amount; valid date; amount and status consistent with the order. |

## EN — Referential checks

The process checks:

- `orders.customer_id` against `customers.customer_id`.
- `inventory.product_id` against `products.product_id`.
- `order_items.order_id` against `orders.order_id`.
- `order_items.product_id` against `products.product_id`.
- `payments.order_id` against `orders.order_id`.

References are checked against parent rows accepted for silver, in dependency order. When a parent row is rejected, child rows that reference it are rejected too.

## EN — Complete order consistency

`orders`, `order_items` and `payments` are published as one unit. Every order must have at least one valid item and exactly one valid payment. The payment amount must equal the sum of `line_total`, and statuses must follow `completed → paid`, `cancelled → failed`, `refunded → refunded` and `pending → pending`.

If an item or payment is rejected, the order header, all its items and its payment are moved to rejected. Valid products and customers are not rejected by this cascade. This prevents silver from containing a partial order that could understate `fact_sales`.

The validation API requires all three order tables to be selected together. A partial selection stops before reading or publishing files so this policy cannot be bypassed.

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
- `rejection_rate`
- `status`

Statuses:

- `passed`: no rejected rows.
- `warning`: rejected rows exist.
- `failed`: validation error, a table with no valid rows or too many rejected rows.

## EN — Silver publication

Before validating rows, the complete bronze technical contract is checked. A
table with missing, null or inconsistent metadata is marked as `failed` and does
not publish silver. Every non-empty table must have one `ingestion_id`, shared by
all tables in the partition. This also stops a partial ingestion from mixing
snapshots from different attempts.

Validation stops when any table has no valid rows, including an empty bronze
input. It checks every table before publishing silver files and records `failed`
for the table that blocked the run. Its rejected records remain available for
diagnosis.

It also blocks publication when more than 10% of a table is rejected. The limit
can be adjusted between 0 and 1 with `--max-rejection-rate`; changing it should
follow an explicit business decision.

The empty-table and rejection-rate checks run after the complete-order policy
and before any silver file is written.

The `--allow-empty` and `--max-rejection-rate 1` options bypass both checks for
controlled demonstrations. `make quality-demo` uses them because a small
dataset can have just one row per table and deliberately reject it.

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
Cascading rejection can produce more than one rejected row per table.

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
- Audit is a local file, not production observability.
- No automatic correction/reprocessing flow for rejected records.
