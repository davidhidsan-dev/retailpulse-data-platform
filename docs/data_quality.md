# Calidad de datos — Fase 3

## Objetivo

La Fase 3 valida los snapshots Parquet de bronze antes de que puedan alimentar modelos analíticos. El proceso conserva los registros válidos en silver, aísla los inválidos para su diagnóstico y registra métricas por tabla en una auditoría persistente.

## Bronze, silver y rejected

- **Bronze** conserva la copia ingerida desde PostgreSQL y sus metadatos técnicos.
- **Silver** contiene únicamente filas que superan todas las reglas aplicables.
- **Rejected** conserva cada fila inválida y explica por qué fue rechazada. Se escribe un archivo por tabla incluso cuando contiene cero filas.

Silver y rejected no modifican los archivos bronze de entrada.

## Reglas por tabla

| Tabla | Controles principales |
|---|---|
| `customers` | `customer_id` presente y único; email presente y con formato básico; país y fecha presentes; segmento sintético permitido. |
| `products` | `product_id` y `sku` presentes y únicos; nombre, categoría y fecha presentes; precio positivo. |
| `inventory` | `product_id` presente y único; stock y nivel de reposición no negativos; fecha de actualización presente. |
| `orders` | `order_id` presente y único; cliente, fecha y país presentes; estado permitido. |
| `order_items` | `order_item_id` presente y único; pedido y producto presentes; cantidad y precio positivos; total de línea consistente con tolerancia de medio céntimo. |
| `payments` | `payment_id` presente y único; un pago por pedido; método y estado permitidos; importe no negativo; fecha presente. |

Cuando una fila incumple varias reglas, `rejection_reason` concatena los motivos mediante `; `.

## Validaciones referenciales

El proceso carga las tablas bronze de la partición y comprueba:

- `orders.customer_id` contra `customers.customer_id`;
- `inventory.product_id` contra `products.product_id`;
- `order_items.order_id` contra `orders.order_id`;
- `order_items.product_id` contra `products.product_id`;
- `payments.order_id` contra `orders.order_id`.

Las referencias se comparan con la entrada bronze completa. Una fila padre rechazada por otra regla no provoca rechazos en cascada de sus filas hijas.

## Rutas y columnas técnicas

Entrada:

```text
data/bronze/postgres/<table>/load_date=YYYY-MM-DD/<table>.parquet
```

Salidas:

```text
data/silver/postgres/<table>/load_date=YYYY-MM-DD/<table>.parquet
data/rejected/postgres/<table>/load_date=YYYY-MM-DD/<table>_rejected.parquet
data/audit/quality_runs.parquet
```

Silver conserva todas las columnas bronze y añade `quality_run_id` y `quality_checked_at`. Rejected añade además `rejection_reason`. El identificador y el timestamp UTC son comunes a las seis tablas de una ejecución.

## Auditoría

`quality_runs.parquet` añade una fila por tabla y ejecución con:

- identificador y timestamp del control;
- fecha de carga, sistema fuente y tabla;
- filas de entrada, válidas y rechazadas;
- estado `passed`, `warning` o `failed`.

`passed` significa que no hubo rechazos, `warning` que al menos una fila fue rechazada y `failed` que la validación de una tabla produjo un error. El historial se amplía en cada ejecución y no contiene los datos de negocio.

## Ejecución

```bash
make quality
python -m src.quality.validate_bronze --load-date 2026-08-21
```

Sin `--load-date` se usa la fecha UTC actual.

## Modo demo de rejected records

El generador sintético principal crea relaciones y valores consistentes. Por eso una ejecución normal puede terminar con cero rejected records: significa que la fuente simulada ha superado las reglas, no que el control de calidad esté inactivo.

Para enseñar el circuito de rechazo existe una utilidad opcional que copia una partición bronze y altera de forma determinista una fila por tabla:

| Tabla | Anomalía de demostración |
|---|---|
| `customers` | Email con valor `invalid-email`. |
| `products` | `unit_price` con valor `-10`. |
| `inventory` | `stock_quantity` con valor `-5`. |
| `orders` | `customer_id=999999999`, inexistente en customers. |
| `order_items` | `line_total` distinto de `quantity * unit_price`. |
| `payments` | `payment_status=unknown_status`. |

Las anomalías no son aleatorias y siempre se aplican a la primera fila. El script exige fechas de origen y destino diferentes, conserva las columnas y metadatos bronze, y nunca modifica la partición fuente.

Ejecución directa:

```bash
python -m src.quality.create_bad_bronze_demo \
  --source-load-date 2026-09-01 \
  --demo-load-date 2099-01-01
python -m src.quality.validate_bronze --load-date 2099-01-01
```

Ejecución mediante Make:

```bash
make quality-demo SOURCE_LOAD_DATE=2026-09-01
make quality-demo SOURCE_LOAD_DATE=2026-09-01 DEMO_LOAD_DATE=2099-01-02
```

`SOURCE_LOAD_DATE` es obligatorio porque debe identificar una partición bronze local existente; `DEMO_LOAD_DATE` usa `2099-01-01` por defecto. La demo no representa la fuente operacional, no debe utilizarse como dataset base y sus archivos permanecen excluidos de Git.

## Limitaciones y Fase 4

Las reglas actuales son deterministas y de alcance local: no miden frescura mediante acuerdos de servicio, no comparan pagos con el total del pedido y no gestionan concurrencia sobre el archivo de auditoría. La Fase 4 incorporará warehouse y modelado analítico con dbt; la orquestación seguirá reservada para una fase posterior.
