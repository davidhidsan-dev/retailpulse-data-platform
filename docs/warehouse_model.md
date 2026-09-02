# Warehouse y modelo analítico — Fase 4

## Objetivo

La Fase 4 lleva los datos validados desde silver a PostgreSQL y usa dbt para construir un modelo analítico inicial. La carga Python y el modelado dbt permanecen separados: Python mueve datasets aprobados; dbt expresa transformaciones, dependencias, documentación y tests analíticos.

## Silver y `warehouse_source`

Silver es la salida Parquet del control de calidad y conserva la partición local por `load_date`. `warehouse_source` es un esquema lógico dentro del PostgreSQL de desarrollo que expone esas seis tablas a dbt:

```text
data/silver/postgres/<table>/load_date=YYYY-MM-DD/<table>.parquet
                              │
                              ▼
warehouse_source.<table>
```

La carga usa reemplazo completo y transaccional en esta fase. Al reemplazar las tablas elimina las vistas dbt dependientes mediante `CASCADE`, por lo que siempre debe ir seguida de `dbt run`. No incluye rejected records ni el historial de auditoría.

## Por qué dbt

dbt mantiene las transformaciones analíticas en SQL versionado y construye el grafo entre fuentes, staging y marts mediante `source()` y `ref()`. También permite ejecutar tests declarativos sobre claves y dominios sin duplicar las reglas fila a fila que ya aplica Python antes de silver.

## Staging y marts

Los seis modelos `stg_*` son vistas simples sobre `warehouse_source`. Seleccionan y castean columnas de negocio y conservan los metadatos de ingesta y calidad para trazabilidad; no agregan métricas.

Los marts se materializan como tablas en el esquema dbt configurado, `analytics` por defecto:

- `dim_customer`: una fila por `customer_id`;
- `dim_product`: una fila por `product_id` y su `sku` comercial;
- `dim_date`: una fila por día entre la fecha mínima y máxima de pedidos;
- `fact_sales`: una fila por `order_item_id`;
- `fact_inventory`: una fila por `product_id`.

## Granos de las tablas de hechos

`fact_sales` une líneas de pedido con pedidos y pagos. Su grano es una línea de pedido, por lo que `order_item_id` debe ser único. Conserva cliente, producto, fecha, estados, método de pago, cantidad, precio y total de línea.

`fact_inventory` une el snapshot de inventario con productos. Su grano es un producto y `is_low_stock` es verdadero cuando `stock_quantity <= reorder_level`.

## Tests dbt

Los tests comprueban claves únicas y no nulas en dimensiones y hechos. `fact_sales` también valida los dominios de `order_status`, `payment_status` y `payment_method`. Estos tests protegen el contrato analítico; la detección y separación de rejected records continúa perteneciendo a la Fase 3 en Python.

## Configuración y ejecución

Crear una vez el perfil local:

```powershell
Copy-Item dbt/profiles.yml.example dbt/profiles.yml
```

Después, desde la raíz:

```powershell
make load-warehouse LOAD_DATE=2026-09-02
make dbt-run
make dbt-test
make dbt-docs-generate
```

El perfil usa las variables `POSTGRES_*` y `DBT_SCHEMA`. Incluye valores locales de ejemplo como fallback, pero `dbt/profiles.yml` y `.env` están ignorados por Git.

## Limitaciones y Fase 5

La carga es full refresh y no mantiene histórico de snapshots en PostgreSQL. No hay claves sustitutas, dimensiones lentamente cambiantes, métricas semánticas ni procesamiento incremental. La Fase 5 añadirá orquestación y operación coordinada del pipeline; Airflow no forma parte de esta fase.
