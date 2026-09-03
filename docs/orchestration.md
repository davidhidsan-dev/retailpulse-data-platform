# Orquestación con Airflow

## Objetivo de la Fase 5

Airflow coordina el pipeline batch completo de RetailPulse y permite observar el estado, duración y logs de cada paso desde una interfaz local. La implementación está pensada para desarrollo y demostración, no para producción.

Airflow no contiene transformaciones ni reglas de negocio. Cada task invoca una interfaz CLI ya existente, por lo que ingesta, calidad, carga y dbt siguen siendo independientes y testeables fuera del orquestador.

## DAG y orden de ejecución

El DAG `retailpulse_batch_pipeline` es manual, queda pausado al crearse y ejecuta una cadena lineal con un reintento por task:

1. `init_source_schema`: crea el esquema operacional.
2. `seed_source_data`: genera y carga la fuente sintética.
3. `ingest_postgres_to_lake`: extrae PostgreSQL hacia raw y bronze.
4. `validate_bronze_quality`: valida bronze y escribe silver, rejected y auditoría.
5. `load_silver_to_warehouse`: publica la partición silver en `warehouse_source`.
6. `dbt_run`: construye staging y marts.
7. `dbt_test`: valida el modelo analítico.

## Fecha de carga

Las tasks de ingesta, calidad y warehouse comparten exactamente la misma `load_date`. En un lanzamiento manual se puede indicar en la configuración del DAG:

```json
{
  "load_date": "2026-09-03"
}
```

Si no se indica, se utiliza la fecha lógica del DAG en formato `YYYY-MM-DD`. Como Airflow 3 puede no asignar fecha lógica a una ejecución exclusivamente manual, en ese caso se usa la fecha UTC de inicio del run. El valor se entrega a Bash mediante una variable de entorno templada y cada CLI vuelve a validar su formato.

## Ejecución local con Airflow

El entorno opcional usa la imagen fijada de Airflow y el modo `standalone`, suficiente para una demostración local. Se conecta al servicio PostgreSQL existente usando el hostname interno `postgres`; monta el repositorio para compartir `data/`, `dags/` y `dbt/`, y conserva los metadatos de Airflow en un volumen Docker. Las dependencias Python de RetailPulse, incluido dbt, se instalan en un virtualenv aislado dentro de la imagen para no alterar las dependencias internas de Airflow.

```bash
make airflow-up
make airflow-ps
make airflow-logs
```

Cuando el servicio esté saludable, abre `http://localhost:8080`. Este entorno local desactiva la autenticación y concede permisos de administración; no debe exponerse fuera del equipo de desarrollo. Activa `retailpulse_batch_pipeline`, pulsa **Trigger DAG** y añade opcionalmente la configuración JSON anterior.

Para detenerlo sin borrar volúmenes:

```bash
make airflow-down
```

El primer arranque construye la imagen e instala las dependencias, por lo que tarda más que los siguientes.

## Ejecución sin Airflow

El flujo manual sigue disponible y no depende del entorno de orquestación:

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

La misma fecha debe usarse desde la ingesta hasta la carga de warehouse cuando se ejecutan los módulos directamente con `--load-date`.

## Limitaciones actuales

- Entorno local de desarrollo, no despliegue de producción.
- Pipeline de carga completa, sin incrementalidad.
- DAG manual y lineal, sin backfills avanzados.
- Sin alertas externas, SLA ni gestión productiva de secretos.
- El seeding forma parte de la demostración; una fuente real se administraría fuera del DAG.

La siguiente fase podrá centrarse en la capa gold, visualización y mejoras operativas sin trasladar lógica de transformación a Airflow.
