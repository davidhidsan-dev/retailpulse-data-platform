# Limitaciones

- El repositorio está en Fase 2 y solo contiene extracción completa hacia raw/bronze.
- PostgreSQL es el único servicio definido en Docker Compose.
- dbt y Airflow no están configurados.
- No se incluyen componentes de streaming ni procesamiento distribuido.
- Los datos fuente son sintéticos y no representan comportamiento comercial real.
- La ingesta no es incremental y el data lake utiliza almacenamiento local.
