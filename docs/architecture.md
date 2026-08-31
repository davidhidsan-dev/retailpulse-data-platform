# Arquitectura

## Estado

Documento actualizado para la Fase 2. PostgreSQL contiene el modelo fuente operacional y Python extrae snapshots hacia un data lake local.

## Capas previstas

- `raw`: snapshots CSV recibidos sin transformar.
- `bronze`: snapshots Parquet con metadatos técnicos de ingesta.
- `silver`: datos limpios, tipados y validados.
- `gold`: modelos analíticos listos para consumo.

Raw y bronze están implementadas para PostgreSQL. Silver, gold y la orquestación se incorporarán en fases posteriores.
