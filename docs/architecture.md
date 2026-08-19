# Arquitectura

## Estado

Documento actualizado para la Fase 1. PostgreSQL contiene únicamente el modelo fuente operacional y los datos sintéticos usados para desarrollo.

## Capas previstas

- `raw`: datos recibidos sin transformar.
- `bronze`: datos ingeridos con metadatos técnicos.
- `silver`: datos limpios, tipados y validados.
- `gold`: modelos analíticos listos para consumo.

Las capas de datos todavía no están implementadas. La orquestación se incorporará en una fase posterior.
