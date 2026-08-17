# Arquitectura

## Estado

Documento inicial de Fase 0. La arquitectura se detallará antes de implementar el pipeline.

## Capas previstas

- `raw`: datos recibidos sin transformar.
- `bronze`: datos ingeridos con metadatos técnicos.
- `silver`: datos limpios, tipados y validados.
- `gold`: modelos analíticos listos para consumo.

PostgreSQL será el servicio de infraestructura inicial. La orquestación se incorporará en una fase posterior.
