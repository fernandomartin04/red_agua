#!/bin/bash
# PRUEBA D · Caída de TimescaleDB mientras el persistidor escribe.
# Qué se espera:
#  - El persistidor no puede guardar el lote, NO hace commit en Kafka y reintenta.
#  - Los mensajes se acumulan en Kafka (el lag crece): Kafka hace de buffer.
#  - Al volver la BD, el persistidor reconecta y vacía el lag. 0 pérdidas.
# Uso: ./caida_timescale.sh <grupo_persistidor>
source "$(dirname "$0")/comun.sh"
GRUPO=${1:?Uso: $0 <grupo_persistidor>}

sleep 30
paso "docker kill timescaledb"
docker kill timescaledb

sleep 15
paso "BD caída: el lag crece porque nadie confirma offsets"
lag_grupo "$GRUPO"

sleep 15
paso "docker start timescaledb"
docker start timescaledb
esperar_healthy timescaledb

sleep 20
paso "Tras la recuperación: el lag debe volver a ~0"
lag_grupo "$GRUPO"
