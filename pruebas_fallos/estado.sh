#!/bin/bash
# Foto del sistema para capturas: contenedores, réplicas y lag del grupo.
# Uso: ./estado.sh [grupo_persistidor]
source "$(dirname "$0")/comun.sh"

paso "Contenedores"
docker ps --filter name=kafka --filter name=timescaledb \
    --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

paso "Topic lecturas (líder e ISR por partición)"
describe_lecturas

if [ -n "$1" ]; then
    paso "Lag del grupo $1"
    lag_grupo "$1"
fi

paso "Filas en TimescaleDB"
$PSQL -tAc "SELECT 'lecturas: ' || count(*) FROM lecturas;"
