#!/bin/bash
# PRUEBA F · Caída del broker coordinador del grupo del persistidor.
# Es la prueba C, pero ahora con la tubería completa hasta la BD.
# Qué se espera:
#  - Se elige otro coordinador; el commit en vuelo se reintenta o falla como
#    "fallo temporal". Como la BD ya tiene el lote (UPSERT), no se pierde nada.
# Uso: ./caida_coordinador.sh <grupo_persistidor>
source "$(dirname "$0")/comun.sh"
GRUPO=${1:?Uso: $0 <grupo_persistidor>}

sleep 30
paso "Buscando el coordinador del grupo $GRUPO"
SALIDA=$(docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:19092 --describe --group "$GRUPO" --state 2>/dev/null)
echo "$SALIDA"
ID=$(echo "$SALIDA" | grep -oP '\(\K[0-9]+(?=\))' | head -1)
if [ -z "$ID" ]; then echo "No se encontró el coordinador (¿está el persistidor en marcha?)"; exit 1; fi
COORD="kafka$ID"

paso "docker kill $COORD (coordinador)"
docker kill "$COORD"

sleep 10
paso "Nuevo coordinador"
docker exec "$(broker_vivo)" /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:19092 --describe --group "$GRUPO" --state 2>/dev/null

sleep 20
paso "docker start $COORD"
docker start "$COORD"
esperar_healthy "$COORD"

sleep 15
paso "Estado final"
describe_lecturas
lag_grupo "$GRUPO"
