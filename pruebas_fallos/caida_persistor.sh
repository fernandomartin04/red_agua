#!/bin/bash
# PRUEBA E · Muerte brusca del persistidor (kill -9) y relanzamiento.
# Qué se espera:
#  - Al morir con kill -9 no hace close(): el último lote puede estar en la BD
#    pero sin commit en Kafka.
#  - Al relanzarlo con el MISMO grupo, retoma desde el último offset confirmado
#    y vuelve a procesar ese lote. El UPSERT lo absorbe: 0 duplicados en la BD.
#  - El lag crece mientras está muerto y se vacía al volver.
# Uso: ./caida_persistor.sh <grupo_persistidor>
source "$(dirname "$0")/comun.sh"
GRUPO=${1:?Uso: $0 <grupo_persistidor>}

sleep 30
paso "kill -9 al persistidor"
pkill -9 -f "persistencia/persistor.py" && echo "persistidor muerto" \
    || echo "No se encontró el proceso del persistidor"

sleep 10
paso "Persistidor muerto: el lag crece"
lag_grupo "$GRUPO"
echo
echo ">>> RELANZA AHORA el persistidor en su terminal con el MISMO grupo: $GRUPO"

sleep 50
paso "Tras relanzarlo: el lag debe volver a ~0"
lag_grupo "$GRUPO"
