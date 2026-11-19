#!/bin/bash
# PRUEBA G · Reinicio completo del sistema (docker compose down + up, SIN -v).
# Se lanza con productor y persistidor PARADOS.
# Qué se espera:
#  - Los contenedores se destruyen pero los volúmenes no: al volver,
#    Kafka conserva los mensajes y los offsets del grupo, y TimescaleDB las filas.
# Uso: ./reinicio_total.sh [grupo_persistidor]
source "$(dirname "$0")/comun.sh"
GRUPO=$1

foto() {
    local b; b=$(broker_vivo)
    echo -n "Mensajes en el topic lecturas: "
    docker exec "$b" /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server localhost:19092 --topic lecturas \
        | awk -F: '{s+=$3} END {print s}'
    $PSQL -tAc "SELECT 'Filas en TimescaleDB: ' || count(*) FROM lecturas;"
    [ -n "$GRUPO" ] && lag_grupo "$GRUPO"
}

paso "ANTES del reinicio"
foto | tee /tmp/antes_reinicio.txt

paso "docker compose down (los volúmenes se conservan)"
docker compose down

paso "Contenedores tras el down"
docker ps -a --filter name=kafka --filter name=timescaledb --format "{{.Names}}\t{{.Status}}"
echo "(ninguno = destruidos)"
docker volume ls --filter name=red-agua

paso "docker compose up -d"
docker compose up -d
for c in kafka1 kafka2 kafka3 timescaledb; do esperar_healthy "$c"; done

paso "DESPUÉS del reinicio"
foto | tee /tmp/despues_reinicio.txt

paso "Comparación"
if diff <(grep -v LAG /tmp/antes_reinicio.txt | grep -E "Mensajes|Filas") \
        <(grep -v LAG /tmp/despues_reinicio.txt | grep -E "Mensajes|Filas") >/dev/null; then
    echo "RESULTADO: OK, mismos mensajes y mismas filas antes y después"
else
    echo "RESULTADO: HAY DIFERENCIAS"
fi
