#!/bin/bash
# Funciones y variables comunes a todas las pruebas de fallos.
# No se ejecuta solo: lo cargan los demás scripts con "source".

cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1   # carpeta red_agua/

BROKERS="kafka1 kafka2 kafka3"
PSQL="docker exec timescaledb psql -U postgres -d red_agua"

# Hora relativa al arranque del script, para las capturas
INICIO=$(date +%s)
paso() {
    echo
    echo "===== [$(( $(date +%s) - INICIO ))s · $(date +%H:%M:%S)] $* ====="
}

# Primer broker que siga vivo (para lanzar los comandos de Kafka desde él)
broker_vivo() {
    for b in $BROKERS; do
        if [ "$(docker inspect -f '{{.State.Running}}' "$b" 2>/dev/null)" = "true" ]; then
            echo "$b"; return
        fi
    done
}

describe_lecturas() {
    local b; b=$(broker_vivo)
    timeout 20 docker exec "$b" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:19092 --describe --topic lecturas \
        || echo "(describe sin respuesta)"
}

# Lag del grupo: mensajes que están en Kafka y el consumidor aún no ha confirmado
lag_grupo() {
    local b; b=$(broker_vivo)
    timeout 20 docker exec "$b" /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server localhost:19092 --describe --group "$1" 2>/dev/null \
        | awk 'NR==1 || /lecturas/ {print $1, $2, $3, $4, $5, $6}' | column -t
    local total
    total=$(timeout 20 docker exec "$b" /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server localhost:19092 --describe --group "$1" 2>/dev/null \
        | awk '/lecturas/ && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}')
    echo "LAG TOTAL = $total"
}

# Espera a que un contenedor vuelva a estar healthy
esperar_healthy() {
    until [ "$(docker inspect -f '{{.State.Health.Status}}' "$1" 2>/dev/null)" = "healthy" ]; do
        sleep 1
    done
    echo "$1 healthy ($(( $(date +%s) - INICIO ))s)"
}
