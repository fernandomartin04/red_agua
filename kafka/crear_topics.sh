#!/bin/bash
# Crea los topics del proyecto. Idempotente: se puede ejecutar varias veces.
set -euo pipefail

BOOTSTRAP="kafka1:19092,kafka2:19092,kafka3:19092"
KT=/opt/kafka/bin/kafka-topics.sh
DIA_MS=86400000

crear() {
  local nombre=$1 particiones=$2
  shift 2
  echo ">> Creando topic '$nombre' ($particiones particiones)"
  $KT --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
      --topic "$nombre" --partitions "$particiones" --replication-factor 3 \
      --config min.insync.replicas=2 "$@"
}

crear lecturas      6 --config retention.ms=$((7 * DIA_MS))
crear fallos-motor  1 --config retention.ms=-1
crear alarmas       3 --config retention.ms=$((30 * DIA_MS))
crear lecturas-dlq  1 --config retention.ms=$((7 * DIA_MS))

echo ">> Estado final:"
$KT --bootstrap-server "$BOOTSTRAP" --describe
