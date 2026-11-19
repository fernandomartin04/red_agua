#!/bin/bash
# Deja el entorno listo antes de CADA prueba:
#  - Brokers y TimescaleDB arrancados y healthy
#  - Líderes de partición repartidos (elección preferida)
#  - Tabla lecturas vacía (para que la verificación sea limpia)
source "$(dirname "$0")/comun.sh"

paso "Arrancando contenedores"
docker compose up -d kafka1 kafka2 kafka3 timescaledb
for c in kafka1 kafka2 kafka3 timescaledb; do esperar_healthy "$c"; done

paso "Elección de líderes preferidos"
docker exec kafka1 /opt/kafka/bin/kafka-leader-election.sh \
    --bootstrap-server localhost:19092 --election-type PREFERRED \
    --all-topic-partitions >/dev/null 2>&1
describe_lecturas

paso "Vaciando la tabla lecturas"
$PSQL -c "TRUNCATE lecturas;"

paso "Listo. Grupo nuevo sugerido para el persistidor:"
echo "persistor-$(date +%H%M%S)"
