#!/bin/bash
DESC="timeout 20 docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe --topic lecturas"

sleep 30
echo "===== [30s] docker kill kafka2 kafka3 ====="
date +%s
docker kill kafka2 kafka3
sleep 15
echo "===== [45s] Estado con solo kafka1 ====="
$DESC || echo "(describe sin respuesta: el clúster no tiene quórum)"

sleep 15
echo "===== [60s] docker start kafka2 kafka3 ====="
date +%s
docker start kafka2 kafka3
sleep 30
echo "===== [90s] Estado tras la recuperación ====="
$DESC
