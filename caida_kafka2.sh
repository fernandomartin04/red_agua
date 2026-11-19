#!/bin/bash
DESC="docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe --topic lecturas"

sleep 30
echo "===== [30s] docker kill kafka2 ====="
docker kill kafka2
sleep 5
$DESC

sleep 25
echo "===== [60s] docker start kafka2 ====="
docker start kafka2
sleep 30
echo "===== [90s] Estado tras la recuperación ====="
$DESC
