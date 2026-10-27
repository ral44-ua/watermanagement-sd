#!/usr/bin/env bash
# Crea los 5 topics de WaterManagement, con 3 particiones cada uno, en el Kafka local.
# Uso, desde la raíz del repo y con el contenedor "broker" arrancado:
#     bash scripts/crear_topics.sh
# Se puede ejecutar varias veces: --if-not-exists no da error si ya existen.

set -e   # si un comando falla, el script se para en vez de seguir

# Git Bash (Windows) convierte las rutas que empiezan por "/" en rutas de Windows
# antes de pasárselas a docker.exe: /opt/kafka/... acabaría siendo
# C:/Program Files/Git/opt/kafka/... y fallaría. Esto lo desactiva.
export MSYS_NO_PATHCONV=1

for topic in wm.requests wm.ws.commands wm.ws.telemetry wm.fo.notifications wm.ws.status; do
  docker exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
done

echo "Topics en el broker:"
docker exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
