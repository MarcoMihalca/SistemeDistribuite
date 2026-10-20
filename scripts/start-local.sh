#!/usr/bin/env bash
set -e

# Porneste 3 noduri de stocare local pe porturi diferite (7001, 7002, 7003)
# Nodul n1 este configurat ca si coordonator pentru Etapa 1.

if [ ! -d "target/classes" ]; then
    echo "Compilare proiect cu Maven..."
    mvn -q package
fi

java -cp target/classes ro.ubm.sd.node.StorageNode --id n1 --port 7001 --dir data/n1 --coordinator &
java -cp target/classes ro.ubm.sd.node.StorageNode --id n2 --port 7002 --dir data/n2 &
java -cp target/classes ro.ubm.sd.node.StorageNode --id n3 --port 7003 --dir data/n3 &

wait
