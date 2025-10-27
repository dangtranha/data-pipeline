#!/bin/bash
echo "Waiting for Kafka Connect to be ready..."
until curl -s http://connect:8083/; do
  sleep 3
done

echo "Registering connector..."
curl -X POST -H "Content-Type: application/json"\
    --data @debezium/config/connector-postgres.json\
    http://localhost:8083/connectors

curl -X POST -H "Content-Type: application/json"\
   --data @debezium/config/connector-minio.json\
    http://localhost:8083/connectors