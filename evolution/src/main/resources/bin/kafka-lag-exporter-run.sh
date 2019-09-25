#!/usr/bin/env bash

set -o errexit \
    -o verbose \
    -o xtrace

sed -i "s/<_BROKER_SERVERS_>/${BROKER_SERVERS}/g" /app/bin/application.conf
sed -i "s/<_KAFKA_LAG_EXPORTER_PORT_>/${KAFKA_LAG_EXPORTER_PORT}/g" /app/bin/application.conf

exec /opt/docker/bin/kafka-lag-exporter \
    -Dconfig.file=/app/bin/application.conf \
    -Dlogback.configurationFile=/app/bin/logback.xml
