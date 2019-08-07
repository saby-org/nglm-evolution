#!/usr/bin/env bash

set -o errexit \
    -o verbose \
    -o xtrace

sed -i "s/<_BROKER_SERVERS_>/${BROKER_SERVERS}/g" /app/bin/application.conf

exec /opt/docker/bin/kafka-lag-exporter \
    -Dconfig.file=/app/bin/application.conf \
    -Dlogback.configurationFile=/app/bin/logback.xml
