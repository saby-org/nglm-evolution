#!/bin/env sh

#################################################################################
#
#  prometheus-run.sh
#
#################################################################################

#
#  sets
#

set -o errexit \
    -o verbose \
    -o xtrace

#
#  update configuration file
#

sed -i "s/<_MONITORING_HOST_>/${MONITORING_HOST}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_PROMETHEUS_APPLICATION_PORT_>/${PROMETHEUS_APPLICATION_PORT}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_PROFILEENGINE_PROMETHEUS_>/${PROFILEENGINE_PROMETHEUS}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_GUIMANAGER_HOST_>/${GUIMANAGER_HOST}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_GUIMANAGER_MONITORING_PORT_>/${GUIMANAGER_MONITORING_PORT}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_CRITERIAAPI_HOST_>/${CRITERIAAPI_HOST}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_CRITERIAAPI_MONITORING_PORT_>/${CRITERIAAPI_MONITORING_PORT}/g" /etc/prometheus/prometheus-application.yml

#
#  run
#

exec /bin/prometheus -web.listen-address=${MONITORING_HOST}:${PROMETHEUS_APPLICATION_PORT} -alertmanager.url=http://${MONITORING_HOST}:${ALERTMANAGER_PORT} -storage.local.retention=${STORAGE_LOCAL_RETENTION} -storage.local.target-heap-size=${STORAGE_LOCAL_TARGET_HEAP_SIZE} -config.file=/etc/prometheus/prometheus-application.yml -storage.local.path=/prometheus -web.console.libraries=/usr/share/prometheus/console_libraries -web.console.templates=/usr/share/prometheus/consoles
