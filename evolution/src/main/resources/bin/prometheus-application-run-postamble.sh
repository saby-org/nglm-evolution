#################################################################################
#
#  prometheus-run-postamble.sh
#
#################################################################################

#
#  run
#

exec /bin/prometheus -web.listen-address=${MONITORING_HOST}:${PROMETHEUS_APPLICATION_PORT} -alertmanager.url=http://${MONITORING_HOST}:${ALERTMANAGER_PORT} -storage.local.retention=${STORAGE_LOCAL_RETENTION} -storage.local.target-heap-size=${STORAGE_LOCAL_TARGET_HEAP_SIZE} -config.file=/etc/prometheus/prometheus-application.yml -storage.local.path=/prometheus -web.console.libraries=/usr/share/prometheus/console_libraries -web.console.templates=/usr/share/prometheus/consoles
