#################################################################################
#
#  prometheus-run-postamble.sh
#
#################################################################################

#
#  run
#

exec /bin/prometheus --web.listen-address=${MONITORING_HOST}:${PROMETHEUS_APPLICATION_PORT} --config.file=/etc/prometheus/prometheus-application.yml --storage.tsdb.path=/prometheus --storage.tsdb.retention.time=${STORAGE_LOCAL_RETENTION} --web.console.libraries=/usr/share/prometheus/console_libraries --web.console.templates=/usr/share/prometheus/consoles
