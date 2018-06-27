FROM prom/prometheus:${prometheus.version}

#
#  permissions
#

RUN chmod 0777 -R /etc/prometheus

#
#  run.sh
#

COPY bin/prometheus-application-run.sh /app/bin/prometheus-application-run.sh
RUN chmod 0755 /app/bin/prometheus-application-run.sh

#
#  configuration
#

COPY config/prometheus-application.yml /etc/prometheus/prometheus-application.yml
COPY config/prometheus-application-alert.rules /etc/prometheus/prometheus-application-alert.rules

#
#  run
#

ENTRYPOINT [ "/app/bin/prometheus-application-run.sh" ]
