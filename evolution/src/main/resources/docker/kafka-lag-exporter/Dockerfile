FROM lightbend/kafka-lag-exporter:${lagexporter.version}
USER root
RUN mkdir -p /app/bin
COPY config/kafka-lag-exporter.conf /app/bin/application.conf
COPY config/kafka-lag-exporter-logback.xml /app/bin/logback.xml
COPY bin/kafka-lag-exporter-run.sh /app/bin/kafka-lag-exporter-run.sh

RUN chmod 0777 /app/bin/application.conf
RUN chmod 0755 /app/bin/kafka-lag-exporter-run.sh

CMD [ "/app/bin/kafka-lag-exporter-run.sh"]