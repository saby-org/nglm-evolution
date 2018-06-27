FROM confluentinc/cp-kafka:${confluent.version}

#
#  permissions
#

RUN chmod 0777 /var/log/kafka

#
#  core
#

COPY lib/ /app/jars

#
#  log4j
#

COPY config/log4j-evol.properties /etc/kafka
RUN chmod 0666 /etc/kafka/log4j-evol.properties
ENV KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:/etc/kafka/log4j-evol-final.properties"

#
#  volume(s)
#

VOLUME ["/app/runtime"]

#
#  project jar and classpath
#

COPY ${project.artifactId}-${project.version}.jar /app/jars
ENV CLASSPATH=/app/jars/log4j-1.2.17.jar:/app/jars/${project.artifactId}-${project.version}.jar

#
#  run.sh
#

COPY bin/iomconfigurationloader-run.sh /app/bin/iomconfigurationloader-run.sh
RUN chmod 0755 /app/bin/iomconfigurationloader-run.sh

#
#  configuration
#

COPY config/iomconfigurationloader_jmx.yml /app/config/iomconfigurationloader_jmx.yml

#
#  run
#

CMD [ "/app/bin/iomconfigurationloader-run.sh" ]
