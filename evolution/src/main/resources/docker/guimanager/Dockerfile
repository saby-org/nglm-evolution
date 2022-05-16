FROM confluentinc/cp-kafka:${confluent.version}

USER root

RUN yum install -y perl

#
#  permissions
#

# confluent 5.0.x: 
RUN chmod 0777 /var/log/kafka /var/log/confluent
RUN chmod 0777 /var/lib/kafka /var/lib/zookeeper
# confluent 6.2.0
RUN chmod 0777 /home/appuser

#
#  core
#

COPY lib/ /app/jars

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
ENV CLASSPATH=/app/jars/log4j-core-2.16.0.jar:/app/jars/${project.artifactId}-${project.version}.jar

#
#  run.sh
#

COPY bin/ev-cub /app/bin/ev-cub
RUN chmod 0755 /app/bin/ev-cub

COPY bin/guimanager-run.sh /app/bin/guimanager-run.sh
RUN chmod 0755 /app/bin/guimanager-run.sh

COPY bin/restoreKafkaBackup.sh /app/bin/restoreKafkaBackup.sh
RUN chmod 0755 /app/bin/restoreKafkaBackup.sh

#
#  configuration
#

COPY config/*_jmx.yml /app/config/

#
#  run
#

LABEL evolution.artifact="${project.artifactId}-${project.version}"

CMD [ "/app/bin/guimanager-run.sh" ]
