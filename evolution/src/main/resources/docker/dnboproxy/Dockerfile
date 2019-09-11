FROM confluentinc/cp-kafka:${confluent.version}

#
#  permissions
#

# confluent 5.0.x: 
RUN chmod 0777 /var/log/kafka /var/log/confluent
RUN chmod 0777 /var/lib/kafka /var/lib/zookeeper

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

COPY bin/ev-cub /app/bin/ev-cub
RUN chmod 0755 /app/bin/ev-cub
COPY bin/dnboproxy-run.sh /app/bin/dnboproxy-run.sh
RUN chmod 0755 /app/bin/dnboproxy-run.sh

#
#  configuration
#

COPY config/dnboproxy_jmx.yml /app/config/dnboproxy_jmx.yml

#
#  run
#

CMD [ "/app/bin/dnboproxy-run.sh" ]
