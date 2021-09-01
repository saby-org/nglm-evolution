FROM confluentinc/cp-kafka:${confluent.version}

USER root

RUN yum install -y perl

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
ENV KAFKA_DEBUG=1
ENV JAVA_DEBUG_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,address=7401,suspend=n"

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
#  additional volume(s)
#

VOLUME ["/app/data"]

#
#  run.sh
#

COPY bin/storeconfiguration-run.sh /app/bin/storeconfiguration-run.sh
RUN chmod 0755 /app/bin/storeconfiguration-run.sh
