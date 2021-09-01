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
#  project jar and classpath
#

COPY ${project.artifactId}-${project.version}.jar /app/jars
ENV CLASSPATH=/app/jars/log4j-1.2.17.jar:/app/jars/${project.artifactId}-${project.version}.jar

#
#  run.sh
#

COPY bin/fakein-run.sh /app/bin/fakein-run.sh
RUN chmod 0755 /app/bin/fakein-run.sh

#
#  run
#

CMD [ "/app/bin/fakein-run.sh" ]
