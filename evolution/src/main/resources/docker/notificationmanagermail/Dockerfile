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
COPY bin/notificationmanagermail-run.sh /app/bin/notificationmanagermail-run.sh
RUN chmod 0755 /app/bin/notificationmanagermail-run.sh

#
#  configuration
#

COPY config/notificationmanagermail_jmx.yml /app/config/notificationmanagermail_jmx.yml

#
#  run
#

CMD [ "/app/bin/notificationmanagermail-run.sh" ]
