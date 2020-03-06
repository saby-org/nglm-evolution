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
COPY bin/guimanager-run.sh /app/bin/guimanager-run.sh
RUN chmod 0755 /app/bin/guimanager-run.sh

#
#  configuration
#

COPY config/guimanager_jmx.yml /app/config/guimanager_jmx.yml

#
#  run
#

CMD [ "/app/bin/guimanager-run.sh" ]
