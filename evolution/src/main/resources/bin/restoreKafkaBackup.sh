#!/bin/bash

# restoreKafkaBackup.sh

echo "***************************************************************************************************"
echo "This script will allow you to restore kafka topics, from data saved by the backupmanager container."
echo "***************************************************************************************************"
echo

rep=""
while [ "$rep" == "" ]
do
  echo -n "--> Backup file : "
  read rep
done

BACKUPFILE=$rep
if [ ! -f $BACKUPFILE ]; then
   echo "file does not exist, exiting"
   exit 1
fi

rep=""
while [ "$rep" == "" ]
do
  echo -n "--> Topic name : "
  read rep
done

TOPIC=$rep

rep=""
while [ "$rep" == "" -a "$r" != "y" -a "$r" != "n" ]
do
  echo -n "We will restore $TOPIC from data in $BACKUPFILE, is that correct (y/n) ? "
  read rep
done

if [ "$rep" != "y" ]; then
  echo "Exiting"
  exit 1
fi

export BACKUP_LOGFILE=/tmp/log.$$.xml
(
cat <<EOF
<!DOCTYPE log4j:configuration SYSTEM "log4j.dtd">
<log4j:configuration debug="true" xmlns:log4j='http://jakarta.apache.org/log4j/'>
	<appender name="stdout"
		class="org.apache.log4j.ConsoleAppender">
		<param name="Target" value="System.out" />
		<layout class="org.apache.log4j.EnhancedPatternLayout">
			<param name="ConversionPattern"
				value="%d{ISO8601}{<_DEPLOYMENT_TIMEZONE_>} %p [%t] %m%n" />
		</layout>
	</appender>

	<category name="com.evolving.nglm">
		<priority value="ERROR" />
	</category>

	<category name="com.evolving.nglm.evolution.backup.kafka">
		<priority value="INFO" />
	</category>

	<root>
		<priority value="ERROR" />
		<appender-ref ref="stdout" />
		<appender-ref ref="eventAppender" />
	</root>

	<category name="org.apache.kafka">
		<priority value="ERROR" />
	</category>
	<category name="io.confluent">
		<priority value="ERROR" />
	</category>
	<category name="org.apache.zookeeper">
		<priority value="ERROR" />
	</category>

</log4j:configuration>

EOF
) > $BACKUP_LOGFILE

java -cp ${project.build.directory}/lib/*:${project.build.directory}/${project.artifactId}-${project.version}.jar:$CONFLUENT_HOME/share/java/kafka/* \
    -Dnglm.schemaRegistryURL=<_REGISTRY_URL_> -Dnglm.converter=Avro \
    -Dzookeeper.connect=<_ZOOKEEPER_SERVERS_> -Dbroker.servers=<_BROKER_SERVERS_> \
    -Dnglm.zookeeper.root=${zookeeper.root} -Dlog4j.configuration=file:$BACKUP_LOGFILE \
    -Dredis.sentinels=<_REDIS_SENTINELS_> -Dsubscriberprofile.endpoints=<_EVOLUTIONENGINE_SUBSCRIBERPROFILE_ENDPOINTS_> \
    com.evolving.nglm.evolution.backup.kafka.BackupTopic $TOPIC $BACKUPFILE 

