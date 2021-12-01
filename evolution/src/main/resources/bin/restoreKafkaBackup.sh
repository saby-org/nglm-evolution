#!/usr/bin/env bash

#################################################################################
#
# restoreKafkaBackup.sh
#
#################################################################################

#
#  sets
#

set -o errexit \
    -o verbose \
    -o xtrace

echo "***************************************************************************************************"
echo "This script will allow you to restore kafka topics, from data saved by the backupmanager container."
echo "***************************************************************************************************"
echo

if [ $# -lt 2 ]
then
  echo "Usage : restoreKafkaBackup.sh <backupfile> <topic> [<logLevel>]"
  echo "logLevel is optional, defaults to INFO, possible values : ERROR WARN INFO DEBUG TRACE"
  exit 1
fi

TOPIC=$2
LOGLEVEL=$3
if [ -z $LOGLEVEL ]; then
	LOGLEVEL=INFO
fi

BACKUPFILE=$1
if [ ! -f $BACKUPFILE ]; then
   echo "Backup file $BACKUPFILE does not exist"
   if [ -z $ENTRYPOINT ]; then # this is only defined in evolution containers
    	echo "Try to execute remotely on the ev-backupmanager container"
   		docker container exec `docker ps -aqf "name=ev-backupmanager"` /app/bin/restoreKafkaBackup.sh $BACKUPFILE $TOPIC $LOGLEVEL
   		exit 0
   else
   		exit 1
   fi
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
				value="%d{ISO8601}{$DEPLOYMENT_TIMEZONE} %p [%t] %m%n" />
		</layout>
	</appender>

	<category name="com.evolving.nglm">
		<priority value="$LOGLEVEL" />
	</category>

	<category name="com.evolving.nglm.evolution.backup.kafka">
		<priority value="$LOGLEVEL" />
	</category>

	<root>
		<priority value="$LOGLEVEL" />
		<appender-ref ref="stdout" />
		<appender-ref ref="eventAppender" />
	</root>

	<category name="org.apache.kafka">
		<priority value="$LOGLEVEL" />
	</category>
	<category name="io.confluent">
		<priority value="$LOGLEVEL" />
	</category>
	<category name="org.apache.zookeeper">
		<priority value="$LOGLEVEL" />
	</category>

</log4j:configuration>

EOF
) > $BACKUP_LOGFILE

java -cp /app/jars/*:/usr/share/java/kafka/* \
    -Dnglm.schemaRegistryURL=$REGISTRY_URL -Dnglm.converter=Avro \
    -Dzookeeper.connect=$ZOOKEEPER_SERVERS -Dbroker.servers=$BROKER_SERVERS \
    -Dnglm.zookeeper.root=${zookeeper.root} -Dlog4j.configuration=file:$BACKUP_LOGFILE \
    -Dredis.sentinels=$REDIS_SENTINELS -Dsubscriberprofile.endpoints=$EVOLUTIONENGINE_SUBSCRIBERPROFILE_ENDPOINTS \
    com.evolving.nglm.evolution.backup.kafka.BackupTopic $TOPIC $BACKUPFILE 

