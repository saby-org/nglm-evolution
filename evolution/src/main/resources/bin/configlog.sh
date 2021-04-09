#!/bin/bash
echo "*********************************************************************************"
echo "This script will allow you to change the log configuration of a running container"
echo "*********************************************************************************"
echo

: ${SWARM_HOSTS:?"SWARM_HOSTS undefined"}

declare -a hosts
for host in $SWARM_HOSTS
do
  hosts+=($host)
done


for ID in "${!hosts[@]}"
do
  if [ $ID -lt 10 ]
  then
    echo "   $ID ... ${hosts[$ID]}"
  else
    echo "  $ID ... ${hosts[$ID]}"
  fi
done

rep=""
while [ "$rep" == "" ]
do
  echo -n "--> Select host to change logging : "
  read rep
done

if [ $rep -lt 0 -o $rep -ge ${#hosts[@]} ]
then
  echo "Unknown index"
  exit 1
fi

HOSTNAME=${hosts[$rep]}
if [ "$HOSTNAME" == "" ]
then
  echo "Unknown index"
  exit 1
fi


export DOCKER_STACK=${DOCKER_STACK:-ev}
declare -a containers
for name in `ssh $HOSTNAME docker ps --format '{{.Names}}' | grep -v ${DOCKER_STACK}-gui_ | sort`
do
  containers+=($name)
done

for ID in "${!containers[@]}"
do
  if [ $ID -lt 10 ]
  then
    echo "   $ID ... ${containers[$ID]}"
  else
    echo "  $ID ... ${containers[$ID]}"
  fi
done

rep=""
while [ "$rep" == "" ]
do
  echo -n "--> Select container to change logging : "
  read rep
done

if [ $rep -lt 0 -o $rep -ge ${#containers[@]} ]
then
  echo "Unknown index"
  exit 1
fi

CONTAINERNAME=${containers[$rep]}
if [ "$CONTAINERNAME" == "" ]
then
  echo "Unknown index"
  exit 1
fi

CONTAINERID=`ssh $HOSTNAME docker ps --format '{{.ID}}' -f "Name=$CONTAINERNAME"`

if [[ "$CONTAINERNAME" =~ ^${DOCKER_STACK}-guimanager_guimanager ]]; then
  FILE=guimanager
elif [[ "$CONTAINERNAME" =~ ^${DOCKER_STACK}-evolutionengine_evolutionengine ]]; then
  FILE=evolutionengine
elif [[ "$CONTAINERNAME" =~ ^${DOCKER_STACK}-thirdpartymanager_thirdpartymanager ]]; then
  FILE=thirdpartyevent
elif [[ "$CONTAINERNAME" =~ ^${DOCKER_STACK}-notificationmanagermail_notificationmail ]]; then
  FILE=mail
elif [[ "$CONTAINERNAME" =~ ^${DOCKER_STACK}-notificationmanagersms_notificationsms ]]; then
  FILE=sms
elif [[ "$CONTAINERNAME" =~ ^${DOCKER_STACK}-notificationmanagerpush_notificationpush ]]; then
  FILE=push
elif [[ "$CONTAINERNAME" =~ ^${DOCKER_STACK}-notificationmanager_notification ]]; then
  FILE=notificationmanager
elif [[ "$CONTAINERNAME" =~ ^${DOCKER_STACK}-commoditydeliverymanager_commoditydeliverymanager ]]; then
  FILE=commoditydelivery
elif [[ "$CONTAINERNAME" =~ ^${DOCKER_STACK}-infulfillmentmanager_infulfillmentmanager ]]; then
  FILE=infulfillment
elif [[ "$CONTAINERNAME" =~ ^${DOCKER_STACK}-purchasefulfillmentmanager_purchasemanager ]]; then
  FILE=purchasefulfillment
elif [[ "$CONTAINERNAME" =~ ^${DOCKER_STACK}-reportmanager_reportmanager ]]; then
  FILE=reportmanager
elif [[ "$CONTAINERNAME" =~ ^${DOCKER_STACK}-reportscheduler_reportscheduler ]]; then
  FILE=reportscheduler
elif [[ "$CONTAINERNAME" =~ ^${DOCKER_STACK}-datacubemanager_datacubemanager ]]; then
  FILE=datacubemanager
elif [[ "$CONTAINERNAME" =~ ^${DOCKER_STACK}-connect_connect ]]; then
  FILE=connect
else
  FILE=generic
fi

if [ $FILE == "connect" ]
then
  LOGFILE=/app/config/log4j-connect.xml
elif [ $FILE == "generic" ]
then
  LOGFILE=/etc/kafka/log4j-evol-final.properties
else
  LOGFILE=/app/config/log4j-${FILE}.xml
fi

TEMPFILE=/tmp/logfile.$$.xml

ssh $HOSTNAME "docker exec -i $CONTAINERID cat $LOGFILE" > ${TEMPFILE}
if [ $? -ne 0 ]
then
  echo "--> unexpected error : exiting"
  exit 1
fi
cp $TEMPFILE ${TEMPFILE}2
vi $TEMPFILE
diff $TEMPFILE ${TEMPFILE}2 > /dev/null 2>&1
if [ $? -ne 1 ]
then
  echo "--> identical files : exiting"
  exit 1
fi
scp $TEMPFILE $HOSTNAME:$TEMPFILE

echo "--> changing log config file in container..."
echo "docker exec -i $CONTAINERID ls -la $LOGFILE"
ssh $HOSTNAME "docker exec -i $CONTAINERID ls -la $LOGFILE"
ssh $HOSTNAME "docker cp $TEMPFILE $CONTAINERID:${LOGFILE}.tmp"
if [ $? -ne 0 ]
then
  echo "--> unexpected error : exiting"
  exit 1
fi
ssh $HOSTNAME "docker exec -i $CONTAINERID cp ${LOGFILE}.tmp ${LOGFILE}"
if [ $? -ne 0 ]
then
  echo "--> unexpected error : exiting"
  exit 1
fi
echo "--> Log config file replaced in container $CONTAINERNAME"

