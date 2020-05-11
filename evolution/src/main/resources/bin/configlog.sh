#!/bin/bash
echo "*********************************************************************************"
echo "This script will allow you to change the log configuration of a running container"
echo "*********************************************************************************"
echo

declare -a containers
for name in `docker ps --format '{{.Names}}' | grep -v ${DOCKER_STACK}-gui_ | sort`
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

CONTAINERID=`docker ps --format '{{.ID}}' -f "Name=$CONTAINERNAME"`

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
elif [[ "$CONTAINERNAME" =~ ^${DOCKER_STACK}-commoditydeliverymanager_commoditydeliverymanager ]]; then
  FILE=comoditydelivery
elif [[ "$CONTAINERNAME" =~ ^${DOCKER_STACK}-infulfillmentmanager_infulfillmentmanager ]]; then
  FILE=infulfillment
elif [[ "$CONTAINERNAME" =~ ^${DOCKER_STACK}-purchasefulfillmentmanager_purchasemanager ]]; then
  FILE=purchasefulfillment
else
  FILE=generic
fi

if [ $FILE == "generic" ]
then
  LOGFILE=/etc/kafka/log4j-evol-final.properties
else
  LOGFILE=/app/config/log4j-${FILE}.xml
fi

TEMPFILE=/tmp/logfile.$$.xml

docker exec -i $CONTAINERID cat $LOGFILE > ${TEMPFILE}
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
echo "--> changing log config file in container..."
docker cp $TEMPFILE $CONTAINERID:${LOGFILE}.tmp
if [ $? -ne 0 ]
then
  echo "--> unexpected error : exiting"
  exit 1
fi
docker exec -i $CONTAINERID cp ${LOGFILE}.tmp ${LOGFILE}
if [ $? -ne 0 ]
then
  echo "--> unexpected error : exiting"
  exit 1
fi
echo "--> Log config file replaced in container $CONTAINERNAME"


