#!/usr/bin/bash

## Disk Cleanup Script
## This script assumes that all nodes in the cluster has login access based on SSH key and password-less sudo access for the App User

read_cleanup_request_index()
{
   CTR=`curl -XGET $ELASTICSEARCH_URL/$CLEANUP_REQUEST_INDEX/_count?pretty -H 'Content-Type: application/json' -d '{"query": {"match": {"status": "REQUESTED"}}}' | grep "count" | cut -d":" -f2 | tr -d '[" ,]'`
   USR=`curl -XGET $ELASTICSEARCH_URL/$CLEANUP_REQUEST_INDEX/_search?pretty -H 'Content-Type: application/json' -d '{"query": {"match": {"status": "REQUESTED"}}}' | grep "requestedBy" | cut -d":" -f2 | tr -d '[" ,]'`
   DOCID=`curl -XGET $ELASTICSEARCH_URL/$CLEANUP_REQUEST_INDEX/_search?pretty -H 'Content-Type: application/json' -d '{"query": {"match": {"status": "REQUESTED"}}}' | grep "_id" |cut -d":" -f2 | tr -d '[" ,]'`
   if test -z "$USR" -o -z "$DOCID" -o -z "$CTR"
   then
      echo -e "INFO: No Cleanup request found in REQUESTED state...\n" >> $LOGFILE
      echo "INFO: No Cleanup request found in REQUESTED state..."
      exit
   fi
}

add_log_to_elasticsearch_index()
{
   X=1
   while test $X -le $STATUS_UPDATE_TIMEOUT
   do
      SUCCESSFUL=`curl -XPOST $ELASTICSEARCH_URL/$CLEANUP_LOG_INDEX/_doc?pretty -H 'Content-Type: application/json' -d "{\"actionType\" : \"$1\",\"node\" : \"$2\",\"user\" : \"$3\",\"actionStartDate\" : \"$4\",\"actionLog\" : \"$5\",\"status\" : \"$6\",\"remarks\" : \"$7\",\"requestId\" : \"$8\"}" | grep "successful" | cut -d":" -f2 | tr -d '[" ,]'`
      if test ! -z "$SUCCESSFUL"
      then
         if test $SUCCESSFUL -gt 0
         then
            break
         fi
      fi
      sleep 1
      X=`expr $X + 1`
   done
   echo "SUCCESSFUL: $SUCCESSFUL"
   if test -z "$SUCCESSFUL"
   then
      echo -e "ERROR: Unable to write Logs to $CLEANUP_LOG_INDEX in $STATUS_UPDATE_TIMEOUT secs!...\n" >> $LOGFILE
      echo "ERROR: Unable to write Logs to $CLEANUP_LOG_INDEX !..."
      exit
   elif test $SUCCESSFUL -lt 1
   then
      echo -e "ERROR: Unable to write Logs to $CLEANUP_LOG_INDEX in $STATUS_UPDATE_TIMEOUT secs!...\n" >> $LOGFILE
      echo "ERROR: Unable to write Logs to $CLEANUP_LOG_INDEX !..."
      exit
   fi
}

update_cleanup_request_index()
{
   X=1
   while test $X -le $STATUS_UPDATE_TIMEOUT
   do
      UPDATED=`curl -XPOST $ELASTICSEARCH_URL/$CLEANUP_REQUEST_INDEX/_update_by_query?pretty -H 'Content-Type: application/json' -d '{  "script": {"source": "ctx._source.status = \"'$2'\"", "lang": "painless"}, "query": {"match": {"_id": "'$1'"}}}' | grep "updated" | cut -d":" -f2 | tr -d '[" ,]'`
      if test ! -z "$UPDATED"
      then
         if test $UPDATED -gt 0
         then
            break
         fi
      fi
      sleep 1
      X=`expr $X + 1`
   done
   echo "UPDATED: $UPDATED"
   if test -z "$UPDATED"
   then
      echo -e "ERROR: Unable to change request status to $2 in $STATUS_UPDATE_TIMEOUT secs!...\n" >> $LOGFILE
      echo "ERROR: Unable to change request status to $2!..."
      add_log_to_elasticsearch_index "Request Status Update" "`hostname`" "$USR" "`date '+%Y-%m-%d %T.%3N%z'`" "NULL" "ERROR" "Unable to change request status to $2!!" "$1"
      exit
   elif test $UPDATED -lt 1
   then
      echo -e "ERROR: Unable to change request status to $2 in $STATUS_UPDATE_TIMEOUT secs!...\n" >> $LOGFILE
      echo "ERROR: Unable to change request status to $2!..."
      add_log_to_elasticsearch_index "Request Status Update" "`hostname`" "$USR" "`date '+%Y-%m-%d %T.%3N%z'`" "NULL" "ERROR" "Unable to change request status to $2!!" "$1"
      exit
   fi
}

STATUS_UPDATE_TIMEOUT=60
LOGTAG="maintenance_action"
LOGNAME="$LOGTAG-`date +%Y%m%d`.log"


CONFIG_FILE_PATH=<_CONFIGURE_LAUNCH_FILE_PATH_>

LOGFILE="$HOME/$LOGNAME"
DT=`date`
echo "===================================" >> $LOGFILE
echo $DT >> $LOGFILE
echo >> $LOGFILE

if test -e $CONFIG_FILE_PATH/configureLaunch.sh
then
   echo "Before Calling configureLaunch.sh..." >> $LOGFILE
   source $CONFIG_FILE_PATH/configureLaunch.sh
   echo "After Calling configureLaunch.sh... NGLM_LOGS is $NGLM_LOGS" >> $LOGFILE
else
   echo -e "ERROR: cannot find configureLaunch.sh file in the given path $CONFIG_FILE_PATH ...\n" >> $LOGFILE
   echo "ERROR: cannot find configureLaunch.sh file in the given path..."
   exit
fi

if test -d $NGLM_LOGS -a -w $NGLM_LOGS
then
   LOGFILE="$NGLM_LOGS/$LOGNAME"
else
   echo -e "ERROR: cannot find folder $NGLM_LOGS or the folder is not writable ...\n" >> $LOGFILE
   echo "ERROR: cannot find folder $NGLM_LOGS or the folder is not writable..."
   exit
fi

DT=`date`

echo "===================================" >> $LOGFILE
echo $DT >> $LOGFILE
echo >> $LOGFILE

if test -z "$ELASTICSEARCH_URL"
then
   echo -e "ERROR: parameter ELASTICSEARCH_URL is not defined ...\n" >> $LOGFILE
   echo "ERROR: parameter ELASTICSEARCH_URL is not defined..."
   exit
fi

if test -z $CLEANUP_LOG_INDEX
then
   echo -e "ERROR: parameter CLEANUP_LOG_INDEX is not defined ...\n" >> $LOGFILE
   echo "ERROR: parameter CLEANUP_LOG_INDEX is not defined..."
   exit
fi
TMP=`curl -I $ELASTICSEARCH_URL/$CLEANUP_LOG_INDEX 2> /dev/null | head -1 | grep "OK"`
if test -z "$TMP"
then
   echo -e "ERROR: cannot access $CLEANUP_LOG_INDEX in the given ELASTICSEARCH_URL...\n" >> $LOGFILE
   echo "ERROR: cannot access $CLEANUP_LOG_INDEX in the given ELASTICSEARCH_URL..."
   exit
fi

if test -z $CLEANUP_REQUEST_INDEX
then
   echo -e "ERROR: parameter CLEANUP_REQUEST_INDEX is not defined ...\n" >> $LOGFILE
   echo "ERROR: parameter CLEANUP_REQUEST_INDEX is not defined ..."
   exit
fi
TMP=`curl -I $ELASTICSEARCH_URL/$CLEANUP_REQUEST_INDEX 2> /dev/null | head -1 | grep "OK"`
if test -z "$TMP"
then
   echo -e "ERROR: cannot access $CLEANUP_REQUEST_INDEX in the given ELASTICSEARCH_URL...\n" >> $LOGFILE
   echo "ERROR: cannot access $CLEANUP_REQUEST_INDEX in the given ELASTICSEARCH_URL..."
   exit
fi

read_cleanup_request_index
if test $CTR -gt 1
then
   echo -e "ERROR: Multiple Cleanup request found in REQUESTED state!...\n" >> $LOGFILE
   echo "ERROR: Multiple Cleanup request found in REQUESTED state!..."
   exit
fi

update_cleanup_request_index "$DOCID" "STARTED"

echo >> $LOGFILE
echo "### START SCRIPT (Request ID: $DOCID) ###" >> $LOGFILE
echo >> $LOGFILE

if test -z $MAINTENANCE_LOG_CLEANUP_ENABLED
then
   MAINTENANCE_LOG_CLEANUP_ENABLED=false
fi
if test -z $MAINTENANCE_LOG_RETENTION_DAYS
then
   MAINTENANCE_LOG_RETENTION_DAYS=3
fi

if test -z $ARCHIVED_CDR_CLEANUP_ENABLED
then
   ARCHIVED_CDR_CLEANUP_ENABLED=false
fi
if test -z $ARCHIVED_CDR_RETENTION_DAYS
then
   ARCHIVED_CDR_RETENTION_DAYS=7
fi

if test -z $DOCKER_PRUNE_ENABLED
then
   DOCKER_PRUNE_ENABLED=false
fi

if test -z $JOURNAL_CLEANUP_NEEDED
then
   JOURNAL_CLEANUP_NEEDED=false
fi
if test -z $JOURNALCTL_RETENTION_DAYS
then
   JOURNALCTL_RETENTION_DAYS=7
fi


echo "Please wait.. this may take several minutes... "

# MAINTENANCE LOG FILES CLEANUP
echo "### MAINTENANCE LOG FILES CLEANUP (Request ID: $DOCID) ###" >> $LOGFILE

if test "$MAINTENANCE_LOG_CLEANUP_ENABLED" == "true"
then
   TMP=`du -ks $NGLM_LOGS`
   TMPL=`echo $TMP | tr -s " " | cut -d" " -f1`
   echo "Total space occupied under $NGLM_LOGS folder before purging is -- $TMPL KB" >> $LOGFILE
   find $NGLM_LOGS -type f -name "$LOGTAG*" -daystart -mtime +$MAINTENANCE_LOG_RETENTION_DAYS -exec rm {} \;
   find $HOME -type f -name "$LOGTAG*" -daystart -mtime +$MAINTENANCE_LOG_RETENTION_DAYS -exec rm {} \;
   TMP=`du -ks $NGLM_LOGS`
   TMPD=`echo $TMP | tr -s " " | cut -d" " -f1`
   echo "Total space after purging maintenance log files older than $MAINTENANCE_LOG_RETENTION_DAYS days is -- $TMPD KB" >> $LOGFILE
   add_log_to_elasticsearch_index "Maintenance Log Cleanup" "`hostname`" "$USR" "`date '+%Y-%m-%d %T.%3N%z'`" "Occupied before cleanup: $TMPL KB, Occupied after cleaning files older than $MAINTENANCE_LOG_RETENTION_DAYS days: $TMPD KB" "SUCCESS" "NULL" "$DOCID"
   echo >> $LOGFILE
else
   echo "WARNING: Maintenance Log file cleanup skipped!" >> $LOGFILE
   add_log_to_elasticsearch_index "Maintenance Log Cleanup" "`hostname`" "$USR" "`date '+%Y-%m-%d %T.%3N%z'`" "NULL" "WARNING" "Maintenance Log file cleanup is not Enabled!" "$DOCID"
fi

echo >> $LOGFILE


# ARCHIVED CDR FILES CLEANUP
echo "### ARCHIVED CDR FILES CLEANUP (Request ID: $DOCID) ###" >> $LOGFILE

if test "$ARCHIVED_CDR_CLEANUP_ENABLED" == "true"
then
   if test -z $NGLM_DATA
   then
      echo -e "WARNING: the parameter NGLM_DATA is not set.. archived CDR purging will not work! \n" >> $LOGFILE
      add_log_to_elasticsearch_index "Archived CDR Cleanup" "`hostname`" "$USR" "`date '+%Y-%m-%d %T.%3N%z'`" "NULL" "WARNING" "Parameter NGLM_DATA is not set.. archived CDR purging will be skipped!" "$DOCID"
   else
      FOLDERS=`ls -d $NGLM_DATA/*archive 2> /dev/null`
      CNT=`echo $FOLDERS | wc -w`
      if test $CNT -lt 1
      then
         echo -e "WARNING: archived folders not found in the path $NGLM_DATA .. archived CDR purging will not work! \n" >> $LOGFILE
         add_log_to_elasticsearch_index "Archived CDR Cleanup" "`hostname`" "$USR" "`date '+%Y-%m-%d %T.%3N%z'`" "NULL" "WARNING" "No Archived folders found.. archived CDR purgingwill be skipped!" "$DOCID"
      else
         TMP=`du -ks $NGLM_DATA`
         TMPL=`echo $TMP | tr -s " " | cut -d" " -f1`
         echo "Total space occupied under $NGLM_DATA folder before purging is -- $TMPL KB" >> $LOGFILE
         for FOLDER in $FOLDERS
         do
            find $FOLDER -type f -daystart -mtime +$ARCHIVED_CDR_RETENTION_DAYS -exec rm {} \;
         done
         TMP=`du -ks $NGLM_DATA`
         TMPD=`echo $TMP | tr -s " " | cut -d" " -f1`
         echo "Total space after purging archived CDR files older than $ARCHIVED_CDR_RETENTION_DAYS days is -- $TMPD KB" >> $LOGFILE
         add_log_to_elasticsearch_index "Archived CDR Cleanup" "`hostname`" "$USR" "`date '+%Y-%m-%d %T.%3N%z'`" "Occupied before cleanup: $TMPL KB, Occupied after cleaning files older than $ARCHIVED_CDR_RETENTION_DAYS days: $TMPD KB" "SUCCESS" "NULL" "$DOCID"
         echo >> $LOGFILE
      fi
   fi
else
   echo "WARNING: Archived CDR file cleanup skipped!" >> $LOGFILE
   add_log_to_elasticsearch_index "Archived CDR Cleanup" "`hostname`" "$USR" "`date '+%Y-%m-%d %T.%3N%z'`" "NULL" "WARNING" "Archived CDR file cleanup is not Enabled!" "$DOCID"
fi

echo >> $LOGFILE

NODES=`docker node ls --format "{{.Hostname}}"`
if test -z $NODES
then
   echo -e "ERROR: cannot run docker command as app user...\n" >> $LOGFILE
   echo "ERROR: cannot run docker command as app user..."
   add_log_to_elasticsearch_index "Docker Image Cleanup" "`hostname`" "$USR" "`date '+%Y-%m-%d %T.%3N%z'`" "NULL" "ERROR" "Cannot run docker command as app user!" "$DOCID"
   update_cleanup_request_index "$DOCID" "ABORTED"
   exit
fi

for NODE in `echo $NODES`
do
   echo "*** $NODE ***" >> $LOGFILE


   if test "$DOCKER_PRUNE_ENABLED" == "true"
   then
      # DOCKER PRUNE
      echo "### DOCKER PRUNE (Request ID: $DOCID) ###" >> $LOGFILE

      TMPL=`ssh $NODE 'docker system df | grep -e "Images" | tr -s " " | cut -d" " -f5'`
      if test -z $TMPL
      then
         echo -e "WARNING: cannot run docker system command on node $NODE ..\n" >> $LOGFILE
         add_log_to_elasticsearch_index "Docker Image Cleanup" "$NODE" "$USR" "`date '+%Y-%m-%d %T.%3N%z'`" "NULL" "WARNING" "Cannot run docker system command!" "$DOCID"
      else
         echo "Reclaimable space from unused images is -- $TMPL" >> $LOGFILE

         VAL=`echo $TMPL | cut -c1`
         if test $VAL -ne 0
         then
            TMPD=`ssh $NODE 'docker image prune -a -f' | tail -1 | cut -d" " -f4`
            echo "Reclaimed space from unused images is -- $TMPD" >> $LOGFILE
         else
            echo "Nothing to reclaim for unused images --" >> $LOGFILE
         fi
         add_log_to_elasticsearch_index "Docker Image Cleanup" "$NODE" "$USR" "`date '+%Y-%m-%d %T.%3N%z'`" "Reclaimable space: $TMPL, Reclaimed space: $TMPD" "SUCCESS" "NULL" "$DOCID"
         echo >> $LOGFILE
      fi
   else
      echo "WARNING: Docker Prune cleanup skipped!" >> $LOGFILE
      add_log_to_elasticsearch_index "Docker Image Cleanup" "$NODE" "$USR" "`date '+%Y-%m-%d %T.%3N%z'`" "NULL" "WARNING" "Docker Image cleanup is not Enabled!" "$DOCID"
   fi

   # JOURNAL LOG CLEANUP
   if test "$JOURNAL_CLEANUP_NEEDED" == "true"
   then
      echo "### JOURNAL LOG CLEANUP (Request ID: $DOCID) ###" >> $LOGFILE

      TMPL=`ssh $NODE 'sudo journalctl --disk-usage -q | cut -d" " -f7' 2> /dev/null`
      if test -z $TMPL
      then
         echo -e "WARNING: cannot run sudo for jounalctl on node $NODE ..\n" >> $LOGFILE
         add_log_to_elasticsearch_index "Journal Log Cleanup" "$NODE" "$USR" "`date '+%Y-%m-%d %T.%3N%z'`" "NULL" "WARNING" "Cannot run sudo for jounalctl!" "$DOCID"
      else
         echo "Total space occupied by archived journal logs for application user is -- $TMPL" >> $LOGFILE

         VAL=`echo $TMPL | cut -c1`
         if test $VAL -ne 0
         then
            ssh $NODE 'sudo journalctl --vacuum-time='$JOURNALCTL_RETENTION_DAYS'd -q' 1> /dev/null 2>&1
            TMPD=`ssh $NODE 'sudo journalctl --disk-usage -q | cut -d" " -f7'`
            echo "Total space occupied after cleaning logs before $JOURNALCTL_RETENTION_DAYS days from now is -- $TMPD" >> $LOGFILE
         else
            echo "Nothing to reclaim for archived journal logs --" >> $LOGFILE
         fi
         add_log_to_elasticsearch_index "Journal Log Cleanup" "$NODE" "$USR" "`date '+%Y-%m-%d %T.%3N%z'`" "Occupied before cleanup: $TMPL, Occupied after cleaning files older than $JOURNALCTL_RETENTION_DAYS days: $TMPD" "SUCCESS" "NULL" "$DOCID"
         echo >> $LOGFILE
      fi
   else
      echo "WARNING: Journal Logs cleanup skipped!" >> $LOGFILE
      add_log_to_elasticsearch_index "Journal Log Cleanup" "$NODE" "$USR" "`date '+%Y-%m-%d %T.%3N%z'`" "NULL" "WARNING" "Journal Log cleanup is not Enabled!" "$DOCID"
   fi
done

update_cleanup_request_index "$DOCID" "COMPLETED"

echo >> $LOGFILE
echo "### END SCRIPT (Request ID: $DOCID) ###" >> $LOGFILE
echo >> $LOGFILE

echo >> $LOGFILE
echo >> $LOGFILE
