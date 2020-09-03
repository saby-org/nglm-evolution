#!/usr/bin/env bash

#################################################################################
#
#  prepare-curl.sh
#
#################################################################################

for var in "$@"
do
    params="$params |||$var|||"
done

echo $params >> /app/setup/connectors/connectors 
