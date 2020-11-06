#!/usr/bin/env bash

#################################################################################
#
#  prepare-curl.sh
#
#################################################################################

TARGET_FILE=$1
shift

for var in "$@"
do
    params="$params |||$var|||"
done

echo $params >> $TARGET_FILE
