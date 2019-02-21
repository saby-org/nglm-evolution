#!/usr/bin/env bash

#################################################################################
#
#  fakein-run.sh
#
#################################################################################

#
#  sets
#

set -o errexit \
    -o verbose \
    -o xtrace

#
#  log4j-evol.properties
#

cat /etc/kafka/log4j-evol.properties | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' > /etc/kafka/log4j-evol-final.properties

#
#  run
#

exec kafka-run-class -name fakeIN -loggc com.evolving.nglm.evolution.fakein.FakeIN $PORT $DETERMINISTIC
