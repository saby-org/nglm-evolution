#!/usr/bin/env bash

#################################################################################
#
#  kafka-multiple-topics.sh
#
#################################################################################

exec kafka-run-class -name topicsetup -loggc com.evolving.nglm.core.KafkaMultipleTopicCreate $1
