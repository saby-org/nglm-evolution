#!/usr/bin/env bash

#################################################################################
#
# java-evolution-setup.sh 
#
#################################################################################
export CLASSPATH=/app/jars/*
#exec java $1 com.evolving.nglm.core.EvolutionSetup $2
exec kafka-run-class -name topicsetup -loggc com.evolving.nglm.core.EvolutionSetup $1

