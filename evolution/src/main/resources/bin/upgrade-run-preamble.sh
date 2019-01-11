#!/usr/bin/env bash

###############################################################################
#
#  upgrade-run.sh
#
###############################################################################

# NGLM upgrade (DB migrations so far)

set -e
set -o pipefail

ZOOKEEPER_SHELL="/usr/bin/zookeeper-shell"
$ZOOKEEPER_SHELL $ZOOKEEPER_SERVERS delete ${zookeeper.root}/upgraded

export PATH="/flyway-5.2.1":$PATH

######### ADD UPGRADES HERE. DO NOT TOUCH THE REST ##########

