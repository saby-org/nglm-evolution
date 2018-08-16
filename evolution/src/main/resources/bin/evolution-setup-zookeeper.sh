  #################################################################################
  #
  #  evolution-setup-zookeeper
  #
  #################################################################################

  #
  #  zookeeper-based configuration
  #
  
  $ZOOKEEPER_SHELL $ZOOKEEPER_SERVERS create ${zookeeper.root}/subscriberGroups "subscriberGroups"
  $ZOOKEEPER_SHELL $ZOOKEEPER_SERVERS create ${zookeeper.root}/subscriberGroups/epochs "subscriberGroupEpochs"
  $ZOOKEEPER_SHELL $ZOOKEEPER_SERVERS create ${zookeeper.root}/subscriberGroups/locks "subscriberGroupLocks"

