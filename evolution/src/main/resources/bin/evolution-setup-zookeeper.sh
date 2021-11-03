#################################################################################
#
#  evolution-setup-zookeeper
#
#################################################################################

#
#  subscriberGroup
#
  
$ZOOKEEPER_SHELL $ZOOKEEPER_SERVERS create ${zookeeper.root}/subscriberGroups "subscriberGroups" || echo "create ${zookeeper.root}/subscriberGroups subscriberGroups"
$ZOOKEEPER_SHELL $ZOOKEEPER_SERVERS create ${zookeeper.root}/subscriberGroups/epochs "subscriberGroupEpochs" || echo "create ${zookeeper.root}/subscriberGroups/epochs subscriberGroupEpochs"
$ZOOKEEPER_SHELL $ZOOKEEPER_SERVERS create ${zookeeper.root}/subscriberGroups/locks "subscriberGroupLocks" || echo "create ${zookeeper.root}/subscriberGroups/locks subscriberGroupLocks"

#
#  stock
#

$ZOOKEEPER_SHELL $ZOOKEEPER_SERVERS create ${zookeeper.root}/stock "stock" || echo "create ${zookeeper.root}/stock stock"
$ZOOKEEPER_SHELL $ZOOKEEPER_SERVERS create ${zookeeper.root}/stock/stocks "stocks" || echo "create ${zookeeper.root}/stock/stocks stocks"
$ZOOKEEPER_SHELL $ZOOKEEPER_SERVERS create ${zookeeper.root}/stock/stockmonitors "stockMonitors" || echo "create ${zookeeper.root}/stock/stockmonitors"
