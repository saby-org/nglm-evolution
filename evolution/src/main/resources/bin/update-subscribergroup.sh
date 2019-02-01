#################################################################################
#
#  update-subscribergroup.sh
#
#################################################################################

#
#  run
#

docker run --net=host --mount type=bind,source=<_NGLM_SUBSCRIBERGROUP_DATA_>,target=/app/data --memory "<_SUBSCRIBERGROUP_CONTAINER_MEMORY_LIMIT_>" --rm ${env.DOCKER_REGISTRY}ev-subscribergroup:${project.name}-${project.version} /app/bin/subscribergroup-run.sh "<_BROKER_SERVERS_>" "<_ZOOKEEPER_SERVERS_>" "<_REDIS_SENTINELS_>" "<_REGISTRY_URL_>" 2 "<_SUBSCRIBERGROUP_HEAP_OPTS_>" subscribergroup_loader <_TRACE_LEVEL_> "$1" "$2" "$3"
