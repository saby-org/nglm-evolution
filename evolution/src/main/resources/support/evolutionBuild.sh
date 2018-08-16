#########################################
#
#  evolutionBuild.sh
#
#########################################

#
#  docker registry
#

export DOCKER_REGISTRY=evolving/

#
#  maven
#

export MAVEN_REPOSITORY=${MAVEN_REPOSITORY:-$HOME/.m2/repository}
