#########################################
#
#  configureBuild.sh
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

#
#  deploy root
#

GITVER=`java -jar $MAVEN_REPOSITORY/com/evolving/jgitver-cli/1.0.0/jgitver-cli-1.0.0.jar`
export DEPLOY_PACKAGE=nglm-evolution-${GITVER}.tgz
export DEPLOY_PACKAGE_BASENAME="${DEPLOY_PACKAGE%.*}"
export DEPLOY_ROOT=$(readlink -f "..")/build/${DEPLOY_PACKAGE_BASENAME}

#
#  java
#

export JAVA8_HOME=${JAVA8_HOME:-/mnt/evol/software/jdk1.8.0_101}
export JAVA_HOME=$JAVA8_HOME

#
#  ptt
#

export RLX_ROOT=/home/evol/working/sandbox/build/RLX
export PTT_HOME=/home/evol/working/sandbox/build/ptt/ptt
export PTT_FILE=$DEPLOY_ROOT/support/ptt.properties

#
#  regression
#

export PRODUCTION_BUILD=false
export USE_REGRESSION=0
