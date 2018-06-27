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
#  regression
#

export PRODUCTION_BUILD=false
export USE_REGRESSION=0
export REGRESSION_DB=Oracle
export DB_ORACLE_DRIVER=oracle.jdbc.driver.OracleDriver
export DB_ORACLE_USER=evol
export DB_ORACLE_PASSWORD=evol
export DB_ORACLE_SID=evoldb
export DB_ORACLE_JDBC_URL=jdbc:oracle:thin:@vubuntu:1521:evoldb
