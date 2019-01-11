###############################################################################
#
#  upgrade-run-flyway.sh (evolution)
#
###############################################################################

# !!! CONVENTION HERE !!!
# Databases to be migrated are found in src/assembly/resources/db
# Every folder under src/assembly/resources/db is a DB
# Every flyway property to be used for a DB is <<DBNAME>>_PROPERTY
# If it's not there, the default is used: AKA NOT PREFIXED.

# e.g. databse src/assembly/resources/db/dbframework
#      DBFRAMEWORK_URL: jdbc:mysql:192.168.40.123:3306/dbframework
#      DBFRAMEWORK_USER: root
#      DBFRAMEWORK_PASSWORD: pass
#      DRIVER: com.mysql.jdbc.Driver ---- NOT PREFIXED. This is a default

# It will pick up the databases by scanning the src/assembly/resources/db for
# folders at build time and transforming that into the DATABASES env var

echo

flyway --version

echo

echo " **************************************"
echo " ****** NGLM MIGRATIONS ***************"
echo " **************************************"

DB_USER=${DB_USER:-'you_see_this_you_know_you_screwed_up--user'}
DB_PASS=${DB_PASS:-'you_see_this_you_know_you_screwed_up--password'}
DB_DRIVER=${DB_DRIVER:-'com.mysql.cj.jdbc.Driver'}
DB_HOST_IP=${DB_HOST_IP:-'you_see_this_you_know_you_screwed_up--host_ip'}
DB_PORT=${DB_PORT:-'you_see_this_you_know_you_screwed_up--port'}

# All subdirs in /databases are considered DB migration folders for the
# DB with the same name
DBS=`find /databases -mindepth 1 -maxdepth 1 -type d | cut -f3 -d'/' | sort`

for DB_NAME in $DBS
do

  echo
  echo " -- Migrating NGLM database: $DB_NAME"
  LOCATION=/databases/$DB_NAME

  # TODO Move this as a supported post release hook rather than running it here
  WITH_REPL="${DB_NAME}_WITH_REPLACEMENT"
  if [[ 'true' == ${!WITH_REPL} ]]; then
    echo
    echo " -- IN SQL migration file replacement enabled: db $DB_NAME / folder $LOCATION"
    echo
    # THE HORROR! We actually do this
    # Replaces every ENV VAR that contains __REPLACEMENT__ in SQL files
    # E.g. if there's a __REPLACEMENT__X=y env var, this will replace X with y
    # in all SQL files
    for V_NAME in `env | grep '__REPLACEMENT__' | cut -d '=' -f1`
    do
      VAR_NAME=`echo "$V_NAME" | sed 's/__REPLACEMENT__//g'`
      VAR_VALUE=${!V_NAME}
      echo " -- Replacing '$VAR_NAME' with '$VAR_VALUE'"
      find $LOCATION -type f -name '*.sql' | xargs -I {} sed -i.bak -e 's#'$VAR_NAME'#'"$VAR_VALUE"'#g' {}
    done
    rm -Rf $LOCATION/*.bak
    echo
    echo " -- IN SQL migration file replacement done."
    echo
  fi

  # Look up in the environment for database specific settings before falling back to
  # defaults. E.g. for the _meta database it'll look up ENV VARS: _meta_URL, _meta_USER, _meta_PASS, _meta_DRIVER.
  DB_URL_ENV_VAR=${DB_NAME}_DB_URL
  DB_USER_ENV_VAR=${DB_NAME}_DB_USER
  DB_PASS_ENV_VAR=${DB_NAME}_DB_PASS
  DB_DRIVER_ENV_VAR=${DB_NAME}_DB_DRIVER

  THE_URL=${!DB_URL_ENV_VAR}
  THE_USER=${!DB_USER_ENV_VAR}
  THE_PASS=${!DB_PASS_ENV_VAR}
  THE_DRIVER=${!DB_DRIVER_ENV_VAR}

  # DB Specific is not there, then fall back to global defaults
  # If nothing else, then the following env vars should be set:
  # DB_HOST_IP, DB_PORT, DB_USER, DB_PASS
  THE_URL=${THE_URL:-"jdbc:mysql://$DB_HOST_IP:$DB_PORT/$DB_NAME?useSSL=false"}
  THE_USER=${THE_USER:-$DB_USER}
  THE_PASS=${THE_PASS:-$DB_PASS}
  THE_DRIVER=${THE_DRIVER:-$DB_DRIVER}

  echo " -- URL: $THE_URL"
  echo " -- Driver: $THE_DRIVER"
  echo " -- User: $THE_USER"
  echo " -- Migrations: $LOCATION"
  echo

  DB_UP=1
  while [[ $DB_UP != 0 ]]; do
    echo " -- Checking NGLM db '$DB_NAME'..."
    if [[ 0 -lt `flyway info -url=$THE_URL -user=$THE_USER -password=$THE_PASS -driver=$THE_DRIVER -locations=filesystem:$LOCATION | grep 'Installed On' | wc -l` ]]; then
      echo " -- NGLM db '$DB_NAME' is up!"
      ((DB_UP--))
    else
      sleep 1
    fi
  done

  echo
  flyway info -url=$THE_URL -user=$THE_USER -password=$THE_PASS -driver=$THE_DRIVER -locations=filesystem:$LOCATION
  echo

  flyway migrate -url=$THE_URL -user=$THE_USER -password=$THE_PASS -driver=$THE_DRIVER -locations=filesystem:$LOCATION

done

echo
echo " **************************************"
echo " ****** NGLM MIGRATIONS DONE **********"
echo " **************************************"
echo

