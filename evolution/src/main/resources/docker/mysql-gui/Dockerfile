FROM mysql:${mysql.gui.version}

# Vanilla MySQL, we just fix the version so far.
# Configs to follow

# Scripts placed here are executed at EVERY container start
# !!! MAKE SURE THEY ARE 1. IDEMPOTENT 2. NON DESTRUCTIVE !!!
# Look into the create_meta_db.sql for details on that
COPY create_meta_db_if_not_exists.sql /docker-entrypoint-initdb.d/
