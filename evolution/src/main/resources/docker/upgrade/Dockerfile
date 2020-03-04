# Import E4O flyway image as it is
FROM evolving/ev-e4o-flyway:${flyway.version} as original

# Import GUI flyway image as it is
FROM evolving/ev-gui.sqlscripts:${flyway.gui.version} as guioriginal

# Main upgrade image
FROM confluentinc/cp-zookeeper:${confluent.version}

RUN apt-get update; exit 0

RUN apt-get install -y curl tar

RUN mkdir -p /app

RUN curl -O https://repo1.maven.org/maven2/org/flywaydb/flyway-commandline/5.2.1/flyway-commandline-5.2.1.tar.gz && \
  tar -xzf flyway-commandline-5.2.1.tar.gz

RUN chmod 0755 /flyway-5.2.1/flyway

COPY databases/ /databases
COPY --from=guioriginal /database/ /databases/

COPY bin/upgrade-run.sh /app/upgrade-run.sh

RUN chmod +x /app/*.sh
RUN chmod uog+rwx /etc/hosts

# Add data from the E4O flyway image
COPY --from=original /usr/local/flyway/classes /usr/local/flyway/classes

CMD /app/upgrade-run.sh
