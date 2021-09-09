# Import E4O flyway image as it is
FROM evolving/ev-e4o-flyway:${flyway.version} as original
#FROM evolving/ev-e4o-flyway:qaveon-dnbo-2.5.0.10.4.6 as original

# Import GUI flyway image as it is
FROM evolving/ev-gui.sqlscripts:${flyway.gui.version} as guioriginal
#FROM evolving/ev-gui.sqlscripts:1.0.70 as guioriginal

# Main upgrade image
FROM confluentinc/cp-zookeeper:${confluent.version}
#FROM confluentinc/cp-zookeeper:6.2.0

USER root

RUN yum install -y perl

RUN yum update; exit 0

RUN yum install -y curl tar

RUN mkdir -p /app

RUN cd /app

RUN curl -O https://repo1.maven.org/maven2/org/flywaydb/flyway-commandline/5.2.1/flyway-commandline-5.2.1.tar.gz && tar -xvzf flyway-commandline-5.2.1.tar.gz && cp -r flyway-5.2.1 /

RUN chmod 0755 /flyway-5.2.1/flyway

COPY databases/ /databases
COPY --from=guioriginal /database/ /databases/

RUN ls -l /app

COPY bin/upgrade-run.sh /app/upgrade-run.sh

RUN chmod +x /app/*.sh
RUN chmod uog+rwx /etc/hosts

# Add data from the E4O flyway image
COPY --from=original /usr/local/flyway/classes /usr/local/flyway/classes

CMD /app/upgrade-run.sh
