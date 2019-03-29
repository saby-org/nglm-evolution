FROM openjdk:8-jre-alpine

#
#  FakeSMTP
#

RUN mkdir -p /opt && wget -q http://nilhcem.github.com/FakeSMTP/downloads/fakeSMTP-latest.zip && unzip fakeSMTP-latest.zip -d /opt && rm fakeSMTP-latest.zip
RUN mv /opt/fakeSMTP*.jar /opt/fakeSMTP.jar

#
#  /var/mail
#

VOLUME ["/var/mail"]

#
#  run
#

CMD java -jar /opt/fakeSMTP.jar --start-server --background --output-dir /var/mail --port $SMTP_PORT
