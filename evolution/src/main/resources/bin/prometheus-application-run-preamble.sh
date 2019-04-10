#!/bin/env sh

#################################################################################
#
#  prometheus-run.sh
#
#################################################################################

#
#  sets
#

set -o errexit \
    -o verbose \
    -o xtrace

#
#  update configuration file
#

sed -i "s/<_MONITORING_HOST_>/${MONITORING_HOST}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_PROMETHEUS_APPLICATION_PORT_>/${PROMETHEUS_APPLICATION_PORT}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_EVOLUTIONENGINE_PROMETHEUS_>/${EVOLUTIONENGINE_PROMETHEUS}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_PROPENSITYENGINE_PROMETHEUS_>/${PROPENSITYENGINE_PROMETHEUS}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_UCGENGINE_PROMETHEUS_>/${UCGENGINE_PROMETHEUS}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_INFULFILLMENTMANAGER_PROMETHEUS_>/${INFULFILLMENTMANAGER_PROMETHEUS}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_EMPTYFULFILLMENTMANAGER_PROMETHEUS_>/${EMPTYFULFILLMENTMANAGER_PROMETHEUS}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_POINTTYPEFULFILLMENTMANAGER_PROMETHEUS_>/${POINTTYPEFULFILLMENTMANAGER_PROMETHEUS}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_COMMODITYDELIVERYMANAGER_PROMETHEUS_>/${COMMODITYDELIVERYMANAGER_PROMETHEUS}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_PURCHASEFULFILLMENTMANAGER_PROMETHEUS_>/${PURCHASEFULFILLMENTMANAGER_PROMETHEUS}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_NOTIFICATIONMANAGER_SMS_PROMETHEUS_>/${NOTIFICATIONMANAGER_SMS_PROMETHEUS}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_NOTIFICATIONMANAGER_MAIL_PROMETHEUS_>/${NOTIFICATIONMANAGER_MAIL_PROMETHEUS}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_GUIMANAGER_HOST_>/${GUIMANAGER_HOST}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_GUIMANAGER_MONITORING_PORT_>/${GUIMANAGER_MONITORING_PORT}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_THIRDPARTYMANAGER_PROMETHEUS_>/${THIRDPARTYMANAGER_PROMETHEUS}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_DNBOPROXY_PROMETHEUS_>/${DNBOPROXY_PROMETHEUS}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_REPORTMANAGER_PROMETHEUS_>/${REPORTMANAGER_PROMETHEUS}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_REPORTSCHEDULER_PROMETHEUS_>/${REPORTSCHEDULER_PROMETHEUS}/g" /etc/prometheus/prometheus-application.yml
