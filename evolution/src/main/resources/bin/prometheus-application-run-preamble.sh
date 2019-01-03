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
sed -i "s/<_GUIMANAGER_HOST_>/${GUIMANAGER_HOST}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_GUIMANAGER_MONITORING_PORT_>/${GUIMANAGER_MONITORING_PORT}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_CRITERIAAPI_HOST_>/${CRITERIAAPI_HOST}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_CRITERIAAPI_MONITORING_PORT_>/${CRITERIAAPI_MONITORING_PORT}/g" /etc/prometheus/prometheus-application.yml
sed -i "s/<_THIRDPARTYMANAGER_PROMETHEUS_>/${THIRDPARTYMANAGER_PROMETHEUS}/g" /etc/prometheus/prometheus-application.yml

#
# remove thirdpartymanager configuration(when not running) 
#

if [ -z "$THIRDPARTYMANAGER_PROMETHEUS" ]; then
  sed -i "/THIRDPARTYMANAGER_PROMETHEUS:/d" /etc/prometheus/prometheus-application.yml
fi
