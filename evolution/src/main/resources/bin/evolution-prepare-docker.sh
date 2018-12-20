#################################################################################
#
#  evolution-prepare-docker.sh
#
#################################################################################

#########################################
#
#  docker pulls
#
#########################################

for SWARM_HOST in $SWARM_HOSTS
do
   echo "evolution-prepare-docker on $SWARM_HOST"
   ssh $SWARM_HOST "
      docker pull evolving/fwk.api:${gui-fwk.version}
      docker pull evolving/fwkauth.api:${gui-fwk.version}
      docker pull evolving/fwk.web:${gui-fwk.version}
      docker pull evolving/csr.api:${gui-csr.version}
      docker pull evolving/csr.web:${gui-csr.version}
      docker pull evolving/itm.api:${gui-itm.version}
      docker pull evolving/itm.web:${gui-itm.version}
      docker pull evolving/jmr.api:${gui-jmr.version}
      docker pull evolving/jmr.web:${gui-jmr.version}
      docker pull evolving/opc.api:${gui-opc.version}
      docker pull evolving/opc.web:${gui-opc.version}
      docker pull evolving/iar.api:${gui-iar.version}
      docker pull evolving/iar.web:${gui-iar.version}
      docker pull evolving/opr.api:${gui-opr.version}
      docker pull evolving/opr.web:${gui-opr.version}
   " &
done

#
#  wait for all pulls to complete
#

wait
echo "evolution-prepare-docker complete"
