package com.evolving.nglm.evolution.datacubes.journeys;

import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.datacubes.ScheduledJob;
import com.evolving.nglm.evolution.datacubes.mapping.GUIManagerClient;
import com.evolving.nglm.evolution.datacubes.mapping.JourneysMap;

public class JourneyTrafficDatacubeDefinitiveJob extends ScheduledJob
{
  protected static final Logger log = LoggerFactory.getLogger(JourneyTrafficDatacubeDefinitiveJob.class);
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private GUIManagerClient guiClient;
  private JourneyTrafficDatacubeGenerator datacube;
  private JourneysMap journeysMap;
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public JourneyTrafficDatacubeDefinitiveJob(long schedulingUniqueID, RestHighLevelClient elasticsearch, GUIManagerClient guiClient) 
  {
    super(schedulingUniqueID, 
        "Definitive-JourneyTraffic",
        Deployment.getJourneyTrafficDatacubePeriodCronEntryString(), 
        Deployment.getBaseTimeZone(),
        true);
    this.guiClient = guiClient;
    this.journeysMap = new JourneysMap();
    this.datacube = new JourneyTrafficDatacubeGenerator(this.jobName, elasticsearch, guiClient);
  }

  /*****************************************
  *
  *  DatacubeScheduling
  *
  *****************************************/
  @Override
  protected void run()
  {
    this.journeysMap.updateFromGUIManager(guiClient);
    for(String journeyID : this.journeysMap.keySet())
      {
        this.datacube.run(journeyID, SystemTime.getCurrentTime());
      }
  }
}
