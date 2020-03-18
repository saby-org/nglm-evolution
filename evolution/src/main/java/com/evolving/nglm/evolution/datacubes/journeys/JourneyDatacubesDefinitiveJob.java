package com.evolving.nglm.evolution.datacubes.journeys;

import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.ScheduledJob;
import com.evolving.nglm.evolution.SegmentationDimensionService;
import com.evolving.nglm.evolution.datacubes.mapping.JourneysMap;

public class JourneyDatacubesDefinitiveJob extends ScheduledJob
{
  protected static final Logger log = LoggerFactory.getLogger(JourneyDatacubesDefinitiveJob.class);
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private JourneyTrafficDatacubeGenerator trafficDatacube;
  private JourneyRewardsDatacubeGenerator rewardsDatacube;
  private JourneysMap journeysMap;
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public JourneyDatacubesDefinitiveJob(long schedulingUniqueID, RestHighLevelClient elasticsearch, SegmentationDimensionService segmentationDimensionService, JourneyService journeyService)
  {
    super(schedulingUniqueID, 
        "Journey(definitive)",
        Deployment.getJourneyTrafficDatacubePeriodCronEntryString(), 
        Deployment.getBaseTimeZone(),
        true);
    this.journeysMap = new JourneysMap(journeyService);
    this.trafficDatacube = new JourneyTrafficDatacubeGenerator(this.jobName+":Traffic", elasticsearch, segmentationDimensionService, journeyService);
    this.rewardsDatacube = new JourneyRewardsDatacubeGenerator(this.jobName+":Rewards", elasticsearch, segmentationDimensionService, journeyService);
  }

  /*****************************************
  *
  *  DatacubeScheduling
  *
  *****************************************/
  @Override
  protected void run()
  {
    this.journeysMap.update();
    for(String journeyID : this.journeysMap.keySet())
      {
        this.trafficDatacube.run(journeyID, SystemTime.getCurrentTime());
        this.rewardsDatacube.run(journeyID, SystemTime.getCurrentTime());
      }
  }
}
