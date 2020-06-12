package com.evolving.nglm.evolution.datacubes.journeys;

import java.util.Calendar;
import java.util.Date;

import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.ScheduledJob;
import com.evolving.nglm.evolution.SegmentationDimensionService;
import com.evolving.nglm.evolution.datacubes.mapping.JourneysMap;

public class JourneyDatacubesJob extends ScheduledJob
{
  protected static final Logger log = LoggerFactory.getLogger(JourneyDatacubesJob.class);
  
  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private JourneyTrafficDatacubeGenerator trafficDatacube;
  private JourneyRewardsDatacubeGenerator rewardsDatacube;
  private JourneysMap journeysMap;
  
  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public JourneyDatacubesJob(long schedulingUniqueID, RestHighLevelClient elasticsearch, SegmentationDimensionService segmentationDimensionService, JourneyService journeyService)
  {
    super(schedulingUniqueID, 
        "Journey",
        Deployment.getJourneyTrafficDatacubePeriodCronEntryString(), 
        Deployment.getBaseTimeZone(),
        false); // do not launch at start, there is no override mechanism for this datacube
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
    // We need to push all journey datacubes at the same timestamp.
    // For the moment we truncate at the HOUR. 
    // Therefore, we must not configure a cron period lower than 1 hour
    // If we want a lower period we will need to retrieve the schedule due date from the job !
    Date now = SystemTime.getCurrentTime();
    Date truncatedHour = RLMDateUtils.truncate(now, Calendar.HOUR, Deployment.getBaseTimeZone());
    Date endOfLastHour = RLMDateUtils.addMilliseconds(truncatedHour, -1); // XX:59:59.999
    
    this.journeysMap.update();
    for(String journeyID : this.journeysMap.keySet())
      {
        this.trafficDatacube.definitive(journeyID, this.journeysMap.getStartDateTime(journeyID), endOfLastHour);
        this.rewardsDatacube.definitive(journeyID, this.journeysMap.getStartDateTime(journeyID), endOfLastHour);
      }
  }
}
