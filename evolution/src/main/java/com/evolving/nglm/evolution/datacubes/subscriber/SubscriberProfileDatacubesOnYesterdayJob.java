package com.evolving.nglm.evolution.datacubes.subscriber;

import java.util.Date;

import org.elasticsearch.client.RestHighLevelClient;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.SegmentationDimensionService;
import com.evolving.nglm.evolution.datacubes.ScheduledJob;

public class SubscriberProfileDatacubesOnYesterdayJob extends ScheduledJob
{
  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  private SubscriberProfileDatacubeGenerator subscriberProfileDatacube;
  
  /*****************************************
  *
  *  constructor
  *  
  *  This will generated a datacube every day at 1:00 am from the subscriberprofile snapshot index of the previous day.
  *
  *****************************************/
  
  public SubscriberProfileDatacubesOnYesterdayJob(long schedulingUniqueID, RestHighLevelClient elasticsearch, SegmentationDimensionService segmentationDimensionService) 
  {
    super(schedulingUniqueID, 
        "SubscriberProfile(yesterday)", 
        Deployment.getYesterdaySubscriberDatacubePeriodCronEntryString(), 
        Deployment.getBaseTimeZone(),
        true);
    this.subscriberProfileDatacube = new SubscriberProfileDatacubeGenerator(this.jobName, elasticsearch, segmentationDimensionService);
  }
  
  /*****************************************
  *
  *  DatacubeScheduling
  *
  *****************************************/
  @Override
  protected void run()
  {
    Date now = SystemTime.getCurrentTime();
    Date yesterday = RLMDateUtils.addDays(now, -1, Deployment.getBaseTimeZone());
    this.subscriberProfileDatacube.run(yesterday, false);
  }

}
