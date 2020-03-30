package com.evolving.nglm.evolution.datacubes.odr;

import java.util.Date;

import org.elasticsearch.client.RestHighLevelClient;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.PaymentMeanService;
import com.evolving.nglm.evolution.SalesChannelService;
import com.evolving.nglm.evolution.ScheduledJob;

public class ODRDatacubeOnYesterdayJob extends ScheduledJob
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private ODRDatacubeGenerator datacube;
  
  /*****************************************
  *
  *  constructor
  *  
  *  This ODR datacube will be generated every day at 1:00 am
  *  and it will aggregate data from the previous day.
  *
  *****************************************/
  
  public ODRDatacubeOnYesterdayJob(long schedulingUniqueID, RestHighLevelClient elasticsearch, OfferService offerService, SalesChannelService salesChannelService, PaymentMeanService paymentMeanService, LoyaltyProgramService loyaltyProgramService, JourneyService journeyService) 
  {
    super(schedulingUniqueID, 
        "ODR(yesterday)",
        Deployment.getYesterdayODRDatacubePeriodCronEntryString(), 
        Deployment.getBaseTimeZone(),
        true);
    this.datacube = new ODRDatacubeGenerator(this.jobName, elasticsearch, offerService, salesChannelService, paymentMeanService, loyaltyProgramService, journeyService);
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
    this.datacube.run(yesterday);
  }

}
