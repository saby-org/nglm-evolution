package com.evolving.nglm.evolution.datacubes.odr;

import java.util.Date;

import org.elasticsearch.client.RestHighLevelClient;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.PaymentMeanService;
import com.evolving.nglm.evolution.SalesChannelService;
import com.evolving.nglm.evolution.ScheduledJob;

public class ODRDatacubeOnTodayJob extends ScheduledJob
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
  *  This ODR datacube will be generated every hours and will aggregate current data from today !
  *  Every hour will update the previous datacube of the day, according to new data.
  *
  *****************************************/
  
  public ODRDatacubeOnTodayJob(long schedulingUniqueID, RestHighLevelClient elasticsearch, OfferService offerService, SalesChannelService salesChannelService, PaymentMeanService paymentMeanService, LoyaltyProgramService loyaltyProgramService, JourneyService journeyService) 
  {
    super(schedulingUniqueID, 
        "ODR(today)", 
        Deployment.getTodayODRDatacubePeriodCronEntryString(), 
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
    this.datacube.run(now);
  }

}
