package com.evolving.nglm.evolution.datacubes.odr;

import org.elasticsearch.client.RestHighLevelClient;

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
  * Properties
  *
  *****************************************/
  private ODRDatacubeGenerator datacube;
  
  /*****************************************
  *
  * Constructor
  *  
  * This will generated a datacube every day from the detailedrecords_offers-YYYY-MM-dd index of the previous day.
  *
  *****************************************/
  public ODRDatacubeOnYesterdayJob(long schedulingUniqueID, RestHighLevelClient elasticsearch, OfferService offerService, SalesChannelService salesChannelService, PaymentMeanService paymentMeanService, LoyaltyProgramService loyaltyProgramService, JourneyService journeyService) 
  {
    super(schedulingUniqueID, 
        "ODR(definitive)",
        Deployment.getYesterdayODRDatacubePeriodCronEntryString(), 
        Deployment.getBaseTimeZone(),
        false);
    this.datacube = new ODRDatacubeGenerator(this.jobName, elasticsearch, offerService, salesChannelService, paymentMeanService, loyaltyProgramService, journeyService);
  }
  
  /*****************************************
  *
  * DatacubeScheduling
  *
  *****************************************/
  @Override
  protected void run()
  {
    this.datacube.definitive();
  }
}
