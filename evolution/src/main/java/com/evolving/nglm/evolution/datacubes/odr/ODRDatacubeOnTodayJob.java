package com.evolving.nglm.evolution.datacubes.odr;

import org.elasticsearch.client.RestHighLevelClient;

import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.PaymentMeanService;
import com.evolving.nglm.evolution.SalesChannelService;
import com.evolving.nglm.evolution.datacubes.ScheduledJob;

public class ODRDatacubeOnTodayJob extends ScheduledJob
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
  * This will generated a datacube preview of the day from the detailedrecords_offers-YYYY-MM-dd index of the day
  * Those data are not definitive, the day is not ended yet, new ODR can still be added.
  *
  *****************************************/
  public ODRDatacubeOnTodayJob(long schedulingUniqueID, RestHighLevelClient elasticsearch, OfferService offerService, SalesChannelService salesChannelService, PaymentMeanService paymentMeanService, LoyaltyProgramService loyaltyProgramService, JourneyService journeyService) 
  {
    super(schedulingUniqueID, 
        "ODR(preview)", 
        Deployment.getTodayODRDatacubePeriodCronEntryString(), 
        Deployment.getBaseTimeZone(),
        true);
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
    this.datacube.preview();
  }

}
