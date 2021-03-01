package com.evolving.nglm.evolution.datacubes;

import java.util.Calendar;
import java.util.Date;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.ScheduledJob;
import com.evolving.nglm.evolution.ScheduledJobConfiguration;
import com.evolving.nglm.evolution.datacubes.generator.BDRDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.generator.JourneyRewardsDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.generator.JourneyTrafficDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.generator.MDRDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.generator.ODRDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.generator.ProgramsChangesDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.generator.ProgramsHistoryDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.generator.SubscriberProfileDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.mapping.JourneysMap;

public class DatacubeJobs
{
  public static ScheduledJob createDatacubeJob(ScheduledJobConfiguration config, DatacubeManager datacubeManager) throws ServerRuntimeException {
    switch(config.getType())
    {
      case ODRDailyPreview:
        return ODRDailyPreview(config, datacubeManager);
      case ODRDailyDefinitive:
        return ODRDailyDefinitive(config, datacubeManager);
      case ODRHourlyPreview:
        return ODRHourlyPreview(config, datacubeManager);
      case ODRHourlyDefinitive:
        return ODRHourlyDefinitive(config, datacubeManager);
      case BDRDailyPreview:
        return BDRDailyPreview(config, datacubeManager);
      case BDRDailyDefinitive:
        return BDRDailyDefinitive(config, datacubeManager);
      case BDRHourlyPreview:
        return BDRHourlyPreview(config, datacubeManager);
      case BDRHourlyDefinitive:
        return BDRHourlyDefinitive(config, datacubeManager);
      case MDRDailyPreview:
        return MDRDailyPreview(config, datacubeManager);
      case MDRDailyDefinitive:
        return MDRDailyDefinitive(config, datacubeManager);
      case MDRHourlyPreview:
        return MDRHourlyPreview(config, datacubeManager);
      case MDRHourlyDefinitive:
        return MDRHourlyDefinitive(config, datacubeManager);
      case LoyaltyProgramsPreview:
        return LoyaltyProgramsPreview(config, datacubeManager);
      case LoyaltyProgramsDefinitive:
        return LoyaltyProgramsDefinitive(config, datacubeManager);
      case SubscriberProfilePreview:
        return SubscriberProfilePreview(config, datacubeManager);
      case SubscriberProfileDefinitive:
        return SubscriberProfileDefinitive(config, datacubeManager);
      case Journeys:
        return JourneyDatacubeDefinitive(config, datacubeManager);
      default:
        throw new ServerRuntimeException("Trying to create a datacube scheduled job of unknown type.");
    }
  }
  
  /*****************************************
   * Loyalty programs preview  
   *
   * This will generate a datacube preview of the day from the subscriberprofile index (not a snapshot one).
   * Those data are not definitive, the day is not ended yet, metrics can still change.
   *****************************************/
  private static ScheduledJob LoyaltyProgramsPreview(ScheduledJobConfiguration config, DatacubeManager datacubeManager) {
    // Datacube generators classes are NOT thread-safe and must be used by only one thread (the AsyncJob thread).
    ProgramsHistoryDatacubeGenerator loyaltyHistoryDatacubePreview = new ProgramsHistoryDatacubeGenerator("LoyaltyPrograms:History(Preview)", config.getTenantID(), datacubeManager);
    ProgramsChangesDatacubeGenerator tierChangesDatacubePreview = new ProgramsChangesDatacubeGenerator("LoyaltyPrograms:Changes(Preview)", config.getTenantID(), datacubeManager);
    
    return new AsyncScheduledJob(config)
    {
      @Override
      protected void asyncRun()
      {
        loyaltyHistoryDatacubePreview.preview();
        tierChangesDatacubePreview.preview();
      }
    };
  }

  /*****************************************
   * Loyalty programs definitive  
   *
   * This will generated a datacube every day from the subscriberprofile snapshot index of the previous day.
   *****************************************/
  private static ScheduledJob LoyaltyProgramsDefinitive(ScheduledJobConfiguration config, DatacubeManager datacubeManager) {
    // Datacube generators classes are NOT thread-safe and must be used by only one thread (the AsyncJob thread).
    ProgramsHistoryDatacubeGenerator loyaltyHistoryDatacubeDefinitive = new ProgramsHistoryDatacubeGenerator("LoyaltyPrograms:History(Definitive)", config.getTenantID(), datacubeManager);
    ProgramsChangesDatacubeGenerator tierChangesDatacubeDefinitive = new ProgramsChangesDatacubeGenerator("LoyaltyPrograms:Changes(Definitive)", config.getTenantID(), datacubeManager);
    
    return new AsyncScheduledJob(config)
    {
      @Override
      protected void asyncRun()
      {
        loyaltyHistoryDatacubeDefinitive.definitive();
        tierChangesDatacubeDefinitive.definitive();
      }
    };
  }
  
  /*****************************************
   * SubscriberProfile preview
   *
   * This will generated a datacube preview of the day from the subscriberprofile index (not a snapshot one).
   * Those data are not definitive, the day is not ended yet, metrics can still change.
   *****************************************/
  private static ScheduledJob SubscriberProfilePreview(ScheduledJobConfiguration config, DatacubeManager datacubeManager) {
    // Datacube generators classes are NOT thread-safe and must be used by only one thread (the AsyncJob thread).
    SubscriberProfileDatacubeGenerator subscriberProfileDatacubePreview = new SubscriberProfileDatacubeGenerator("SubscriberProfile(Preview)", config.getTenantID(), datacubeManager);
    
    return new AsyncScheduledJob(config)
    {
      @Override
      protected void asyncRun()
      {
        subscriberProfileDatacubePreview.preview();
      }
    };
  }
  
  /*****************************************
   * SubscriberProfile definitive
   * 
   * This will generated a datacube every day from the subscriberprofile snapshot index of the previous day.
   *****************************************/
  private static ScheduledJob SubscriberProfileDefinitive(ScheduledJobConfiguration config, DatacubeManager datacubeManager) {
    // Datacube generators classes are NOT thread-safe and must be used by only one thread (the AsyncJob thread).
    SubscriberProfileDatacubeGenerator subscriberProfileDatacubeDefinitive = new SubscriberProfileDatacubeGenerator("SubscriberProfile(Definitive)", config.getTenantID(), datacubeManager);
    
    return new AsyncScheduledJob(config)
    {
      @Override
      protected void asyncRun()
      {
        subscriberProfileDatacubeDefinitive.definitive();
      }
    };
  }
  
  /*****************************************
   * ODR daily preview
   *
   * This will generated a datacube preview of the day from the detailedrecords_offers-YYYY-MM-dd index of the current day
   * Those data are not definitive, the day is not ended yet, new ODR can still be added.
   *****************************************/
  private static ScheduledJob ODRDailyPreview(ScheduledJobConfiguration config, DatacubeManager datacubeManager) {
    // Datacube generators classes are NOT thread-safe and must be used by only one thread (the AsyncJob thread).
    ODRDatacubeGenerator dailyOdrDatacubePreview = new ODRDatacubeGenerator("ODR:Daily(Preview)", config.getTenantID(), datacubeManager);
    
    return new AsyncScheduledJob(config)
    {
      @Override
      protected void asyncRun()
      {
        dailyOdrDatacubePreview.dailyPreview();
      }
    };
  }
  
  /*****************************************
   * ODR daily definitive
   *
   * This will generated a datacube every day from the detailedrecords_offers-YYYY-MM-dd index of the previous day.
   *****************************************/
  private static ScheduledJob ODRDailyDefinitive(ScheduledJobConfiguration config, DatacubeManager datacubeManager) {
    // Datacube generators classes are NOT thread-safe and must be used by only one thread (the AsyncJob thread).
    ODRDatacubeGenerator dailyOdrDatacubeDefinitive = new ODRDatacubeGenerator("ODR:Daily(Definitive)", config.getTenantID(), datacubeManager);
    
    return new AsyncScheduledJob(config)
    {
      @Override
      protected void asyncRun()
      {
        dailyOdrDatacubeDefinitive.dailyDefinitive();
      }
    };
  }
  
  
  /*****************************************
   * ODR hourly preview
   *
   * This will generated a datacube preview of every hour from the detailedrecords_offers-YYYY-MM-dd index of the current day
   * Those data are not definitive, the day is not ended yet, new ODR can still be added.
   *****************************************/
  private static ScheduledJob ODRHourlyPreview(ScheduledJobConfiguration config, DatacubeManager datacubeManager) {
    // Datacube generators classes are NOT thread-safe and must be used by only one thread (the AsyncJob thread).
    ODRDatacubeGenerator hourlyOdrDatacubePreview = new ODRDatacubeGenerator("ODR:Hourly(Preview)", config.getTenantID(), datacubeManager);
    
    return new AsyncScheduledJob(config)
    {
      @Override
      protected void asyncRun()
      {
        hourlyOdrDatacubePreview.hourlyPreview();
      }
    };
  }
  
  /*****************************************
   * ODR hourly definitive
   *
   * This will generated a datacube of every hour from the detailedrecords_offers-YYYY-MM-dd index of the previous day.
   *****************************************/
  private static ScheduledJob ODRHourlyDefinitive(ScheduledJobConfiguration config, DatacubeManager datacubeManager) {
    // Datacube generators classes are NOT thread-safe and must be used by only one thread (the AsyncJob thread).
    ODRDatacubeGenerator hourlyOdrDatacubeDefinitive = new ODRDatacubeGenerator("ODR:Hourly(Definitive)", config.getTenantID(), datacubeManager);
    
    return new AsyncScheduledJob(config)
    {
      @Override
      protected void asyncRun()
      {
        hourlyOdrDatacubeDefinitive.hourlyDefinitive();
      }
    };
  }

  /*****************************************
   * BDR daily preview
   *
   * This will generated a datacube preview of the day from the detailedrecords_bonuses-YYYY-MM-dd index of the day
   * Those data are not definitive, the day is not ended yet, new BDR can still be added.
   *****************************************/
  private static ScheduledJob BDRDailyPreview(ScheduledJobConfiguration config, DatacubeManager datacubeManager) {
    // Datacube generators classes are NOT thread-safe and must be used by only one thread (the AsyncJob thread).
    BDRDatacubeGenerator dailyBdrDatacubePreview = new BDRDatacubeGenerator("BDR:Daily(Preview)", config.getTenantID(), datacubeManager);
    
    return new AsyncScheduledJob(config)
    {
      @Override
      protected void asyncRun()
      {
        dailyBdrDatacubePreview.dailyPreview();
      }
    };
  }
  
  /*****************************************
   * BDR daily definitive
   *
   * This will generated a datacube every day from the detailedrecords_bonuses-YYYY-MM-dd index of the previous day.
   *****************************************/
  private static ScheduledJob BDRDailyDefinitive(ScheduledJobConfiguration config, DatacubeManager datacubeManager) {
    // Datacube generators classes are NOT thread-safe and must be used by only one thread (the AsyncJob thread).
    BDRDatacubeGenerator dailyBdrDatacubeDefinitive = new BDRDatacubeGenerator("BDR:Daily(Definitive)", config.getTenantID(), datacubeManager);
    
    return new AsyncScheduledJob(config)
    {
      @Override
      protected void asyncRun()
      {
        dailyBdrDatacubeDefinitive.dailyDefinitive();
      }
    };
  }
  
  /*****************************************
   * BDR hourly preview
   *
   * This will generated a datacube preview of every hour from the detailedrecords_bonuses-YYYY-MM-dd index of the current day
   * Those data are not definitive, the day is not ended yet, new BDR can still be added.
   *****************************************/
  private static ScheduledJob BDRHourlyPreview(ScheduledJobConfiguration config, DatacubeManager datacubeManager) {
    // Datacube generators classes are NOT thread-safe and must be used by only one thread (the AsyncJob thread).
    BDRDatacubeGenerator hourlyBdrDatacubePreview = new BDRDatacubeGenerator("BDR:Hourly(Preview)", config.getTenantID(), datacubeManager);
    
    return new AsyncScheduledJob(config)
    {
      @Override
      protected void asyncRun()
      {
        hourlyBdrDatacubePreview.hourlyPreview();
      }
    };
  }
  
  /*****************************************
   * BDR hourly definitive
   *
   * This will generated a datacube of every hour from the detailedrecords_bonuses-YYYY-MM-dd index of the previous day.
   *****************************************/
  private static ScheduledJob BDRHourlyDefinitive(ScheduledJobConfiguration config, DatacubeManager datacubeManager) {
    // Datacube generators classes are NOT thread-safe and must be used by only one thread (the AsyncJob thread).
    BDRDatacubeGenerator hourlyBdrDatacubeDefinitive = new BDRDatacubeGenerator("BDR:Hourly(Definitive)", config.getTenantID(), datacubeManager);
    
    return new AsyncScheduledJob(config)
    {
      @Override
      protected void asyncRun()
      {
        hourlyBdrDatacubeDefinitive.hourlyDefinitive();
      }
    };
  }

  /*****************************************
   * MDR daily preview
   *
   * This will generated a datacube preview of the day from the detailedrecords_messages-YYYY-MM-dd index of the day
   * Those data are not definitive, the day is not ended yet, new MDR can still be added.
   *****************************************/
  private static ScheduledJob MDRDailyPreview(ScheduledJobConfiguration config, DatacubeManager datacubeManager) {
    // Datacube generators classes are NOT thread-safe and must be used by only one thread (the AsyncJob thread).
    MDRDatacubeGenerator dailyMdrDatacubePreview = new MDRDatacubeGenerator("MDR:Daily(Preview)", config.getTenantID(), datacubeManager);
    
    return new AsyncScheduledJob(config)
    {
      @Override
      protected void asyncRun()
      {
        dailyMdrDatacubePreview.dailyPreview();
      }
    };
  }
  
  /*****************************************
   * MDR daily definitive
   *
   * This will generated a datacube every day from the detailedrecords_messages-YYYY-MM-dd index of the previous day.
   *****************************************/
  private static ScheduledJob MDRDailyDefinitive(ScheduledJobConfiguration config, DatacubeManager datacubeManager) {
    // Datacube generators classes are NOT thread-safe and must be used by only one thread (the AsyncJob thread).
    MDRDatacubeGenerator dailyMdrDatacubeDefinitive = new MDRDatacubeGenerator("MDR:Daily(Definitive)", config.getTenantID(), datacubeManager);
    
    return new AsyncScheduledJob(config)
    {
      @Override
      protected void asyncRun()
      {
        dailyMdrDatacubeDefinitive.dailyDefinitive();
      }
    };
  }
  
  /*****************************************
   * MDR hourly preview
   *
   * This will generated a datacube preview of every hour from the detailedrecords_messages-YYYY-MM-dd index of the current day
   * Those data are not definitive, the day is not ended yet, new MDR can still be added.
   *****************************************/
  private static ScheduledJob MDRHourlyPreview(ScheduledJobConfiguration config, DatacubeManager datacubeManager) {
    // Datacube generators classes are NOT thread-safe and must be used by only one thread (the AsyncJob thread).
    MDRDatacubeGenerator hourlyMdrDatacubePreview = new MDRDatacubeGenerator("MDR:Hourly(Preview)", config.getTenantID(), datacubeManager);
    
    return new AsyncScheduledJob(config)
    {
      @Override
      protected void asyncRun()
      {
        hourlyMdrDatacubePreview.hourlyPreview();
      }
    };
  }
  
  /*****************************************
   * MDR hourly definitive
   *
   * This will generated a datacube of every hour from the detailedrecords_messages-YYYY-MM-dd index of the previous day.
   *****************************************/
  private static ScheduledJob MDRHourlyDefinitive(ScheduledJobConfiguration config, DatacubeManager datacubeManager) {
    // Datacube generators classes are NOT thread-safe and must be used by only one thread (the AsyncJob thread).
    MDRDatacubeGenerator hourlyMdrDatacubeDefinitive = new MDRDatacubeGenerator("MDR:Hourly(Definitive)", config.getTenantID(), datacubeManager);
    
    return new AsyncScheduledJob(config)
    {
      @Override
      protected void asyncRun()
      {
        hourlyMdrDatacubeDefinitive.hourlyDefinitive();
      }
    };
  }

  /*****************************************
   * Journey datacube
   *
   * This will generated both datacube_journeytraffic and datacube_journeyrewards every hour from journeystatistic indexes
   * /!\ Do not launch at start in production, there is no override mechanism for this datacube (no preview)
   * /!\ Do not configure a cron period lower than 1 hour (require code changes)
   *****************************************/
  private static ScheduledJob JourneyDatacubeDefinitive(ScheduledJobConfiguration config, DatacubeManager datacubeManager) {
    // Datacube generators classes are NOT thread-safe and must be used by only one thread (the AsyncJob thread).
    JourneyTrafficDatacubeGenerator trafficDatacube = new JourneyTrafficDatacubeGenerator("Journey:Traffic", config.getTenantID(), datacubeManager);
    JourneyRewardsDatacubeGenerator rewardsDatacube = new JourneyRewardsDatacubeGenerator("Journey:Rewards", config.getTenantID(), datacubeManager);
    DatacubeWriter datacubeWriter = datacubeManager.getDatacubeWriter();
    JourneysMap journeysMap = datacubeManager.getJourneysMap();
    
    return new AsyncScheduledJob(config)
    {
      @Override
      protected void asyncRun()
      {
        // We need to push all journey datacubes at the same timestamp.
        // For the moment we truncate at the HOUR. 
        // Therefore, we must not configure a cron period lower than 1 hour
        // If we want a lower period we will need to retrieve the schedule due date from the job !
        Date now = SystemTime.getCurrentTime();
        Date truncatedHour = RLMDateUtils.truncate(now, Calendar.HOUR, config.getTimeZone());
        Date endOfLastHour = RLMDateUtils.addMilliseconds(truncatedHour, -1); // XX:59:59.999
       
        journeysMap.update();
        
        // Special: All those datacubes are still made sequentially, therefore we prevent any writing during it to optimize computation time.
        datacubeWriter.pause();
        
        for(String journeyID : journeysMap.keySet()) {
          trafficDatacube.definitive(journeyID, journeysMap.getStartDateTime(journeyID), endOfLastHour);
          rewardsDatacube.definitive(journeyID, journeysMap.getStartDateTime(journeyID), endOfLastHour);
        }
        
        // Restart writing if allowed. Flush all data generated
        datacubeWriter.restart();
      }
    };
  }
}
