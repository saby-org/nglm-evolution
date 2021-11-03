package com.evolving.nglm.evolution.elasticsearch;

import java.util.Date;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.ScheduledJob;
import com.evolving.nglm.evolution.ScheduledJobConfiguration;
import com.evolving.nglm.evolution.VoucherService;
import com.evolving.nglm.evolution.datacubes.AsyncScheduledJob;

public class ElasticsearchJobs
{
  public static ScheduledJob createElasticsearchJob(ScheduledJobConfiguration config) throws ServerRuntimeException {
    switch(config.getType())
    {
      case SubscriberProfileSnapshot:
        return Snapshot(config);
      case JourneystatisticCleanUp:
        return JourneystatisticCleanUp(config);
      case ExpiredVoucherCleanUp:
        return VoucherCleanUp(config);
      default:
        throw new ServerRuntimeException("Trying to create an Elasticsearch scheduled job of unknown type.");
    }
  }
  
  /*****************************************
   * 
   * Snapshot 
   *
   *****************************************/
  private static ScheduledJob Snapshot(ScheduledJobConfiguration config) {
    SnapshotTask snapshotTask = new SnapshotTask("Snapshot:subscriberprofile", "subscriberprofile", "subscriberprofile_snapshot", ElasticsearchManager.getElasticsearchClientAPI(), config.getTimeZone());

    return new AsyncScheduledJob(config)
    {
      @Override
      protected void asyncRun()
      {
        Date now = SystemTime.getCurrentTime();
        // This snapshot is done the day after the "saved" day (after midnight, in the morning usually)
        Date yesterday = RLMDateUtils.addDays(now, -1, config.getTimeZone());
        snapshotTask.run(yesterday);
      }
    };
  }

  /*****************************************
   * 
   * Snapshot 
   *
   *****************************************/
  private static ScheduledJob JourneystatisticCleanUp(ScheduledJobConfiguration config) {
    JourneyCleanUpTask journeyCleanUpTask = new JourneyCleanUpTask(ElasticsearchManager.getJourneyService(), ElasticsearchManager.getElasticsearchClientAPI(), config.getTenantID());

    return new AsyncScheduledJob(config)
    {
      @Override
      protected void asyncRun()
      {
        journeyCleanUpTask.start();
      }
    };
  }

  /*****************************************
   * 
   * Snapshot 
   *
   *****************************************/
  private static ScheduledJob VoucherCleanUp(ScheduledJobConfiguration config) {
    VoucherService voucherService = ElasticsearchManager.getVoucherService();

    return new AsyncScheduledJob(config)
    {
      @Override
      protected void asyncRun()
      {
        voucherService.cleanUpVouchersJob(config.getTenantID());
      }
    };
  }

}
