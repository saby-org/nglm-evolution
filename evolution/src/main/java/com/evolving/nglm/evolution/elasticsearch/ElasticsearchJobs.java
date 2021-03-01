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
  public static ScheduledJob createElasticsearchJob(ScheduledJobConfiguration config, ElasticsearchManager elasticsearchManager) throws ServerRuntimeException {
    switch(config.getType())
    {
      case SubscriberProfileSnapshot:
        return Snapshot(config, elasticsearchManager);
      case JourneystatisticCleanUp:
        return JourneystatisticCleanUp(config, elasticsearchManager);
      case ExpiredVoucherCleanUp:
        return VoucherCleanUp(config, elasticsearchManager);
      default:
        throw new ServerRuntimeException("Trying to create an Elasticsearch scheduled job of unknown type.");
    }
  }
  
  /*****************************************
   * 
   * Snapshot 
   *
   *****************************************/
  private static ScheduledJob Snapshot(ScheduledJobConfiguration config, ElasticsearchManager elasticsearchManager) {
    SnapshotTask snapshotTask = new SnapshotTask("Snapshot:subscriberprofile", "subscriberprofile", "subscriberprofile_snapshot", elasticsearchManager.getElasticsearchClientAPI(), config.getTimeZone());

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
  private static ScheduledJob JourneystatisticCleanUp(ScheduledJobConfiguration config, ElasticsearchManager elasticsearchManager) {
    JourneyCleanUpTask journeyCleanUpTask = new JourneyCleanUpTask(elasticsearchManager.getJourneyService(), elasticsearchManager.getElasticsearchClientAPI(), config.getTenantID());

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
  private static ScheduledJob VoucherCleanUp(ScheduledJobConfiguration config, ElasticsearchManager elasticsearchManager) {
    VoucherService voucherService = elasticsearchManager.getVoucherService();

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
