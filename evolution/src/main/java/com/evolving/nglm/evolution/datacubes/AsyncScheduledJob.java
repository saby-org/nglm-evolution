package com.evolving.nglm.evolution.datacubes;

import com.evolving.nglm.evolution.ScheduledJob;

public abstract class AsyncScheduledJob extends ScheduledJob
{
  private boolean running = false;
  
  public AsyncScheduledJob(long schedulingUniqueID, String jobName, String periodicGenerationCronEntry, String baseTimeZone, boolean scheduleAtStart)
  {
    super(schedulingUniqueID, jobName, periodicGenerationCronEntry, baseTimeZone, scheduleAtStart);
  }
  
  public void finish() {
    log.info("Job-{" + this.jobName + "}: Asynchronous job just finished.");
    this.running = false;
  }
  
  protected abstract void asyncRun();
  
  private Runnable executeInThread = new Runnable() {
    @Override public void run() {
      asyncRun();
      finish();
    }
  };

  @Override
  protected void run()
  {
    if(running) {
      log.warn("Job-{" + this.jobName + "}: Cancelled: the previous instance of this job is still running.");
      return;
    }
    
    running = true;
    // Start a new thread that will call asyncRun() and then callback finish()
    new Thread(executeInThread).start();
  }
}
