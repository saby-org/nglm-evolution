package com.evolving.nglm.evolution.datacubes;

import com.evolving.nglm.evolution.ScheduledJob;
import com.evolving.nglm.evolution.ScheduledJobConfiguration;

public abstract class AsyncScheduledJob extends ScheduledJob
{
  /*****************************************
  *
  *  data
  *
  *****************************************/
  private boolean running = false;

  /*****************************************
  *
  *  constructors
  *
  *****************************************/
  public AsyncScheduledJob(ScheduledJobConfiguration config)
  {
    super(config);
  }

  /*****************************************
  *
  *  methods
  *
  *****************************************/
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
