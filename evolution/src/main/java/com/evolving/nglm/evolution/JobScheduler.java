/****************************************************************************
*
*  JobScheduler.java 
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SystemTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class JobScheduler
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(JobScheduler.class);
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private volatile boolean stopRequested = false;
  private SortedSet<ScheduledJob> schedule = new TreeSet<ScheduledJob>();
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public JobScheduler()
  {
  }

  public ScheduledJob findJob(long jobID)
  {
    for (ScheduledJob job : schedule)
      {
        if (job.getjobID() == jobID)
          {
            return job;
          }
      }
    return null;
  }

  
  /*****************************************
  *
  *  getAllJobs
  *  Note that all info in the returned List must be checked before used for validity : jobs might have changed afterwards.
  *  
  *
  *****************************************/

  public List<ScheduledJob> getAllJobs()
  {
    List<ScheduledJob> result = new ArrayList<>();
    synchronized (this)
    {
      //
      //  shallow copy set
      //
      
      for (ScheduledJob job : schedule)
        {
          result.add(job);
        }

      //
      //  notify
      //

      this.notifyAll();
    }
    return result;
  }
  
  /*****************************************
  *
  *  schedule
  *
  *****************************************/

  public void schedule(ScheduledJob job)
  {
    synchronized (this)
      {
        //
        //  add to the schedule
        //
        
        schedule.add(job);

        //
        //  notify
        //

        this.notifyAll();
      }
  }

  /*****************************************
  *
  *  deschedule
  *
  *****************************************/

  public void deschedule(ScheduledJob job)
  {
    synchronized (this)
      {
        //
        //  remove from the schedule
        //

        schedule.remove(job);

        //
        //  notify
        //

        this.notifyAll();
      }
  }

  /*****************************************
  *
  *  stop
  *
  *****************************************/

  public void stop()
  {
    synchronized (this)
      {
        stopRequested = true;
        this.notifyAll();
      }
  }

  /*****************************************
  *
  *  runScheduler
  *
  *****************************************/

  public void runScheduler()
  {
    /*****************************************
    *
    *  runScheduler
    *
    *****************************************/

    NGLMRuntime.registerSystemTimeDependency(this);
    while (! stopRequested)
      {
        /*****************************************
        *
        *  process schedule
        *
        *****************************************/
            
        synchronized (this)
          {
            /*****************************************
            *
            *  process schedule
            *
            *****************************************/

            Date now = SystemTime.getCurrentTime();
            long nextWaitDuration = -1;
            
            //
            // Run all jobs with scheduling date in the past
            //
            
            while (! stopRequested)
              {
                if (schedule.isEmpty())
                  {
                    // If scheduler is entirely empty, wait a bit before trying again. Otherwise process takes all CPU.
                    try { this.wait(1000); } catch (InterruptedException e) {}
                    break;
                  }
                ScheduledJob job = schedule.first();
                if(job == null) 
                  {
                    break;
                  }

                Date nextPeriodicEvaluation = job.getNextGenerationDate();
                if (now.before(nextPeriodicEvaluation))
                  {
                    nextWaitDuration = nextPeriodicEvaluation.getTime() - now.getTime();
                    break;
                  }

                //
                //  run job
                //
                
                schedule.remove(job);
                job.call();
                schedule.add(job);
                
                now = SystemTime.getCurrentTime();
              }
            
            try
              {
                if (nextWaitDuration >= 0)
                  {
                    log.info("Scheduler will now sleep for "+ nextWaitDuration +" ms.");
                    this.wait(nextWaitDuration); // wait till the next scheduled job
                  }
                else 
                  {
                    log.info("Scheduler will now sleep indefinitly.");
                    this.wait(); // wait indefinitely till a notification
                  }
              }
            catch (InterruptedException e)
              {
                //
              }
          }
      }    
  }
}
