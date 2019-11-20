/****************************************************************************
*
*  DatacubeScheduler.java 
*
****************************************************************************/

package com.evolving.nglm.evolution.datacubes;

import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SystemTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.SortedSet;
import java.util.TreeSet;

public class DatacubeScheduler
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(DatacubeScheduler.class);
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private volatile boolean stopRequested = false;
  private SortedSet<DatacubeScheduling> schedule = new TreeSet<DatacubeScheduling>();
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public DatacubeScheduler()
  {
  }

  /*****************************************
  *
  *  schedule
  *
  *****************************************/

  public void schedule(DatacubeScheduling datacube)
  {
    synchronized (this)
      {
        //
        //  add to the schedule
        //
        
        schedule.add(datacube);

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

  public void deschedule(DatacubeScheduling datacube)
  {
    synchronized (this)
      {
        //
        //  remove from the schedule
        //

        schedule.remove(datacube);

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
            // Generate all datacubes with scheduling date in the past
            //
            
            while (! stopRequested)
              {
                DatacubeScheduling datacube = schedule.first();
                if(datacube == null) 
                  {
                    break;
                  }

                Date nextPeriodicEvaluation = datacube.getNextGenerationDate();
                if (now.before(nextPeriodicEvaluation))
                  {
                    nextWaitDuration = nextPeriodicEvaluation.getTime() - now.getTime();
                    break;
                  }

                //
                //  generate datacube
                //
                
                schedule.remove(datacube);
                datacube.generate();
                schedule.add(datacube);
                
                now = SystemTime.getCurrentTime();
              }
            
            try
              {
                if (nextWaitDuration >= 0)
                  {
                    log.info("Scheduler will now sleep for "+ nextWaitDuration +" ms.");
                    this.wait(nextWaitDuration); // wait till the next scheduled datacube generation
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
