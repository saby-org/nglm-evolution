/*****************************************************************************
*
*  WorkItemScheduler.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import java.util.Date;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;

public class WorkItemScheduler<E> implements Runnable
{
  private SortedSet<ScheduleEntry> schedule;
  private LinkedBlockingQueue<E> queue;
  private Thread scheduler;
  private boolean isActive = true;
  private static long entryCounter = 0;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public WorkItemScheduler(LinkedBlockingQueue<E> queue, String name)
  {
    this.schedule = new TreeSet<ScheduleEntry>();
    this.queue = queue;
    this.scheduler = new Thread(this, name);
    this.scheduler.start();
  }

  /*****************************************
  *
  *  shutdown
  *
  *****************************************/

  public synchronized void shutdown()
  {
    isActive = false;
    this.notifyAll();
  }
  
  /*****************************************
  *
  *  schedule
  *
  *****************************************/

  public synchronized void schedule(Date time, int priorityClass, E item)
  {
    ScheduleEntry entry = new ScheduleEntry(time,priorityClass,item);
    schedule.add(entry);
    this.notifyAll();
  }

  public void schedule(Date time, E item)
  {
    schedule(time,0,item);
  }

  /*****************************************
  *
  *  size
  *
  *****************************************/

  public synchronized int size()
  {
    return schedule.size();
  }

  /*****************************************
  *
  *  clear
  *
  *****************************************/

  public synchronized void clear()
  {
    schedule.clear();
    this.notifyAll();
  }

  /*****************************************
  *
  *  run
  *
  *****************************************/

  public void run()
  {
    NGLMRuntime.registerSystemTimeDependency(this);    
    while (true)
      {
        synchronized(this)
          {
            /*****************************************
            *
            *  terminate thread if inactive
            *
            *****************************************/

            if (! isActive) return;

            /*****************************************
            *
            *  next entry
            *
            *****************************************/

            try
              {
                ScheduleEntry<E> entry = (ScheduleEntry<E>) ((! schedule.isEmpty()) ? schedule.first() : null);
                Date now = SystemTime.getCurrentTime();
                if (entry != null && ! entry.completeTime.after(now))
                  {
                    schedule.remove(entry);
                    queue.put(entry.getItem());
                  }
                else if (entry != null)
                  {
                    long timeout = entry.completeTime.getTime() - now.getTime();
                    if (timeout > 0) 
                      {
                        this.wait(timeout);
                      }
                  }
                else
                  {
                    this.wait(0);
                  }
              }
            catch (InterruptedException e)
              {
              }
          }
      }
  }

  /*****************************************
  *
  *  ScheduleEntry
  *
  *****************************************/

  private class ScheduleEntry<E> implements Comparable<E>
  {
    //
    //  data
    //

    private Date completeTime;
    private E item;
    private int priorityClass;

    //
    //  uniqueID (for comparator)
    //

    private long uniqueID;

    //
    // constructor
    //

    ScheduleEntry(Date completeTime, int priorityClass, E item)
    {
      this.completeTime = completeTime;
      this.item = item;
      this.priorityClass = priorityClass;
      this.uniqueID = entryCounter++;
    }

    //
    //
    //

    E getItem() { return item; }

    //
    //  equals (for SortedSet)
    //

    public boolean equals(Object obj)
    {
      boolean result = false;
      if (obj instanceof ScheduleEntry)
        {
          ScheduleEntry entry = (ScheduleEntry) obj;
          result = (uniqueID == entry.uniqueID);
        }
      return result;
    }

    //
    //  compare (for SortedSet)
    //

    public int compareTo(Object obj)
    {
      int result = -1;
      if (obj instanceof ScheduleEntry)
        {
          ScheduleEntry entry = (ScheduleEntry) obj;
          result = completeTime.compareTo(entry.completeTime);
          if (result == 0) result = (int) (priorityClass - entry.priorityClass);
          if (result == 0) result = (int) (uniqueID - entry.uniqueID);
        }
      return result;
    }
  }
}
