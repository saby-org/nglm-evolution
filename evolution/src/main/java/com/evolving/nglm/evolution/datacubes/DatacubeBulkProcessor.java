package com.evolving.nglm.evolution.datacubes;

import java.util.LinkedList;
import java.util.List;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;

/**
 * This class must be thread-safe because it will be called by dedicated threads of every datacubes at the same time.
 */
public class DatacubeBulkProcessor // BulkWaitingQueue
{  
  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private BulkRequest waitingQueue;
  private final int waitingQueueNumberLimit;
  private final long waitingQueueSizeLimit;
  private LinkedList<BulkRequest> readyPool;  // BulkRequests waiting to be send
  private Object lock;
  

  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public DatacubeBulkProcessor(int waitingQueueNumberLimit, long waitingQueueSizeLimit) 
  {
    this.waitingQueue = new BulkRequest();
    this.waitingQueueNumberLimit = waitingQueueNumberLimit;
    this.waitingQueueSizeLimit = waitingQueueSizeLimit;
    this.readyPool = new LinkedList<BulkRequest>();
    this.lock = new Object();
  }
  
  /*****************************************
  *
  * Waiting queue
  * Those actions are performed under lock synchronization
  *
  *****************************************/
  // MUST BE CALL IN A SYNCHRONIZED STATEMENT
  private void checkWaitingQueueSize() {
    if(waitingQueue.estimatedSizeInBytes() >= waitingQueueSizeLimit || waitingQueue.numberOfActions() >= waitingQueueNumberLimit) {
      this.readyPool.add(this.waitingQueue);
      // Reset the waiting queue
      this.waitingQueue = new BulkRequest();
    }
  }
  
  public void add(UpdateRequest request) {
    synchronized(lock) {
      checkWaitingQueueSize();
      waitingQueue.add(request);
    }
  }
  
  public void add(List<UpdateRequest> requests) {
    synchronized(lock) {
      for(UpdateRequest request : requests) {
        checkWaitingQueueSize();
        waitingQueue.add(request);
      }
    }
  }

  public void add(IndexRequest request) {
    synchronized(lock) {
      checkWaitingQueueSize();
      waitingQueue.add(request);
    }
  }

  public void add(DeleteRequest request) {
    synchronized(lock) {
      checkWaitingQueueSize();
      waitingQueue.add(request);
    }
  }
  
  // User must test if BulkRequest contains requests
  public BulkRequest pop() {
    synchronized(lock) {
      if(this.readyPool.isEmpty()) {
        BulkRequest result = this.waitingQueue;
        // Reset the waiting queue
        this.waitingQueue = new BulkRequest();
        return result;
      }
      else {
        return this.readyPool.pop();
      }
    }
  }
  
  // Number of actions in pool & waiting queue.
  public long getTotalNumber() {
    synchronized(lock) {
      long result = waitingQueue.numberOfActions();
      for(BulkRequest bulk : readyPool) {
        result += bulk.numberOfActions();
      }
      
      return result;
    }
  }
}
