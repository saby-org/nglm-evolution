package com.evolving.nglm.evolution.propensity;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import org.json.simple.JSONObject;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

public class PropensityState {

  // those are what is stored in zookeeper (you don't increment directly those one)
  private AtomicLong presentationCount;
  private final static String presentationCountJsonKey = "presentationCount";
  private AtomicLong acceptanceCount;
  private final static String acceptanceCountJsonKey = "acceptanceCount";
  // those are a local in memory not yet commit to zookeeper (so on crash we loose those!), that the ones we do increment
  private AtomicLong localPresentationCount;
  private AtomicLong localAcceptanceCount;
  // those are a local in memory under commit to zookeeper (those hold what should be currently being writing to zookeeper, among the previous local ones, but not confirmed yet write is OK)
  private AtomicLong localUnderCommitPresentationCount;
  private AtomicLong localUnderCommitAcceptanceCount;

  // the global view accessors
  public long getAcceptanceCount(){return acceptanceCount.get();}
  public long getPresentationCount() {return presentationCount.get();}
  public void addAcceptanceCount(Long toAdd){this.acceptanceCount.getAndAdd(toAdd);}
  public void addPresentationCount(Long toAdd){this.presentationCount.getAndAdd(toAdd);}
  public void setAcceptanceCount(Long value){this.acceptanceCount.set(value);}
  public void setPresentationCount(Long value){this.presentationCount.set(value);}

  // the local view accessors
  public void incrementLocalPresentationCount(){this.localPresentationCount.incrementAndGet();}
  public void incrementLocalAcceptanceCount(){this.localAcceptanceCount.incrementAndGet();}
  // called while going to try to commit to zookeeper
  public long getLocalPresentationCountForCommit(){
    Long toRet = this.localPresentationCount.get();
    this.localUnderCommitPresentationCount.set(toRet);
    return toRet;
  }
  public long getLocalAcceptanceCountForCommit(){
    Long toRet = this.localAcceptanceCount.get();
    this.localUnderCommitAcceptanceCount.set(toRet);
    return toRet;
  }
  // called once commit success to zookeeper
  public void confirmCommit(){
    this.localPresentationCount.addAndGet(this.localUnderCommitPresentationCount.getAndSet(0L)*-1);
    this.localAcceptanceCount.addAndGet(this.localUnderCommitAcceptanceCount.getAndSet(0L)*-1);
  }


  public PropensityState(long presentationCount, long acceptanceCount, long localPresentationCount, long localAcceptanceCount) {
    this.presentationCount = new AtomicLong(presentationCount);
    this.acceptanceCount = new AtomicLong(acceptanceCount);
    this.localPresentationCount = new AtomicLong(localPresentationCount);
    this.localAcceptanceCount = new AtomicLong(localAcceptanceCount);
    this.localUnderCommitPresentationCount = new AtomicLong(0L);
    this.localUnderCommitAcceptanceCount = new AtomicLong(0L);
  }

  public PropensityState() {
    this(0L,0L,0L,0L);
  }

  public PropensityState(JSONObject jsonObject) {
    this();
    this.presentationCount.getAndSet(JSONUtilities.decodeLong(jsonObject,presentationCountJsonKey,0L));
    this.acceptanceCount.getAndSet(JSONUtilities.decodeLong(jsonObject,acceptanceCountJsonKey,0L));
  }

  public JSONObject toJson() {
    JSONObject toReturn=new JSONObject();
    toReturn.put(presentationCountJsonKey,getPresentationCount());
    toReturn.put(acceptanceCountJsonKey,getAcceptanceCount());
    return toReturn;
  }

  //  Propensity computation
  // @param presentationThreshold: number of presentation needed to return the actual propensity (without using a weighted propensity with the initial one)
  // @param daysThreshold: during this time window we will return a weighted propensity using the number of days since the effective start date
  public double getPropensity(double initialPropensity, Date effectiveStartDate, int presentationThreshold, int daysThreshold, int tenantID) {
    double currentPropensity = initialPropensity;
    // we sum up global data we got from zookeeper + the one we just have locally yet (it is a "bit" more accurate that just the global one counters)
    long acceptanceCount = this.acceptanceCount.get() + this.localAcceptanceCount.get();
    long presentationCount = this.presentationCount.get() + this.presentationCount.get();

    if (presentationCount>0) {
      currentPropensity = ((double) acceptanceCount) / ((double) presentationCount);
    }
    if(presentationCount < presentationThreshold) {
      double lambda = RLMDateUtils.daysBetween(effectiveStartDate, SystemTime.getCurrentTime(), Deployment.getDeployment(tenantID).getBaseTimeZone()) / ((double) daysThreshold);
      if(lambda < 1) {
        return currentPropensity * lambda + initialPropensity * (1 - lambda);
      }
    }
    return currentPropensity;
  }

  @Override
  public String toString() {
    return "PropensityState{" +
            "presentationCount=" + presentationCount +
            ", acceptanceCount=" + acceptanceCount +
            ", localPresentationCount=" + localPresentationCount +
            ", localAcceptanceCount=" + localAcceptanceCount +
            ", localUnderCommitPresentationCount=" + localUnderCommitPresentationCount +
            ", localUnderCommitAcceptanceCount=" + localUnderCommitAcceptanceCount +
            '}';
  }
}
