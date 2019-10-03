/****************************************************************************
*
*  ScoringStrategyService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;

import com.evolving.nglm.core.SystemTime;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import org.json.simple.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ScoringStrategyService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(ScoringStrategyService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private ScoringStrategyListener scoringStrategyListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ScoringStrategyService(String bootstrapServers, String groupID, String scoringStrategyTopic, boolean masterService, ScoringStrategyListener scoringStrategyListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "ScoringStrategyService", groupID, scoringStrategyTopic, masterService, getSuperListener(scoringStrategyListener), "putScoringStrategy", "removeScoringStrategy", notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public ScoringStrategyService(String bootstrapServers, String groupID, String scoringStrategyTopic, boolean masterService, ScoringStrategyListener scoringStrategyListener)
  {
    this(bootstrapServers, groupID, scoringStrategyTopic, masterService, scoringStrategyListener, true);
  }

  //
  //  constructor
  //

  public ScoringStrategyService(String bootstrapServers, String groupID, String scoringStrategyTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, scoringStrategyTopic, masterService, (ScoringStrategyListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(ScoringStrategyListener scoringStrategyListener)
  {
    GUIManagedObjectListener superListener = null;
    if (scoringStrategyListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { scoringStrategyListener.scoringStrategyActivated((ScoringStrategy) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { scoringStrategyListener.scoringStrategyDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getSummaryJSONRepresentation
  *
  *****************************************/

  @Override protected JSONObject getSummaryJSONRepresentation(GUIManagedObject guiManagedObject)
  {
    JSONObject result = super.getSummaryJSONRepresentation(guiManagedObject);
    result.put("offerTypeID", guiManagedObject.getJSONRepresentation().get("offerTypeID"));
    return result;
  }
  
  /*****************************************
  *
  *  getScoringStrategies
  *
  *****************************************/

  public String generateScoringStrategyID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredScoringStrategy(String scoringStrategyID) { return getStoredGUIManagedObject(scoringStrategyID); }
  public GUIManagedObject getStoredScoringStrategy(String scoringStrategyID, boolean includeArchived) { return getStoredGUIManagedObject(scoringStrategyID, includeArchived); }
  public Collection<GUIManagedObject> getStoredScoringStrategies() { return getStoredGUIManagedObjects(); }
  public Collection<GUIManagedObject> getStoredScoringStrategies(boolean includeArchived) { return getStoredGUIManagedObjects(includeArchived); }
  public boolean isActiveScoringStrategy(GUIManagedObject scoringStrategyUnchecked, Date date) { return isActiveGUIManagedObject(scoringStrategyUnchecked, date); }
  public ScoringStrategy getActiveScoringStrategy(String scoringStrategyID, Date date) { return (ScoringStrategy) getActiveGUIManagedObject(scoringStrategyID, date); }
  public Collection<ScoringStrategy> getActiveScoringStrategies(Date date) { return (Collection<ScoringStrategy>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putScoringStrategy
  *
  *****************************************/

  public void putScoringStrategy(GUIManagedObject scoringStrategy, boolean newObject, String userID) { putGUIManagedObject(scoringStrategy, SystemTime.getCurrentTime(), newObject, userID); }

  /*****************************************
  *
  *  removeScoringStrategy
  *
  *****************************************/

  public void removeScoringStrategy(String scoringStrategyID, String userID) { removeGUIManagedObject(scoringStrategyID, SystemTime.getCurrentTime(), userID); }

  /*****************************************
  *
  *  interface ScoringStrategyListener
  *
  *****************************************/

  public interface ScoringStrategyListener
  {
    public void scoringStrategyActivated(ScoringStrategy scoringStrategy);
    public void scoringStrategyDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  scoringStrategyListener
    //

    ScoringStrategyListener scoringStrategyListener = new ScoringStrategyListener()
    {
      @Override public void scoringStrategyActivated(ScoringStrategy scoringStrategy) { System.out.println("scoringStrategy activated: " + scoringStrategy.getScoringStrategyID()); }
      @Override public void scoringStrategyDeactivated(String guiManagedObjectID) { System.out.println("scoringStrategy deactivated: " + guiManagedObjectID); }
    };

    //
    //  scoringStrategyService
    //

    ScoringStrategyService scoringStrategyService = new ScoringStrategyService(Deployment.getBrokerServers(), "example-scoringstrategyservice-001", Deployment.getScoringStrategyTopic(), false, scoringStrategyListener);
    scoringStrategyService.start();

    //
    //  sleep forever
    //

    while (true)
      {
        try
          {
            Thread.sleep(Long.MAX_VALUE);
          }
        catch (InterruptedException e)
          {
            //
            //  ignore
            //
          }
      }
  }
}
