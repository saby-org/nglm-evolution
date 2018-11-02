/****************************************************************************
*
*  PresentationStrategyService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

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

public class PresentationStrategyService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(PresentationStrategyService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private PresentationStrategyListener presentationStrategyListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public PresentationStrategyService(String bootstrapServers, String groupID, String presentationStrategyTopic, boolean masterService, PresentationStrategyListener presentationStrategyListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "PresentationStrategyService", groupID, presentationStrategyTopic, masterService, getSuperListener(presentationStrategyListener), notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public PresentationStrategyService(String bootstrapServers, String groupID, String presentationStrategyTopic, boolean masterService, PresentationStrategyListener presentationStrategyListener)
  {
    this(bootstrapServers, groupID, presentationStrategyTopic, masterService, presentationStrategyListener, true);
  }

  //
  //  constructor
  //

  public PresentationStrategyService(String bootstrapServers, String groupID, String presentationStrategyTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, presentationStrategyTopic, masterService, (PresentationStrategyListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(PresentationStrategyListener presentationStrategyListener)
  {
    GUIManagedObjectListener superListener = null;
    if (presentationStrategyListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { presentationStrategyListener.presentationStrategyActivated((PresentationStrategy) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(GUIManagedObject guiManagedObject) { presentationStrategyListener.presentationStrategyDeactivated((PresentationStrategy) guiManagedObject); }
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
    result.put("salesChannelIDs", guiManagedObject.getJSONRepresentation().get("salesChannelIDs"));
    result.put("effectiveStartDate", guiManagedObject.getJSONRepresentation().get("effectiveStartDate"));
    result.put("effectiveEndDate", guiManagedObject.getJSONRepresentation().get("effectiveEndDate"));
    return result;
  }
  
  /*****************************************
  *
  *  getPresentationStrategies
  *
  *****************************************/

  public String generatePresentationStrategyID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredPresentationStrategy(String presentationStrategyID) { return getStoredGUIManagedObject(presentationStrategyID); }
  public Collection<GUIManagedObject> getStoredPresentationStrategies() { return getStoredGUIManagedObjects(); }
  public boolean isActivePresentationStrategy(GUIManagedObject presentationStrategyUnchecked, Date date) { return isActiveGUIManagedObject(presentationStrategyUnchecked, date); }
  public PresentationStrategy getActivePresentationStrategy(String presentationStrategyID, Date date) { return (PresentationStrategy) getActiveGUIManagedObject(presentationStrategyID, date); }
  public Collection<PresentationStrategy> getActivePresentationStrategies(Date date) { return (Collection<PresentationStrategy>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putPresentationStrategy
  *
  *****************************************/

  public void putPresentationStrategy(PresentationStrategy presentationStrategy, ScoringStrategyService scoringStrategyService) throws GUIManagerException
  {
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    //  validate scoring strategies
    //

    presentationStrategy.validateScoringStrategies(scoringStrategyService, now);

    //
    //  put
    //

    putGUIManagedObject(presentationStrategy, now);
  }

  /*****************************************
  *
  *  putIncompletePresentationStrategy
  *
  *****************************************/

  public void putIncompletePresentationStrategy(IncompleteObject presentationStrategy)
  {
    putGUIManagedObject(presentationStrategy, SystemTime.getCurrentTime());
  }

  /*****************************************
  *
  *  removePresentationStrategy
  *
  *****************************************/

  public void removePresentationStrategy(String presentationStrategyID) { removeGUIManagedObject(presentationStrategyID, SystemTime.getCurrentTime()); }

  /*****************************************
  *
  *  interface PresentationStrategyListener
  *
  *****************************************/

  public interface PresentationStrategyListener
  {
    public void presentationStrategyActivated(PresentationStrategy presentationStrategy);
    public void presentationStrategyDeactivated(PresentationStrategy presentationStrategy);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  presentationStrategyListener
    //

    PresentationStrategyListener presentationStrategyListener = new PresentationStrategyListener()
    {
      @Override public void presentationStrategyActivated(PresentationStrategy presentationStrategy) { System.out.println("presentationStrategy activated: " + presentationStrategy.getPresentationStrategyID()); }
      @Override public void presentationStrategyDeactivated(PresentationStrategy presentationStrategy) { System.out.println("presentationStrategy deactivated: " + presentationStrategy.getPresentationStrategyID()); }
    };

    //
    //  presentationStrategyService
    //

    PresentationStrategyService presentationStrategyService = new PresentationStrategyService(Deployment.getBrokerServers(), "example-presentationstrategyservice-001", Deployment.getPresentationStrategyTopic(), false, presentationStrategyListener);
    presentationStrategyService.start();

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
