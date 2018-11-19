/****************************************************************************
*
*  OfferObjectiveService.java
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

public class OfferObjectiveService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(OfferObjectiveService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private OfferObjectiveListener offerObjectiveListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public OfferObjectiveService(String bootstrapServers, String groupID, String catalogObjectiveTopic, boolean masterService, OfferObjectiveListener offerObjectiveListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "OfferObjectiveService", groupID, catalogObjectiveTopic, masterService, getSuperListener(offerObjectiveListener), "putOfferObjective", "removeOfferObjective", notifyOnSignificantChange);
  }
  //
  //  constructor
  //
  
  public OfferObjectiveService(String bootstrapServers, String groupID, String offerObjectiveTopic, boolean masterService, OfferObjectiveListener offerObjectiveListener)
  {
    this(bootstrapServers, groupID, offerObjectiveTopic, masterService, offerObjectiveListener, true);
  }

  //
  //  constructor
  //

  public OfferObjectiveService(String bootstrapServers, String groupID, String offerObjectiveTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, offerObjectiveTopic, masterService, (OfferObjectiveListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(OfferObjectiveListener offerObjectiveListener)
  {
    GUIManagedObjectListener superListener = null;
    if (offerObjectiveListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { offerObjectiveListener.offerObjectiveActivated((OfferObjective) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { offerObjectiveListener.offerObjectiveDeactivated(guiManagedObjectID); }
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
    result.put("name", guiManagedObject.getJSONRepresentation().get("name"));
    result.put("display", guiManagedObject.getJSONRepresentation().get("display"));
    result.put("icon", guiManagedObject.getJSONRepresentation().get("icon"));
    return result;
  }
  
  /*****************************************
  *
  *  getOfferObjectives
  *
  *****************************************/

  public String generateOfferObjectiveID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredOfferObjective(String offerObjectiveID) { return getStoredGUIManagedObject(offerObjectiveID); }
  public Collection<GUIManagedObject> getStoredOfferObjectives() { return getStoredGUIManagedObjects(); }
  public boolean isActiveOfferObjective(GUIManagedObject offerObjectiveUnchecked, Date date) { return isActiveGUIManagedObject(offerObjectiveUnchecked, date); }
  public OfferObjective getActiveOfferObjective(String offerObjectiveID, Date date) { return (OfferObjective) getActiveGUIManagedObject(offerObjectiveID, date); }
  public Collection<OfferObjective> getActiveOfferObjectives(Date date) { return (Collection<OfferObjective>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putOfferObjective
  *
  *****************************************/

  public void putOfferObjective(GUIManagedObject offerObjective, boolean newObject, String userID) { putGUIManagedObject(offerObjective, SystemTime.getCurrentTime(), newObject, userID); }

  /*****************************************
  *
  *  putIncompleteOfferObjective
  *
  *****************************************/

  public void putIncompleteOfferObjective(IncompleteObject offerObjective, boolean newObject, String userID)
  {
    putGUIManagedObject(offerObjective, SystemTime.getCurrentTime(), newObject, userID);
  }

  /*****************************************
  *
  *  removeOfferObjective
  *
  *****************************************/

  public void removeOfferObjective(String offerObjectiveID, String userID) { removeGUIManagedObject(offerObjectiveID, SystemTime.getCurrentTime(), userID); }

  /*****************************************
  *
  *  interface OfferObjectiveListener
  *
  *****************************************/

  public interface OfferObjectiveListener
  {
    public void offerObjectiveActivated(OfferObjective offerObjective);
    public void offerObjectiveDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  offerObjectiveListener
    //

    OfferObjectiveListener offerObjectiveListener = new OfferObjectiveListener()
    {
      @Override public void offerObjectiveActivated(OfferObjective offerObjective) { System.out.println("offerObjective activated: " + offerObjective.getOfferObjectiveID()); }
      @Override public void offerObjectiveDeactivated(String guiManagedObjectID) { System.out.println("offerObjective deactivated: " + guiManagedObjectID); }
    };

    //
    //  offerObjectiveService
    //

    OfferObjectiveService offerObjectiveService = new OfferObjectiveService(Deployment.getBrokerServers(), "example-offerObjectiveservice-001", Deployment.getOfferObjectiveTopic(), false, offerObjectiveListener);
    offerObjectiveService.start();

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
