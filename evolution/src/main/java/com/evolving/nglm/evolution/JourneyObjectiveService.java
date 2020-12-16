/****************************************************************************
*
*  JourneyObjectiveService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
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
import org.json.simple.JSONArray;
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

public class JourneyObjectiveService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(JourneyObjectiveService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private JourneyObjectiveListener journeyObjectiveListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  @Deprecated // groupID not needed
  public JourneyObjectiveService(String bootstrapServers, String groupID, String catalogObjectiveTopic, boolean masterService, JourneyObjectiveListener journeyObjectiveListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "JourneyObjectiveService", groupID, catalogObjectiveTopic, masterService, getSuperListener(journeyObjectiveListener), "putJourneyObjective", "removeJourneyObjective", notifyOnSignificantChange);
  }
  //
  //  constructor
  //

  @Deprecated // groupID not needed
  public JourneyObjectiveService(String bootstrapServers, String groupID, String journeyObjectiveTopic, boolean masterService, JourneyObjectiveListener journeyObjectiveListener)
  {
    this(bootstrapServers, groupID, journeyObjectiveTopic, masterService, journeyObjectiveListener, true);
  }

  //
  //  constructor
  //

  @Deprecated // groupID not needed
  public JourneyObjectiveService(String bootstrapServers, String groupID, String journeyObjectiveTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, journeyObjectiveTopic, masterService, (JourneyObjectiveListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(JourneyObjectiveListener journeyObjectiveListener)
  {
    GUIManagedObjectListener superListener = null;
    if (journeyObjectiveListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { journeyObjectiveListener.journeyObjectiveActivated((JourneyObjective) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { journeyObjectiveListener.journeyObjectiveDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getJourneyObjectives
  *
  *****************************************/

  public String generateJourneyObjectiveID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredJourneyObjective(String journeyObjectiveID, int tenantID) { return getStoredGUIManagedObject(journeyObjectiveID, tenantID); }
  public GUIManagedObject getStoredJourneyObjective(String journeyObjectiveID, boolean includeArchived, int tenantID) { return getStoredGUIManagedObject(journeyObjectiveID, includeArchived, tenantID); }
  public Collection<GUIManagedObject> getStoredJourneyObjectives(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredJourneyObjectives(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActiveJourneyObjective(GUIManagedObject journeyObjectiveUnchecked, Date date) { return isActiveGUIManagedObject(journeyObjectiveUnchecked, date); }
  public JourneyObjective getActiveJourneyObjective(String journeyObjectiveID, Date date, int tenantID) { return (JourneyObjective) getActiveGUIManagedObject(journeyObjectiveID, date, tenantID); }
  public Collection<JourneyObjective> getActiveJourneyObjectives(Date date, int tenantID) { return (Collection<JourneyObjective>) getActiveGUIManagedObjects(date, tenantID); }

  /*****************************************
  *
  *  putJourneyObjective
  *
  *****************************************/

  public void putJourneyObjective(GUIManagedObject journeyObjective, JourneyObjectiveService journeyObjectiveService, ContactPolicyService contactPolicyService, CatalogCharacteristicService catalogCharacteristicService, boolean newObject, String userID, int tenantID) throws GUIManagerException
  {
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    //  validate
    //

    if (journeyObjective instanceof JourneyObjective)
      {
        ((JourneyObjective) journeyObjective).validate(journeyObjectiveService, contactPolicyService, catalogCharacteristicService, now, tenantID);
      }

    //
    //  put
    //

    putGUIManagedObject(journeyObjective, SystemTime.getCurrentTime(), newObject, userID, tenantID);
  }

  /*****************************************
  *
  *  putJourneyObjective
  *
  *****************************************/

  public void putJourneyObjective(IncompleteObject journeyObjective, JourneyObjectiveService journeyObjectiveService, ContactPolicyService contactPolicyService, CatalogCharacteristicService catalogCharacteristicService, boolean newObject, String userID, int tenantID)
  {
    try
      {
        putJourneyObjective((GUIManagedObject) journeyObjective, journeyObjectiveService, contactPolicyService, catalogCharacteristicService, newObject, userID, tenantID);
      }
    catch (GUIManagerException e)
      {
        throw new RuntimeException(e);
      }
  }
  
  /*****************************************
  *
  *  removeJourneyObjective
  *
  *****************************************/

  public void removeJourneyObjective(String journeyObjectiveID, String userID, int tenantID) { removeGUIManagedObject(journeyObjectiveID, SystemTime.getCurrentTime(), userID, tenantID); }

  /*****************************************
  *
  *  interface JourneyObjectiveListener
  *
  *****************************************/

  public interface JourneyObjectiveListener
  {
    public void journeyObjectiveActivated(JourneyObjective journeyObjective);
    public void journeyObjectiveDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  journeyObjectiveListener
    //

    JourneyObjectiveListener journeyObjectiveListener = new JourneyObjectiveListener()
    {
      @Override public void journeyObjectiveActivated(JourneyObjective journeyObjective) { System.out.println("journeyObjective activated: " + journeyObjective.getJourneyObjectiveID()); }
      @Override public void journeyObjectiveDeactivated(String guiManagedObjectID) { System.out.println("journeyObjective deactivated: " + guiManagedObjectID); }
    };

    //
    //  journeyObjectiveService
    //

    JourneyObjectiveService journeyObjectiveService = new JourneyObjectiveService(Deployment.getBrokerServers(), "example-journeyObjectiveservice-001", Deployment.getJourneyObjectiveTopic(), false, journeyObjectiveListener);
    journeyObjectiveService.start();

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
