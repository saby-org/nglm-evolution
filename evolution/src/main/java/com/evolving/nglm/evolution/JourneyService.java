/****************************************************************************
*
*  JourneyService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.Journey.JourneyStatus;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;

import com.evolving.nglm.core.SystemTime;

import org.json.simple.JSONObject;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

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
import java.util.stream.Collectors;

public class JourneyService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(JourneyService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private JourneyListener journeyListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public JourneyService(String bootstrapServers, String groupID, String journeyTopic, boolean masterService, JourneyListener journeyListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "JourneyService", groupID, journeyTopic, masterService, getSuperListener(journeyListener), "putJourney", "removeJourney", notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public JourneyService(String bootstrapServers, String groupID, String journeyTopic, boolean masterService, JourneyListener journeyListener)
  {
    this(bootstrapServers, groupID, journeyTopic, masterService, journeyListener, true);
  }

  //
  //  constructor
  //

  public JourneyService(String bootstrapServers, String groupID, String journeyTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, journeyTopic, masterService, (JourneyListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(JourneyListener journeyListener)
  {
    GUIManagedObjectListener superListener = null;
    if (journeyListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { journeyListener.journeyActivated((Journey) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { journeyListener.journeyDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getJSONRepresentation
  *
  *****************************************/

  @Override protected JSONObject getJSONRepresentation(GUIManagedObject guiManagedObject)
  {
    JSONObject result = super.getJSONRepresentation(guiManagedObject);
    result.put("status", getJourneyStatus(guiManagedObject).getExternalRepresentation());
    return result;
  }
  
  /*****************************************
  *
  *  getSummaryJSONRepresentation
  *
  *****************************************/

  @Override protected JSONObject getSummaryJSONRepresentation(GUIManagedObject guiManagedObject)
  {
    JSONObject result = super.getSummaryJSONRepresentation(guiManagedObject);
    result.put("status", getJourneyStatus(guiManagedObject).getExternalRepresentation());
    if (guiManagedObject.getGUIManagedObjectType().equals(GUIManagedObjectType.BulkCampaign))
      {
        result.put("journeyTemplateID", guiManagedObject.getJSONRepresentation().get("journeyTemplateID"));
      }
    return result;
  }
  
  /*****************************************
  *
  *  getJourneys
  *
  *****************************************/

  public String generateJourneyID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredJourney(String journeyID) { return getStoredGUIManagedObject(journeyID); }
  public GUIManagedObject getStoredJourney(String journeyID, boolean includeArchived) { return getStoredGUIManagedObject(journeyID, includeArchived); }
  public Collection<GUIManagedObject> getStoredJourneys() { return getStoredGUIManagedObjects(); }
  public Collection<GUIManagedObject> getStoredJourneys(boolean includeArchived) { return getStoredGUIManagedObjects(includeArchived); }
  public boolean isActiveJourney(GUIManagedObject journeyUnchecked, Date date) { return isActiveGUIManagedObject(journeyUnchecked, date); }
  public Journey getActiveJourney(String journeyID, Date date) 
  { 
    Journey activeJourney = (Journey) getActiveGUIManagedObject(journeyID, date);
    if (!Deployment.getAutoApproveGuiObjects() && activeJourney != null)
      {
        return JourneyStatus.StartedApproved == activeJourney.getApproval() ? activeJourney : null; 
      }
    else
      {
        return activeJourney; 
      }
  }
  public Collection<Journey> getActiveJourneys(Date date) 
  { 
    Collection<Journey> activeJourney = (Collection<Journey>) getActiveGUIManagedObjects(date);
    if (!Deployment.getAutoApproveGuiObjects()) activeJourney = activeJourney.stream().filter(journey -> JourneyStatus.StartedApproved == journey.getApproval()).collect(Collectors.toList());
    return activeJourney;
  }

  /*****************************************
  *
  *  putJourney
  *
  *****************************************/

  public void putJourney(GUIManagedObject journey, JourneyObjectiveService journeyObjectiveService, CatalogCharacteristicService catalogCharacteristicService, TargetService targetService, boolean newObject, String userID) throws GUIManagerException
  {
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    //  validate
    //

    if (journey instanceof Journey)
      {
        ((Journey) journey).validate(journeyObjectiveService, catalogCharacteristicService, targetService, now);
      }

    //
    //  put
    //

    putGUIManagedObject(journey, now, newObject, userID);
  }
  
  /*****************************************
  *
  *  putJourney
  *
  *****************************************/

  public void putJourney(IncompleteObject journey, JourneyObjectiveService journeyObjectiveService, CatalogCharacteristicService catalogCharacteristicService, TargetService targetService, boolean newObject, String userID)
  {
    try
      {
        putJourney((GUIManagedObject) journey, journeyObjectiveService, catalogCharacteristicService, targetService, newObject, userID);
      }
    catch (GUIManagerException e)
      {
        throw new RuntimeException(e);
      }
  }

  /*****************************************
  *
  *  removeJourney
  *
  *****************************************/

  public void removeJourney(String journeyID, String userID) { removeGUIManagedObject(journeyID, SystemTime.getCurrentTime(), userID); }

  /*****************************************
  *
  *  getJourneyStatus
  *
  *****************************************/

  public JourneyStatus getJourneyStatus(GUIManagedObject guiManagedObject)
  {
    Date now = SystemTime.getCurrentTime();
    JourneyStatus status = JourneyStatus.Unknown;
    status = (status == JourneyStatus.Unknown && !guiManagedObject.getAccepted()) ? JourneyStatus.NotValid : status;
    if (Deployment.getAutoApproveGuiObjects())
      {
        status = (status == JourneyStatus.Unknown && isActiveGUIManagedObject(guiManagedObject, now)) ? JourneyStatus.Running : status;
        status = (status == JourneyStatus.Unknown && guiManagedObject.getEffectiveEndDate().before(now)) ? JourneyStatus.Complete : status;
        status = (status == JourneyStatus.Unknown && guiManagedObject.getActive() && guiManagedObject.getEffectiveStartDate().after(now)) ? JourneyStatus.Started : status;
        status = (status == JourneyStatus.Unknown && ! guiManagedObject.getActive() && guiManagedObject.getEffectiveStartDate().before(now)) ? JourneyStatus.Suspended : status;
      }
    if (!Deployment.getAutoApproveGuiObjects() && status == JourneyStatus.Unknown)
      {
        Journey journey = (Journey) guiManagedObject;
        status = (status == JourneyStatus.Unknown && journey.getApproval() == JourneyStatus.Pending) ? JourneyStatus.Pending : status;
        status = (status == JourneyStatus.Unknown && journey.getApproval() == JourneyStatus.PendingNotApproved) ? JourneyStatus.PendingNotApproved : status;
        status = (status == JourneyStatus.Unknown && journey.getApproval() == JourneyStatus.WaitingForApproval) ? JourneyStatus.WaitingForApproval : status;
        status = (status == JourneyStatus.Unknown && isActiveGUIManagedObject(guiManagedObject, now)) ? JourneyStatus.Running : status;
        status = (status == JourneyStatus.Unknown && guiManagedObject.getEffectiveEndDate().before(now)) ? JourneyStatus.Complete : status;
        status = (status == JourneyStatus.Unknown && guiManagedObject.getActive() && guiManagedObject.getEffectiveStartDate().after(now)) ? JourneyStatus.StartedApproved : status;
        status = (status == JourneyStatus.Unknown && ! guiManagedObject.getActive() && guiManagedObject.getEffectiveStartDate().before(now)) ? JourneyStatus.Suspended : status;
      }
    status = (status == JourneyStatus.Unknown) ? JourneyStatus.Pending : status;
    return status;
  }

  /*****************************************
  *
  *  interface JourneyListener
  *
  *****************************************/

  public interface JourneyListener
  {
    public void journeyActivated(Journey journey);
    public void journeyDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  journeyListener
    //

    JourneyListener journeyListener = new JourneyListener()
    {
      @Override public void journeyActivated(Journey journey) { System.out.println("journey activated: " + journey.getJourneyID()); }
      @Override public void journeyDeactivated(String guiManagedObjectID) { System.out.println("journey deactivated: " + guiManagedObjectID); }
    };

    //
    //  journeyService
    //

    JourneyService journeyService = new JourneyService(Deployment.getBrokerServers(), "example-journeyservice-001", Deployment.getJourneyTopic(), false, journeyListener);
    journeyService.start();

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
