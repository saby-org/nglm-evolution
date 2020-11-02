/****************************************************************************
*
*  JourneyService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.stream.Collectors;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.Journey.JourneyStatus;

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
    JSONObject fullJSON = getJSONRepresentation(guiManagedObject);
    
    //
    //  recurrence field
    //
    
    boolean recurrence = JSONUtilities.decodeBoolean(fullJSON, "recurrence", Boolean.FALSE);
    Integer occurrenceNumber =  JSONUtilities.decodeInteger(fullJSON, "occurrenceNumber", recurrence);
    JSONObject scheduler = JSONUtilities.decodeJSONObject(fullJSON, "scheduler", recurrence);
    Integer numberOfOccurrences =  null;
    Integer lastCompletedOccurrenceNumber =  null;
    if (recurrence)
      {
        Collection<Journey> allRecs = getAllRecurrentJourneysByID(guiManagedObject.getGUIManagedObjectID(), true);
        
        //
        //  filter completed
        //
        
        allRecs = allRecs.stream().filter(journey -> JourneyStatus.Complete == getJourneyStatus(journey)).collect(Collectors.toList());
        numberOfOccurrences =  JSONUtilities.decodeInteger(scheduler, "numberOfOccurrences", recurrence);
        lastCompletedOccurrenceNumber =  allRecs.size();
      }
    
    JSONObject result = super.getSummaryJSONRepresentation(guiManagedObject);
    result.put("status", getJourneyStatus(guiManagedObject).getExternalRepresentation());
    result.put("recurrence", recurrence);
    result.put("occurrenceNumber", occurrenceNumber);
    result.put("numberOfOccurrences", numberOfOccurrences);
    result.put("lastCompletedOccurrenceNumber", lastCompletedOccurrenceNumber);
    
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
    if (!Deployment.getAutoApproveGuiObjects() && activeJourney != null && GUIManagedObjectType.Workflow != activeJourney.getGUIManagedObjectType()&& GUIManagedObjectType.LoyaltyWorkflow != activeJourney.getGUIManagedObjectType())
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
  public Collection<Journey> getActiveRecurrentJourneys(Date date) { return getActiveJourneys(date).stream().filter( journey -> journey.getRecurrence()).collect(Collectors.toList()); }
  public Collection<Journey> getAllRecurrentJourneysByID(String parentJourneyID, boolean includeArchived) 
  { 
    Collection<Journey> subJourneys = new ArrayList<Journey>();
    for (GUIManagedObject uncheckedJourney : getStoredJourneys(includeArchived))
      {
        if (uncheckedJourney.getAccepted())
          {
            Journey checkedJourney = (Journey) uncheckedJourney;
            if (parentJourneyID.equals(checkedJourney.getRecurrenceId())) subJourneys.add(checkedJourney);
          }
        
      }
    return subJourneys;
  }
  public Collection<Journey> getActiveAndCompletedRecurrentJourneys(Date now)
  {
    Collection<Journey> result = new ArrayList<Journey>();
    for (GUIManagedObject uncheckedJourney : getStoredJourneys())
      {
        if (uncheckedJourney.getAccepted())
          {
            boolean activeAndCompleted = true;
            Journey journey = (Journey) uncheckedJourney;
            
            //
            //  recurrent
            //
            
            activeAndCompleted = activeAndCompleted && journey.getRecurrence();
            
            //
            //  active / completed
            //
            
            activeAndCompleted = activeAndCompleted && journey.getActive() && journey.getEffectiveStartDate().compareTo(now) <= 0;
            
            
            //
            // Approved
            //
            
            if (!Deployment.getAutoApproveGuiObjects())
              {
                activeAndCompleted = activeAndCompleted && JourneyStatus.StartedApproved == journey.getApproval();
              }
            
            //
            //  add
            //
            
            if (activeAndCompleted) result.add(journey);
          }
      }
    return result;
  }
  
  public boolean isAChildJourney(GUIManagedObject journey)
  {
    JSONObject journeyJSON = getJSONRepresentation(journey);
    boolean result = false;
    String recurrenceId = JSONUtilities.decodeString(journeyJSON, "recurrenceId", false);
    result = !JSONUtilities.decodeBoolean(journeyJSON, "recurrence", Boolean.FALSE) && !(recurrenceId == null || recurrenceId.isEmpty());
    return result;
  }
  
  /*****************************************
  *
  *  putJourney
  *
  *****************************************/

  public void putJourney(GUIManagedObject journey, JourneyObjectiveService journeyObjectiveService, CatalogCharacteristicService catalogCharacteristicService, TargetService targetService, SubscriberMessageTemplateService subscriberMessageTemplateService, boolean newObject, String userID) throws GUIManagerException
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
        ((Journey) journey).createOrConsolidateHardcodedMessageTemplates(subscriberMessageTemplateService, journey.getGUIManagedObjectID(), this);
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

  public void putJourney(IncompleteObject journey, JourneyObjectiveService journeyObjectiveService, CatalogCharacteristicService catalogCharacteristicService, TargetService targetService, SubscriberMessageTemplateService subscriberMessageTemplateService, boolean newObject, String userID)
  {
    try
      {
        putJourney((GUIManagedObject) journey, journeyObjectiveService, catalogCharacteristicService, targetService, subscriberMessageTemplateService, newObject, userID);
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
