/****************************************************************************
*
*  JourneyService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Collection;
import java.util.Date;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.Journey.JourneyStatus;

public class JourneyTemplateService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(JourneyTemplateService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private JourneyTemplateListener journeyTemplateListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public JourneyTemplateService(String bootstrapServers, String groupID, String journeyTemplateTopic, boolean masterService, JourneyTemplateListener journeyTemplateListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "JourneyTemplateService", groupID, journeyTemplateTopic, masterService, getSuperListener(journeyTemplateListener), "putJourneyTemplate", "removeJourneyTemplate", notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public JourneyTemplateService(String bootstrapServers, String groupID, String journeyTemplateTopic, boolean masterService, JourneyTemplateListener journeyTemplateListener)
  {
    this(bootstrapServers, groupID, journeyTemplateTopic, masterService, journeyTemplateListener, true);
  }

  //
  //  constructor
  //

  public JourneyTemplateService(String bootstrapServers, String groupID, String journeyTemplateTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, journeyTemplateTopic, masterService, (JourneyTemplateListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(JourneyTemplateListener journeyTemplateListener)
  {
    GUIManagedObjectListener superListener = null;
    if (journeyTemplateListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { journeyTemplateListener.journeyTemplateActivated((Journey) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { journeyTemplateListener.journeyTemplateDeactivated(guiManagedObjectID); }
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
    return result;
  }
  
  /*****************************************
  *
  *  getJourneyTemplates
  *
  *****************************************/

  public String generateJourneyTemplateID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredJourneyTemplate(String journeyTemplateID) { return getStoredGUIManagedObject(journeyTemplateID); }
  public GUIManagedObject getStoredJourneyTemplate(String journeyTemplateID, boolean includeArchived) { return getStoredGUIManagedObject(journeyTemplateID, includeArchived); }
  public Collection<GUIManagedObject> getStoredJourneyTemplates() { return getStoredGUIManagedObjects(); }
  public Collection<GUIManagedObject> getStoredJourneyTemplates(boolean includeArchived) { return getStoredGUIManagedObjects(includeArchived); }
  public boolean isActiveJourneyTemplate(GUIManagedObject journeyTemplateUnchecked, Date date) { return isActiveGUIManagedObject(journeyTemplateUnchecked, date); }
  public Journey getActiveJourneyTemplate(String journeyTemplateID, Date date) { return (Journey) getActiveGUIManagedObject(journeyTemplateID, date); }
  public Collection<Journey> getActiveJourneyTemplates(Date date) { return (Collection<Journey>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putJourneyTemplate
  *
  *****************************************/

  public void putJourneyTemplate(GUIManagedObject journeyTemplate, JourneyObjectiveService journeyObjectiveService, CatalogCharacteristicService catalogCharacteristicService, TargetService targetService, boolean newObject, String userID) throws GUIManagerException
  {
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    //  validate
    //

    if (journeyTemplate instanceof Journey)
      {
        ((Journey) journeyTemplate).validate(journeyObjectiveService, catalogCharacteristicService, targetService, now);
      }

    //
    //  put
    //

    putGUIManagedObject(journeyTemplate, now, newObject, userID);
  }
  
  /*****************************************
  *
  *  putJourneyTemplate
  *
  *****************************************/

  public void putJourneyTemplate(IncompleteObject journeyTemplate, JourneyObjectiveService journeyObjectiveService, CatalogCharacteristicService catalogCharacteristicService, TargetService targetService, boolean newObject, String userID)
  {
    try
      {
        putJourneyTemplate((GUIManagedObject) journeyTemplate, journeyObjectiveService, catalogCharacteristicService, targetService, newObject, userID);
      }
    catch (GUIManagerException e)
      {
        throw new RuntimeException(e);
      }
  }

  /*****************************************
  *
  *  removeJourneyTemplate
  *
  *****************************************/

  public void removeJourneyTemplate(String journeyTemplateID, String userID) { removeGUIManagedObject(journeyTemplateID, SystemTime.getCurrentTime(), userID); }

  /*****************************************
  *
  *  getJourneyStatus
  *
  *****************************************/

  private JourneyStatus getJourneyStatus(GUIManagedObject guiManagedObject)
  {
    Date now = SystemTime.getCurrentTime();
    JourneyStatus status = JourneyStatus.Unknown;
    status = (status == JourneyStatus.Unknown && !guiManagedObject.getAccepted()) ? JourneyStatus.NotValid : status;
    status = (status == JourneyStatus.Unknown && isActiveGUIManagedObject(guiManagedObject, now)) ? JourneyStatus.Running : status;
    status = (status == JourneyStatus.Unknown && guiManagedObject.getEffectiveEndDate().before(now)) ? JourneyStatus.Complete : status;
    status = (status == JourneyStatus.Unknown && guiManagedObject.getActive() && guiManagedObject.getEffectiveStartDate().after(now)) ? JourneyStatus.Started : status;
    status = (status == JourneyStatus.Unknown && ! guiManagedObject.getActive() && guiManagedObject.getEffectiveStartDate().before(now)) ? JourneyStatus.Suspended : status;
    status = (status == JourneyStatus.Unknown) ? JourneyStatus.Pending : status;
    return status;
  }

  /*****************************************
  *
  *  interface JourneyTemplateListener
  *
  *****************************************/

  public interface JourneyTemplateListener
  {
    public void journeyTemplateActivated(Journey journey);
    public void journeyTemplateDeactivated(String guiManagedObjectID);
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

    JourneyTemplateListener journeyTemplateListener = new JourneyTemplateListener()
    {
      @Override public void journeyTemplateActivated(Journey journeyTemplate) { System.out.println("journey activated: " + journeyTemplate.getJourneyID()); }
      @Override public void journeyTemplateDeactivated(String guiManagedObjectID) { System.out.println("journey deactivated: " + guiManagedObjectID); }
    };

    //
    //  journeyService
    //

    JourneyTemplateService journeyTemplateService = new JourneyTemplateService(Deployment.getBrokerServers(), "example-journeytemplateservice-001", Deployment.getJourneyTemplateTopic(), false, journeyTemplateListener);
    journeyTemplateService.start();

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
