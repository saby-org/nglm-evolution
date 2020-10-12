/****************************************************************************
*
*  SubscriberMessageTemplateService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.GUIService;

public class SubscriberMessageTemplateService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SubscriberMessageTemplateService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private TemplateListener templateListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SubscriberMessageTemplateService(String bootstrapServers, String groupID, String templateTopic, boolean masterService, TemplateListener templateListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "SubscriberMessageTemplateService", groupID, templateTopic, masterService, getSuperListener(templateListener), "putTemplate", "removeTemplate", notifyOnSignificantChange);
  }

  //
  //  constructor
  //
  
  public SubscriberMessageTemplateService(String bootstrapServers, String groupID, String templateTopic, boolean masterService, TemplateListener templateListener)
  {
    this(bootstrapServers, groupID, templateTopic, masterService, templateListener, true);
  }

  //
  //  constructor
  //

  public SubscriberMessageTemplateService(String bootstrapServers, String groupID, String templateTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, templateTopic, masterService, (TemplateListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(TemplateListener templateListener)
  {
    GUIManagedObjectListener superListener = null;
    if (templateListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { templateListener.messageTemplateActivated((SubscriberMessageTemplate) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { templateListener.messageTemplateDeactivated(guiManagedObjectID); }
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
    result.put("languageIDs", JSONUtilities.encodeArray((guiManagedObject instanceof SubscriberMessageTemplate) ? ((SubscriberMessageTemplate) guiManagedObject).getLanguages() : new ArrayList<String>()));
    result.put("areaAvailability", guiManagedObject.getJSONRepresentation().get("areaAvailability"));
    result.put("communicationChannelID", JSONUtilities.decodeString(guiManagedObject.getJSONRepresentation(), "communicationChannelID", false));
    return result;
  }
  
  /*****************************************
  *
  *  getSubscriberMessageTemplates
  *
  *****************************************/

  public String generateSubscriberMessageTemplateID() { return generateGUIManagedObjectID(); }
  public boolean isActiveSubscriberMessageTemplate(GUIManagedObject templateUnchecked, Date date) { return isActiveGUIManagedObject(templateUnchecked, date); }
  public GUIManagedObject getStoredSubscriberMessageTemplate(String templateID) { return getStoredGUIManagedObject(templateID); }
  public GUIManagedObject getStoredSubscriberMessageTemplate(String templateID, boolean includeArchived) { return getStoredGUIManagedObject(templateID, includeArchived); }
  public Collection<GUIManagedObject> getStoredSubscriberMessageTemplates() { return getStoredGUIManagedObjects(); }
  public Collection<GUIManagedObject> getStoredSubscriberMessageTemplates(boolean includeArchived) { return getStoredGUIManagedObjects(includeArchived); }
  public SubscriberMessageTemplate getActiveSubscriberMessageTemplate(String templateID, Date date) { return (SubscriberMessageTemplate) getActiveGUIManagedObject(templateID, date); }
  public Collection<SubscriberMessageTemplate> getActiveSubscriberMessageTemplates(Date date) { return (Collection<SubscriberMessageTemplate>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putSubscriberMessageTemplate
  *
  *****************************************/

  public void putSubscriberMessageTemplate(SubscriberMessageTemplate template, boolean newObject, String userID) throws GUIManagerException
  {
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    //  put
    //

    putGUIManagedObject(template, now, newObject, userID);
  }

  /*****************************************
  *
  *  putIncompleteSubscriberMessageTemplate
  *
  *****************************************/

  public void putIncompleteSubscriberMessageTemplate(IncompleteObject template, boolean newObject, String userID)
  {
    putGUIManagedObject(template, SystemTime.getCurrentTime(), newObject, userID);
  }

  /*****************************************
  *
  *  removeTemplate
  *
  *****************************************/

  public void removeSubscriberMessageTemplate(String templateID, String userID) { removeGUIManagedObject(templateID, SystemTime.getCurrentTime(), userID); }

  /*****************************************
  *
  *  getStoredMailTemplates
  *
  *****************************************/

  public Collection<GUIManagedObject> getStoredMailTemplates(boolean externalOnly, boolean includeArchived)
  {
    Set<GUIManagedObject> result = new HashSet<GUIManagedObject>();
    for (GUIManagedObject template : getStoredSubscriberMessageTemplates(includeArchived))
      {
        switch (template.getGUIManagedObjectType())
          {
            case MailMessageTemplate:
              if (! externalOnly || ! template.getInternalOnly())
                {
                  result.add(template);
                }
              break;
          }
      }
    return result;
  }

  /*****************************************
  *
  *  getStoredSMSTemplates
  *
  *****************************************/

  public Collection<GUIManagedObject> getStoredSMSTemplates(boolean externalOnly, boolean includeArchived)
  {
    Set<GUIManagedObject> result = new HashSet<GUIManagedObject>();
    for (GUIManagedObject template : getStoredSubscriberMessageTemplates(includeArchived))
      {
        switch (template.getGUIManagedObjectType())
          {
            case SMSMessageTemplate:
              if (! externalOnly || ! template.getInternalOnly())
                {
                  result.add(template);
                }
              break;
          }
      }
    return result;
  }

  /*****************************************
  *
  *  getStoredPushTemplates
  *
  *****************************************/

  public Collection<GUIManagedObject> getStoredPushTemplates(boolean externalOnly, boolean includeArchived)
  {
    Set<GUIManagedObject> result = new HashSet<GUIManagedObject>();
    for (GUIManagedObject template : getStoredSubscriberMessageTemplates(includeArchived))
      {
        switch (template.getGUIManagedObjectType())
          {
            case PushMessageTemplate:
              if (! externalOnly || ! template.getInternalOnly())
                {
                  result.add(template);
                }
              break;
          }
      }
    return result;
  }
  
  /*****************************************
  *
  *  getStoredDialogTemplates
  *
  *****************************************/

  public Collection<GUIManagedObject> getStoredDialogTemplates(boolean externalOnly, boolean includeArchived)
  {
    Set<GUIManagedObject> result = new HashSet<GUIManagedObject>();
    for (GUIManagedObject template : getStoredSubscriberMessageTemplates(includeArchived))
      {
        switch (template.getGUIManagedObjectType())
          {
            case DialogTemplate:
              if (! externalOnly || ! template.getInternalOnly())
                {
                  result.add(template);
                }
              break;
          }
      }
    return result;
  }

  /*****************************************
  *
  *  interface TemplateListener
  *
  *****************************************/

  public interface TemplateListener
  {
    public void messageTemplateActivated(SubscriberMessageTemplate template);
    public void messageTemplateDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  templateListener
    //

    TemplateListener templateListener = new TemplateListener()
    {
      @Override public void messageTemplateActivated(SubscriberMessageTemplate template) { System.out.println("sms template activated: " + template.getSubscriberMessageTemplateID()); }
      @Override public void messageTemplateDeactivated(String guiManagedObjectID) { System.out.println("sms template deactivated: " + guiManagedObjectID); }
    };

    //
    //  templateService
    //

    SubscriberMessageTemplateService templateService = new SubscriberMessageTemplateService(Deployment.getBrokerServers(), "example-001", Deployment.getSubscriberMessageTemplateTopic(), false, templateListener);
    templateService.start();

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
