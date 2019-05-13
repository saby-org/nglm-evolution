/****************************************************************************
*
*  TemplateService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

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

public class SMSTemplateService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SMSTemplateService.class);

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

  public SMSTemplateService(String bootstrapServers, String groupID, String templateTopic, boolean masterService, TemplateListener templateListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "TemplateService", groupID, templateTopic, masterService, getSuperListener(templateListener), "putTemplate", "removeTemplate", notifyOnSignificantChange);
  }

  //
  //  constructor
  //
  
  public SMSTemplateService(String bootstrapServers, String groupID, String templateTopic, boolean masterService, TemplateListener templateListener)
  {
    this(bootstrapServers, groupID, templateTopic, masterService, templateListener, true);
  }

  //
  //  constructor
  //

  public SMSTemplateService(String bootstrapServers, String groupID, String templateTopic, boolean masterService)
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
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { templateListener.templateSMSActivated((SMSTemplate) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { templateListener.templateSMSDeactivated(guiManagedObjectID); }
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
    List<String> languages = new ArrayList<String>();
    if(guiManagedObject.getJSONRepresentation().get("message") != null){

      Map<String, SupportedLanguage> supportedLanguages = Deployment.getSupportedLanguages();
      try
      {
        SMSMessage smsMessage = new SMSMessage(JSONUtilities.decodeJSONArray(guiManagedObject.getJSONRepresentation(), "message"), CriterionContext.Profile);
        DialogMessage messages = smsMessage.getMessageText();
        if(messages.getMessageTextByLanguage() != null){
          for(String languageName : messages.getMessageTextByLanguage().keySet()){
            for(SupportedLanguage supportedLanguage : supportedLanguages.values()){
              if(supportedLanguage.getName().equals(languageName)){
                languages.add(supportedLanguage.getID());
                break;
              }
            }
          }
        }
      } catch (JSONUtilitiesException | GUIManagerException e)
      {
        e.printStackTrace();
      }
      result.put("languageIDs", JSONUtilities.encodeArray(languages));
    }
    return result;
  }
  
  /*****************************************
  *
  *  getSMSTemplates
  *
  *****************************************/

  public String generateSMSTemplateID() { return generateGUIManagedObjectID(); }
  public boolean isActiveSMSTemplate(GUIManagedObject templateUnchecked, Date date) { return isActiveGUIManagedObject(templateUnchecked, date); }
  public GUIManagedObject getStoredSMSTemplate(String templateID) { return getStoredGUIManagedObject(templateID); }
  public Collection<GUIManagedObject> getStoredSMSTemplates() { return getStoredGUIManagedObjects(); }
  public SMSTemplate getActiveSMSTemplate(String templateID, Date date) { return (SMSTemplate) getActiveGUIManagedObject(templateID, date); }
  public Collection<SMSTemplate> getActiveSMSTemplates(Date date) { return (Collection<SMSTemplate>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putSMSTemplate
  *
  *****************************************/

  public void putSMSTemplate(SMSTemplate template, boolean newObject, String userID) throws GUIManagerException
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
  *  putIncompleteSMSTemplate
  *
  *****************************************/

  public void putIncompleteSMSTemplate(IncompleteObject template, boolean newObject, String userID)
  {
    putGUIManagedObject(template, SystemTime.getCurrentTime(), newObject, userID);
  }

  /*****************************************
  *
  *  removeTemplate
  *
  *****************************************/

  public void removeSMSTemplate(String templateID, String userID) { removeGUIManagedObject(templateID, SystemTime.getCurrentTime(), userID); }

  /*****************************************
  *
  *  interface TemplateListener
  *
  *****************************************/

  public interface TemplateListener
  {
    public void templateSMSActivated(SMSTemplate template);
    public void templateSMSDeactivated(String guiManagedObjectID);
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
      @Override public void templateSMSActivated(SMSTemplate template) { System.out.println("sms template activated: " + template.getSMSTemplateID()); }
      @Override public void templateSMSDeactivated(String guiManagedObjectID) { System.out.println("sms template deactivated: " + guiManagedObjectID); }
    };

    //
    //  templateService
    //

    SMSTemplateService templateService = new SMSTemplateService(Deployment.getBrokerServers(), "example-001", Deployment.getSMSTemplateTopic(), false, templateListener);
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
