/****************************************************************************
*
*  TemplateService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Collection;
import java.util.Date;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.GUIService;

public class MailTemplateService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(MailTemplateService.class);

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

  public MailTemplateService(String bootstrapServers, String groupID, String templateTopic, boolean masterService, TemplateListener templateListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "TemplateService", groupID, templateTopic, masterService, getSuperListener(templateListener), "putTemplate", "removeTemplate", notifyOnSignificantChange);
  }

  //
  //  constructor
  //
  
  public MailTemplateService(String bootstrapServers, String groupID, String templateTopic, boolean masterService, TemplateListener templateListener)
  {
    this(bootstrapServers, groupID, templateTopic, masterService, templateListener, true);
  }

  //
  //  constructor
  //

  public MailTemplateService(String bootstrapServers, String groupID, String templateTopic, boolean masterService)
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
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { templateListener.templateMailActivated((MailTemplate) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { templateListener.templateMailDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getMailTemplates
  *
  *****************************************/

  public String generateMailTemplateID() { return generateGUIManagedObjectID(); }
  public boolean isActiveMailTemplate(GUIManagedObject templateUnchecked, Date date) { return isActiveGUIManagedObject(templateUnchecked, date); }
  public GUIManagedObject getStoredMailTemplate(String templateID) { return getStoredGUIManagedObject(templateID); }
  public Collection<GUIManagedObject> getStoredMailTemplates() { return getStoredGUIManagedObjects(); }
  public MailTemplate getActiveMailTemplate(String templateID, Date date) { return (MailTemplate) getActiveGUIManagedObject(templateID, date); }
  public Collection<MailTemplate> getActiveMailTemplates(Date date) { return (Collection<MailTemplate>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putMailTemplate
  *
  *****************************************/

  public void putMailTemplate(MailTemplate template, boolean newObject, String userID) throws GUIManagerException
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
  *  putIncompleteMailTemplate
  *
  *****************************************/

  public void putIncompleteMailTemplate(IncompleteObject template, boolean newObject, String userID)
  {
    putGUIManagedObject(template, SystemTime.getCurrentTime(), newObject, userID);
  }

  /*****************************************
  *
  *  removeTemplate
  *
  *****************************************/

  public void removeMailTemplate(String templateID, String userID) { removeGUIManagedObject(templateID, SystemTime.getCurrentTime(), userID); }

  /*****************************************
  *
  *  interface TemplateListener
  *
  *****************************************/

  public interface TemplateListener
  {
    public void templateMailActivated(MailTemplate template);
    public void templateMailDeactivated(String guiManagedObjectID);
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
      @Override public void templateMailActivated(MailTemplate template) { System.out.println("mail template activated: " + template.getMailTemplateID()); }
      @Override public void templateMailDeactivated(String guiManagedObjectID) { System.out.println("mail template deactivated: " + guiManagedObjectID); }
    };

    //
    //  templateService
    //

    MailTemplateService templateService = new MailTemplateService(Deployment.getBrokerServers(), "example-001", Deployment.getMailTemplateTopic(), false, templateListener);
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
