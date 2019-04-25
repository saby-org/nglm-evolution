/****************************************************************************
*
*  UCGRuleService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;

public class UCGRuleService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(UCGRuleService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private UCGRuleListener ucgRuleListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public UCGRuleService(String bootstrapServers, String groupID, String ucgRuleTopic, boolean masterService, UCGRuleListener ucgRuleListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "UCGRuleService", groupID, ucgRuleTopic, masterService, getSuperListener(ucgRuleListener), "putUCGRule", "removeUCGRule", notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public UCGRuleService(String bootstrapServers, String groupID, String ucgRuleTopic, boolean masterService, UCGRuleListener ucgRuleListener)
  {
    this(bootstrapServers, groupID, ucgRuleTopic, masterService, ucgRuleListener, true);
  }

  //
  //  constructor
  //

  public UCGRuleService(String bootstrapServers, String groupID, String ucgRuleTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, ucgRuleTopic, masterService, (UCGRuleListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(UCGRuleListener ucgRuleListener)
  {
    GUIManagedObjectListener superListener = null;
    if (ucgRuleListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { ucgRuleListener.ucgRuleActivated((UCGRule) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { ucgRuleListener.ucgRuleDeactivated(guiManagedObjectID); }
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
    result.put("status",guiManagedObject.getJSONRepresentation().get("status"));
    result.put("calculatedSize",guiManagedObject.getJSONRepresentation().get("calculatedSize"));
    return result;
  }

  /*****************************************
  *
  *  getUCGRule
  *
  *****************************************/

  public String generateUCGRuleID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredUCGRule(String ucgRuleId) { return getStoredGUIManagedObject(ucgRuleId); }
  public Collection<GUIManagedObject> getStoredUCGRules() { return getStoredGUIManagedObjects(); }
  public boolean isActiveUCGRule(GUIManagedObject ucgRuleUnchecked, Date date) { return isActiveGUIManagedObject(ucgRuleUnchecked, date); }
  public UCGRule getActiveUCGRule(String ucgRuleID, Date date) { return (UCGRule) getActiveGUIManagedObject(ucgRuleID, date); }
  public Collection<UCGRule> getActiveUCGRules(Date date) { return (Collection<UCGRule>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putUCGRule
  *
  *****************************************/

  public void putUCGRule(GUIManagedObject ucgRule, SegmentationDimensionService segmentationDimensionService, boolean newObject, String userID) throws GUIManagerException
  {
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    //  validate
    //

    if (ucgRule instanceof UCGRule)
      {
        ((UCGRule) ucgRule).validate(this, segmentationDimensionService, now);
      }

    //
    //  put
    //

    putGUIManagedObject(ucgRule, now, newObject, userID);
  }

  /*****************************************
  *
  *  putUCGRule
  *
  *****************************************/

  public void putUCGRule(IncompleteObject ucgRule, SegmentationDimensionService segmentationDimensionService, boolean newObject, String userID)
  {
    try
      {
        putUCGRule((GUIManagedObject) ucgRule, segmentationDimensionService, newObject, userID);
      }
    catch (GUIManagerException e)
      {
        throw new RuntimeException(e);
      }
  }

  /*****************************************
  *
  *  removeUCGRule
  *
  *****************************************/

  public void removeUCGRule(String ucgRule, String userID) { removeGUIManagedObject(ucgRule, SystemTime.getCurrentTime(), userID); }

  /*****************************************
  *
  *  interface UCGRuleListener
  *
  *****************************************/

  public interface UCGRuleListener
  {
    public void ucgRuleActivated(UCGRule ucgRule);
    public void ucgRuleDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  ucgRuleListener
    //

    UCGRuleListener ucgRuleListener = new UCGRuleListener()
    {
      @Override public void ucgRuleActivated(UCGRule ucgRule) { System.out.println("ucgRule activated: " + ucgRule.getUCGRuleID()); }
      @Override public void ucgRuleDeactivated(String guiManagedObjectID) { System.out.println("ucgRule deactivated: " + guiManagedObjectID); }
    };

    //
    //  ucgRuleService
    //

    UCGRuleService ucgRuleService = new UCGRuleService(Deployment.getBrokerServers(), "example-ucgRuleService-001", Deployment.getUCGRuleTopic(), false, ucgRuleListener);
    ucgRuleService.start();

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
