/****************************************************************************
*
*  SegmentationRuleService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.SystemTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

public class SegmentationRuleService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SegmentationRuleService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private SegmentationRuleListener segmentationRuleListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SegmentationRuleService(String bootstrapServers, String groupID, String segmentationRuleTopic, boolean masterService, SegmentationRuleListener segmentationRuleListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "segmentationRuleService", groupID, segmentationRuleTopic, masterService, getSuperListener(segmentationRuleListener), "putSegmentationRule", "removeSegmentationRule", notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public SegmentationRuleService(String bootstrapServers, String groupID, String segmentationRuleTopic, boolean masterService, SegmentationRuleListener segmentationRuleListener)
  {
    this(bootstrapServers, groupID, segmentationRuleTopic, masterService, segmentationRuleListener, true);
  }

  //
  //  constructor
  //

  public SegmentationRuleService(String bootstrapServers, String groupID, String segmentationRuleTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, segmentationRuleTopic, masterService, (SegmentationRuleListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(SegmentationRuleListener segmentationRuleListener)
  {
    GUIManagedObjectListener superListener = null;
    if (segmentationRuleListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { segmentationRuleListener.segmentationRuleActivated((SegmentationRule) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { segmentationRuleListener.segmentationRuleDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getSegmentationRules
  *
  *****************************************/

  public String generateSegmentationRuleID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredSegmentationRule(String segmentationRuleID) { return getStoredGUIManagedObject(segmentationRuleID); }
  public Collection<GUIManagedObject> getStoredSegmentationRules() { return getStoredGUIManagedObjects(); }
  public boolean isActiveSegmentationRule(GUIManagedObject segmentationRuleUnchecked, Date date) { return isActiveGUIManagedObject(segmentationRuleUnchecked, date); }
  public SegmentationRule getActiveSegmentationRule(String segmentationRuleID, Date date) { return (SegmentationRule) getActiveGUIManagedObject(segmentationRuleID, date); }
  public Collection<SegmentationRule> getActiveSegmentationRules(Date date) { return (Collection<SegmentationRule>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putSegmentationRule
  *
  *****************************************/

  public void putSegmentationRule(GUIManagedObject segmentationRule, boolean newObject, String userID) { putGUIManagedObject(segmentationRule, SystemTime.getCurrentTime(), newObject, userID); }

  /*****************************************
  *
  *  removeSegmentationRule
  *
  *****************************************/

  public void removeSegmentationRule(String segmentationRuleID, String userID) { removeGUIManagedObject(segmentationRuleID, SystemTime.getCurrentTime(), userID); }

  /*****************************************
  *
  *  interface SegmentationRuleListener
  *
  *****************************************/

  public interface SegmentationRuleListener
  {
    public void segmentationRuleActivated(SegmentationRule segmentationRule);
    public void segmentationRuleDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  segmentationRuleListener
    //

	  SegmentationRuleListener segmentationRuleListener = new SegmentationRuleListener()
    {
      @Override public void segmentationRuleActivated(SegmentationRule segmentationRule) { System.out.println("segmentation rule activated: " + segmentationRule.getSegmentationRuleID()); }
      @Override public void segmentationRuleDeactivated(String guiManagedObjectID) { System.out.println("segmentation rule deactivated: " + guiManagedObjectID); }
    };

    //
    //  segmentationRuleService
    //

    SegmentationRuleService segmentationRuleService = new SegmentationRuleService(Deployment.getBrokerServers(), "example-segmentationruleservice-001", Deployment.getSegmentationRuleTopic(), false, segmentationRuleListener);
    segmentationRuleService.start();

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
