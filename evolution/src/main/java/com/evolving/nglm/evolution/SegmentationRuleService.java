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

  public SegmentationRuleService(String bootstrapServers, String groupID, String segmentationRuleTopic, boolean masterService, SegmentationRuleListener segmentationRuleListener)
  {
    super(bootstrapServers, "segmentationRuleService", groupID, segmentationRuleTopic, masterService, getSuperListener(segmentationRuleListener));
  }

  //
  //  constructor
  //

  public SegmentationRuleService(String bootstrapServers, String groupID, String segmentationRuleTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, segmentationRuleTopic, masterService, (SegmentationRuleListener) null);
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
          @Override public void guiManagedObjectDeactivated(GUIManagedObject guiManagedObject) { segmentationRuleListener.segmentationRuleDeactivated((SegmentationRule) guiManagedObject); }
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

  public void putSegmentationRule(GUIManagedObject segmentationRule) { putGUIManagedObject(segmentationRule, SystemTime.getCurrentTime()); }

  /*****************************************
  *
  *  removeSegmentationRule
  *
  *****************************************/

  public void removeSegmentationRule(String segmentationRuleID) { removeGUIManagedObject(segmentationRuleID, SystemTime.getCurrentTime()); }

  /*****************************************
  *
  *  interface SegmentationRuleListener
  *
  *****************************************/

  public interface SegmentationRuleListener
  {
    public void segmentationRuleActivated(SegmentationRule segmentationRule);
    public void segmentationRuleDeactivated(SegmentationRule segmentationRule);
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
      @Override public void segmentationRuleDeactivated(SegmentationRule segmentationRule) { System.out.println("segmentation rule deactivated: " + segmentationRule.getSegmentationRuleID()); }
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
