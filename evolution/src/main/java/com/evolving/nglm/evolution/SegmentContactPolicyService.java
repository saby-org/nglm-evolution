/*****************************************************************************
*
*  SegmentContactPolicy.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Collection;
import java.util.Date;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class SegmentContactPolicyService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SegmentContactPolicyService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private SegmentContactPolicyListener segmentContactPolicyListener = null;
  private KafkaProducer<byte[], byte[]> kafkaProducer = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SegmentContactPolicyService(String bootstrapServers, String groupID, String segmentContactPolicyTopic, boolean masterService, SegmentContactPolicyListener segmentContactPolicyListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "SegmentContactPolicyService", groupID, segmentContactPolicyTopic, masterService, getSuperListener(segmentContactPolicyListener), "putSegmentContactPolicy", "removeSegmentContactPolicy", notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public SegmentContactPolicyService(String bootstrapServers, String groupID, String segmentContactPolicyTopic, boolean masterService, SegmentContactPolicyListener segmentContactPolicyListener)
  {
    this(bootstrapServers, groupID, segmentContactPolicyTopic, masterService, segmentContactPolicyListener, true);
  }

  //
  //  constructor
  //

  public SegmentContactPolicyService(String bootstrapServers, String groupID, String segmentContactPolicyTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, segmentContactPolicyTopic, masterService, (SegmentContactPolicyListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(SegmentContactPolicyListener segmentContactPolicyListener)
  {
    GUIManagedObjectListener superListener = null;
    if (segmentContactPolicyListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { segmentContactPolicyListener.segmentContactPolicyActivated((SegmentContactPolicy) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { segmentContactPolicyListener.segmentContactPolicyDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getSegmentContactPolicys
  *
  *****************************************/

  public String generateSegmentContactPolicyID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredSegmentContactPolicy(String segmentContactPolicyID) { return getStoredGUIManagedObject(segmentContactPolicyID); }
  public GUIManagedObject getStoredSegmentContactPolicy(String segmentContactPolicyID, boolean includeArchived) { return getStoredGUIManagedObject(segmentContactPolicyID, includeArchived); }
  public Collection<GUIManagedObject> getStoredSegmentContactPolicys(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredSegmentContactPolicys(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActiveSegmentContactPolicy(GUIManagedObject segmentContactPolicyUnchecked, Date date) { return isActiveGUIManagedObject(segmentContactPolicyUnchecked, date); }
  public SegmentContactPolicy getActiveSegmentContactPolicy(String segmentContactPolicyID, Date date) { return (SegmentContactPolicy) getActiveGUIManagedObject(segmentContactPolicyID, date); }
  public Collection<SegmentContactPolicy> getActiveSegmentContactPolicys(Date date, int tenantID) { return (Collection<SegmentContactPolicy>) getActiveGUIManagedObjects(date, tenantID); }
  public SegmentContactPolicy getSingletonSegmentContactPolicy() { return getActiveSegmentContactPolicy(SegmentContactPolicy.singletonID, SystemTime.getCurrentTime()); }

  /*****************************************
  *
  *  putSegmentContactPolicy
  *
  *****************************************/

  public void putSegmentContactPolicy(GUIManagedObject segmentContactPolicy, ContactPolicyService contactPolicyService, SegmentationDimensionService dimensionService, boolean newObject, String userID) throws GUIManagerException
  {
    //
    //  now
    //            

    Date now = SystemTime.getCurrentTime();

    //
    //  validate
    //

    if (segmentContactPolicy instanceof SegmentContactPolicy)
      {
        ((SegmentContactPolicy) segmentContactPolicy).validate(contactPolicyService, dimensionService, now);
      }

    //
    //  put
    //

    putGUIManagedObject(segmentContactPolicy, now, newObject, userID);
  }

  /*****************************************
  *
  *  putSegmentContactPolicy
  *
  *****************************************/

  public void putSegmentContactPolicy(IncompleteObject segmentContactPolicy, ContactPolicyService contactPolicyService, SegmentationDimensionService dimensionService, boolean newObject, String userID)
  {
    try
      {
        putSegmentContactPolicy((GUIManagedObject) segmentContactPolicy, contactPolicyService, dimensionService, newObject, userID);
      }
    catch (GUIManagerException e)
      {
        throw new RuntimeException(e);
      }
  }

  /*****************************************
  *
  *  removeSegmentContactPolicy
  *
  *****************************************/

  public void removeSegmentContactPolicy(String segmentContactPolicyID, String userID, int tenantID) { removeGUIManagedObject(segmentContactPolicyID, SystemTime.getCurrentTime(), userID, tenantID); }

  /*****************************************
  *
  *  interface SegmentContactPolicyListener
  *
  *****************************************/

  public interface SegmentContactPolicyListener
  {
    public void segmentContactPolicyActivated(SegmentContactPolicy segmentContactPolicy);
    public void segmentContactPolicyDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  targetListener
    //

    SegmentContactPolicyListener segmentContactPolicyListener = new SegmentContactPolicyListener()
    {
      @Override public void segmentContactPolicyActivated(SegmentContactPolicy segmentContactPolicy) { System.out.println("SegmentContactPolicy activated: " + segmentContactPolicy.getGUIManagedObjectID()); }
      @Override public void segmentContactPolicyDeactivated(String guiManagedObjectID) { System.out.println("SegmentContactPolicy deactivated: " + guiManagedObjectID); }
    };

    //
    //  segmentContactPolicyService
    //

    SegmentContactPolicyService segmentContactPolicyService = new SegmentContactPolicyService(Deployment.getBrokerServers(), "example-segmentcontactpolicyservice-001", Deployment.getSegmentContactPolicyTopic(), false, segmentContactPolicyListener);
    segmentContactPolicyService.start();

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
