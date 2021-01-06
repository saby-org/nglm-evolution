/****************************************************************************
*
*  ContactPolicyService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;

import com.evolving.nglm.core.SystemTime;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import org.json.simple.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ContactPolicyService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(ContactPolicyService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private ContactPolicyListener contactPolicyListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  @Deprecated // groupID not needed
  public ContactPolicyService(String bootstrapServers, String groupID, String catalogObjectiveTopic, boolean masterService, ContactPolicyListener contactPolicyListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "ContactPolicyService", groupID, catalogObjectiveTopic, masterService, getSuperListener(contactPolicyListener), "putContactPolicy", "removeContactPolicy", notifyOnSignificantChange);
  }
  //
  //  constructor
  //

  @Deprecated // groupID not needed
  public ContactPolicyService(String bootstrapServers, String groupID, String contactPolicyTopic, boolean masterService, ContactPolicyListener contactPolicyListener)
  {
    this(bootstrapServers, groupID, contactPolicyTopic, masterService, contactPolicyListener, true);
  }

  //
  //  constructor
  //

  @Deprecated // groupID not needed
  public ContactPolicyService(String bootstrapServers, String groupID, String contactPolicyTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, contactPolicyTopic, masterService, (ContactPolicyListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(ContactPolicyListener contactPolicyListener)
  {
    GUIManagedObjectListener superListener = null;
    if (contactPolicyListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { contactPolicyListener.contactPolicyActivated((ContactPolicy) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { contactPolicyListener.contactPolicyDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getContactPolicies
  *
  *****************************************/

  public String generateContactPolicyID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredContactPolicy(String contactPolicyID) { return getStoredGUIManagedObject(contactPolicyID); }
  public GUIManagedObject getStoredContactPolicy(String contactPolicyID, boolean includeArchived) { return getStoredGUIManagedObject(contactPolicyID, includeArchived); }
  public Collection<GUIManagedObject> getStoredContactPolicies(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredContactPolicies(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActiveContactPolicy(GUIManagedObject contactPolicyUnchecked, Date date) { return isActiveGUIManagedObject(contactPolicyUnchecked, date); }
  public ContactPolicy getActiveContactPolicy(String contactPolicyID, Date date) { return (ContactPolicy) getActiveGUIManagedObject(contactPolicyID, date); }
  public Collection<ContactPolicy> getActiveContactPolicies(Date date, int tenantID) { return (Collection<ContactPolicy>) getActiveGUIManagedObjects(date, tenantID); }

  /*****************************************
  *
  *  putContactPolicy
  *
  *****************************************/

  public void putContactPolicy(GUIManagedObject contactPolicy, boolean newObject, String userID) throws GUIManagerException
  {
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    //  validate
    //

    if (contactPolicy instanceof ContactPolicy)
      {
        ((ContactPolicy) contactPolicy).validate(now);
      }

    //
    //  put
    //

    putGUIManagedObject(contactPolicy, SystemTime.getCurrentTime(), newObject, userID);
  }

  /*****************************************
  *
  *  putContactPolicy
  *
  *****************************************/

  public void putContactPolicy(IncompleteObject contactPolicy, boolean newObject, String userID)
  {
    try
      {
        putContactPolicy((GUIManagedObject) contactPolicy, newObject, userID);
      }
    catch (GUIManagerException e)
      {
        throw new RuntimeException(e);
      }
  }
  
  /*****************************************
  *
  *  removeContactPolicy
  *
  *****************************************/

  public void removeContactPolicy(String contactPolicyID, String userID, int tenantID) { removeGUIManagedObject(contactPolicyID, SystemTime.getCurrentTime(), userID, tenantID); }
  
  /***********************************************
  *
  *  getAllJourneyObjectiveNamesUsingContactPolicy
  *
  ***********************************************/
  
  public List<String> getAllJourneyObjectiveNamesUsingContactPolicy(String contactPolicyID, JourneyObjectiveService journeyObjectiveService, int tenantID)
  {
    List<String> result = new ArrayList<String>(); 
    for(GUIManagedObject guiManagedObject : journeyObjectiveService.getStoredGUIManagedObjects(tenantID))
      {
        if(guiManagedObject instanceof JourneyObjective)
          {
            JourneyObjective journeyObjective = (JourneyObjective) guiManagedObject;
            if(journeyObjective.getContactPolicyID().equals(contactPolicyID))
              {
                result.add(journeyObjective.getGUIManagedObjectDisplay());
              }
          }
      }
    return result;
  }
  
  /**************************************
  *
  *  getAllSegmentNamesUsingContactPolicy
  *
  ***************************************/
  
  public List<String> getAllSegmentIDsUsingContactPolicy(String contactPolicyID, SegmentContactPolicyService segmentContactPolicyService, int tenantID)
  {
    List<String> result = new ArrayList<String>();
    for(GUIManagedObject guiManagedObject : segmentContactPolicyService.getStoredGUIManagedObjects(tenantID))
      {
        if(guiManagedObject instanceof SegmentContactPolicy)
          {
            SegmentContactPolicy policy = (SegmentContactPolicy) guiManagedObject;
            if(policy.getSegments() != null)
              {
                for(Entry<String, String> segmentEntry : policy.getSegments().entrySet())
                  {
                    if(segmentEntry.getValue().equals(contactPolicyID))
                      {
                        result.add(segmentEntry.getKey());
                      }
                  }
              }
          }
      }
    return result;
  }

  /*****************************************
  *
  *  interface ContactPolicyListener
  *
  *****************************************/

  public interface ContactPolicyListener
  {
    public void contactPolicyActivated(ContactPolicy contactPolicy);
    public void contactPolicyDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  contactPolicyListener
    //

    ContactPolicyListener contactPolicyListener = new ContactPolicyListener()
    {
      @Override public void contactPolicyActivated(ContactPolicy contactPolicy) { System.out.println("contactPolicy activated: " + contactPolicy.getContactPolicyID()); }
      @Override public void contactPolicyDeactivated(String guiManagedObjectID) { System.out.println("contactPolicy deactivated: " + guiManagedObjectID); }
    };

    //
    //  contactPolicyService
    //

    ContactPolicyService contactPolicyService = new ContactPolicyService(Deployment.getBrokerServers(), "example-contactPolicieservice-001", Deployment.getContactPolicyTopic(), false, contactPolicyListener);
    contactPolicyService.start();

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
