/****************************************************************************
*
*  DeliverableService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DeliverableService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(DeliverableService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private DeliverableListener deliverableListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public DeliverableService(String bootstrapServers, String groupID, String deliverableTopic, boolean masterService, DeliverableListener deliverableListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "DeliverableService", groupID, deliverableTopic, masterService, getSuperListener(deliverableListener), "putDeliverable", "removeDeliverable", notifyOnSignificantChange);
  }
  
  //
  //  constructor
  //

  public DeliverableService(String bootstrapServers, String groupID, String deliverableTopic, boolean masterService, DeliverableListener deliverableListener)
  {
    this(bootstrapServers, groupID, deliverableTopic, masterService, deliverableListener, true);
  }

  //
  //  constructor
  //

  public DeliverableService(String bootstrapServers, String groupID, String deliverableTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, deliverableTopic, masterService, (DeliverableListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(DeliverableListener deliverableListener)
  {
    GUIManagedObjectListener superListener = null;
    if (deliverableListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { deliverableListener.deliverableActivated((Deliverable) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { deliverableListener.deliverableDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getDeliverables
  *
  *****************************************/

  public String generateDeliverableID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredDeliverable(String deliverableID, int tenantID) { return getStoredGUIManagedObject(deliverableID, tenantID); }
  public GUIManagedObject getStoredDeliverable(String deliverableID, boolean includeArchived, int tenantID) { return getStoredGUIManagedObject(deliverableID, includeArchived, tenantID); }
  public Collection<GUIManagedObject> getStoredDeliverables(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredDeliverables(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActiveDeliverableThroughInterval(GUIManagedObject deliverableUnchecked, Date startDate, Date endDate) { return isActiveThroughInterval(deliverableUnchecked, startDate, endDate); }
  public boolean isActiveDeliverable(GUIManagedObject deliverableUnchecked, Date date) { return isActiveGUIManagedObject(deliverableUnchecked, date); }
  public Deliverable getActiveDeliverable(String deliverableID, Date date, int tenantID) { return (Deliverable) getActiveGUIManagedObject(deliverableID, date, tenantID); }
  public Collection<Deliverable> getActiveDeliverables(Date date, int tenantID) { return (Collection<Deliverable>) getActiveGUIManagedObjects(date, tenantID); }

  //
  //  getStoredDeliverableByName
  //
  
  public GUIManagedObject getStoredDeliverableByName(String deliverableName, boolean includeArchived, int tenantID)
  {
    GUIManagedObject result = null;
    for (GUIManagedObject guiManagedObject : getStoredDeliverables(includeArchived, tenantID))
      {
        if (Objects.equals(deliverableName, guiManagedObject.getGUIManagedObjectName()))
          {
            result = guiManagedObject;
            break;
          }
      }
    return result;
  }
  
  //
  //  getStoredDeliverableByName
  //
  
  public GUIManagedObject getStoredDeliverableByName(String deliverableName, int tenantID) { return getStoredDeliverableByName(deliverableName, false, tenantID); }

  /*****************************************
  *
  *  putDeliverable
  *
  *****************************************/

  public void putDeliverable(GUIManagedObject deliverable, boolean newObject, String userID, int tenantID) { putGUIManagedObject(deliverable, SystemTime.getCurrentTime(), newObject, userID, tenantID); }

  /*****************************************
  *
  *  removeDeliverable
  *
  *****************************************/

  public void removeDeliverable(String deliverableID, String userID, int tenantID) { removeGUIManagedObject(deliverableID, SystemTime.getCurrentTime(), userID, tenantID); }

  /*****************************************
  *
  *  interface DeliverableListener
  *
  *****************************************/

  public interface DeliverableListener
  {
    public void deliverableActivated(Deliverable deliverable);
    public void deliverableDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  deliverableListener
    //

    DeliverableListener deliverableListener = new DeliverableListener()
    {
      @Override public void deliverableActivated(Deliverable deliverable) { System.out.println("deliverable activated: " + deliverable.getDeliverableID()); }
      @Override public void deliverableDeactivated(String guiManagedObjectID) { System.out.println("deliverable deactivated: " + guiManagedObjectID); }
    };

    //
    //  deliverableService
    //

    DeliverableService deliverableService = new DeliverableService(Deployment.getBrokerServers(), "example-deliverableservice-001", Deployment.getDeliverableTopic(), false, deliverableListener);
    deliverableService.start();

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
