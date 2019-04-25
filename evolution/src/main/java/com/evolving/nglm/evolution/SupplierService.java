/****************************************************************************
*
*  SupplierService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

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
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SupplierService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SupplierService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private SupplierListener supplierListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SupplierService(String bootstrapServers, String groupID, String supplierTopic, boolean masterService, SupplierListener supplierListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "SupplierService", groupID, supplierTopic, masterService, getSuperListener(supplierListener), "putSupplier", "removeSupplier", notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public SupplierService(String bootstrapServers, String groupID, String supplierTopic, boolean masterService, SupplierListener supplierListener)
  {
    this(bootstrapServers, groupID, supplierTopic, masterService, supplierListener, true);
  }

  //
  //  constructor
  //

  public SupplierService(String bootstrapServers, String groupID, String supplierTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, supplierTopic, masterService, (SupplierListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(SupplierListener supplierListener)
  {
    GUIManagedObjectListener superListener = null;
    if (supplierListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { supplierListener.supplierActivated((Supplier) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { supplierListener.supplierDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getSuppliers
  *
  *****************************************/

  public String generateSupplierID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredSupplier(String supplierID) { return getStoredGUIManagedObject(supplierID); }
  public Collection<GUIManagedObject> getStoredSuppliers() { return getStoredGUIManagedObjects(); }
  public boolean isActiveSupplier(GUIManagedObject supplierUnchecked, Date date) { return isActiveGUIManagedObject(supplierUnchecked, date); }
  public Supplier getActiveSupplier(String supplierID, Date date) { return (Supplier) getActiveGUIManagedObject(supplierID, date); }
  public Collection<Supplier> getActiveSuppliers(Date date) { return (Collection<Supplier>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putSupplier
  *
  *****************************************/

  public void putSupplier(GUIManagedObject supplier, boolean newObject, String userID) { putGUIManagedObject(supplier, SystemTime.getCurrentTime(), newObject, userID); }

  /*****************************************
  *
  *  removeSupplier
  *
  *****************************************/

  public void removeSupplier(String supplierID, String userID) { removeGUIManagedObject(supplierID, SystemTime.getCurrentTime(), userID); }

  /*****************************************
  *
  *  interface SupplierListener
  *
  *****************************************/

  public interface SupplierListener
  {
    public void supplierActivated(Supplier supplier);
    public void supplierDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  supplierListener
    //

    SupplierListener supplierListener = new SupplierListener()
    {
      @Override public void supplierActivated(Supplier supplier) { System.out.println("supplier activated: " + supplier.getSupplierID()); }
      @Override public void supplierDeactivated(String guiManagedObjectID) { System.out.println("supplier deactivated: " + guiManagedObjectID); }
    };

    //
    //  supplierService
    //

    SupplierService supplierService = new SupplierService(Deployment.getBrokerServers(), "example-supplierservice-001", Deployment.getSupplierTopic(), false, supplierListener);
    supplierService.start();

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
