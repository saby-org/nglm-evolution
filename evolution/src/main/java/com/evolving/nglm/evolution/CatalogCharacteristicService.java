/****************************************************************************
*
*  CatalogCharacteristicService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;

import com.rii.utilities.SystemTime;

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

public class CatalogCharacteristicService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(CatalogCharacteristicService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private CatalogCharacteristicListener catalogCharacteristicListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public CatalogCharacteristicService(String bootstrapServers, String groupID, String catalogCharacteristicTopic, boolean masterService, CatalogCharacteristicListener catalogCharacteristicListener)
  {
    super(bootstrapServers, "CatalogCharacteristicService", groupID, catalogCharacteristicTopic, masterService, getSuperListener(catalogCharacteristicListener));
  }

  //
  //  constructor
  //

  public CatalogCharacteristicService(String bootstrapServers, String groupID, String catalogCharacteristicTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, catalogCharacteristicTopic, masterService, (CatalogCharacteristicListener) null);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(CatalogCharacteristicListener catalogCharacteristicListener)
  {
    GUIManagedObjectListener superListener = null;
    if (catalogCharacteristicListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { catalogCharacteristicListener.catalogCharacteristicActivated((CatalogCharacteristic) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(GUIManagedObject guiManagedObject) { catalogCharacteristicListener.catalogCharacteristicDeactivated((CatalogCharacteristic) guiManagedObject); }
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
    result.put("name", guiManagedObject.getJSONRepresentation().get("display"));
    result.put("type", guiManagedObject.getJSONRepresentation().get("display"));
    result.put("unit", guiManagedObject.getJSONRepresentation().get("display"));
    return result;
  }
  
  /*****************************************
  *
  *  getCatalogCharacteristics
  *
  *****************************************/

  public String generateCatalogCharacteristicID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredCatalogCharacteristic(String catalogCharacteristicID) { return getStoredGUIManagedObject(catalogCharacteristicID); }
  public Collection<GUIManagedObject> getStoredCatalogCharacteristics() { return getStoredGUIManagedObjects(); }
  public boolean isActiveCatalogCharacteristic(GUIManagedObject catalogCharacteristicUnchecked, Date date) { return isActiveGUIManagedObject(catalogCharacteristicUnchecked, date); }
  public CatalogCharacteristic getActiveCatalogCharacteristic(String catalogCharacteristicID, Date date) { return (CatalogCharacteristic) getActiveGUIManagedObject(catalogCharacteristicID, date); }
  public Collection<CatalogCharacteristic> getActiveCatalogCharacteristics(Date date) { return (Collection<CatalogCharacteristic>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putCatalogCharacteristic
  *
  *****************************************/

  public void putCatalogCharacteristic(GUIManagedObject catalogCharacteristic) { putGUIManagedObject(catalogCharacteristic, SystemTime.getCurrentTime()); }

  /*****************************************
  *
  *  putIncompleteCatalogCharacteristic
  *
  *****************************************/

  public void putIncompleteCatalogCharacteristic(IncompleteObject catalogCharacteristic)
  {
    putGUIManagedObject(catalogCharacteristic, SystemTime.getCurrentTime());
  }

  /*****************************************
  *
  *  removeCatalogCharacteristic
  *
  *****************************************/

  public void removeCatalogCharacteristic(String catalogCharacteristicID) { removeGUIManagedObject(catalogCharacteristicID, SystemTime.getCurrentTime()); }

  /*****************************************
  *
  *  interface CatalogCharacteristicListener
  *
  *****************************************/

  public interface CatalogCharacteristicListener
  {
    public void catalogCharacteristicActivated(CatalogCharacteristic catalogCharacteristic);
    public void catalogCharacteristicDeactivated(CatalogCharacteristic catalogCharacteristic);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  catalogCharacteristicListener
    //

    CatalogCharacteristicListener catalogCharacteristicListener = new CatalogCharacteristicListener()
    {
      @Override public void catalogCharacteristicActivated(CatalogCharacteristic catalogCharacteristic) { System.out.println("catalogCharacteristic activated: " + catalogCharacteristic.getCatalogCharacteristicID()); }
      @Override public void catalogCharacteristicDeactivated(CatalogCharacteristic catalogCharacteristic) { System.out.println("catalogCharacteristic deactivated: " + catalogCharacteristic.getCatalogCharacteristicID()); }
    };

    //
    //  catalogCharacteristicService
    //

    CatalogCharacteristicService catalogCharacteristicService = new CatalogCharacteristicService(Deployment.getBrokerServers(), "example-catalogCharacteristicservice-001", Deployment.getCatalogCharacteristicTopic(), false, catalogCharacteristicListener);
    catalogCharacteristicService.start();

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
