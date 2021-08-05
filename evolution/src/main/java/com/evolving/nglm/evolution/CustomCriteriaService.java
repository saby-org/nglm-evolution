/****************************************************************************
*
*  CustomCriteriaService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

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

public class CustomCriteriaService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(CustomCriteriaService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private CustomCriteriaListener customCriteriaListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public CustomCriteriaService(String bootstrapServers, String groupID, String customCriteriaTopic, boolean masterService, CustomCriteriaListener customCriteriaListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "CustomCriteriaService", groupID, customCriteriaTopic, masterService, getSuperListener(customCriteriaListener), "putCustomCriteria", "removeCustomCriteria", notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public CustomCriteriaService(String bootstrapServers, String groupID, String customerCriteriaTopic, boolean masterService, CustomCriteriaListener customCriteriaListener)
  {
    this(bootstrapServers, groupID, customerCriteriaTopic, masterService, customCriteriaListener, true);
  }

  //
  //  constructor
  //

  public CustomCriteriaService(String bootstrapServers, String groupID, String customCriteriaTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, customCriteriaTopic, masterService, (CustomCriteriaListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(CustomCriteriaListener customCriteriaListener)
  {
    GUIManagedObjectListener superListener = null;
    if (customCriteriaListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { customCriteriaListener.customCriteriaActivated((CustomCriteria) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { customCriteriaListener.customCriteriaDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getCustomCriterias
  *
  *****************************************/

  public String generateCustomCriteriaID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredCustomCriteria(String customCriteriaID) { return getStoredGUIManagedObject(customCriteriaID); }
  public GUIManagedObject getStoredCustomCriteria(String customCriteriaID, boolean includeArchived) { return getStoredGUIManagedObject(customCriteriaID, includeArchived); }
  public Collection<GUIManagedObject> getStoredCustomCriterias(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredCustomCriterias(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActiveCustomCriteria(GUIManagedObject customCriteriaUnchecked, Date date) { return isActiveGUIManagedObject(customCriteriaUnchecked, date); }
  public CustomCriteria getActiveCustomCriteria(String customCriteriaID, Date date) { return (CustomCriteria) getActiveGUIManagedObject(customCriteriaID, date); }
  public Collection<CustomCriteria> getActiveCustomCriterias(Date date, int tenantID) { return (Collection<CustomCriteria>) getActiveGUIManagedObjects(date, tenantID); }

  /*****************************************
  *
  *  putCustomCriteria
  *
  *****************************************/

  public void putCustomCriteria(GUIManagedObject customCriteria, boolean newObject, String userID, CustomCriteriaService customCriteriaService) throws GUIManagerException { 
    
    //
    //  put
    //

    putGUIManagedObject(customCriteria, SystemTime.getCurrentTime(), newObject, userID); 
    
  }
  
  /*****************************************
  *
  *  putCustomCriteria
  *
  *****************************************/

  public void putCustomCriteria(IncompleteObject customCriteria,  boolean newObject, String userID, CustomCriteriaService customCriteriaService)
  {
    try
      {
        putCustomCriteria((GUIManagedObject) customCriteria, newObject, userID, customCriteriaService);
      }
    catch (GUIManagerException e)
      {
        throw new RuntimeException(e);
      }
  }

  /*****************************************
  *
  *  removeCustomCriteria
  *
  *****************************************/

  public void removeCustomCriteria(String customCriteriaID, String userID, int tenantID) { removeGUIManagedObject(customCriteriaID, SystemTime.getCurrentTime(), userID, tenantID); }

  /*****************************************
  *
  *  interface CustomCriteriaListener
  *
  *****************************************/

  public interface CustomCriteriaListener
  {
    public void customCriteriaActivated(CustomCriteria customCriteria);
    public void customCriteriaDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  customCriteriaListener
    //

    CustomCriteriaListener customCriteriaListener = new CustomCriteriaListener()
    {
      @Override public void customCriteriaActivated(CustomCriteria customCriteria) { System.out.println("customCriteria activated: " + customCriteria.getCustomCriteriaID()); }
      @Override public void customCriteriaDeactivated(String guiManagedObjectID) { System.out.println("customCriteria deactivated: " + guiManagedObjectID); }
    };

    //
    //  customCriteriaService
    //

    CustomCriteriaService customCriteriaService = new CustomCriteriaService(Deployment.getBrokerServers(), "example-customCriteria-001", Deployment.getCustomCriteriaTopic(), false, customCriteriaListener);
    customCriteriaService.start();

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
