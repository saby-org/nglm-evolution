/****************************************************************************
*
*  ResellerService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
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

public class ResellerService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(ResellerService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private ResellerListener resellerListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ResellerService(String bootstrapServers, String groupID, String resellerTopic, boolean masterService, ResellerListener resellerListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "ResellerService", groupID, resellerTopic, masterService, getSuperListener(resellerListener), "putReseller", "removeReseller", notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public ResellerService(String bootstrapServers, String groupID, String resellerTopic, boolean masterService, ResellerListener resellerListener)
  {
    this(bootstrapServers, groupID, resellerTopic, masterService, resellerListener, true);
  }

  //
  //  constructor
  //

  public ResellerService(String bootstrapServers, String groupID, String resellerTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, resellerTopic, masterService, (ResellerListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(ResellerListener resellerListener)
  {
    GUIManagedObjectListener superListener = null;
    if (resellerListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { resellerListener.resellerActivated((Reseller) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { resellerListener.resellerDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getResellers
  *
  *****************************************/

  public String generateResellerID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredReseller(String resellerID, int tenantID) { return getStoredGUIManagedObject(resellerID, tenantID); }
  public GUIManagedObject getStoredReseller(String resellerID, boolean includeArchived, int tenantID) { return getStoredGUIManagedObject(resellerID, includeArchived, tenantID); }
  public Collection<GUIManagedObject> getStoredResellers(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredResellers(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActiveReseller(GUIManagedObject resellerUnchecked, Date date) { return isActiveGUIManagedObject(resellerUnchecked, date); }
  public Reseller getActiveReseller(String resellerID, Date date, int tenantID) { return (Reseller) getActiveGUIManagedObject(resellerID, date, tenantID); }
  public Collection<Reseller> getActiveResellers(Date date, int tenantID) { return (Collection<Reseller>) getActiveGUIManagedObjects(date, tenantID); }

  /*****************************************
  *
  *  putReseller
  *
  *****************************************/

  public void putReseller(GUIManagedObject reseller, boolean newObject, String userID, ResellerService resellerService, int tenantID) throws GUIManagerException { 
    
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    //  validate
    //

    if (reseller instanceof Reseller)
      {
        ((Reseller) reseller).validate(resellerService, now, tenantID);
      }

    //
    //  put
    //

    putGUIManagedObject(reseller, SystemTime.getCurrentTime(), newObject, userID, tenantID); 
    
  }
  
  /*****************************************
  *
  *  putReseller
  *
  *****************************************/

  public void putReseller(IncompleteObject reseller,  boolean newObject, String userID, ResellerService resellerService, int tenantID)
  {
    try
      {
        putReseller((GUIManagedObject) reseller, newObject, userID, resellerService, tenantID);
      }
    catch (GUIManagerException e)
      {
        throw new RuntimeException(e);
      }
  }

  /*****************************************
  *
  *  removeReseller
  *
  *****************************************/

  public void removeReseller(String resellerID, String userID, int tenantID) { removeGUIManagedObject(resellerID, SystemTime.getCurrentTime(), userID, tenantID); }

  /*****************************************
  *
  *  interface PartnerListener
  *
  *****************************************/

  public interface ResellerListener
  {
    public void resellerActivated(Reseller reseller);
    public void resellerDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  partnerListener
    //

    ResellerListener resellerListener = new ResellerListener()
    {
      @Override public void resellerActivated(Reseller reseller) { System.out.println("reseller activated: " + reseller.getResellerID()); }
      @Override public void resellerDeactivated(String guiManagedObjectID) { System.out.println("reseller deactivated: " + guiManagedObjectID); }
    };

    //
    //  partnerService
    //

    ResellerService resellerService = new ResellerService(Deployment.getBrokerServers(), "example-resellerservice-001", Deployment.getResellerTopic(), false, resellerListener);
    resellerService.start();

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
