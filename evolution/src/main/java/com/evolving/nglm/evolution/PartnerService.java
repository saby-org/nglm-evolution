/****************************************************************************
*
*  PartnerService.java
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

public class PartnerService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(PartnerService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private PartnerListener partnerListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public PartnerService(String bootstrapServers, String groupID, String partnerTopic, boolean masterService, PartnerListener partnerListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "PartnerService", groupID, partnerTopic, masterService, getSuperListener(partnerListener), "putPartner", "removePartner", notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public PartnerService(String bootstrapServers, String groupID, String partnerTopic, boolean masterService, PartnerListener partnerListener)
  {
    this(bootstrapServers, groupID, partnerTopic, masterService, partnerListener, true);
  }

  //
  //  constructor
  //

  public PartnerService(String bootstrapServers, String groupID, String partnerTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, partnerTopic, masterService, (PartnerListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(PartnerListener partnerListener)
  {
    GUIManagedObjectListener superListener = null;
    if (partnerListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { partnerListener.partnerActivated((Partner) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { partnerListener.partnerDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getPartners
  *
  *****************************************/

  public String generatePartnerID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredPartner(String partnerID) { return getStoredGUIManagedObject(partnerID); }
  public GUIManagedObject getStoredPartner(String partnerID, boolean includeArchived) { return getStoredGUIManagedObject(partnerID, includeArchived); }
  public Collection<GUIManagedObject> getStoredPartners() { return getStoredGUIManagedObjects(); }
  public Collection<GUIManagedObject> getStoredPartners(boolean includeArchived) { return getStoredGUIManagedObjects(includeArchived); }
  public boolean isActivePartner(GUIManagedObject partnerUnchecked, Date date) { return isActiveGUIManagedObject(partnerUnchecked, date); }
  public Partner getActivePartner(String partnerID, Date date) { return (Partner) getActiveGUIManagedObject(partnerID, date); }
  public Collection<Partner> getActivePartners(Date date) { return (Collection<Partner>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putPartner
  *
  *****************************************/

  public void putPartner(GUIManagedObject partner, boolean newObject, String userID) { putGUIManagedObject(partner, SystemTime.getCurrentTime(), newObject, userID); }

  /*****************************************
  *
  *  removePartner
  *
  *****************************************/

  public void removePartner(String partnerID, String userID) { removeGUIManagedObject(partnerID, SystemTime.getCurrentTime(), userID); }

  /*****************************************
  *
  *  interface PartnerListener
  *
  *****************************************/

  public interface PartnerListener
  {
    public void partnerActivated(Partner partner);
    public void partnerDeactivated(String guiManagedObjectID);
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

    PartnerListener partnerListener = new PartnerListener()
    {
      @Override public void partnerActivated(Partner partner) { System.out.println("partner activated: " + partner.getPartnerID()); }
      @Override public void partnerDeactivated(String guiManagedObjectID) { System.out.println("partner deactivated: " + guiManagedObjectID); }
    };

    //
    //  partnerService
    //

    PartnerService partnerService = new PartnerService(Deployment.getBrokerServers(), "example-partnerservice-001", Deployment.getPartnerTopic(), false, partnerListener);
    partnerService.start();

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
