/****************************************************************************
*
*  SalesChannelService.java
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

public class SalesChannelService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SalesChannelService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private SalesChannelListener salesChannelListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SalesChannelService(String bootstrapServers, String groupID, String salesChannelTopic, boolean masterService, SalesChannelListener salesChannelListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "SalesChannelService", groupID, salesChannelTopic, masterService, getSuperListener(salesChannelListener), "putSalesChannel", "removeSalesChannel", notifyOnSignificantChange);
  }

  //
  //  constructor
  //
  
  public SalesChannelService(String bootstrapServers, String groupID, String salesChannelTopic, boolean masterService, SalesChannelListener salesChannelListener)
  {
    this(bootstrapServers, groupID, salesChannelTopic, masterService, salesChannelListener, true);
  }

  //
  //  constructor
  //

  public SalesChannelService(String bootstrapServers, String groupID, String salesChannelTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, salesChannelTopic, masterService, (SalesChannelListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(SalesChannelListener salesChannelListener)
  {
    GUIManagedObjectListener superListener = null;
    if (salesChannelListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { salesChannelListener.salesChannelActivated((SalesChannel) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { salesChannelListener.salesChannelDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getSalesChannels
  *
  *****************************************/

  public String generateSalesChannelID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredSalesChannel(String salesChannelID) { return getStoredGUIManagedObject(salesChannelID); }
  public Collection<GUIManagedObject> getStoredSalesChannels() { return getStoredGUIManagedObjects(); }
  public boolean isActiveSalesChannel(GUIManagedObject salesChannelUnchecked, Date date) { return isActiveGUIManagedObject(salesChannelUnchecked, date); }
  public SalesChannel getActiveSalesChannel(String salesChannelID, Date date) { return (SalesChannel) getActiveGUIManagedObject(salesChannelID, date); }
  public Collection<SalesChannel> getActiveSalesChannels(Date date) { return (Collection<SalesChannel>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putSalesChannel
  *
  *****************************************/

  public void putSalesChannel(GUIManagedObject salesChannel, CallingChannelService callingChannelService, PartnerService partnerService, boolean newObject, String userID) throws GUIManagerException
  {
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    //  validate
    //

    if (salesChannel instanceof SalesChannel)
      {
        ((SalesChannel) salesChannel).validate(callingChannelService, partnerService, now);
      }
    
    //
    //  put
    //

    putGUIManagedObject(salesChannel, now, newObject, userID);
  }
  
  /*****************************************
  *
  *  putSalesChannel
  *
  *****************************************/

  public void putSalesChannel(IncompleteObject product, CallingChannelService callingChannelService, PartnerService partnerService, boolean newObject, String userID)
  {
    try
      {
        putSalesChannel((GUIManagedObject) product, callingChannelService, partnerService, newObject, userID);
      }
    catch (GUIManagerException e)
      {
        throw new RuntimeException(e);
      }
  }
  
  /*****************************************
  *
  *  removeSalesChannel
  *
  *****************************************/

  public void removeSalesChannel(String salesChannelID, String userID) { removeGUIManagedObject(salesChannelID, SystemTime.getCurrentTime(), userID); }

  /*****************************************
  *
  *  interface SalesChannelListener
  *
  *****************************************/

  public interface SalesChannelListener
  {
    public void salesChannelActivated(SalesChannel salesChannel);
    public void salesChannelDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  salesChannelListener
    //

    SalesChannelListener salesChannelListener = new SalesChannelListener()
    {
      @Override public void salesChannelActivated(SalesChannel salesChannel) { System.out.println("salesChannel activated: " + salesChannel.getSalesChannelID()); }
      @Override public void salesChannelDeactivated(String guiManagedObjectID) { System.out.println("salesChannel deactivated: " + guiManagedObjectID); }
    };

    //
    //  salesChannelService
    //

    SalesChannelService salesChannelService = new SalesChannelService(Deployment.getBrokerServers(), "example-saleschannelservice-001", Deployment.getSalesChannelTopic(), false, salesChannelListener);
    salesChannelService.start();

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
