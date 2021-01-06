/****************************************************************************
*
*  CallingChannelService.java
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

public class CallingChannelService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(CallingChannelService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private CallingChannelListener callingChannelListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public CallingChannelService(String bootstrapServers, String groupID, String callingChannelTopic, boolean masterService, CallingChannelListener callingChannelListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "CallingChannelService", groupID, callingChannelTopic, masterService, getSuperListener(callingChannelListener), "putCallingChannel", "removeCallingChannel", notifyOnSignificantChange);
  }

  //
  //  constructor
  //
  
  public CallingChannelService(String bootstrapServers, String groupID, String callingChannelTopic, boolean masterService, CallingChannelListener callingChannelListener)
  {
    this(bootstrapServers, groupID, callingChannelTopic, masterService, callingChannelListener, true);
  }

  //
  //  constructor
  //

  public CallingChannelService(String bootstrapServers, String groupID, String callingChannelTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, callingChannelTopic, masterService, (CallingChannelListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(CallingChannelListener callingChannelListener)
  {
    GUIManagedObjectListener superListener = null;
    if (callingChannelListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { callingChannelListener.callingChannelActivated((CallingChannel) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { callingChannelListener.callingChannelDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getCallingChannels
  *
  *****************************************/

  public String generateCallingChannelID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredCallingChannel(String callingChannelID) { return getStoredGUIManagedObject(callingChannelID); }
  public GUIManagedObject getStoredCallingChannel(String callingChannelID, boolean includeArchived) { return getStoredGUIManagedObject(callingChannelID, includeArchived); }
  public Collection<GUIManagedObject> getStoredCallingChannels(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredCallingChannels(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActiveCallingChannelThroughInterval(GUIManagedObject deliverableUnchecked, Date startDate, Date endDate) { return isActiveThroughInterval(deliverableUnchecked, startDate, endDate); }
  public boolean isActiveCallingChannel(GUIManagedObject callingChannelUnchecked, Date date) { return isActiveGUIManagedObject(callingChannelUnchecked, date); }
  public CallingChannel getActiveCallingChannel(String callingChannelID, Date date) { return (CallingChannel) getActiveGUIManagedObject(callingChannelID, date); }
  public Collection<CallingChannel> getActiveCallingChannels(Date date, int tenantID) { return (Collection<CallingChannel>) getActiveGUIManagedObjects(date, tenantID); }

  /*****************************************
  *
  *  putCallingChannel
  *
  *****************************************/

  public void putCallingChannel(GUIManagedObject callingChannel, boolean newObject, String userID) { putGUIManagedObject(callingChannel, SystemTime.getCurrentTime(), newObject, userID); }

  /*****************************************
  *
  *  removeCallingChannel
  *
  *****************************************/

  public void removeCallingChannel(String callingChannelID, String userID, int tenantID) { removeGUIManagedObject(callingChannelID, SystemTime.getCurrentTime(), userID, tenantID); }

  /*****************************************
  *
  *  interface CallingChannelListener
  *
  *****************************************/

  public interface CallingChannelListener
  {
    public void callingChannelActivated(CallingChannel callingChannel);
    public void callingChannelDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  callingChannelListener
    //

    CallingChannelListener callingChannelListener = new CallingChannelListener()
    {
      @Override public void callingChannelActivated(CallingChannel callingChannel) { System.out.println("callingChannel activated: " + callingChannel.getCallingChannelID()); }
      @Override public void callingChannelDeactivated(String guiManagedObjectID) { System.out.println("callingChannel deactivated: " + guiManagedObjectID); }
    };

    //
    //  callingChannelService
    //

    CallingChannelService callingChannelService = new CallingChannelService(Deployment.getBrokerServers(), "example-callingchannelservice-001", Deployment.getCallingChannelTopic(), false, callingChannelListener);
    callingChannelService.start();

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
