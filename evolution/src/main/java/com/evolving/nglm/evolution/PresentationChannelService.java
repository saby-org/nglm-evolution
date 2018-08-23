/****************************************************************************
*
*  PresentationChannelService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

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

public class PresentationChannelService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(PresentationChannelService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private PresentationChannelListener presentationChannelListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public PresentationChannelService(String bootstrapServers, String groupID, String presentationChannelTopic, boolean masterService, PresentationChannelListener presentationChannelListener)
  {
    super(bootstrapServers, "PresentationChannelService", groupID, presentationChannelTopic, masterService, getSuperListener(presentationChannelListener));
  }

  //
  //  constructor
  //

  public PresentationChannelService(String bootstrapServers, String groupID, String presentationChannelTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, presentationChannelTopic, masterService, (PresentationChannelListener) null);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(PresentationChannelListener presentationChannelListener)
  {
    GUIManagedObjectListener superListener = null;
    if (presentationChannelListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { presentationChannelListener.presentationChannelActivated((PresentationChannel) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(GUIManagedObject guiManagedObject) { presentationChannelListener.presentationChannelDeactivated((PresentationChannel) guiManagedObject); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getPresentationChannels
  *
  *****************************************/

  public String generatePresentationChannelID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredPresentationChannel(String presentationChannelID) { return getStoredGUIManagedObject(presentationChannelID); }
  public Collection<GUIManagedObject> getStoredPresentationChannels() { return getStoredGUIManagedObjects(); }
  public boolean isActivePresentationChannel(GUIManagedObject presentationChannelUnchecked, Date date) { return isActiveGUIManagedObject(presentationChannelUnchecked, date); }
  public PresentationChannel getActivePresentationChannel(String presentationChannelID, Date date) { return (PresentationChannel) getActiveGUIManagedObject(presentationChannelID, date); }
  public Collection<PresentationChannel> getActivePresentationChannels(Date date) { return (Collection<PresentationChannel>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putPresentationChannel
  *
  *****************************************/

  public void putPresentationChannel(GUIManagedObject presentationChannel) { putGUIManagedObject(presentationChannel, SystemTime.getCurrentTime()); }

  /*****************************************
  *
  *  removePresentationChannel
  *
  *****************************************/

  public void removePresentationChannel(String presentationChannelID) { removeGUIManagedObject(presentationChannelID, SystemTime.getCurrentTime()); }

  /*****************************************
  *
  *  interface PresentationChannelListener
  *
  *****************************************/

  public interface PresentationChannelListener
  {
    public void presentationChannelActivated(PresentationChannel presentationChannel);
    public void presentationChannelDeactivated(PresentationChannel presentationChannel);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  presentationChannelListener
    //

    PresentationChannelListener presentationChannelListener = new PresentationChannelListener()
    {
      @Override public void presentationChannelActivated(PresentationChannel presentationChannel) { System.out.println("presentationChannel activated: " + presentationChannel.getPresentationChannelID()); }
      @Override public void presentationChannelDeactivated(PresentationChannel presentationChannel) { System.out.println("presentationChannel deactivated: " + presentationChannel.getPresentationChannelID()); }
    };

    //
    //  presentationChannelService
    //

    PresentationChannelService presentationChannelService = new PresentationChannelService(Deployment.getBrokerServers(), "example-presentationchannelservice-001", Deployment.getPresentationChannelTopic(), false, presentationChannelListener);
    presentationChannelService.start();

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
