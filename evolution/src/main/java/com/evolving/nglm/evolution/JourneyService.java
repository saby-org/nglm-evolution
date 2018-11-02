/****************************************************************************
*
*  JourneyService.java
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

public class JourneyService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(JourneyService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private JourneyListener journeyListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public JourneyService(String bootstrapServers, String groupID, String journeyTopic, boolean masterService, JourneyListener journeyListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "JourneyService", groupID, journeyTopic, masterService, getSuperListener(journeyListener), notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public JourneyService(String bootstrapServers, String groupID, String journeyTopic, boolean masterService, JourneyListener journeyListener)
  {
    this(bootstrapServers, groupID, journeyTopic, masterService, journeyListener, true);
  }

  //
  //  constructor
  //

  public JourneyService(String bootstrapServers, String groupID, String journeyTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, journeyTopic, masterService, (JourneyListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(JourneyListener journeyListener)
  {
    GUIManagedObjectListener superListener = null;
    if (journeyListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { journeyListener.journeyActivated((Journey) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(GUIManagedObject guiManagedObject) { journeyListener.journeyDeactivated((Journey) guiManagedObject); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getJourneys
  *
  *****************************************/

  public String generateJourneyID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredJourney(String journeyID) { return getStoredGUIManagedObject(journeyID); }
  public Collection<GUIManagedObject> getStoredJourneys() { return getStoredGUIManagedObjects(); }
  public boolean isActiveJourney(GUIManagedObject journeyUnchecked, Date date) { return isActiveGUIManagedObject(journeyUnchecked, date); }
  public Journey getActiveJourney(String journeyID, Date date) { return (Journey) getActiveGUIManagedObject(journeyID, date); }
  public Collection<Journey> getActiveJourneys(Date date) { return (Collection<Journey>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putJourney
  *
  *****************************************/

  public void putJourney(GUIManagedObject journey) { putGUIManagedObject(journey, SystemTime.getCurrentTime()); }

  /*****************************************
  *
  *  removeJourney
  *
  *****************************************/

  public void removeJourney(String journeyID) { removeGUIManagedObject(journeyID, SystemTime.getCurrentTime()); }

  /*****************************************
  *
  *  interface JourneyListener
  *
  *****************************************/

  public interface JourneyListener
  {
    public void journeyActivated(Journey journey);
    public void journeyDeactivated(Journey journey);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  journeyListener
    //

    JourneyListener journeyListener = new JourneyListener()
    {
      @Override public void journeyActivated(Journey journey) { System.out.println("journey activated: " + journey.getJourneyID()); }
      @Override public void journeyDeactivated(Journey journey) { System.out.println("journey deactivated: " + journey.getJourneyID()); }
    };

    //
    //  journeyService
    //

    JourneyService journeyService = new JourneyService(Deployment.getBrokerServers(), "example-journeyservice-001", Deployment.getJourneyTopic(), false, journeyListener);
    journeyService.start();

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
