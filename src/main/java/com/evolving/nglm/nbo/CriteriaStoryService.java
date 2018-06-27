/****************************************************************************
*
*  CriteriaStoryService.java
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

public class CriteriaStoryService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(CriteriaStoryService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private CriteriaStoryListener criteriaStoryListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public CriteriaStoryService(String bootstrapServers, String groupID, String criteriaStoryTopic, boolean masterService, CriteriaStoryListener criteriaStoryListener)
  {
    super(bootstrapServers, "CriteriaStoryService", groupID, criteriaStoryTopic, masterService, getSuperListener(criteriaStoryListener));
  }

  //
  //  constructor
  //

  public CriteriaStoryService(String bootstrapServers, String groupID, String criteriaStoryTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, criteriaStoryTopic, masterService, (CriteriaStoryListener) null);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(CriteriaStoryListener criteriaStoryListener)
  {
    GUIManagedObjectListener superListener = null;
    if (criteriaStoryListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { criteriaStoryListener.criteriaStoryActivated((CriteriaStory) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(GUIManagedObject guiManagedObject) { criteriaStoryListener.criteriaStoryDeactivated((CriteriaStory) guiManagedObject); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getCriteriaStories
  *
  *****************************************/

  public String generateCriteriaStoryID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredCriteriaStory(String criteriaStoryID) { return getStoredGUIManagedObject(criteriaStoryID); }
  public Collection<GUIManagedObject> getStoredCriteriaStories() { return getStoredGUIManagedObjects(); }
  public boolean isActiveCriteriaStory(GUIManagedObject criteriaStoryUnchecked, Date date) { return isActiveGUIManagedObject(criteriaStoryUnchecked, date); }
  public CriteriaStory getActiveCriteriaStory(String criteriaStoryID, Date date) { return (CriteriaStory) getActiveGUIManagedObject(criteriaStoryID, date); }
  public Collection<CriteriaStory> getActiveCriteriaStories(Date date) { return (Collection<CriteriaStory>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putCriteriaStory
  *
  *****************************************/

  public void putCriteriaStory(GUIManagedObject criteriaStory) { putGUIManagedObject(criteriaStory, SystemTime.getCurrentTime()); }

  /*****************************************
  *
  *  removeCriteriaStory
  *
  *****************************************/

  public void removeCriteriaStory(String criteriaStoryID) { removeGUIManagedObject(criteriaStoryID, SystemTime.getCurrentTime()); }

  /*****************************************
  *
  *  interface CriteriaStoryListener
  *
  *****************************************/

  public interface CriteriaStoryListener
  {
    public void criteriaStoryActivated(CriteriaStory criteriaStory);
    public void criteriaStoryDeactivated(CriteriaStory criteriaStory);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  criteriaStoryListener
    //

    CriteriaStoryListener criteriaStoryListener = new CriteriaStoryListener()
    {
      @Override public void criteriaStoryActivated(CriteriaStory criteriaStory) { System.out.println("criteriaStory activated: " + criteriaStory.getCriteriaStoryID()); }
      @Override public void criteriaStoryDeactivated(CriteriaStory criteriaStory) { System.out.println("criteriaStory deactivated: " + criteriaStory.getCriteriaStoryID()); }
    };

    //
    //  criteriaStoryService
    //

    CriteriaStoryService criteriaStoryService = new CriteriaStoryService(Deployment.getBrokerServers(), "example-criteriastoryservice-001", Deployment.getCriteriaStoryTopic(), false, criteriaStoryListener);
    criteriaStoryService.start();

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
