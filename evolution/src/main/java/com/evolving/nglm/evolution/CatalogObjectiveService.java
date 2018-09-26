/****************************************************************************
*
*  CatalogObjectiveService.java
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

public class CatalogObjectiveService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(CatalogObjectiveService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private CatalogObjectiveListener catalogObjectiveListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public CatalogObjectiveService(String bootstrapServers, String groupID, String catalogObjectiveTopic, boolean masterService, CatalogObjectiveListener catalogObjectiveListener)
  {
    super(bootstrapServers, "CatalogObjectiveService", groupID, catalogObjectiveTopic, masterService, getSuperListener(catalogObjectiveListener));
  }

  //
  //  constructor
  //

  public CatalogObjectiveService(String bootstrapServers, String groupID, String catalogObjectiveTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, catalogObjectiveTopic, masterService, (CatalogObjectiveListener) null);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(CatalogObjectiveListener catalogObjectiveListener)
  {
    GUIManagedObjectListener superListener = null;
    if (catalogObjectiveListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { catalogObjectiveListener.catalogObjectiveActivated((CatalogObjective) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(GUIManagedObject guiManagedObject) { catalogObjectiveListener.catalogObjectiveDeactivated((CatalogObjective) guiManagedObject); }
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
    result.put("name", guiManagedObject.getJSONRepresentation().get("name"));
    result.put("display", guiManagedObject.getJSONRepresentation().get("display"));
    result.put("catalogObjectiveSectionID", guiManagedObject.getJSONRepresentation().get("catalogObjectiveSectionID"));
    return result;
  }
  
  /*****************************************
  *
  *  getCatalogObjectives
  *
  *****************************************/

  public String generateCatalogObjectiveID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredCatalogObjective(String catalogObjectiveID) { return getStoredGUIManagedObject(catalogObjectiveID); }
  public Collection<GUIManagedObject> getStoredCatalogObjectives() { return getStoredGUIManagedObjects(); }
  public boolean isActiveCatalogObjective(GUIManagedObject catalogObjectiveUnchecked, Date date) { return isActiveGUIManagedObject(catalogObjectiveUnchecked, date); }
  public CatalogObjective getActiveCatalogObjective(String catalogObjectiveID, Date date) { return (CatalogObjective) getActiveGUIManagedObject(catalogObjectiveID, date); }
  public Collection<CatalogObjective> getActiveCatalogObjectives(Date date) { return (Collection<CatalogObjective>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putCatalogObjective
  *
  *****************************************/

  public void putCatalogObjective(GUIManagedObject catalogObjective) { putGUIManagedObject(catalogObjective, SystemTime.getCurrentTime()); }

  /*****************************************
  *
  *  putIncompleteCatalogObjective
  *
  *****************************************/

  public void putIncompleteCatalogObjective(IncompleteObject catalogObjective)
  {
    putGUIManagedObject(catalogObjective, SystemTime.getCurrentTime());
  }

  /*****************************************
  *
  *  removeCatalogObjective
  *
  *****************************************/

  public void removeCatalogObjective(String catalogObjectiveID) { removeGUIManagedObject(catalogObjectiveID, SystemTime.getCurrentTime()); }

  /*****************************************
  *
  *  interface CatalogObjectiveListener
  *
  *****************************************/

  public interface CatalogObjectiveListener
  {
    public void catalogObjectiveActivated(CatalogObjective catalogObjective);
    public void catalogObjectiveDeactivated(CatalogObjective catalogObjective);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  catalogObjectiveListener
    //

    CatalogObjectiveListener catalogObjectiveListener = new CatalogObjectiveListener()
    {
      @Override public void catalogObjectiveActivated(CatalogObjective catalogObjective) { System.out.println("catalogObjective activated: " + catalogObjective.getCatalogObjectiveID()); }
      @Override public void catalogObjectiveDeactivated(CatalogObjective catalogObjective) { System.out.println("catalogObjective deactivated: " + catalogObjective.getCatalogObjectiveID()); }
    };

    //
    //  catalogObjectiveService
    //

    CatalogObjectiveService catalogObjectiveService = new CatalogObjectiveService(Deployment.getBrokerServers(), "example-catalogObjectiveservice-001", Deployment.getCatalogObjectiveTopic(), false, catalogObjectiveListener);
    catalogObjectiveService.start();

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
