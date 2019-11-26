/****************************************************************************
*
*  DatacubeManager.java 
*
****************************************************************************/

package com.evolving.nglm.evolution.datacubes;

import com.evolving.nglm.core.*;
import com.evolving.nglm.core.utilities.UtilitiesException;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.http.HttpHost;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatacubeManager
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(DatacubeManager.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  private RestHighLevelClient elasticsearchRestClient;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public DatacubeManager(String[] args)
  {
    /*****************************************
    *
    *  args
    *
    *****************************************/
    
    String elasticsearchServerHost = args[2];
    Integer elasticsearchServerPort = Integer.parseInt(args[3]);
    
    /*****************************************
    *
    *  runtime
    *
    *****************************************/
    
    NGLMRuntime.initialize(true);

    /*****************************************
    *
    *  shutdown hook
    *
    *****************************************/
    
    NGLMRuntime.addShutdownHook(new ShutdownHook(this));

    /*****************************************
    *
    *  initialize Elasticsearch REST client
    *  
    *****************************************/
    
    try
      {
        elasticsearchRestClient = new RestHighLevelClient(RestClient.builder(new HttpHost(elasticsearchServerHost, elasticsearchServerPort, "http")));
      }
    catch (ElasticsearchException e)
      {
        throw new ServerRuntimeException("could not initialize elasticsearch client", e);
      }
  }

  /*****************************************
  *
  *  run
  *
  *****************************************/

  public void run()
  {
    DatacubeScheduler datacubeScheduler = new DatacubeScheduler();
    
    //
    // Adding datacubes scheduling
    //
    
    long uniqueID = 0;
    
    DatacubeScheduling yesOdr = new YesterdayODRDatacubeScheduling(uniqueID++, elasticsearchRestClient);
    if(yesOdr.properlyConfigured)
      {
        datacubeScheduler.schedule(yesOdr);
      }
    
    DatacubeScheduling todOdr = new TodayODRDatacubeScheduling(uniqueID++, elasticsearchRestClient);
    if(todOdr.properlyConfigured)
      {
        datacubeScheduler.schedule(todOdr);
      }
    
    DatacubeScheduling yesLoy = new YesterdayLoyaltyDatacubeScheduling(uniqueID++, elasticsearchRestClient);
    if(yesLoy.properlyConfigured)
      {
        datacubeScheduler.schedule(yesLoy);
      }
    
    DatacubeScheduling todLoy = new TodayLoyaltyDatacubeScheduling(uniqueID++, elasticsearchRestClient);
    if(todLoy.properlyConfigured)
      {
        datacubeScheduler.schedule(todLoy);
      }
    
    DatacubeScheduling yesTier = new YesterdayTierDatacubeScheduling(uniqueID++, elasticsearchRestClient);
    if(yesTier.properlyConfigured)
      {
        datacubeScheduler.schedule(yesTier);
      }
    
    DatacubeScheduling todTier = new TodayTierDatacubeScheduling(uniqueID++, elasticsearchRestClient);
    if(todTier.properlyConfigured)
      {
        datacubeScheduler.schedule(todTier);
      }
    

    log.info("Starting scheduler");
    datacubeScheduler.runScheduler();
  }

  /*****************************************
  *
  *  class ShutdownHook
  *
  *****************************************/

  private static class ShutdownHook implements NGLMRuntime.NGLMShutdownHook
  {
    //
    //  data
    //

    private DatacubeManager datacubemanager;

    //
    //  constructor
    //

    private ShutdownHook(DatacubeManager datacubemanager)
    {
      this.datacubemanager = datacubemanager;
    }

    //
    //  shutdown
    //

    @Override public void shutdown(boolean normalShutdown)
    {
      datacubemanager.shutdownUCGEngine(normalShutdown);
    }
  }

  /****************************************
  *
  *  shutdownUCGEngine
  *
  ****************************************/
  
  private void shutdownUCGEngine(boolean normalShutdown)
  {
    /*****************************************
    *
    *  stop threads
    *
    *****************************************/

    /*****************************************
    *
    *  close resources
    *
    *****************************************/

    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("Stopped DatacubeManager");
  }

  /*****************************************
  *
  *  engine
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  instance  
    //

    DatacubeManager datacubemanager = new DatacubeManager(args);
    log.info("Service initialized");

    //
    //  run
    //

    datacubemanager.run();
  }
}
