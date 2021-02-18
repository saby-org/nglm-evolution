/****************************************************************************
*
*  UCGEngine.java 
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.*;
import com.evolving.nglm.core.utilities.UtilitiesException;
import com.evolving.nglm.evolution.UCGRuleService.UCGRuleListener;
import com.evolving.nglm.evolution.UCGState.UCGGroup;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.google.common.collect.Sets;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.simple.JSONObject;

import java.io.IOException;

public class UCGEngine
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(UCGEngine.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  private volatile boolean stopRequested = false;
  private DynamicCriterionFieldService dynamicCriterionFieldService = null;
  private UCGRuleService ucgRuleService = null;
  private SegmentationDimensionService segmentationDimensionService = null;
  private ReferenceDataReader<String,UCGState> ucgStateReader = null;
  private KafkaProducer<byte[], byte[]> kafkaProducer = null;
  private BlockingQueue<UCGRule> evaluationRequests = new LinkedBlockingQueue<UCGRule>();
  private Thread ucgEvaluatorThread = null;
  private UCGEngineStatistics ucgEngineStatistics = null;
  private ElasticsearchClientAPI elasticsearchRestClient;
  private String subscriberGroupField = null;
  private static final HashMap<String,String> bucketAggCounters = new HashMap<>();

  /*****************************************
  *
  *  static initialization
  *
  *****************************************/

  static
  {
    //
    //  adding elements that will be used for sub bucket agg.
    //  now will be only subscribers in control group
    //  the key of hashSet is property name from UCG rule object.
    //  When the UCGRule objects will contain more than one count, here will be added the new values

    bucketAggCounters.put("ucgSubscribers","def left = doc.universalControlGroup.value; return left == true;");
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public UCGEngine(String[] args)
  {
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
    *  dynamicCriterionFieldsService
    *
    *****************************************/

    dynamicCriterionFieldService = new DynamicCriterionFieldService(Deployment.getBrokerServers(), "ucgengine-dynamiccriterionfieldservice", Deployment.getDynamicCriterionFieldTopic(), false);
    dynamicCriterionFieldService.start();
    CriterionContext.initialize(dynamicCriterionFieldService);
    
    /*****************************************
    *
    *  ucgRuleService
    *
    *****************************************/

    UCGRuleListener ucgListener = new UCGRuleListener()
    {
      @Override public void ucgRuleActivated(UCGRule ucgRule) { ruleActivated(ucgRule); }
      @Override public void ucgRuleDeactivated(String guiManagedObjectID) { }
    };
    ucgRuleService = new UCGRuleService(Deployment.getBrokerServers(), "ucgengine-ucgruleservice", Deployment.getUCGRuleTopic(), false, ucgListener);
    ucgRuleService.start();

    /*****************************************
    *
    *  segmentationDimensionService
    *
    *****************************************/

    segmentationDimensionService = new SegmentationDimensionService(Deployment.getBrokerServers(), "ucgengine-segmentationdimensionservice", Deployment.getSegmentationDimensionTopic(), false);
    segmentationDimensionService.start();
    
    /*****************************************
    *
    *  ucgStateReader
    *
    *****************************************/

    ucgStateReader = ReferenceDataReader.<String,UCGState>startReader("ucgengine-ucgstate", Deployment.getBrokerServers(), Deployment.getUCGStateTopic(), UCGState::unpack);

    /*****************************************
    *
    *  kafka producer
    *
    *****************************************/

    Properties kafkaProducerProperties = new Properties();
    kafkaProducerProperties.put("bootstrap.servers", Deployment.getBrokerServers());
    kafkaProducerProperties.put("acks", "all");
    kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProducer = new KafkaProducer<byte[], byte[]>(kafkaProducerProperties);

    /*****************************************
    *
    *  monitoring object
    *
    *****************************************/

    ucgEngineStatistics = new UCGEngineStatistics("ucgengine");

    /*****************************************
    *
    *  ucgEvaluator
    *
    *****************************************/

    Runnable ucgEvaluator = new Runnable() { @Override public void run() { runUCGEvaluator(); } };
    ucgEvaluatorThread = new Thread(ucgEvaluator, "UCGEvaluator");
    ucgEvaluatorThread.start();

    /*****************************************
    *
    *  initialize elastic search rest client 
    *****************************************/
    
    try
      {
        elasticsearchRestClient = new ElasticsearchClientAPI("UCGEngine");
        subscriberGroupField = CriterionContext.Profile.getCriterionFields().get("subscriber.segments").getESField();
      }
    catch (ElasticsearchException e)
      {
        throw new ServerRuntimeException("could not initialize elasticsearch client", e);
      }
  }

  /*****************************************
  *
  *  ruleActivated
  *
  *****************************************/

  private void ruleActivated(UCGRule activeUCGRule)
  {
    UCGState ucgState = ucgStateReader.get(UCGState.getSingletonKey());
    boolean evaluate = true;
    evaluate = evaluate && activeUCGRule.getRefreshEpoch() != null;
    evaluate = evaluate && (ucgState == null || ! Objects.equals(activeUCGRule.getUCGRuleID(), ucgState.getUCGRuleID()) || ! Objects.equals(activeUCGRule.getRefreshEpoch(), ucgState.getRefreshEpoch()));
    if (evaluate)
      {
        evaluationRequests.add(activeUCGRule);
      }
  }

  /*****************************************
  *
  *  runUCGEvaluator
  *
  *****************************************/

  private void runUCGEvaluator()
  {
    /*****************************************
    *
    *  abort if ucg evaluation is not configured
    *
    *****************************************/

    if (Deployment.getUCGEvaluationCronEntry() == null) return;

    /*****************************************
    *
    *  cron
    *
    *****************************************/

    CronFormat ucgEvaluation = null;
    try
      {
        ucgEvaluation = new CronFormat(Deployment.getUCGEvaluationCronEntry(), TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
      }
    catch (UtilitiesException e)
      {
        log.error("bad ucgEvaluationCronEntry {}", e.getMessage());
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.info(stackTraceWriter.toString());
        return;
      }

    /*****************************************
    *
    *  main loop
    *
    *****************************************/

    NGLMRuntime.registerSystemTimeDependency(this);
    Date now = SystemTime.getCurrentTime();
    Date nextUCGEvaluation = ucgEvaluation.previous(now);
    while (! stopRequested)
      {
        /*****************************************
        *
        *  wait until reload schedule is needed
        *
        *****************************************/

        synchronized (this)
          {
            while (! stopRequested && now.before(nextUCGEvaluation))
              {
                try
                  {
                    this.wait(nextUCGEvaluation.getTime() - now.getTime());
                  }
                catch (InterruptedException e)
                  {
                  }
                now = SystemTime.getCurrentTime();
              }
          }

        //
        //  abort if stopping
        //

        if (stopRequested)
          {
            continue;
          }
          
        /*****************************************
        *
        *  log start
        *
        *****************************************/

        log.info("ucgEvaluation {} (start)", nextUCGEvaluation);

        /*****************************************
        *
        *  ucg evaluation
        *
        *****************************************/

        //
        //  active rle
        //

        UCGRule activeUCGRule = null;
        for (UCGRule ucgRule : ucgRuleService.getActiveUCGRules(now))
          {
            activeUCGRule = ucgRule;
          }

        //
        //  evaluate (if necessary)
        //

        boolean evaluate = true;
        evaluate = evaluate && activeUCGRule != null;
        evaluate = evaluate && activeUCGRule.getRefreshEpoch() != null;
        evaluate = evaluate && (ucgStateReader.get(UCGState.getSingletonKey()) == null || ucgStateReader.get(UCGState.getSingletonKey()).getEvaluationDate().before(nextUCGEvaluation));
        if (evaluate)
          {
            evaluationRequests.add(activeUCGRule);
          }

        /*****************************************
        *
        *  log end
        *
        *****************************************/

        log.info("ucgEvaluation {} (end)", nextUCGEvaluation);

        /*****************************************
        *
        *  nextUCGEvaluation
        *
        *****************************************/

        nextUCGEvaluation = ucgEvaluation.next(RLMDateUtils.addSeconds(nextUCGEvaluation,1));
        log.info("ucgEvaluation {} (next)", nextUCGEvaluation);
      }
  }

  /*****************************************
  *
  *  run
  *
  *****************************************/

  public void run()
  {
    ConnectSerde<StringKey> stringKeySerde = StringKey.serde();
    ConnectSerde<UCGState> ucgStateSerde = UCGState.serde();
    while (! stopRequested)
      {
        /*****************************************
        *
        *  get next request -- drain additional requests and keep the last
        *
        *****************************************/

        UCGRule ucgRule = null;
        while (! stopRequested && ucgRule == null)
          {
            try
              {
                ucgRule = evaluationRequests.take();
                while (evaluationRequests.size() > 0)
                  {
                    ucgRule = evaluationRequests.take();
                  }
              }
            catch (InterruptedException e)
              {

              }
          }

        //
        //  stopRequested
        //

        if (stopRequested)
          {
            continue;
          }

        /*****************************************
        *
        *  now
        *
        *****************************************/

        Date now = SystemTime.getCurrentTime();

        /*****************************************
        *
        *  segmentsByDimension
        *
        *****************************************/

        List<Set<String>> segmentsByDimension = new LinkedList<Set<String>>();
        for (String dimensionID : ucgRule.getSelectedDimensions())
          {
            Set<String> segmentIDs = new HashSet<String>();
            SegmentationDimension dimension = segmentationDimensionService.getActiveSegmentationDimension(dimensionID, now);
            if (dimension != null)
              {
                for (Segment segment : dimension.getSegments())
                  {
                    segmentIDs.add(segment.getID());
                  }
              }
            segmentsByDimension.add(segmentIDs);
          }

        /*****************************************
        *
        *  strata
        *
        *****************************************/

        Set<List<String>> strata = Sets.cartesianProduct(segmentsByDimension);

        /*****************************************
        *
        *  evaluate
        *
        *****************************************/

        try
          {
            //
            //  record start time for latency logging
            //

            long startTime =  System.nanoTime();

            //
            //  calculate
            //

            UCGState ucgState = new UCGState(ucgRule, this.calculateUCGGroups(strata), now);

            //
            //  send result
            //

            kafkaProducer.send(new ProducerRecord<byte[], byte[]>(Deployment.getUCGStateTopic(), stringKeySerde.serializer().serialize(Deployment.getUCGStateTopic(), new StringKey(UCGState.getSingletonKey())), ucgStateSerde.serializer().serialize(Deployment.getUCGStateTopic(), ucgState)));

            //
            //  log processing time
            // 

            long estimatedTime = System.nanoTime() - startTime;
            double elapsedTimeInSecond = (double) estimatedTime / 1_000_000_000;
            log.info("Method 2 time {}",String.valueOf(elapsedTimeInSecond));
          }
        catch (Exception ex)
          {
            log.error("Error calculating stratum", ex.getMessage());
          }
      }
  }

  /*****************************************
  *
  *  CalculateUCGStrata
  *  calculate the count of subs and control group in each strata
  *
  *****************************************/

  private UCGGroup calculateUCGStrata(List<String> sample) throws Exception
  {
    int segmentCount = 0;
    String buckeAggtName = "UCGCount";
    int ucgCount = 0;

    BoolQueryBuilder query = QueryBuilders.boolQuery();
    List<FiltersAggregator.KeyedFilter> aggFilters = new ArrayList<>();
    try 
      {
        for (String segmentId : sample) 
          {
            query = query.filter(QueryBuilders.termQuery(subscriberGroupField, segmentId));
          }
  
        for(Map.Entry<String,String> entry : bucketAggCounters.entrySet())
          {
            aggFilters.add(new FiltersAggregator.KeyedFilter(entry.getKey(),QueryBuilders.scriptQuery(new Script(entry.getValue()))));
          }
      }
    catch (Exception ex)
      {
        throw ex;
      }
    
    try
      {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().sort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).query(query).size(0);
  
        // add bucket aggregation. Current version support only one bucket aggregation (in bucket will be aggregation specified in bucketAggCriteria
        AggregationBuilder aggregation = null;
        FiltersAggregator.KeyedFilter [] filterArray = new FiltersAggregator.KeyedFilter [aggFilters.size()];
        filterArray = aggFilters.toArray(filterArray);
        aggregation = AggregationBuilders.filters("SubscriberStateBucket",filterArray);
        searchSourceBuilder.aggregation(aggregation);
  
        //search in ES
        SearchRequest searchRequest = new SearchRequest("subscriberprofile").source(searchSourceBuilder);
        SearchResponse searchResponse = elasticsearchRestClient.search(searchRequest, RequestOptions.DEFAULT);
  
        segmentCount = (int)searchResponse.getHits().getTotalHits().value;
  
        Filters aggResultFilters = searchResponse.getAggregations().get("SubscriberStateBucket");
        ucgCount = (int)aggResultFilters.getBucketByKey("ucgSubscribers").getDocCount();
      }
    catch (IOException e)
      {
        throw e;
      }
    return new UCGGroup(new HashSet<String>(sample), ucgCount, segmentCount);
  }

  /*****************************************
  *
  *  calculateUCGGroups
  *  calculate in one ES query all ucg groups
  *
  *****************************************/

  private Set<UCGGroup> calculateUCGGroups(Set<List<String>> strata) throws Exception
  {
    HashSet<UCGGroup> returnUcgGroup = new HashSet<>();
    List<FiltersAggregator.KeyedFilter> aggFilters = new ArrayList<>();
    List<FiltersAggregator.KeyedFilter> subAggFilters = new ArrayList<>();
    List<BoolQueryBuilder> queries = new ArrayList<BoolQueryBuilder>();
    try 
      {
        //create subaggregation used for ucg count and other info in the future
        for (Map.Entry<String, String> entry : bucketAggCounters.entrySet()) 
          {
            subAggFilters.add(new FiltersAggregator.KeyedFilter(entry.getKey(), QueryBuilders.scriptQuery(new Script(entry.getValue()))));
          }
        //create bucket agg foreach stratum
        for (List<String> stratum : strata) 
          {
  
            BoolQueryBuilder query = QueryBuilders.boolQuery();
            for (String segmentId : stratum) 
              {
                query = query.filter(QueryBuilders.termQuery(subscriberGroupField, segmentId));
              }
            //the key foreach bucket will be stratum serialized. This will be deserialized at read
            HashMap<String,List<String>> key = new HashMap<>();
            key.put("stratum",stratum);
            aggFilters.add(new FiltersAggregator.KeyedFilter(JSONUtilities.encodeObject(key).toJSONString(), query));
  
          }
        //create main aggregation
        AggregationBuilder aggregation = null;
        FiltersAggregator.KeyedFilter[] filterArray = new FiltersAggregator.KeyedFilter[aggFilters.size()];
        filterArray = aggFilters.toArray(filterArray);
        aggregation = AggregationBuilders.filters("SubscriberStateBucket", filterArray);
  
        //create subagregation
        AggregationBuilder subAggregation = null;
        FiltersAggregator.KeyedFilter[] subFilterArray = new FiltersAggregator.KeyedFilter[subAggFilters.size()];
        subFilterArray = subAggFilters.toArray(subFilterArray);
        subAggregation = AggregationBuilders.filters("SubscriberState", subFilterArray);
  
        //append subbagregation to main aggregation
        aggregation.subAggregation(subAggregation);
        //create search builder
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).aggregation(aggregation).size(0);
        //search in ES
        SearchRequest searchRequest = new SearchRequest("subscriberprofile").source(searchSourceBuilder);
        SearchResponse searchResponse = elasticsearchRestClient.search(searchRequest, RequestOptions.DEFAULT);
        Filters agg = searchResponse.getAggregations().get("SubscriberStateBucket");
        agg.getBuckets().size();
        //      Filters subAgg = agg.getBuckets().get(0).getAggregations().get("SubscriberState");
        //      subAgg.getBucketByKey("ucgSubscribers").getDocCount();
        for (Filters.Bucket element : agg.getBuckets())
          {
            Filters subAgg = element.getAggregations().get("SubscriberState");
            //deserialize bucket key that contain stratum list
            JSONObject jsonRoot = (JSONObject) (new org.json.simple.parser.JSONParser()).parse(element.getKey().toString());
            List<String> stratum = JSONUtilities.decodeJSONArray(jsonRoot,"stratum",true);
            //add UCGGroup object for each bucket aggregation item
            returnUcgGroup.add(new UCGGroup(new HashSet<String>(stratum),(int)subAgg.getBucketByKey("ucgSubscribers").getDocCount(),(int)element.getDocCount()));
          }
      }
    catch (Exception ex) 
      {
        throw ex;
      }

    return returnUcgGroup;
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

    private UCGEngine ucgEngine;

    //
    //  constructor
    //

    private ShutdownHook(UCGEngine ucgEngine)
    {
      this.ucgEngine = ucgEngine;
    }

    //
    //  shutdown
    //

    @Override public void shutdown(boolean normalShutdown)
    {
      ucgEngine.shutdownUCGEngine(normalShutdown);
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

    synchronized (this)
      {
        stopRequested = true;
        if (ucgEvaluatorThread != null) ucgEvaluatorThread.interrupt();
        this.notifyAll();
      }

    /*****************************************
    *
    *  close resources
    *
    *****************************************/

    if (ucgRuleService != null) ucgRuleService.stop();
    if (segmentationDimensionService != null) segmentationDimensionService.stop();
    if (dynamicCriterionFieldService != null) dynamicCriterionFieldService.stop();
    if (kafkaProducer != null) kafkaProducer.close();

    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("Stopped UCGEngine");
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

    UCGEngine engine = new UCGEngine(args);

    //
    //  run
    //

    engine.run();
  }
}
