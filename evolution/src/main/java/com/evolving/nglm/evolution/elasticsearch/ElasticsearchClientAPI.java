package com.evolving.nglm.evolution.elasticsearch;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.DeliveryRequest;
import com.evolving.nglm.evolution.GUIManager;
import com.evolving.nglm.evolution.GUIManager.API;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.JourneyMetricDeclaration;
import com.evolving.nglm.evolution.MailNotificationManager.MailNotificationManagerRequest;
import com.evolving.nglm.evolution.NotificationManager.NotificationManagerRequest;
import com.evolving.nglm.evolution.PushNotificationManager.PushNotificationManagerRequest;
import com.evolving.nglm.evolution.SMSNotificationManager.SMSNotificationManagerRequest;
import com.evolving.nglm.evolution.ThirdPartyManager;
import com.evolving.nglm.evolution.datacubes.generator.BDRDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.generator.MDRDatacubeGenerator;
import com.evolving.nglm.evolution.reports.ReportMonoPhase;
import com.evolving.nglm.evolution.reports.bdr.BDRReportDriver;
import com.evolving.nglm.evolution.reports.bdr.BDRReportMonoPhase;
import com.evolving.nglm.evolution.reports.journeycustomerstatistics.JourneyCustomerStatisticsReportDriver;
import com.evolving.nglm.evolution.reports.notification.NotificationReportDriver;
import com.evolving.nglm.evolution.reports.notification.NotificationReportMonoPhase;
import com.evolving.nglm.evolution.reports.odr.ODRReportDriver;
import com.evolving.nglm.evolution.reports.odr.ODRReportMonoPhase;

public class ElasticsearchClientAPI extends RestHighLevelClient
{
  /*****************************************
  *
  * Static
  *
  *****************************************/
  private static final Logger log = LoggerFactory.getLogger(ElasticsearchClientAPI.class);
  
  /**
   * The maximum value of from + size for searches to one index.
   */
  public static final int MAX_RESULT_WINDOW = 10000;

  /**
   * The maximum number of buckets allowed in a single response is limited by a
   * dynamic cluster setting named search.max_buckets. It defaults to 10,000,
   * requests that try to return more than the limit will fail with an exception.
   */
  public static final int MAX_BUCKETS = 10000;
  
  // TODO factorize with SimpleESSinkConnector ?  // TODO retrieve from Deployment.json
  private static final int CONNECTTIMEOUT = 5; // in seconds
  private static final int QUERYTIMEOUT = 60;  // in seconds
  
  /*****************************************
  *
  * Temporary
  * @rl: this will need to be moved in a specific class with various ES info
  * that will also be used in "sink connectors"
  *
  *****************************************/
  public static final String JOURNEYSTATISTIC_STATUS_FIELD = "status";
  public static final String JOURNEYSTATISTIC_NODEID_FIELD = "nodeID";
  public static final String JOURNEYSTATISTIC_REWARD_FIELD = "rewards";
  public static final String JOURNEYSTATISTIC_SAMPLE_FIELD = "sample";
  public static final String[] specialExit = {"NotEligible", "Excluded", "ObjectiveLimitReached", "UCG"};
  public static String getJourneyIndex(String journeyID) {
    if(journeyID == null) {
      return "";
    }
    return "journeystatistic-" + journeyID.toLowerCase(); // same rule as JourneyStatisticESSinkConnector
  }

  /*****************************************
  *
  * Constructor wrapper (because super() must be the first statement in a constructor)
  *
  *****************************************/
  private static RestClientBuilder initRestClientBuilder(ElasticsearchConnectionSettings elasticsearchConnectionSettings) {
    RestClientBuilder restClientBuilder = RestClient.builder(elasticsearchConnectionSettings.getHosts());
    restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback()
    {
      @Override
      public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder)
      {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(elasticsearchConnectionSettings.getUser(), elasticsearchConnectionSettings.getPassword()));
        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
      }
    });
    
    restClientBuilder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback()
    {
      @Override public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder)
      {
        return requestConfigBuilder.setConnectTimeout(elasticsearchConnectionSettings.getConnectTimeout()).setSocketTimeout(elasticsearchConnectionSettings.getQueryTimeout());
      }
    });
    
    return restClientBuilder;
  }

  public static HttpHost[] parseServersConf(String servers) throws IllegalArgumentException {

    if (servers == null || servers.trim().isEmpty()) throw new IllegalArgumentException("bad servers conf for "+servers);

    List<HttpHost> toRet = new ArrayList<>();
    for(String serverString:servers.trim().split(",")) {
      String[] server = serverString.split(":");
      if(server.length!=2) throw new IllegalArgumentException("bad server conf for "+server);
      try { toRet.add(new HttpHost(server[0],Integer.valueOf(server[1]),"http")); }
      catch (NumberFormatException e) { throw new IllegalArgumentException("bad server port conf for "+server); }
    }

    return toRet.toArray(new HttpHost[toRet.size()]);

  }

  private Sniffer sniffer;// can be null if targeting only 1 host;
  
  /*****************************************
  *
  * Constructors
  *
  *****************************************/
  public ElasticsearchClientAPI(String connectionSettingsConfigName) throws ElasticsearchStatusException, ElasticsearchException {
    this(connectionSettingsConfigName,false);
  }
  public ElasticsearchClientAPI(String connectionSettingsConfigName, boolean forConnect/*this as a special default*/) throws ElasticsearchStatusException, ElasticsearchException {
    super(initRestClientBuilder(Deployment.getElasticsearchConnectionSettings(connectionSettingsConfigName, forConnect)));
    if(Deployment.getElasticsearchConnectionSettings(connectionSettingsConfigName, forConnect).getHosts().length>1) sniffer = Sniffer.builder(this.getLowLevelClient()).build();//if only 1 host provided, we do not put Sniffer (to keep the previous behavior ESRouter only)
    log.info("new ElasticsearchClientAPI created from elasticsearchConnectionSetting "+connectionSettingsConfigName);
  }

  // this is dirty, but could not find a clean way to intercept the super close call (can not just override super method)
  // anyway I don't think there is much valid reason to have instances with lifetime smaller than jvm, if so it need to be closed using this one
  // ( so CAN NOT BE USED in a try-with-resources statement )
  public void closeCleanly() throws IOException {
    if(sniffer!=null) sniffer.close();
    super.close();
  }
  
  /*****************************************
  *
  * API
  *
  *****************************************/
  public long getJourneySubscriberCount(String journeyID) throws ElasticsearchClientException {
    try {
      //
      // Build Elasticsearch query
      // 
      String index = getJourneyIndex(journeyID);
      CountRequest countRequest = new CountRequest(index).query(QueryBuilders.matchAllQuery());
      
      //
      // Send request & retrieve response synchronously (blocking call)
      // 
      CountResponse countResponse = this.count(countRequest, RequestOptions.DEFAULT);
      
      //
      // Check search response
      //
      // @rl TODO checking status seems useless because it raises exception
      if (countResponse.getFailedShards() > 0) {
        throw new ElasticsearchClientException("Elasticsearch answered with bad status.");
      }

      //
      // Send result
      //
      return countResponse.getCount();
    }
    catch (ElasticsearchClientException e) { // forward
      throw e;
    }
    catch (ElasticsearchStatusException e)
    {
      if(e.status() == RestStatus.NOT_FOUND) { // index not found
        log.debug(e.getMessage());
        return 0;
      }
      e.printStackTrace();
      throw new ElasticsearchClientException(e.getDetailedMessage());
    }
    catch (ElasticsearchException e) {
      e.printStackTrace();
      throw new ElasticsearchClientException(e.getDetailedMessage());
    }
    catch (Exception e) {
      e.printStackTrace();
      throw new ElasticsearchClientException(e.getMessage());
    }
  }

  public Map<String, Long> getJourneyNodeCount(String journeyID) throws ElasticsearchClientException {
    try {
      Map<String, Long> result = new HashMap<String, Long>();
  
      //
      // Build Elasticsearch query
      // 
      String index = getJourneyIndex(journeyID);
      String bucketName = "NODE_ID";
      SearchSourceBuilder searchSourceRequest = new SearchSourceBuilder()
          .query(QueryBuilders.matchAllQuery())
          .size(0)
          .aggregation(AggregationBuilders.terms(bucketName).field(JOURNEYSTATISTIC_NODEID_FIELD).size(MAX_BUCKETS));
      SearchRequest searchRequest = new SearchRequest(index).source(searchSourceRequest);
        //
      // Send request & retrieve response synchronously (blocking call)
      // 
      SearchResponse searchResponse = this.search(searchRequest, RequestOptions.DEFAULT);
      
      //
      // Check search response
      //
      // @rl TODO checking status seems useless because it raises exception
      if (searchResponse.isTimedOut()
          || searchResponse.getFailedShards() > 0) {
        throw new ElasticsearchClientException("Elasticsearch answered with bad status.");
      }
      
      if(searchResponse.getAggregations() == null) {
        throw new ElasticsearchClientException("Aggregation is missing in search response.");
      }
      
      ParsedStringTerms buckets = searchResponse.getAggregations().get(bucketName);
      if(buckets == null) {
        throw new ElasticsearchClientException("Buckets are missing in search response.");
      }
      
      //
      // Fill result map
      //
      for(Bucket bucket : buckets.getBuckets()) {
        result.put(bucket.getKeyAsString(), bucket.getDocCount());
      }
      
      return result;
    }
    catch (ElasticsearchClientException e) { // forward
      throw e;
    }
    catch (ElasticsearchStatusException e)
    {
      if(e.status() == RestStatus.NOT_FOUND) { // index not found
        log.debug(e.getMessage());
        return new HashMap<String, Long>();
      }
      e.printStackTrace();
      throw new ElasticsearchClientException(e.getDetailedMessage());
    }
    catch (ElasticsearchException e) {
      e.printStackTrace();
      throw new ElasticsearchClientException(e.getDetailedMessage());
    }
    catch (Exception e) {
      e.printStackTrace();
      throw new ElasticsearchClientException(e.getMessage());
    }
  }
  
  

  // @return map<STATUS,count>
  public Map<String, Long> getJourneyStatusCount(String journeyID) throws ElasticsearchClientException {
    return getGeneric("STATUS", JOURNEYSTATISTIC_STATUS_FIELD, journeyID);
  }

  // @return map<STATUS,count>
  public Map<String, Long> getJourneyBonusesCount(String journeyDisplay, int tenantID) throws ElasticsearchClientException {
    return getJourneyGenericDeliveryCount(journeyDisplay, "returnCode", BDRDatacubeGenerator.DATACUBE_ES_INDEX(tenantID));  // tX_datacube_bdr
  }


  // @return map<STATUS,count>
  public Map<String, Long> getJourneyMessagesCount(String journeyDisplay, int tenantID) throws ElasticsearchClientException {
    return getJourneyGenericDeliveryCount(journeyDisplay, "returnCode", MDRDatacubeGenerator.DATACUBE_ES_INDEX(tenantID));  // tX_datacube_messages
  }
  
  // @return map<STATUS,count>
  private Map<String, Long> getJourneyGenericDeliveryCount(String journeyDisplay, String campaignFilter, String datacubeIndex) throws ElasticsearchClientException {
    try {
      Map<String, Long> result = new HashMap<String, Long>();
  
      //
      // Build Elasticsearch query
      // 
      String statusBucketName = "STATUS";
      String sumBucketName = "SUM";
      SearchSourceBuilder searchSourceRequest = new SearchSourceBuilder()
          .query(QueryBuilders.boolQuery()
              .filter(QueryBuilders.termQuery("filter.feature", journeyDisplay))
              .mustNot(QueryBuilders.termQuery("period", 3600000))) // hack: filter out any hourly publication (definitive & preview) 
          .size(0)
          .aggregation(AggregationBuilders.terms(statusBucketName).field("filter.returnCode").size(MAX_BUCKETS)
              .subAggregation(AggregationBuilders.sum(sumBucketName).field("count")));
      SearchRequest searchRequest = new SearchRequest(datacubeIndex).source(searchSourceRequest);
      
      //
      // Send request & retrieve response synchronously (blocking call)
      // 
      SearchResponse searchResponse = this.search(searchRequest, RequestOptions.DEFAULT);
      
      //
      // Check search response
      //
      // @rl TODO checking status seems useless because it raises exception
      if (searchResponse.isTimedOut()
          || searchResponse.getFailedShards() > 0) {
        throw new ElasticsearchClientException("Elasticsearch answered with bad status.");
      }
      
      if(searchResponse.getAggregations() == null) {
        throw new ElasticsearchClientException("Aggregation is missing in search response.");
      }
      
      ParsedStringTerms buckets = searchResponse.getAggregations().get(statusBucketName);
      if(buckets == null) {
        throw new ElasticsearchClientException("Buckets are missing in search response.");
      }
      
      //
      // Fill result map
      //
      for(Bucket bucket : buckets.getBuckets()) {
        ParsedSum metricBucket = bucket.getAggregations().get(sumBucketName);
        if (metricBucket == null) {
          throw new ElasticsearchClientException("Unable to extract "+sumBucketName+" metric, aggregation is missing.");
        }
        
        result.put(bucket.getKeyAsString(), (long) metricBucket.getValue());
      }
      
      return result;
    }
    catch (ElasticsearchClientException e) { // forward
      throw e;
    }
    catch (ElasticsearchStatusException e)
    {
      if(e.status() == RestStatus.NOT_FOUND) { // index not found
        log.debug(e.getMessage());
        return new HashMap<String, Long>();
      }
      e.printStackTrace();
      throw new ElasticsearchClientException(e.getDetailedMessage());
    }
    catch (ElasticsearchException e) {
      e.printStackTrace();
      throw new ElasticsearchClientException(e.getDetailedMessage());
    }
    catch (Exception e) {
      e.printStackTrace();
      throw new ElasticsearchClientException(e.getMessage());
    }
  }
  
  // @return map<REWARD,count>
  public Map<String, Long> getDistributedRewards(String journeyID, String journeyDisplay) throws ElasticsearchClientException {

    // find list of rewards given by this journey
    
    List<String> rewards = new ArrayList<>();
    try {
      //
      // Build Elasticsearch query
      // 
      SearchSourceBuilder searchSourceRequest = new SearchSourceBuilder()
          .query(QueryBuilders.boolQuery()
              .filter(QueryBuilders.termQuery("journeyID", journeyID))); 
      String mappingIndex = "mapping_journeyrewards";
      SearchRequest searchRequest = new SearchRequest(mappingIndex).source(searchSourceRequest);

      // Read all docs from ES, on esIndex[i]
      // Write to topic, one message per document
      int scroolKeepAlive = ReportMonoPhase.getScrollKeepAlive();
      Scroll scroll = new Scroll(TimeValue.timeValueSeconds(scroolKeepAlive));
      searchRequest.source().size(ReportMonoPhase.getScrollSize());
      searchRequest.scroll(scroll);

      //
      // Send request & retrieve response synchronously (blocking call)
      // 
      SearchResponse searchResponse = this.search(searchRequest, RequestOptions.DEFAULT);
      String scrollId = searchResponse.getScrollId(); // always null

      //
      // Check search response
      //
      // @rl TODO checking status seems useless because it raises exception
      if (searchResponse.isTimedOut()
          || searchResponse.getFailedShards() > 0) {
      
      SearchHits hits = searchResponse.getHits();
      if (hits != null) {
        SearchHit[] searchHits = hits.getHits();
        while (searchHits != null && searchHits.length > 0) {
          if (log.isDebugEnabled()) log.debug("processing " + searchHits.length + " hits");
          for (SearchHit searchHit : searchHits)
            {
              Map<String, Object> sourceMap = searchHit.getSourceAsMap();
              if (sourceMap != null) {
                Object reward = sourceMap.get("reward");
                if (reward instanceof String) {
                  rewards.add((String) reward);
                }
              }
            }
          SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
          scrollRequest.scroll(scroll);
          searchResponse = this.searchScroll(scrollRequest, RequestOptions.DEFAULT);
          scrollId = searchResponse.getScrollId();
          searchHits = searchResponse.getHits().getHits();
        }
      }
      if (scrollId != null)
        {
          ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
          clearScrollRequest.addScrollId(scrollId);
          clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
          scrollId = null;
        }
      }
    }
    catch (ElasticsearchStatusException e)
    {
      if(e.status() == RestStatus.NOT_FOUND) { // index not found
        log.debug(e.getMessage());
        return new HashMap<String, Long>();
      }
      StringWriter stackTraceWriter = new StringWriter();
      e.printStackTrace(new PrintWriter(stackTraceWriter, true));
      log.info("Error in getDistributedRewards : " + e.getLocalizedMessage() + " stack : " + stackTraceWriter.toString());
      throw new ElasticsearchClientException(e.getDetailedMessage());
    }
    catch (ElasticsearchException e) {
      StringWriter stackTraceWriter = new StringWriter();
      e.printStackTrace(new PrintWriter(stackTraceWriter, true));
      log.info("Error in getDistributedRewards : " + e.getLocalizedMessage() + " stack : " + stackTraceWriter.toString());
      throw new ElasticsearchClientException(e.getMessage());
    }
    catch (Exception e) {
      StringWriter stackTraceWriter = new StringWriter();
      e.printStackTrace(new PrintWriter(stackTraceWriter, true));
      log.info("Error in getDistributedRewards : " + e.getLocalizedMessage() + " stack : " + stackTraceWriter.toString());
      throw new ElasticsearchClientException(e.getMessage());
    }
    
    // do aggregation per reward (without painless script)
    
    try {
      Map<String, Long> result = new HashMap<String, Long>();
  
      //
      // Build Elasticsearch query
      // 
      String index = "datacube_journeyrewards-" + journeyID.toLowerCase();
      SearchSourceBuilder searchSourceRequest = new SearchSourceBuilder()
          .query(QueryBuilders.boolQuery()
              .filter(QueryBuilders.termQuery("filter.journey", journeyDisplay))) 
          .size(0);

      for(String reward : rewards) 
        {
          searchSourceRequest.aggregation(AggregationBuilders.sum(reward).field("metric.reward." + reward));
        }
      SearchRequest searchRequest = new SearchRequest(index).source(searchSourceRequest);
      
      //
      // Send request & retrieve response synchronously (blocking call)
      // 
      SearchResponse searchResponse = this.search(searchRequest, RequestOptions.DEFAULT);

      //
      // Check search response
      //
      if (searchResponse.status() == RestStatus.NOT_FOUND) {
        return result; // empty map
      }

      // TODO checking status seems useless because it raises exception
      if (searchResponse.isTimedOut()
          || searchResponse.getFailedShards() > 0) {
            throw new ElasticsearchClientException("Elasticsearch answered with bad status.");
      }

      Aggregations aggregations = searchResponse.getAggregations();
      if (aggregations != null) {
        for (String reward : rewards) 
          {
            ParsedSum sum = aggregations.get(reward);
            if (sum == null) {
              log.error("Sum aggregation missing in search response for " + reward);
            }
            else {
              result.put(reward, (long) sum.getValue());
            }
          }
      }
      return result;
    }
    catch (ElasticsearchClientException e) { // forward
      throw e;
    }
    catch (ElasticsearchStatusException e)
    {
      if(e.status() == RestStatus.NOT_FOUND) { // index not found
        log.debug(e.getMessage());
        return new HashMap<String, Long>();
      }
      StringWriter stackTraceWriter = new StringWriter();
      e.printStackTrace(new PrintWriter(stackTraceWriter, true));
      log.info("Error in getDistributedRewards : " + e.getLocalizedMessage() + " stack : " + stackTraceWriter.toString());
      throw new ElasticsearchClientException(e.getDetailedMessage());
    }
    catch (ElasticsearchException e) {
      StringWriter stackTraceWriter = new StringWriter();
      e.printStackTrace(new PrintWriter(stackTraceWriter, true));
      log.info("Error in getDistributedRewards : " + e.getLocalizedMessage() + " stack : " + stackTraceWriter.toString());
      throw new ElasticsearchClientException(e.getDetailedMessage());
    }
    catch (Exception e) {
      StringWriter stackTraceWriter = new StringWriter();
      e.printStackTrace(new PrintWriter(stackTraceWriter, true));
      log.info("Error in getDistributedRewards : " + e.getLocalizedMessage() + " stack : " + stackTraceWriter.toString());
      throw new ElasticsearchClientException(e.getMessage());
    }
  }
  
  public long getSpecialExitCount(String journeyID) throws ElasticsearchClientException{
	  long count = 0;
	  if(journeyID==null)return count;
	  String index = getJourneyIndex(journeyID);
	  BoolQueryBuilder query=QueryBuilders.boolQuery();
      for(String reason : specialExit)
        query=((BoolQueryBuilder) query).should(QueryBuilders.termQuery("status", reason)); 
      log.debug("SpecialExit count query"+query.toString());
      CountRequest countRequest = new CountRequest(index).query(query);
      try {
		CountResponse countResponse = this.count(countRequest, RequestOptions.DEFAULT);
		if(countResponse!=null)
		count=countResponse.getCount();
	} catch (IndexNotFoundException ese) {
		log.info("No Index Found" + index);
		count=0; 
    }catch (Exception e) {
		e.printStackTrace();
		count=0; 
	} 
      log.debug("Sum aggregation of special exit is for journey id:"+journeyID+" is:" + count); 
	  return count;
  }
  
  /*
  public Map<String, Long> getJourneyStatusCount(String journeyID) throws ElasticsearchClientException {
    return getGeneric("STATUS", JOURNEYSTATISTIC_STATUS_FIELD, journeyID);
  }
  */

  public long getLoyaltyProgramCount(String loyaltyProgramID) throws ElasticsearchClientException
  {
    try {
      QueryBuilder queryLoyaltyProgramID = QueryBuilders.termQuery("loyaltyPrograms.programID", loyaltyProgramID);
      QueryBuilder queryEmptyExitDate = QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("loyaltyPrograms.loyaltyProgramExitDate"));
      QueryBuilder query = QueryBuilders.nestedQuery("loyaltyPrograms",
                                            QueryBuilders.boolQuery()
                                                            .filter(queryLoyaltyProgramID)
                                                            .filter(queryEmptyExitDate), ScoreMode.Total);
      CountRequest countRequest = new CountRequest("subscriberprofile").query(query);
      CountResponse countResponse = this.count(countRequest, RequestOptions.DEFAULT);
      if (countResponse.getFailedShards() > 0) {
        throw new ElasticsearchClientException("Elasticsearch answered with bad status.");
      }
      return countResponse.getCount();
    }
    catch (ElasticsearchClientException e) { // forward
      throw e;
    }
    catch (ElasticsearchStatusException e)
    {
      if(e.status() == RestStatus.NOT_FOUND) { // index not found
        log.debug(e.getMessage());
        return 0;
      }
      e.printStackTrace();
      throw new ElasticsearchClientException(e.getDetailedMessage());
    }
    catch (ElasticsearchException e) {
      e.printStackTrace();
      throw new ElasticsearchClientException(e.getDetailedMessage());
    }
    catch (Exception e) {
      e.printStackTrace();
      throw new ElasticsearchClientException(e.getMessage());
    }
  }

  public Map<String, Map<String, Long>> getMetricsPerStatus(String journeyID, int tenantID) throws ElasticsearchClientException
  {
    Map<String, Map<String, Long>> result = new LinkedHashMap<>();
    /*
     * "Entered"   -> { "rechargeAmountPrior" -> 123, "rechargeAmountDuring" -> 98, ...}
     * "Converted" -> { "rechargeAmountPrior" -> 123, "rechargeAmountDuring" -> 98, ...}
     * ....
     */
    try {
      
      /*
 
      build a request containing all rewards, to journeystatistic-JJJJJJ
  
         "aggs": {
            "status": {
              "terms": {
                "field": "status"
              },
              "aggs": {
                "rechargeAmountPrior": {
                  "sum": {
                    "field": "rechargeAmountPrior"
                  }
                },
                "rechargeAmountDuring": {
                  "sum": {
                    "field": "rechargeAmountDuring"
                  }
                },
                "rechargeAmountPost": {
                  "sum": {
                    "field": "rechargeAmountPost"
                  }
                }
              }
            }
          }
        }

      result :
              "aggregations": {
                  "status": {
                      "buckets": [
                          {
                              "key": "Entered",
                              "doc_count": 11,
                              "rechargeAmountPrior": {
                                  "value": 123
                              },
                              "rechargeAmountDuring": {
                                  "value": 119
                              },
                              "rechargeAmountPost": {
                                  "value": 49
                              }
                          },
                          {
                              "key": "Converted",
                              "doc_count": 5,
                              "rechargeAmountPrior": {
                                  "value": 15
                              },.....
                          }
                      ]
                  }
              }
       */
      
      //
      // Build Elasticsearch query
      // 
      String index = getJourneyIndex(journeyID);
      AggregationBuilder aggregationStatus =
          AggregationBuilders.terms(JOURNEYSTATISTIC_STATUS_FIELD)
                             .field(JOURNEYSTATISTIC_STATUS_FIELD).size(MAX_BUCKETS);
      String field = "";
      for (JourneyMetricDeclaration journeyMetricDeclaration : Deployment.getDeployment(tenantID).getJourneyMetricConfiguration().getMetrics().values()) {
        field = journeyMetricDeclaration.getESFieldPrior();
        aggregationStatus = aggregationStatus.subAggregation(AggregationBuilders.sum(field).field(field));
        field = journeyMetricDeclaration.getESFieldDuring();
        aggregationStatus = aggregationStatus.subAggregation(AggregationBuilders.sum(field).field(field));
        field = journeyMetricDeclaration.getESFieldPost();
        aggregationStatus = aggregationStatus.subAggregation(AggregationBuilders.sum(field).field(field));
      }

      SearchSourceBuilder searchSourceRequest = new SearchSourceBuilder()
          .query(QueryBuilders.matchAllQuery())
          .size(0)
          .aggregation(aggregationStatus);

      SearchRequest searchRequest = new SearchRequest(index).source(searchSourceRequest);
      
      //
      // Send request & retrieve response synchronously (blocking call)
      // 
      SearchResponse searchResponse = this.search(searchRequest, RequestOptions.DEFAULT);

      //
      // Check search response
      //
      if (searchResponse.status() == RestStatus.NOT_FOUND) {
        return result; // empty map
      }

      // TODO checking status seems useless because it raises exception
      if (searchResponse.isTimedOut()
          || searchResponse.getFailedShards() > 0) {
            throw new ElasticsearchClientException("Elasticsearch answered with bad status.");
      }

      Aggregations aggregations = searchResponse.getAggregations();

      if(aggregations == null) {
        throw new ElasticsearchClientException("Aggregation is missing in search response.");
      }

      ParsedStringTerms buckets = aggregations.get(JOURNEYSTATISTIC_STATUS_FIELD);
      if(buckets == null) {
        throw new ElasticsearchClientException("Buckets are missing in search response.");
      }

      /*
       * Fill result map
       * "Entered"   -> { "rechargeAmountPrior" -> 123, "rechargeAmountDuring" -> 98, ...}
       * "Converted" -> { "rechargeAmountPrior" -> 123, "rechargeAmountDuring" -> 98, ...}
       * ....
       */
      for(Bucket bucket : buckets.getBuckets()) {
        Aggregations subAgg = bucket.getAggregations();
        Map<String, Long> result2 = new LinkedHashMap<>();
        for (JourneyMetricDeclaration journeyMetricDeclaration : Deployment.getJourneyMetricConfiguration().getMetrics().values()) {
          extractFieldFromSubAgg(journeyMetricDeclaration.getESFieldPrior(),  subAgg, result2);
          extractFieldFromSubAgg(journeyMetricDeclaration.getESFieldDuring(), subAgg, result2);
          extractFieldFromSubAgg(journeyMetricDeclaration.getESFieldPost(),   subAgg, result2);
        }
        result.put(bucket.getKeyAsString(), result2);
      }
      return result;
    }
    catch (ElasticsearchClientException e) { // forward
      throw e;
    }
    catch (ElasticsearchStatusException e)
    {
      if(e.status() == RestStatus.NOT_FOUND) { // index not found
        log.debug(e.getMessage());
        return result;
      }
      throw new ElasticsearchClientException(e.getDetailedMessage());
    }
    catch (ElasticsearchException e) {
      throw new ElasticsearchClientException(e.getDetailedMessage());
    }
    catch (IOException e)
      {
        log.debug(e.getLocalizedMessage());
        throw new ElasticsearchClientException(e.getLocalizedMessage());
      }
  }

  private void extractFieldFromSubAgg(String field, Aggregations subAgg, Map<String, Long> result2)
  {
    ParsedSum sum = subAgg.get(field);
    if (sum == null) {
      log.error("Sum aggregation missing in search response for " + field);
    } else {
      result2.put(field, (long) sum.getValue());
    }
  }

  public Map<String, Long> getByAbTesting(String journeyID) throws ElasticsearchClientException
  {
    return getGeneric("SAMPLE", JOURNEYSTATISTIC_SAMPLE_FIELD, journeyID);
  }
  
  private Map<String, Long> getGeneric(String bucketName, String field, String journeyID) throws ElasticsearchClientException {
    try {
      Map<String, Long> result = new HashMap<String, Long>();

      //
      // Build Elasticsearch query
      // 
      String index = getJourneyIndex(journeyID);
      SearchSourceBuilder searchSourceRequest = new SearchSourceBuilder()
          .query(QueryBuilders.matchAllQuery())
          .size(0)
          .aggregation(AggregationBuilders.terms(bucketName).field(field).size(MAX_BUCKETS));
      SearchRequest searchRequest = new SearchRequest(index).source(searchSourceRequest);

      //
      // Send request & retrieve response synchronously (blocking call)
      // 
      SearchResponse searchResponse = this.search(searchRequest, RequestOptions.DEFAULT);

      //
      // Check search response
      //
      if(searchResponse.status() == RestStatus.NOT_FOUND) {
        return result; // empty map
      }

      // @rl TODO checking status seems useless because it raises exception
      if (searchResponse.isTimedOut()
          || searchResponse.getFailedShards() > 0) {
            throw new ElasticsearchClientException("Elasticsearch answered with bad status.");
      }

      if(searchResponse.getAggregations() == null) {
        throw new ElasticsearchClientException("Aggregation is missing in search response.");
      }

      ParsedStringTerms buckets = searchResponse.getAggregations().get(bucketName);
      if(buckets == null) {
        throw new ElasticsearchClientException("Buckets are missing in search response.");
      }

      //
      // Fill result map
      //
      for(Bucket bucket : buckets.getBuckets()) {
        result.put(bucket.getKeyAsString(), bucket.getDocCount());
      }

      return result;
    }
    catch (ElasticsearchClientException e) { // forward
      throw e;
    }
    catch (ElasticsearchStatusException e)
    {
      if(e.status() == RestStatus.NOT_FOUND) { // index not found
        log.debug(e.getMessage());
        return new HashMap<String, Long>();
      }
      e.printStackTrace();
      throw new ElasticsearchClientException(e.getDetailedMessage());
    }
    catch (ElasticsearchException e) {
      e.printStackTrace();
      throw new ElasticsearchClientException(e.getDetailedMessage());
    }
    catch (Exception e) {
      e.printStackTrace();
      throw new ElasticsearchClientException(e.getMessage());
    }
  }
  
  public List<String> getAllIndices(String indexPrefix)
  {
    List<String> result = new ArrayList<String>();
    try
      {
        GetIndexResponse getIndexResponse = this.indices().get(new GetIndexRequest(indexPrefix + "*"), RequestOptions.DEFAULT);
        String[] indices = getIndexResponse.getIndices();
        if (indices != null && indices.length > 0) result = Arrays.asList(indices);
      } 
    catch (IOException e)
      {
        if (log.isErrorEnabled()) log.error("elastic search getAllIndices error, {}", e.getMessage());
        e.printStackTrace();
      }
    return result;
  }

  /*********************************
   * 
   * Utils for GUIManager/ThirdPartyManager
   * 
   ********************************/
  //
  // getSearchRequest
  //
  public SearchRequest getSearchRequest(API api, String subscriberId, Date startDate, List<QueryBuilder> filters, int tenantID)
  {
    String timeZone = Deployment.getDeployment(tenantID).getTimeZone();
    SearchRequest searchRequest = null;
    String index = null;
    BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("subscriberID", subscriberId));
    Date indexFilterDate = RLMDateUtils.addDays(SystemTime.getCurrentTime(), -7, timeZone);
    
    //
    //  filters
    //
    
    for (QueryBuilder filter : filters)
      {
        query = query.filter(filter);
      }
    
    switch (api)
    {
      case getCustomerBDRs:
        if (startDate != null)
          {
            if (indexFilterDate.before(startDate))
              {
                List<String> esIndexDates = BDRReportMonoPhase.getEsIndexDates(startDate, SystemTime.getCurrentTime());
                String indexCSV = BDRReportMonoPhase.getESIndices(BDRReportDriver.ES_INDEX_BDR_INITIAL, esIndexDates);
                index = this.getExistingIndices(indexCSV, BDRReportMonoPhase.getESAllIndices(BDRReportDriver.ES_INDEX_BDR_INITIAL));
              }
            else
              {
                index = BDRReportMonoPhase.getESAllIndices(BDRReportDriver.ES_INDEX_BDR_INITIAL);
              }
            query = query.filter(QueryBuilders.rangeQuery("eventDatetime").gte(RLMDateUtils.formatDateForElasticsearchDefault(startDate)));
          }
        else
          {
            index = BDRReportMonoPhase.getESAllIndices(BDRReportDriver.ES_INDEX_BDR_INITIAL);
          }
        break;
        
      case getCustomerODRs:
        if (startDate != null)
          {
            if (indexFilterDate.before(startDate))
              {
                List<String> esIndexDates = ODRReportMonoPhase.getEsIndexDates(startDate, SystemTime.getCurrentTime());
                String indexCSV = ODRReportMonoPhase.getESIndices(ODRReportDriver.ES_INDEX_ODR_INITIAL, esIndexDates);
                index = this.getExistingIndices(indexCSV, ODRReportMonoPhase.getESAllIndices(ODRReportDriver.ES_INDEX_ODR_INITIAL));
              }
            else
              {
                index = ODRReportMonoPhase.getESAllIndices(ODRReportDriver.ES_INDEX_ODR_INITIAL);
              }
            query = query.filter(QueryBuilders.rangeQuery("eventDatetime").gte(RLMDateUtils.formatDateForElasticsearchDefault(startDate)));
          }
        else
          {
            index = ODRReportMonoPhase.getESAllIndices(ODRReportDriver.ES_INDEX_ODR_INITIAL);
          }
        break;
        
      case getCustomerMessages:
        if (startDate != null)
          {
            if (indexFilterDate.before(startDate))
              {
                List<String> esIndexDates = NotificationReportMonoPhase.getEsIndexDates(startDate, SystemTime.getCurrentTime(), true, tenantID);
                String indexCSV = NotificationReportMonoPhase.getESIndices(NotificationReportDriver.ES_INDEX_NOTIFICATION_INITIAL, esIndexDates);
                index = this.getExistingIndices(indexCSV, NotificationReportMonoPhase.getESAllIndices(NotificationReportDriver.ES_INDEX_NOTIFICATION_INITIAL));
              }
            else
              {
                index = NotificationReportMonoPhase.getESAllIndices(NotificationReportDriver.ES_INDEX_NOTIFICATION_INITIAL);
              }
            query = query.filter(QueryBuilders.rangeQuery("creationDate").gte(RLMDateUtils.formatDateForElasticsearchDefault(startDate)));
          }
        else
          {
            index = NotificationReportMonoPhase.getESAllIndices(NotificationReportDriver.ES_INDEX_NOTIFICATION_INITIAL);
          }
        break;
        
      case getCustomerCampaigns:
      case getCustomerJourneys:
        index = JourneyCustomerStatisticsReportDriver.JOURNEY_ES_INDEX + "*";
        break;
        
      default:
        break;
    }
    
    //
    //  searchRequest
    //
    
    searchRequest = new SearchRequest(index).source(new SearchSourceBuilder().query(query));
    
    //
    //  return
    //
    
    return searchRequest;
  }
  
  
  // UGLY - Adapter for ThirdPartyManager 
  public SearchRequest getSearchRequest(ThirdPartyManager.API api, String subscriberId, Date startDate, List<QueryBuilder> filters, int tenantID) {
    switch (api)
    {
      case getCustomerBDRs:
        return getSearchRequest(GUIManager.API.getCustomerBDRs, subscriberId, startDate, filters, tenantID);
      case getCustomerODRs:
        return getSearchRequest(GUIManager.API.getCustomerODRs, subscriberId, startDate, filters, tenantID);
      case getCustomerMessages:
        return getSearchRequest(GUIManager.API.getCustomerMessages, subscriberId, startDate, filters, tenantID);
      case getCustomerCampaigns:
      case getCustomerJourneys:
        return getSearchRequest(GUIManager.API.getCustomerCampaigns, subscriberId, startDate, filters, tenantID);
      default:
        return getSearchRequest(GUIManager.API.Unknown, subscriberId, startDate, filters, tenantID);
    }
  }

  //
  // getNotificationDeliveryRequest
  //
  public static DeliveryRequest getNotificationDeliveryRequest(String requestClass, SearchHit hit)
  {
    DeliveryRequest deliveryRequest = null;
    if (requestClass.equals(MailNotificationManagerRequest.class.getName()))
      {
        deliveryRequest = new MailNotificationManagerRequest(hit.getSourceAsMap());
      }
    else if(requestClass.equals(SMSNotificationManagerRequest.class.getName()))
      {
        deliveryRequest = new SMSNotificationManagerRequest(hit.getSourceAsMap());
      }
    else if (requestClass.equals(NotificationManagerRequest.class.getName()))
      {
        deliveryRequest = new NotificationManagerRequest(hit.getSourceAsMap());
      }
    else if (requestClass.equals(PushNotificationManagerRequest.class.getName()))
      {
        deliveryRequest = new PushNotificationManagerRequest(hit.getSourceAsMap());
      }
    else
      {
        if (log.isErrorEnabled()) log.error("invalid requestclass {}", requestClass);
      }
    return deliveryRequest;
  }

  //
  // getESHits
  //
  
  public List<SearchHit> getESHits(SearchRequest searchRequest) throws GUIManagerException
  {
    List<SearchHit> hits = new ArrayList<SearchHit>();
    Scroll scroll = new Scroll(TimeValue.timeValueSeconds(10L));
    searchRequest.scroll(scroll);
    searchRequest.source().size(1000);
    try
      {
        SearchResponse searchResponse = this.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId(); // always null
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        while (searchHits != null && searchHits.length > 0)
          {
            //
            //  add
            //
            
            hits.addAll(new ArrayList<SearchHit>(Arrays.asList(searchHits)));
            
            //
            //  scroll
            //
            
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId); 
            scrollRequest.scroll(scroll);
            searchResponse = this.searchScroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
          }
      } 
    catch (IOException e)
      {
        log.error("IOException in ES qurery {}", e.getMessage());
        throw new GUIManagerException(e);
      }
    
    //
    //  return
    //
    
    return hits;
  }

  //
  // getExistingIndices
  //
  
  public String getExistingIndices(String indexCSV, String defaulteValue)
  {
    String result = null;
    StringBuilder existingIndexes = new StringBuilder();
    boolean firstEntry = true;
    
    if (indexCSV != null)
      {
        for (String index : indexCSV.split(","))
          {
            if(index.endsWith("*")) 
              {
                if (!firstEntry) existingIndexes.append(",");
                existingIndexes.append(index); 
                firstEntry = false;
                continue;
              }
            else
              {
                GetIndexRequest request = new GetIndexRequest(index);
                request.local(false); 
                request.humanReadable(true); 
                request.includeDefaults(false); 
                try
                {
                  boolean exists = this.indices().exists(request, RequestOptions.DEFAULT);
                  if (exists) 
                    {
                      if (!firstEntry) existingIndexes.append(",");
                      existingIndexes.append(index);
                      firstEntry = false;
                    }
                } 
              catch (IOException e)
                {
                  log.info("Exception " + e.getLocalizedMessage());
                }
              }
          }
        result = existingIndexes.toString();
      }
    result = result == null || result.trim().isEmpty() ? defaulteValue : result;
    if (log.isDebugEnabled()) log.debug("reading data from index {}", result);
    return result;
  }
  
  public List<String> getAlreadyOptInSubscriberIDs(String loyaltyProgramID) throws ElasticsearchClientException
  {
    List<String> result = new ArrayList<String>();
    try
      {
        QueryBuilder queryLoyaltyProgramID = QueryBuilders.termQuery("loyaltyPrograms.programID", loyaltyProgramID);
        QueryBuilder queryEmptyExitDate = QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("loyaltyPrograms.loyaltyProgramExitDate"));
        QueryBuilder query = QueryBuilders.nestedQuery("loyaltyPrograms", QueryBuilders.boolQuery().filter(queryLoyaltyProgramID).filter(queryEmptyExitDate), ScoreMode.Total);
        SearchRequest searchRequest = new SearchRequest("subscriberprofile").source(new SearchSourceBuilder().query(query));
        List<SearchHit> hits = getESHits(searchRequest);
        for (SearchHit hit : hits)
          {
            result.add(hit.getId());
          }
        return result;
      } 
    catch (ElasticsearchStatusException e)
      {
        if (e.status() == RestStatus.NOT_FOUND)
          { 
            // index not found
            log.debug(e.getMessage());
            return result;
          }
        e.printStackTrace();
        throw new ElasticsearchClientException(e.getDetailedMessage());
      } 
    catch (ElasticsearchException e)
      {
        e.printStackTrace();
        throw new ElasticsearchClientException(e.getDetailedMessage());
      } 
    catch (Exception e)
      {
        e.printStackTrace();
        throw new ElasticsearchClientException(e.getMessage());
      }
  }
}
