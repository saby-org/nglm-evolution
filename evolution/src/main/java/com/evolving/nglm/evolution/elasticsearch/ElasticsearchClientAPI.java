package com.evolving.nglm.evolution.elasticsearch;

import java.io.IOException;
import java.util.ArrayList;
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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.JourneyMetricDeclaration;
import com.evolving.nglm.evolution.datacubes.generator.BDRDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.generator.JourneyRewardsDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.generator.JourneyTrafficDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.generator.MDRDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.mapping.JourneyRewardsMap;
import com.evolving.nglm.evolution.reports.ReportMonoPhase;

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
  private static RestClientBuilder initRestClientBuilder(String serverHost, int serverPort, int connectTimeoutInMs, int queryTimeoutInMs, String userName, String userPassword) {
    RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(serverHost, serverPort, "http"));
    restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback()
    {
      @Override
      public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder)
      {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, userPassword));
        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
      }
    });
    
    restClientBuilder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback()
    {
      @Override public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder)
      {
        return requestConfigBuilder.setConnectTimeout(connectTimeoutInMs).setSocketTimeout(queryTimeoutInMs);
      }
    });
    
    return restClientBuilder;
  }
  
  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public ElasticsearchClientAPI(String serverHost, int serverPort, int connectTimeoutInMs, int queryTimeoutInMs, String userName, String userPassword) throws ElasticsearchStatusException, ElasticsearchException {
    super(initRestClientBuilder(serverHost, serverPort, connectTimeoutInMs, queryTimeoutInMs, userName, userPassword));
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
  public Map<String, Long> getJourneyBonusesCount(String journeyDisplay) throws ElasticsearchClientException {
    return getJourneyGenericDeliveryCount(journeyDisplay, "returnCode", BDRDatacubeGenerator.DATACUBE_ES_INDEX);  // datacube_bdr
  }

  // @return map<STATUS,count>
  public Map<String, Long> getJourneyMessagesCount(String journeyDisplay) throws ElasticsearchClientException {
    return getJourneyGenericDeliveryCount(journeyDisplay, "returnCode", MDRDatacubeGenerator.DATACUBE_ES_INDEX);  // datacube_messages
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
      
      int scroolKeepAlive = ReportMonoPhase.getScrollKeepAlive();
      Scroll scroll = new Scroll(TimeValue.timeValueSeconds(scroolKeepAlive));
      searchRequest.source().size(ReportMonoPhase.getScrollSize());
      searchRequest.scroll(scroll);
      SearchResponse searchResponse = this.search(searchRequest, RequestOptions.DEFAULT);
      String scrollId = searchResponse.getScrollId();
      SearchHit[] searchHits = searchResponse.getHits().getHits();
      while (searchHits != null && searchHits.length > 0)
        {
          if (log.isDebugEnabled()) log.debug("got " + searchHits.length + " hits");
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

  public Map<String, Map<String, Long>> getMetricsPerStatus(String journeyID) throws ElasticsearchClientException
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
      for (JourneyMetricDeclaration journeyMetricDeclaration : Deployment.getJourneyMetricDeclarations().values()) {
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
        for (JourneyMetricDeclaration journeyMetricDeclaration : Deployment.getJourneyMetricDeclarations().values()) {
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
}
