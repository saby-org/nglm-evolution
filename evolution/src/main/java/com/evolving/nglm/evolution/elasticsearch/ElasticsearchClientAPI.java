package com.evolving.nglm.evolution.elasticsearch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.datacubes.mapping.JourneyRewardsMap;

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
  public static String JOURNEYSTATISTIC_STATUS_FIELD = "status";
  public static String JOURNEYSTATISTIC_NODEID_FIELD = "nodeID";
  public static String JOURNEYSTATISTIC_REWARD_FIELD = "rewards";
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
  
  public Map<String, Long> getJourneyStatusCount(String journeyID) throws ElasticsearchClientException {
    try {
      Map<String, Long> result = new HashMap<String, Long>();
  
      //
      // Build Elasticsearch query
      // 
      String index = getJourneyIndex(journeyID);
      String bucketName = "STATUS";
      SearchSourceBuilder searchSourceRequest = new SearchSourceBuilder()
          .query(QueryBuilders.matchAllQuery())
          .size(0)
          .aggregation(AggregationBuilders.terms(bucketName).field(JOURNEYSTATISTIC_STATUS_FIELD).size(MAX_BUCKETS));
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

  public Map<String, Long> getDistributedRewards(String journeyID) throws ElasticsearchClientException
  {
    try {
      Map<String, Long> result = new HashMap<>();
      
      /*
       *  1) get list of rewards with :
       *  
       *  utiliser l'objet java map xxrewards
       *  qui est utilisé dans datacube_journeyrewards
       *  update it before use :
       *   this.journeyRewardsList.update(this.journeyID, this.getDataESIndex());
 
      2) build a request containing all rewards, to journeystatistic-JJJJJJ
      
           {
              "size": 0,
              "aggs": {
                "GigaBytes": {
                  "sum": {
                    "field": "rewards.GigaBytes"
                  }
                },
                "disp_PTT_100138": {
                  "sum": {
                    "field": "rewards.disp_PTT_100138"
                  }
                }
              }
            }
            
           *   result :
              
              "aggregations": {
                "disp_PTT_100138": {
                  "value": 4.0
                },
                "GigaBytes": {
                  "value": 20.0
                }
              }
       */
      
      //
      // Build Elasticsearch query
      // 
      String index = getJourneyIndex(journeyID);
      SearchSourceBuilder searchSourceRequest = new SearchSourceBuilder()
          .query(QueryBuilders.matchAllQuery())
          .size(0);

      JourneyRewardsMap journeyRewardsList = new JourneyRewardsMap(this);
      journeyRewardsList.update(journeyID, index);
      
      for(String reward : journeyRewardsList.getRewards()) 
        {
          searchSourceRequest.aggregation(AggregationBuilders.sum(reward).field(JOURNEYSTATISTIC_REWARD_FIELD + "." + reward));
        }
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

      Aggregations aggregations = searchResponse.getAggregations();
      if(aggregations == null) {
        throw new ElasticsearchClientException("aggregations are missing in search response.");
      }

      //
      // Fill result map
      //
      for (String reward : journeyRewardsList.getRewards()) 
        {
          ParsedStringTerms buckets = aggregations.get(reward);
          if (buckets.getBuckets().size() != 1)
            {
              log.error("very strange");
            }
          for (Bucket bucket : buckets.getBuckets())
            {
              result.put(reward, bucket.getDocCount());              
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

  public Map<String, Integer> getByAbTesting(String journeyID)
  {
    Map<String, Integer> res = new HashMap<>();
    return res;
  }
}
