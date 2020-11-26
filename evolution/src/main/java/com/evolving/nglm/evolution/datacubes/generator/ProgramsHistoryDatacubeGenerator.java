package com.evolving.nglm.evolution.datacubes.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedNested;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedReverseNested;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.SubscriberProfileDatacubeMetric;
import com.evolving.nglm.evolution.datacubes.mapping.LoyaltyProgramsMap;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

public class ProgramsHistoryDatacubeGenerator extends DatacubeGenerator
{
  private static final String DATACUBE_ES_INDEX = "datacube_loyaltyprogramshistory";
  private static final String DATA_ES_INDEX = "subscriberprofile";
  private static final String DATA_POINT_EARNED = "_Earned";
  private static final String DATA_POINT_REDEEMED = "_Redeemed";
  private static final String DATA_POINT_EXPIRED = "_Expired";
  private static final String DATA_METRIC_PREFIX = "metric_";

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private LoyaltyProgramsMap loyaltyProgramsMap;
  private Map<String, SubscriberProfileDatacubeMetric> customMetrics;

  private boolean previewMode;
  private String metricTargetDay;
  private Date metricTargetDayStart;
  private Date metricTargetDayAfterStart;

  /*****************************************
  *
  * Constructors
  *
  *****************************************/
  public ProgramsHistoryDatacubeGenerator(String datacubeName, RestHighLevelClient elasticsearch, LoyaltyProgramService loyaltyProgramService)
  {
    super(datacubeName, elasticsearch);

    this.loyaltyProgramsMap = new LoyaltyProgramsMap(loyaltyProgramService);
    //TODO: this.subscriberStatusDisplayMapping = new SubscriberStatusMap();
  }

  /*****************************************
  *
  * Elasticsearch indices settings
  *
  *****************************************/
  @Override protected String getDataESIndex() { return DATA_ES_INDEX; }
  @Override protected String getDatacubeESIndex() { return DATACUBE_ES_INDEX; }

  /*****************************************
  *
  * Datacube generation phases
  *
  *****************************************/
  @Override
  protected boolean runPreGenerationPhase() throws ElasticsearchException, IOException, ClassCastException
  {
    loyaltyProgramsMap.update();
    this.customMetrics = Deployment.getSubscriberProfileDatacubeMetrics();
    //TODO: subscriberStatusDisplayMapping.updateFromElasticsearch(elasticsearch);
    return true;
  }

  @Override
  protected SearchRequest getElasticsearchRequest()
  {
    //
    // Target index
    //
    String ESIndex = getDataESIndex();
    
    //
    // Filter query
    //
    // Hack: When a newly created subscriber in Elasticsearch comes first by ExtendedSubscriberProfile sink connector,
    // it has not yet any of the "product" main (& mandatory) fields.
    // Those comes when the SubscriberProfile sink connector push them.
    // For a while, it is possible a document in subscriberprofile index miss many product fields required by datacube generation.
    // Therefore, we filter out those subscribers with missing data by looking for lastUpdateDate
    QueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders
        .rangeQuery("lastUpdateDate")
        .gte(RLMDateUtils.printTimestamp(metricTargetDayStart))
        .lt(RLMDateUtils.printTimestamp(metricTargetDayAfterStart)));
    
    //
    // Aggregations
    //
    List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
    sources.add(new TermsValuesSourceBuilder("loyaltyProgramID").field("loyaltyPrograms.programID"));
    sources.add(new TermsValuesSourceBuilder("tier").field("loyaltyPrograms.tierName").missingBucket(false)); // Missing means opt-out. Do not count them here
    sources.add(new TermsValuesSourceBuilder("redeemer").field("loyaltyPrograms.rewardTodayRedeemer").missingBucket(false)); // Missing should NOT happen
    
    //
    // Sub Aggregation STATUS(filter) with metrics
    //
    TermsAggregationBuilder metrics = AggregationBuilders.terms("STATUS").field("evolutionSubscriberStatus").missing("undefined"); // default bucket for status=null
    
    //
    // Rewards
    //
    List<String> rewardIdList = new ArrayList<String>(); // Purpose is to have only one occurrence by rewardID (remove duplicate when different programs have the same reward)
    for(String programID : loyaltyProgramsMap.keySet()) {
      String rewardID = loyaltyProgramsMap.getRewardPointsID(programID, "getElasticsearchRequest-rewardID");
      if(!rewardIdList.contains(rewardID)) {
        rewardIdList.add(rewardID);
      }
    }
    
    for(String rewardID: rewardIdList) {
      metrics.subAggregation(AggregationBuilders.sum(rewardID + DATA_POINT_EARNED).field("pointFluctuations." + rewardID + ".today.earned"));
      metrics.subAggregation(AggregationBuilders.sum(rewardID + DATA_POINT_REDEEMED).field("pointFluctuations." + rewardID + ".today.redeemed"));
      metrics.subAggregation(AggregationBuilders.sum(rewardID + DATA_POINT_EXPIRED).field("pointFluctuations." + rewardID + ".today.expired"));
    }
    
    //
    // Subscriber Metrics
    //
    for(String metricID: customMetrics.keySet()) {
      SubscriberProfileDatacubeMetric metric = customMetrics.get(metricID);
      
      metrics.subAggregation(AggregationBuilders.sum(DATA_METRIC_PREFIX + metricID).field(metric.getTodayESField()));
    }
    
    AggregationBuilder aggregation = AggregationBuilders.nested("DATACUBE", "loyaltyPrograms").subAggregation(
        AggregationBuilders.composite("LOYALTY-COMPOSITE", sources).size(ElasticsearchClientAPI.MAX_BUCKETS).subAggregation(
            AggregationBuilders.reverseNested("REVERSE").subAggregation(metrics) // *metrics is STATUS with metrics
        )
    );
    
    //
    // Datacube request
    //
    SearchSourceBuilder datacubeRequest = new SearchSourceBuilder()
        .sort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
        .query(query)
        .aggregation(aggregation)
        .size(0);
    
    return new SearchRequest(ESIndex).source(datacubeRequest);
  }
  
  @Override
  protected void embellishFilters(Map<String, Object> filters)
  {
    String loyaltyProgramID = (String) filters.remove("loyaltyProgramID");
    filters.put("loyaltyProgram", loyaltyProgramsMap.getDisplay(loyaltyProgramID, "loyaltyProgram"));
    
    // "tier" stay the same 
    // "evolutionSubscriberStatus" stay the same. TODO: retrieve display for evolutionSubscriberStatus
    // "redeemer" stay the same    
  }

  @Override
  protected List<Map<String, Object>> extractDatacubeRows(SearchResponse response, String timestamp, long period) throws ClassCastException
  {
    List<Map<String,Object>> result = new ArrayList<Map<String,Object>>();
    
    if (response.isTimedOut()
        || response.getFailedShards() > 0
        || response.getSkippedShards() > 0
        || response.status() != RestStatus.OK) {
      log.error("Elasticsearch search response return with bad status in {} generation.", getDatacubeName());
      return result;
    }
    
    if(response.getAggregations() == null) {
      log.error("Main aggregation is missing in {} search response.", getDatacubeName());
      return result;
    }
    
    ParsedNested parsedNested = response.getAggregations().get("DATACUBE");
    if(parsedNested == null || parsedNested.getAggregations() == null) {
      log.error("Nested aggregation is missing in {} search response.", getDatacubeName());
      return result;
    }
    
    ParsedComposite parsedComposite = parsedNested.getAggregations().get("LOYALTY-COMPOSITE");
    if(parsedComposite == null || parsedComposite.getBuckets() == null) {
      log.error("Composite buckets are missing in {} search response.", getDatacubeName());
      return result;
    }
    
    for(ParsedComposite.ParsedBucket bucket: parsedComposite.getBuckets()) {
      //
      // Extract one part of the filter
      //
      Map<String, Object> filters = bucket.getKey();
      for(String key: filters.keySet()) {
        if(filters.get(key) == null) {
          filters.replace(key, UNDEFINED_BUCKET_VALUE);
        }
      }
      
      String loyaltyProgramID = (String) filters.get("loyaltyProgramID");
      String rewardID = loyaltyProgramsMap.getRewardPointsID(loyaltyProgramID, "extractDatacubeRows-rewardID");

      //
      // Extract the second part of the filter
      //
      if(bucket.getAggregations() == null) {
        log.error("Aggregations in bucket is missing in {} search response.", getDatacubeName());
        continue;
      }
      
      ParsedReverseNested parsedReverseNested = bucket.getAggregations().get("REVERSE");
      if(parsedReverseNested == null || parsedReverseNested.getAggregations() == null) {
        log.error("Reverse nested aggregation is missing in {} search response.", getDatacubeName());
        continue;
      }
      
      ParsedTerms parsedTerms = parsedReverseNested.getAggregations().get("STATUS");
      if(parsedTerms == null || parsedTerms.getBuckets() == null) {
        log.error("Composite buckets are missing in {} search response.", getDatacubeName());
        continue;
      }

      for(org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket statusBucket: parsedTerms.getBuckets()) {
        Map<String, Object> filtersCopy = new HashMap<String, Object>(filters);
        filtersCopy.put("evolutionSubscriberStatus", statusBucket.getKey());
        long docCount = statusBucket.getDocCount();

        //
        // Extract metrics
        //
        HashMap<String, Object> metrics = new HashMap<String,Object>();
        if (statusBucket.getAggregations() == null) {
          log.error("Unable to extract metrics, aggregations are missing.");
          continue;
        }
        
        //
        // Extract rewards
        // 
        if (rewardID != null) { // Otherwise no reward for this loyalty program
          ParsedSum rewardEarned = statusBucket.getAggregations().get(rewardID + DATA_POINT_EARNED);
          if (rewardEarned == null) {
            log.error("Unable to extract rewards.earned metric for reward: " + rewardID + ", aggregation is missing.");
            continue;
          }
          metrics.put("rewards.earned", (int) rewardEarned.getValue());
          
          ParsedSum rewardRedeemed = statusBucket.getAggregations().get(rewardID + DATA_POINT_REDEEMED);
          if (rewardRedeemed == null) {
            log.error("Unable to extract rewards.redeemed metric for reward: " + rewardID + ", aggregation is missing.");
            continue;
          }
          metrics.put("rewards.redeemed", (int) rewardRedeemed.getValue());
          
          ParsedSum rewardExpired = statusBucket.getAggregations().get(rewardID + DATA_POINT_EXPIRED);
          if (rewardExpired == null) {
            log.error("Unable to extract rewards.expired metric for reward: " + rewardID + ", aggregation is missing.");
            continue;
          }
          metrics.put("rewards.expired", (int) rewardExpired.getValue());
        }
        
        //
        // Subscriber Metrics
        //
        for(String metricID: customMetrics.keySet()) {
          SubscriberProfileDatacubeMetric subscriberProfileCustomMetric = customMetrics.get(metricID);
          
          ParsedSum customMetric = statusBucket.getAggregations().get(DATA_METRIC_PREFIX + metricID);
          if (customMetric == null) {
            log.error("Unable to extract custom." + metricID + ", aggregation is missing.");
            continue;
          }
          metrics.put("custom." + subscriberProfileCustomMetric.getDisplay(), (int) customMetric.getValue());
        }
        
        //
        // Build row
        //
        Map<String, Object> row = extractRow(filtersCopy, docCount, timestamp, period, metrics);
        result.add(row);
      }
    }
    
    return result;
  }
  
  /*****************************************
  *
  * DocumentID settings
  *
  *****************************************/
  /**
   * For the moment, we only publish with a period of the day (except for preview)
   * In order to keep only one document per day (for each combination of filters), we use the following trick:
   * We only use the day as a timestamp (without the hour) in the document ID definition.
   * This way, preview documents will override each other till be overriden by the definitive one at 23:59:59.999 
   * 
   * Be careful, it only works if we ensure to publish the definitive one. 
   * Already existing combination of filters must be published even if there is 0 count inside, in order to 
   * override potential previews.
   */
  @Override
  protected String getDocumentID(Map<String,Object> filters, String timestamp) {
    return this.extractDocumentIDFromFilter(filters, this.metricTargetDay);
  }
  
  /*****************************************
  *
  * Datacube name for logs
  *
  *****************************************/
  @Override
  protected String getDatacubeName() {
    return super.getDatacubeName() + (this.previewMode ? "(preview)" : "(definitive)");
  }
  
  /*****************************************
  *
  * Run
  *
  *****************************************/
  /**
   * The definitive datacube is at yesterday_23:59:59.999+ZZZZ
   *
   * In this datacube, period is not used ATM, but still set at one day.
   */
  public void definitive()
  {
    Date now = SystemTime.getCurrentTime();
    Date yesterday = RLMDateUtils.addDays(now, -1, Deployment.getBaseTimeZone());
    
    // Dates: YYYY-MM-dd 00:00:00.000
    Date beginningOfYesterday = RLMDateUtils.truncate(yesterday, Calendar.DATE, Deployment.getBaseTimeZone());
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, Deployment.getBaseTimeZone());

    this.previewMode = false;
    this.metricTargetDay = RLMDateUtils.printDay(yesterday);
    this.metricTargetDayStart = beginningOfYesterday;
    this.metricTargetDayAfterStart = beginningOfToday;

    //
    // Timestamp & period
    //
    Date endOfYesterday = RLMDateUtils.addMilliseconds(beginningOfToday, -1);                               // 23:59:59.999
    String timestamp = RLMDateUtils.printTimestamp(endOfYesterday);
    long targetPeriod = beginningOfToday.getTime() - beginningOfYesterday.getTime();    // most of the time 86400000ms (24 hours)
    
    this.run(timestamp, targetPeriod);
  }
  
  /**
   * A preview is a datacube generation on the today's day. 
   *
   * In this datacube, period is not used ATM, but still set at one day.
   */
  public void preview()
  {
    Date now = SystemTime.getCurrentTime();
    Date tomorrow = RLMDateUtils.addDays(now, 1, Deployment.getBaseTimeZone());
    
    // Dates: YYYY-MM-dd 00:00:00.000
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, Deployment.getBaseTimeZone());
    Date beginningOfTomorrow = RLMDateUtils.truncate(tomorrow, Calendar.DATE, Deployment.getBaseTimeZone());
    
    this.previewMode = true;
    this.metricTargetDay = RLMDateUtils.printDay(now);
    this.metricTargetDayStart = beginningOfToday;
    this.metricTargetDayAfterStart = beginningOfTomorrow;

    //
    // Timestamp & period
    //
    String timestamp = RLMDateUtils.printTimestamp(now);
    long targetPeriod = now.getTime() - beginningOfToday.getTime() + 1; // +1 !
    
    this.run(timestamp, targetPeriod);
  }
}
