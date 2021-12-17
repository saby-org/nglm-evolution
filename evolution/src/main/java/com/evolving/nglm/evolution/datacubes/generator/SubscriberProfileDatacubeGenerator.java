package com.evolving.nglm.evolution.datacubes.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite.ParsedBucket;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.range.ParsedRange;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.SegmentationDimensionService;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.DatacubeManager;
import com.evolving.nglm.evolution.datacubes.DatacubeWriter;
import com.evolving.nglm.evolution.datacubes.SimpleDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.SubscriberProfileDatacubeMetric;
import com.evolving.nglm.evolution.datacubes.mapping.SegmentationDimensionsMap;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

public class SubscriberProfileDatacubeGenerator extends DatacubeGenerator
{
  private static final String DATACUBE_ES_INDEX_SUFFIX = "_datacube_subscriberprofile";
  public static final String DATACUBE_ES_INDEX(int tenantID) { return "t" + tenantID + DATACUBE_ES_INDEX_SUFFIX; }
  private static final String DATA_ES_INDEX = "subscriberprofile";
  private static final String FILTER_STRATUM_PREFIX = "stratum.";
  private static final String METRIC_PREFIX = "metric_";

  //EVPRO-1172
  private static final String STATUS_PREVIOUS_EVOLUTION = "status_previous_evolutionSubscriberStatus";
  private static final String STATUS_PREVIOUS_UCG = "status_previous_universalControlGroup";
  private static final String EVOLUTION_STATUS = "evolutionSubscriberStatus";
  private static final String UCG = "universalControlGroup";
  private static final String EVOLUTION_STATUS_PREVIOUS = "previousEvolutionSubscriberStatus";
  private static final String UCG_PREVIOUS = "universalControlGroupPrevious";

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private SegmentationDimensionsMap segmentationDimensionList;

  private String metricTargetDay;
  private long metricTargetDayStartTime;
  private long metricTargetDayDuration;
  private long metricTargetDayAfterStartTime;
  private long metricTargetDayAfterDuration;

  /*****************************************
  *
  * Constructors
  *
  *****************************************/
  public SubscriberProfileDatacubeGenerator(String datacubeName, ElasticsearchClientAPI elasticsearch, DatacubeWriter datacubeWriter, SegmentationDimensionService segmentationDimensionService, int tenantID, String timeZone)
  {
    super(datacubeName, elasticsearch, datacubeWriter, tenantID, timeZone);

    this.segmentationDimensionList = new SegmentationDimensionsMap(segmentationDimensionService);
  }
  
  public SubscriberProfileDatacubeGenerator(String datacubeName, int tenantID, DatacubeManager datacubeManager) {
    this(datacubeName,
        datacubeManager.getElasticsearchClientAPI(),
        datacubeManager.getDatacubeWriter(),
        datacubeManager.getSegmentationDimensionService(),
        tenantID,
        Deployment.getDeployment(tenantID).getTimeZone());
  }

  /*****************************************
  *
  * Elasticsearch indices settings
  *
  *****************************************/
  @Override protected String getDataESIndex() { return DATA_ES_INDEX; }
  @Override protected String getDatacubeESIndex() { return DATACUBE_ES_INDEX(this.tenantID); }
  
  //
  // Subset of subscriberprofile
  //
  // Hack: When a newly created subscriber in Elasticsearch comes first by ExtendedSubscriberProfile sink connector,
  // it has not yet any of the "product" main (& mandatory) fields.
  // Those comes when the SubscriberProfile sink connector push them.
  // For a while, it is possible a document in subscriberprofile index miss many product fields required by datacube generation.
  // Therefore, we filter out those subscribers with missing data
  protected List<QueryBuilder> getFilterQueries() 
  {
    return Collections.singletonList(QueryBuilders.existsQuery("lastUpdateDate"));
  }
  
  
  /*****************************************
  *
  * Filters settings
  *
  *****************************************/
  private List<String> getFilterFields() { 
	    //
	    // Build filter fields
	    //
	    List<String> filterFields = new ArrayList<String>();

	    // getFilterFields is called after runPreGenerationPhase. It safe to assume segmentationDimensionList is up to date.
	    // Filter out "non statistics" dimensions.
	    for(String dimensionID: segmentationDimensionList.keySet()) {
	      if (segmentationDimensionList.isFlaggedStatistics(dimensionID)) {
	        filterFields.add(FILTER_STRATUM_PREFIX + dimensionID);
	      }
	    }
	    
	  filterFields.add(EVOLUTION_STATUS);
	  filterFields.add(UCG);
	  filterFields.add(UCG_PREVIOUS);
	  filterFields.add(EVOLUTION_STATUS_PREVIOUS);
	  return filterFields; 
}
  
  
  @Override
  protected boolean runPreGenerationPhase() throws ElasticsearchException, IOException, ClassCastException
  {
    this.segmentationDimensionList.update();
    
    return true;
  }
  
  @Override 
  protected void embellishFilters(Map<String, Object> filters) 
  {
    //
    // Special dimension with all, for Grafana 
    //
    filters.put(FILTER_STRATUM_PREFIX + "Global", " ");
    
    //
    // subscriberStratum dimensions
    //
    for(String dimensionID: segmentationDimensionList.keySet()) {
      if (segmentationDimensionList.isFlaggedStatistics(dimensionID)) {
        String fieldName = FILTER_STRATUM_PREFIX + dimensionID;
        String segmentID = (String) filters.remove(fieldName);
        
        String newFieldName = FILTER_STRATUM_PREFIX + segmentationDimensionList.getDimensionDisplay(dimensionID, fieldName);
        filters.put(newFieldName, segmentationDimensionList.getSegmentDisplay(dimensionID, segmentID, fieldName));
      }
    }
  }

  /*****************************************
  *
  * Metrics settings
  *
  *****************************************/
  private List<AggregationBuilder> getMetricAggregations()
  {
    String targetDayBeginningIncluded = metricTargetDayStartTime + "L";
    String targetDayEndExcluded = (metricTargetDayStartTime+metricTargetDayDuration) + "L";
    String targetDayAfterBeginningIncluded = metricTargetDayAfterStartTime + "L";
    String targetDayAfterEndExcluded = (metricTargetDayAfterStartTime+metricTargetDayAfterDuration) + "L";
    
    List<AggregationBuilder> metricAggregations = new ArrayList<AggregationBuilder>();
    
    Map<String, SubscriberProfileDatacubeMetric> customMetrics = Deployment.getSubscriberProfileDatacubeConfiguration().getMetrics();
    for(String metricID: customMetrics.keySet()) {
      SubscriberProfileDatacubeMetric customMetric = customMetrics.get(metricID);
      AggregationBuilder customMetricAgg = AggregationBuilders.sum(METRIC_PREFIX+metricID)
          .script(new Script(ScriptType.INLINE, "painless", "def left = 0;"
          + " def updateTime = doc['lastUpdateDate'].value.toInstant().toEpochMilli();"
          + " if(updateTime >= "+ targetDayAfterBeginningIncluded +" && updateTime < "+ targetDayAfterEndExcluded +"){"
            + " left = params._source['"+ customMetric.getYesterdayESField() +"'];"
          + " } else if(updateTime >= "+ targetDayBeginningIncluded +" && updateTime < "+ targetDayEndExcluded +"){"
            + " left = params._source['"+ customMetric.getTodayESField() +"'];"
          + " } return left;", Collections.emptyMap()));
      metricAggregations.add(customMetricAgg);
    }  
    
    //EVPRO-1172:
    //Add status.previous and ucg.previous
    int periodROI = Deployment.getSubscriberProfileDatacubeConfiguration().getPeriodROI();
    String unitTimeROI = Deployment.getSubscriberProfileDatacubeConfiguration().getTimeUnitROI();
    //diff = currentDate-period
    Date diffPeriod = null;
    Date now = SystemTime.getCurrentTime();
    if(unitTimeROI.equals("day")) {
    	diffPeriod = RLMDateUtils.addDays(now, -1*periodROI, this.getTimeZone());
    } if(unitTimeROI.equals("week")) {
    	diffPeriod = RLMDateUtils.addWeeks(now, -1*periodROI, this.getTimeZone());
    } if(unitTimeROI.equals("month")) {
    	diffPeriod = RLMDateUtils.addMonths(now, -1*periodROI, this.getTimeZone());
    } 

    AggregationBuilder customMetricAgg = AggregationBuilders.range("DATE_BUCKETS_EVOLUTION_SUBSCRIBER_STATUS")
    		.field("evolutionSubscriberStatusChangeDate")
    		.addUnboundedTo(diffPeriod.getTime())
    		.addUnboundedFrom(diffPeriod.getTime());
    metricAggregations.add(customMetricAgg);
    
    AggregationBuilder customMetricAgg_UCG = AggregationBuilders.range("DATE_BUCKETS_UCG")
    		.field("universalControlGroupChangeDate")
    		.addUnboundedTo(diffPeriod.getTime())
    		.addUnboundedFrom(diffPeriod.getTime());
    metricAggregations.add(customMetricAgg_UCG); 

    return metricAggregations;
  }

  private Map<String, Long> extractMetrics(ParsedBucket compositeBucket) throws ClassCastException
  {    
    HashMap<String, Long> metrics = new HashMap<String,Long>();
    
    if (compositeBucket.getAggregations() == null) {
      log.error("Unable to extract metrics, aggregation is missing.");
      return metrics;
    }

    Map<String, SubscriberProfileDatacubeMetric> customMetrics = Deployment.getSubscriberProfileDatacubeConfiguration().getMetrics();
    for(String metricID: customMetrics.keySet()) {
      SubscriberProfileDatacubeMetric customMetric = customMetrics.get(metricID);
      
      ParsedSum metricBucket = compositeBucket.getAggregations().get(METRIC_PREFIX+metricID);
      if (metricBucket == null) {
        log.error("Unable to extract "+metricID+" custom metric, aggregation is missing.");
        return metrics;
      }
      metrics.put("custom." + customMetric.getDisplay(), new Long((int) metricBucket.getValue()));
    }
    
    //EVPRO-1172: ROI data
    ParsedRange parsedDateBuckets = compositeBucket.getAggregations().get("DATE_BUCKETS_EVOLUTION_SUBSCRIBER_STATUS");
    if(parsedDateBuckets == null || parsedDateBuckets.getBuckets() == null) {
      log.error("Date Range buckets DATE_BUCKETS_EVOLUTION_SUBSCRIBER_STATUS are missing in search response.");
      return metrics;
    }
    for(org.elasticsearch.search.aggregations.bucket.range.Range.Bucket dateBucket: parsedDateBuckets.getBuckets()) {
      //
      // Extract metrics prefix (from date)
      //
      if(dateBucket.getKeyAsString().startsWith("*-")) {
    	  metrics.put("custom."+STATUS_PREVIOUS_EVOLUTION, new Long(dateBucket.getDocCount()));
      }      
    }
    ParsedRange parsedDateBucketsUCG = compositeBucket.getAggregations().get("DATE_BUCKETS_UCG");
    if(parsedDateBucketsUCG == null || parsedDateBucketsUCG.getBuckets() == null) {
      log.error("Date Range buckets DATE_BUCKETS_UCG are missing in search response.");
      return metrics;
    }
    for(org.elasticsearch.search.aggregations.bucket.range.Range.Bucket dateBucket: parsedDateBucketsUCG.getBuckets()) {
      //
      // Extract metrics prefix (from date)
      //
      if(dateBucket.getKeyAsString().startsWith("*-")) {
    	  metrics.put("custom."+STATUS_PREVIOUS_UCG, new Long(dateBucket.getDocCount()));
      }      
    }
    return metrics;
  }
  
  /*****************************************
  *
  * DocumentID settings
  *
  *****************************************/
  /**
   * In order to override preview documents, we use the following trick: the timestamp used in the document ID must be 
   * the timestamp of the definitive push (and not the time we publish it).
   * This way, preview documents will override each other till be overriden by the definitive one running the day after.
   * 
   * Be careful, it only works if we ensure to publish the definitive one. 
   * Already existing combination of filters must be published even if there is 0 count inside, in order to 
   * override potential previews.
   */
  @Override
  protected String getDocumentID(Map<String,Object> filters, String timestamp) {
    return this.extractDocumentIDFromFilter(filters, this.metricTargetDay, "default");
  }

  /*****************************************
  *
  * Run
  *
  *****************************************/
  /**
   * The definitive datacube is generated on yesterday target day for metrics
   * Rows will be timestamped at yesterday_23:59:59.999+ZZZZ
   * This way it shows that the metric are computed for this day (yesterday) but at the very end of the day.
   */
  public void definitive()
  {
    Date now = SystemTime.getCurrentTime();
    Date yesterday = RLMDateUtils.addDays(now, -1, this.getTimeZone());
    Date tomorrow = RLMDateUtils.addDays(now, 1, this.getTimeZone());
    
    // Dates: yyyy-MM-dd 00:00:00.000
    Date beginningOfYesterday = RLMDateUtils.truncate(yesterday, Calendar.DATE, this.getTimeZone());
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, this.getTimeZone());
    Date beginningOfTomorrow = RLMDateUtils.truncate(tomorrow, Calendar.DATE, this.getTimeZone());

    this.metricTargetDay = this.printDay(yesterday);
    this.metricTargetDayStartTime = beginningOfYesterday.getTime();
    this.metricTargetDayDuration = beginningOfToday.getTime() - beginningOfYesterday.getTime();
    this.metricTargetDayAfterStartTime = beginningOfToday.getTime();
    this.metricTargetDayAfterDuration = beginningOfTomorrow.getTime() - beginningOfToday.getTime();

    //
    // Timestamp & period
    //
    Date endOfYesterday = RLMDateUtils.addMilliseconds(beginningOfToday, -1);                               // 23:59:59.999
    String timestamp = this.printTimestamp(endOfYesterday);
    long targetPeriod = beginningOfToday.getTime() - beginningOfYesterday.getTime();    // most of the time 86400000ms (24 hours)
    
    this.run(timestamp, targetPeriod);
  }
  
  /**
   * A preview is a datacube generation on the today's day. 
   * Because the day is still not ended, it won't be the definitive value of metrics.
   * Reminder, custom metrics target a whole day (today recharge, today usage, etc.)
   */ 
  public void preview()
  {
    Date now = SystemTime.getCurrentTime();
    Date tomorrow = RLMDateUtils.addDays(now, 1, this.getTimeZone());
    Date twodaysafter = RLMDateUtils.addDays(now, 2, this.getTimeZone());

    // Dates: yyyy-MM-dd 00:00:00.000
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, this.getTimeZone());
    Date beginningOfTomorrow = RLMDateUtils.truncate(tomorrow, Calendar.DATE, this.getTimeZone());
    Date beginningOfTwodaysafter = RLMDateUtils.truncate(twodaysafter, Calendar.DATE, this.getTimeZone());

    this.metricTargetDay = this.printDay(now);
    this.metricTargetDayStartTime = beginningOfToday.getTime();
    this.metricTargetDayDuration = beginningOfTomorrow.getTime() - beginningOfToday.getTime();
    this.metricTargetDayAfterStartTime = beginningOfTomorrow.getTime();
    this.metricTargetDayAfterDuration = beginningOfTwodaysafter.getTime() - beginningOfTomorrow.getTime();

    //
    // Timestamp & period
    //
    String timestamp = this.printTimestamp(now);
    long targetPeriod = now.getTime() - beginningOfToday.getTime() + 1; // +1 !
    
    this.run(timestamp, targetPeriod);
  }
  
  @Override
  protected SearchRequest getElasticsearchRequest() 
  {
	//
    // Target index
    //
    String ESIndex = getDataESIndex();
    
    List<String> datacubeFilterFields = getFilterFields();
    List<AggregationBuilder> datacubeMetricAggregations = getMetricAggregations();
    
    //
    // Composite sources are created from datacube filters
    //
    List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
    for(String datacubeFilter: datacubeFilterFields) {
      TermsValuesSourceBuilder sourceTerms = new TermsValuesSourceBuilder(datacubeFilter).field(datacubeFilter).missingBucket(true);
      sources.add(sourceTerms);
    }
    CompositeAggregationBuilder compositeAggregation = AggregationBuilders.composite("DATACUBE", sources).size(ElasticsearchClientAPI.MAX_BUCKETS);
    //
    // Metric aggregations
    //
    for(AggregationBuilder subaggregation : datacubeMetricAggregations) {
      compositeAggregation = compositeAggregation.subAggregation(subaggregation);
    }
    
    //
    // Build query - we use filter because we don't care about score, therefore it is faster.
    //
    BoolQueryBuilder query = QueryBuilders.boolQuery();
    query.filter().add(QueryBuilders.termQuery("tenantID", this.tenantID)); // filter to keep only tenant related items !
    query.filter().addAll(getFilterQueries()); // Additional filters
    
    //
    // Datacube request
    //
    SearchSourceBuilder datacubeRequest = new SearchSourceBuilder()
        .sort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
        .query(query)
        .aggregation(compositeAggregation)
        .size(0);
    
    return new SearchRequest(ESIndex).source(datacubeRequest);
  }
  @Override
  protected List<Map<String, Object>> extractDatacubeRows(SearchResponse response, String timestamp, long period) throws ClassCastException 
  {
	List<Map<String,Object>> result = new ArrayList<Map<String,Object>>();
	    
	    if (response.isTimedOut()
	        || response.getFailedShards() > 0
	        || response.status() != RestStatus.OK) {
	      log.error("Elasticsearch search response return with bad status.");
	      log.error(response.toString());
	      return result;
	    }
	    
	    if(response.getAggregations() == null) {
	      log.error("Main aggregation is missing in search response.");
	      return result;
	    }
	    
	    ParsedComposite compositeBuckets = response.getAggregations().get("DATACUBE");
	    if(compositeBuckets == null) {
	      log.error("Composite buckets are missing in search response.");
	      return result;
	    }
	    
	    for(ParsedBucket bucket: compositeBuckets.getBuckets()) {
	      long docCount = bucket.getDocCount();
	
	      //
	      // Extract filter, replace null
	      //
	      Map<String, Object> filters = bucket.getKey();
	      for(String key: filters.keySet()) {
	        if(filters.get(key) == null) {
	          filters.replace(key, UNDEFINED_BUCKET_VALUE);
	        }
	      }
	      
	      // Special filter: tenantID 
	      filters.put("tenantID", this.tenantID);
	      
	      //
	      // Extract metrics
	      //
	      Map<String, Long> metrics = extractMetrics(bucket);
	      
	      //
	      // Build row
	      //
	      Map<String, Object> row = extractRow(filters, docCount, timestamp, period, metrics);
	      result.add(row);
	    }
	    
	    
	    return result;
  }
  
}
