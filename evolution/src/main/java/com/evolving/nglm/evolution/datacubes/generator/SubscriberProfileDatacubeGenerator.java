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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite.ParsedBucket;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.SegmentationDimensionService;
import com.evolving.nglm.evolution.datacubes.DatacubeWriter;
import com.evolving.nglm.evolution.datacubes.SimpleDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.SubscriberProfileDatacubeMetric;
import com.evolving.nglm.evolution.datacubes.mapping.SegmentationDimensionsMap;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

public class SubscriberProfileDatacubeGenerator extends SimpleDatacubeGenerator
{
  private static final String DATACUBE_ES_INDEX = "datacube_subscriberprofile";
  private static final String DATA_ES_INDEX = "subscriberprofile";
  private static final String FILTER_STRATUM_PREFIX = "stratum.";
  private static final String METRIC_PREFIX = "metric_";

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private List<String> filterFields;
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
  public SubscriberProfileDatacubeGenerator(String datacubeName, ElasticsearchClientAPI elasticsearch, DatacubeWriter datacubeWriter, SegmentationDimensionService segmentationDimensionService)
  {
    super(datacubeName, elasticsearch, datacubeWriter);

    this.segmentationDimensionList = new SegmentationDimensionsMap(segmentationDimensionService);
    
    //
    // Filter fields
    //
    this.filterFields = new ArrayList<String>();
  }

  /*****************************************
  *
  * Elasticsearch indices settings
  *
  *****************************************/
  @Override protected String getDataESIndex() { return DATA_ES_INDEX; }
  @Override protected String getDatacubeESIndex() { return DATACUBE_ES_INDEX; }
  
  //
  // Subset of subscriberprofile
  //
  // Hack: When a newly created subscriber in Elasticsearch comes first by ExtendedSubscriberProfile sink connector,
  // it has not yet any of the "product" main (& mandatory) fields.
  // Those comes when the SubscriberProfile sink connector push them.
  // For a while, it is possible a document in subscriberprofile index miss many product fields required by datacube generation.
  // Therefore, we filter out those subscribers with missing data
  @Override
  protected QueryBuilder getSubsetQuery() 
  {
    return QueryBuilders.boolQuery().must(QueryBuilders.existsQuery("lastUpdateDate"));
  }
  
  
  /*****************************************
  *
  * Filters settings
  *
  *****************************************/
  @Override protected List<String> getFilterFields() { return filterFields; }
  
  @Override
  protected boolean runPreGenerationPhase() throws ElasticsearchException, IOException, ClassCastException
  {
    this.segmentationDimensionList.update();
    this.filterFields = new ArrayList<String>();
    for(String dimensionID: segmentationDimensionList.keySet())
      {
        this.filterFields.add(FILTER_STRATUM_PREFIX + dimensionID);
      }
    
    if(this.filterFields.isEmpty()) {
      log.warn("Found no dimension defined.");
      return false;
    }
    
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
    for(String dimensionID: segmentationDimensionList.keySet())
      {
        String fieldName = FILTER_STRATUM_PREFIX + dimensionID;
        String segmentID = (String) filters.remove(fieldName);
        
        String newFieldName = FILTER_STRATUM_PREFIX + segmentationDimensionList.getDimensionDisplay(dimensionID, fieldName);
        filters.put(newFieldName, segmentationDimensionList.getSegmentDisplay(dimensionID, segmentID, fieldName));
      }
  }

  /*****************************************
  *
  * Metrics settings
  *
  *****************************************/
  @Override
  protected List<AggregationBuilder> getMetricAggregations()
  {
    String targetDayBeginningIncluded = metricTargetDayStartTime + "L";
    String targetDayEndExcluded = (metricTargetDayStartTime+metricTargetDayDuration) + "L";
    String targetDayAfterBeginningIncluded = metricTargetDayAfterStartTime + "L";
    String targetDayAfterEndExcluded = (metricTargetDayAfterStartTime+metricTargetDayAfterDuration) + "L";
    
    List<AggregationBuilder> metricAggregations = new ArrayList<AggregationBuilder>();
    
    Map<String, SubscriberProfileDatacubeMetric> customMetrics = Deployment.getSubscriberProfileDatacubeMetrics();
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
        
    return metricAggregations;
  }

  @Override
  protected Map<String, Long> extractMetrics(ParsedBucket compositeBucket) throws ClassCastException
  {    
    HashMap<String, Long> metrics = new HashMap<String,Long>();
    
    if (compositeBucket.getAggregations() == null) {
      log.error("Unable to extract metrics, aggregation is missing.");
      return metrics;
    }

    Map<String, SubscriberProfileDatacubeMetric> customMetrics = Deployment.getSubscriberProfileDatacubeMetrics();
    for(String metricID: customMetrics.keySet()) {
      SubscriberProfileDatacubeMetric customMetric = customMetrics.get(metricID);
      
      ParsedSum metricBucket = compositeBucket.getAggregations().get(METRIC_PREFIX+metricID);
      if (metricBucket == null) {
        log.error("Unable to extract "+metricID+" custom metric, aggregation is missing.");
        return metrics;
      }
      metrics.put("custom." + customMetric.getDisplay(), new Long((int) metricBucket.getValue()));
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
    Date yesterday = RLMDateUtils.addDays(now, -1, Deployment.getSystemTimeZone());  // TODO EVPRO-99 use systemTimeZone instead of baseTimeZone, is it correct
    Date tomorrow = RLMDateUtils.addDays(now, 1, Deployment.getSystemTimeZone());  // TODO EVPRO-99 use systemTimeZone instead of baseTimeZone, is it correct
    
    // Dates: YYYY-MM-dd 00:00:00.000
    Date beginningOfYesterday = RLMDateUtils.truncate(yesterday, Calendar.DATE, Deployment.getSystemTimeZone()); // TODO EVPRO-99 use systemTimeZone instead of baseTimeZone, is it correct
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, Deployment.getSystemTimeZone());  // TODO EVPRO-99 use systemTimeZone instead of baseTimeZone, is it correct
    Date beginningOfTomorrow = RLMDateUtils.truncate(tomorrow, Calendar.DATE, Deployment.getSystemTimeZone());  // TODO EVPRO-99 use systemTimeZone instead of baseTimeZone, is it correct

    this.metricTargetDay = RLMDateUtils.printDay(yesterday);
    this.metricTargetDayStartTime = beginningOfYesterday.getTime();
    this.metricTargetDayDuration = beginningOfToday.getTime() - beginningOfYesterday.getTime();
    this.metricTargetDayAfterStartTime = beginningOfToday.getTime();
    this.metricTargetDayAfterDuration = beginningOfTomorrow.getTime() - beginningOfToday.getTime();

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
   * Because the day is still not ended, it won't be the definitive value of metrics.
   * Reminder, custom metrics target a whole day (today recharge, today usage, etc.)
   */ 
  public void preview()
  {
    Date now = SystemTime.getCurrentTime();
    Date tomorrow = RLMDateUtils.addDays(now, 1, Deployment.getSystemTimeZone());  // TODO EVPRO-99 use systemTimeZone instead of baseTimeZone, is it correct
    Date twodaysafter = RLMDateUtils.addDays(now, 2, Deployment.getSystemTimeZone());  // TODO EVPRO-99 use systemTimeZone instead of baseTimeZone, is it correct

    // Dates: YYYY-MM-dd 00:00:00.000
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, Deployment.getSystemTimeZone());  // TODO EVPRO-99 use systemTimeZone instead of baseTimeZone, is it correct
    Date beginningOfTomorrow = RLMDateUtils.truncate(tomorrow, Calendar.DATE, Deployment.getSystemTimeZone());  // TODO EVPRO-99 use systemTimeZone instead of baseTimeZone, is it correct
    Date beginningOfTwodaysafter = RLMDateUtils.truncate(twodaysafter, Calendar.DATE, Deployment.getSystemTimeZone());  // TODO EVPRO-99 use systemTimeZone instead of baseTimeZone, is it correct

    this.metricTargetDay = RLMDateUtils.printDay(now);
    this.metricTargetDayStartTime = beginningOfToday.getTime();
    this.metricTargetDayDuration = beginningOfTomorrow.getTime() - beginningOfToday.getTime();
    this.metricTargetDayAfterStartTime = beginningOfTomorrow.getTime();
    this.metricTargetDayAfterDuration = beginningOfTwodaysafter.getTime() - beginningOfTomorrow.getTime();

    //
    // Timestamp & period
    //
    String timestamp = RLMDateUtils.printTimestamp(now);
    long targetPeriod = now.getTime() - beginningOfToday.getTime() + 1; // +1 !
    
    this.run(timestamp, targetPeriod);
  }
}
