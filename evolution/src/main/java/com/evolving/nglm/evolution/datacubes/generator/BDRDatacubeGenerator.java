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
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite.ParsedBucket;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.OfferObjectiveService;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.PaymentMeanService;
import com.evolving.nglm.evolution.SalesChannelService;
import com.evolving.nglm.evolution.SegmentationDimensionService;
import com.evolving.nglm.evolution.datacubes.DatacubeManager;
import com.evolving.nglm.evolution.datacubes.DatacubeUtils;
import com.evolving.nglm.evolution.datacubes.DatacubeWriter;
import com.evolving.nglm.evolution.datacubes.SimpleDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.mapping.DeliverablesMap;
import com.evolving.nglm.evolution.datacubes.mapping.JourneysMap;
import com.evolving.nglm.evolution.datacubes.mapping.LoyaltyProgramsMap;
import com.evolving.nglm.evolution.datacubes.mapping.ModulesMap;
import com.evolving.nglm.evolution.datacubes.mapping.OffersMap;
import com.evolving.nglm.evolution.datacubes.mapping.PaymentMeansMap;
import com.evolving.nglm.evolution.datacubes.mapping.SalesChannelsMap;
import com.evolving.nglm.evolution.datacubes.mapping.SegmentationDimensionsMap;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

public class BDRDatacubeGenerator extends SimpleDatacubeGenerator
{
  private static final String DATACUBE_ES_INDEX_SUFFIX = "_datacube_bdr";
  public static final String DATACUBE_ES_INDEX(int tenantID) { return "t" + tenantID + DATACUBE_ES_INDEX_SUFFIX; }
  private static final String DATA_ES_INDEX_PREFIX = "detailedrecords_bonuses-";
  private static final String METRIC_TOTAL_QUANTITY = "totalQty";
  private static final String FILTER_STRATUM_PREFIX = "stratum.";

  /*****************************************
  *
  * Properties
  *
  *****************************************/

  private List<AggregationBuilder> metricAggregations;
  private OffersMap offersMap;
  private ModulesMap modulesMap;
  private LoyaltyProgramsMap loyaltyProgramsMap;
  private DeliverablesMap deliverablesMap;
  private JourneysMap journeysMap;
  private SegmentationDimensionsMap segmentationDimensionList;

  private boolean hourlyMode;
  private String targetWeek;
  private Date targetWindowStart;
  private Date targetWindowEnd;
  private String targetTimestamp;

  /*****************************************
  *
  * Constructors
  *
  *****************************************/
  public BDRDatacubeGenerator(String datacubeName, ElasticsearchClientAPI elasticsearch, DatacubeWriter datacubeWriter, SegmentationDimensionService segmentationDimensionService, OfferService offerService, OfferObjectiveService offerObjectiveService, LoyaltyProgramService loyaltyProgramService, JourneyService journeyService, int tenantID, String timeZone)  
  {
    super(datacubeName, elasticsearch, datacubeWriter, tenantID, timeZone);
    this.segmentationDimensionList = new SegmentationDimensionsMap(segmentationDimensionService);
    this.offersMap = new OffersMap(offerService);
    this.modulesMap = new ModulesMap();
    this.loyaltyProgramsMap = new LoyaltyProgramsMap(loyaltyProgramService);
    this.deliverablesMap = new DeliverablesMap();
    this.journeysMap = new JourneysMap(journeyService);
    
    //
    // Data Aggregations
    // - totalAmount
    //
    this.metricAggregations = new ArrayList<AggregationBuilder>();
    
    AggregationBuilder totalAmount = AggregationBuilders.sum(METRIC_TOTAL_QUANTITY).field("deliverableQty");
    metricAggregations.add(totalAmount);
  }
  
  public BDRDatacubeGenerator(String datacubeName, int tenantID, DatacubeManager datacubeManager) {
    this(datacubeName,
        datacubeManager.getElasticsearchClientAPI(),
        datacubeManager.getDatacubeWriter(),
        datacubeManager.getSegmentationDimensionService(),
        datacubeManager.getOfferService(),
        datacubeManager.getOfferObjectiveService(),
        datacubeManager.getLoyaltyProgramService(),
        datacubeManager.getJourneyService(),
        tenantID,
        Deployment.getDeployment(tenantID).getTimeZone());
  }
  
  /*****************************************
  *
  * Elasticsearch indices settings
  *
  *****************************************/
  @Override protected String getDatacubeESIndex() { return DATACUBE_ES_INDEX(this.tenantID); }
  @Override protected String getDataESIndex() { return (DATA_ES_INDEX_PREFIX+targetWeek); }
  
  //
  // Target day
  //
  @Override
  protected List<QueryBuilder> getFilterQueries() 
  {
    return Collections.singletonList(QueryBuilders.boolQuery().must(QueryBuilders
        .rangeQuery("eventDatetime")
        .gte(this.printTimestamp(this.targetWindowStart))
        .lt(this.printTimestamp(this.targetWindowEnd)))); // End not included
  }
  
  /*****************************************
  *
  * Filters settings
  *
  *****************************************/
  @Override 
  protected List<String> getFilterFields() {
    //
    // Build filter fields
    //
    List<String> filterFields = new ArrayList<String>();
    filterFields.add("moduleID");
    filterFields.add("featureID");
    filterFields.add("providerID");
    filterFields.add("operation");
    filterFields.add("deliverableID");
    filterFields.add("returnCode");
    filterFields.add("origin");

    // getFilterFields is called after runPreGenerationPhase. It safe to assume segmentationDimensionList is up to date.
    // Regarding dimensions, in BDR ES indices, there is already only the "statistics" ones (see BDRSinkConnector.java)
    // Nonetheless, to avoid having plenty of "null" dimensions in the datacube (and pollute the index settings) we also 
    // filter out "non statistics" dimensions here.
    for(String dimensionID: segmentationDimensionList.keySet()) {
      if (segmentationDimensionList.isFlaggedStatistics(dimensionID)) {
        filterFields.add(FILTER_STRATUM_PREFIX + dimensionID);
      }
    }
    
    return filterFields; 
  }
  
  @Override
  protected CompositeValuesSourceBuilder<?> getSpecialSourceFilter() {
    if(this.hourlyMode) {
      return new DateHistogramValuesSourceBuilder("timestamp").field("eventDatetime").calendarInterval(DateHistogramInterval.HOUR);
    } else {
      return null;
    }
  }
  
  @Override
  protected boolean runPreGenerationPhase() throws ElasticsearchException, IOException, ClassCastException
  {
    if(!isESIndexAvailable(getDataESIndex())) {
      log.info("Elasticsearch index [" + getDataESIndex() + "] does not exist.");
      return false;
    }
    
    // TODO EVPRO-99 check if the update must be splitt per tenant... ??? ...???
    offersMap.update();
    modulesMap.updateFromElasticsearch(elasticsearch);
    loyaltyProgramsMap.update();
    deliverablesMap.updateFromElasticsearch(elasticsearch);
    journeysMap.update();
    segmentationDimensionList.update();

    return true;
  }
  
  @Override
  protected void embellishFilters(Map<String, Object> filters)
  {
    String moduleID = (String) filters.remove("moduleID");
    filters.put("module", modulesMap.getDisplay(moduleID, "module"));

    DatacubeUtils.embelishFeature(filters, moduleID, modulesMap, loyaltyProgramsMap, deliverablesMap, offersMap, journeysMap);
    
    String providerID = (String) filters.remove("providerID");
    filters.put("provider", Deployment.getFulfillmentProviders().get(providerID).getProviderName());
    
    String deliverableID = (String) filters.remove("deliverableID");
    filters.put("deliverable", deliverablesMap.getDisplay(deliverableID, "deliverable"));

    DatacubeUtils.embelishReturnCode(filters);
    
    // Specials for timestamp (will not be display in filter but extracted later in DatacubeGenerator)
    if(this.hourlyMode) {
      Long time = (Long) filters.remove("timestamp");
      // The timestamp retrieve from the Date Histogram is the START of the interval. 
      // But here we want to timestamp at the END (+ 1hour -1ms)
      Date date = new Date(time + 3600*1000 - 1);
      filters.put("timestamp", this.printTimestamp(date));
    }
    
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
  @Override protected List<AggregationBuilder> getMetricAggregations() { return this.metricAggregations; }
    
  @Override
  protected Map<String, Long> extractMetrics(ParsedBucket compositeBucket) throws ClassCastException
  {
    HashMap<String, Long> metrics = new HashMap<String,Long>();
    if (compositeBucket.getAggregations() == null) {
      log.error("Unable to extract metrics, aggregation is missing.");
      return metrics;
    }
    
    ParsedSum dataTotalQuantityBucket = compositeBucket.getAggregations().get(METRIC_TOTAL_QUANTITY);
    if (dataTotalQuantityBucket == null) {
      log.error("Unable to extract totalQuantity metric, aggregation is missing.");
      return metrics;
    }
    metrics.put(METRIC_TOTAL_QUANTITY, new Long((int) dataTotalQuantityBucket.getValue()));
    
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
    if(this.hourlyMode) {
      // @rl: It also works in hourly mode because there is the real timestamp in filters !
      return this.extractDocumentIDFromFilter(filters, this.targetTimestamp, "HOURLY");
    } else {
      return this.extractDocumentIDFromFilter(filters, this.targetTimestamp, "DAILY");
    }
  }
  
  /*****************************************
  *
  * Run
  *
  *****************************************/
  /**
   * The definitive datacube is generated on yesterday, for a period of 1 day (~24 hours except for some special days)
   * Rows will be timestamped at yesterday_23:59:59.999+ZZZZ
   * Timestamp is the last millisecond of the period (therefore included).
   * This way it shows that *count* is computed for this day (yesterday) but at the very end of the day.
   */
  public void dailyDefinitive()
  {
    Date now = SystemTime.getCurrentTime();
    Date yesterday = RLMDateUtils.addDays(now, -1, this.getTimeZone());
    Date beginningOfYesterday = RLMDateUtils.truncate(yesterday, Calendar.DATE, this.getTimeZone());
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, this.getTimeZone());        // 00:00:00.000
    Date endOfYesterday = RLMDateUtils.addMilliseconds(beginningOfToday, -1);                     // 23:59:59.999
    
    //
    // Run configurations
    //
    this.hourlyMode = false;
    this.targetWeek = RLMDateUtils.formatDateISOWeek(yesterday, this.timeZone);
    this.targetWindowStart = beginningOfYesterday;
    this.targetWindowEnd = beginningOfToday;
    this.targetTimestamp = this.printTimestamp(endOfYesterday);

    //
    // Timestamp & period
    //
    String timestamp = this.printTimestamp(endOfYesterday);
    long targetPeriod = beginningOfToday.getTime() - beginningOfYesterday.getTime();    // most of the time 86400000ms (24 hours)
    
    this.run(timestamp, targetPeriod);
  }
  
  /**
   * A preview is a datacube generation on the today's day. 
   * Timestamp is the last millisecond of the period (therefore included).
   * Because the day is still not ended, it won't be the definitive value of *count*.
   */
  public void dailyPreview()
  {
    Date now = SystemTime.getCurrentTime();
    Date tomorrow = RLMDateUtils.addDays(now, 1, this.getTimeZone());
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, this.getTimeZone());
    Date beginningOfTomorrow = RLMDateUtils.truncate(tomorrow, Calendar.DATE, this.getTimeZone());        // 00:00:00.000
    Date endOfToday = RLMDateUtils.addMilliseconds(beginningOfTomorrow, -1);                              // 23:59:59.999
    
    //
    // Run configurations
    //
    this.hourlyMode = false;
    this.targetWeek = RLMDateUtils.formatDateISOWeek(now, this.timeZone);
    this.targetWindowStart = beginningOfToday;
    this.targetWindowEnd = beginningOfTomorrow;
    this.targetTimestamp = this.printTimestamp(endOfToday);

    //
    // Timestamp & period
    //
    String timestamp = this.printTimestamp(now);
    long targetPeriod = now.getTime() - beginningOfToday.getTime() + 1; // +1 !
    
    this.run(timestamp, targetPeriod);
  }

  /**
   * The definitive datacube is generated on yesterday, for 24 periods of 1 hour (except for some special days)
   * Rows will be timestamped at yesterday_XX:59:59.999+ZZZZ
   * Timestamp is the last millisecond of the period (therefore included).
   * This way it shows that *count* is computed for this hour but at the very end of it.
   */
  public void hourlyDefinitive()
  {
    Date now = SystemTime.getCurrentTime();
    Date yesterday = RLMDateUtils.addDays(now, -1, this.getTimeZone());
    Date beginningOfYesterday = RLMDateUtils.truncate(yesterday, Calendar.DATE, this.getTimeZone());
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, this.getTimeZone());        // 00:00:00.000
    Date endOfYesterday = RLMDateUtils.addMilliseconds(beginningOfToday, -1);                               // 23:59:59.999
    
    //
    // Run configurations
    //
    this.hourlyMode = true;
    this.targetWeek = RLMDateUtils.formatDateISOWeek(yesterday, this.timeZone);
    this.targetWindowStart = beginningOfYesterday;
    this.targetWindowEnd = beginningOfToday;
    this.targetTimestamp = this.printTimestamp(endOfYesterday);

    //
    // Timestamp & period
    //
    String timestamp = null; // Should not be used, will be override.
    long targetPeriod = 3600*1000; // 1 hour
    
    this.run(timestamp, targetPeriod);
  }
  
  /**
   * A preview is a datacube generation on the today's day. 
   * Timestamp is the last millisecond of the period (therefore included).
   * Because the day is still not ended, it won't be the definitive value of *count*.
   * Even for hour periods that had already ended, because ODR can still be added with timestamp of previous hours.
   */
  public void hourlyPreview()
  {
    Date now = SystemTime.getCurrentTime();
    Date tomorrow = RLMDateUtils.addDays(now, 1, this.getTimeZone());
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, this.getTimeZone());
    Date beginningOfTomorrow = RLMDateUtils.truncate(tomorrow, Calendar.DATE, this.getTimeZone());        // 00:00:00.000
    Date endOfToday = RLMDateUtils.addMilliseconds(beginningOfTomorrow, -1);                                        // 23:59:59.999
    
    //
    // Run configurations
    //
    this.hourlyMode = true;
    this.targetWeek = RLMDateUtils.formatDateISOWeek(now, this.timeZone);
    this.targetWindowStart = beginningOfToday;
    this.targetWindowEnd = beginningOfTomorrow;
    this.targetTimestamp = this.printTimestamp(endOfToday);

    //
    // Timestamp & period
    //
    String timestamp = null; // Should not be used, will be override.
    long targetPeriod = 3600*1000; // 1 hour
    
    this.run(timestamp, targetPeriod);
  }
}
