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
import org.elasticsearch.search.aggregations.AggregationBuilder;
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
import com.evolving.nglm.evolution.SubscriberMessageTemplateService;
import com.evolving.nglm.evolution.datacubes.DatacubeUtils;
import com.evolving.nglm.evolution.datacubes.DatacubeWriter;
import com.evolving.nglm.evolution.datacubes.SimpleDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.mapping.DeliverablesMap;
import com.evolving.nglm.evolution.datacubes.mapping.JourneysMap;
import com.evolving.nglm.evolution.datacubes.mapping.LoyaltyProgramsMap;
import com.evolving.nglm.evolution.datacubes.mapping.ModulesMap;
import com.evolving.nglm.evolution.datacubes.mapping.OffersMap;
import com.evolving.nglm.evolution.datacubes.mapping.SalesChannelsMap;
import com.evolving.nglm.evolution.datacubes.mapping.SubscriberMessageTemplatesMap;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

public class MDRDatacubeGenerator extends SimpleDatacubeGenerator
{
  public static final String DATACUBE_ES_INDEX = "datacube_messages";
  private static final String DATA_ES_INDEX_PREFIX = "detailedrecords_messages-";

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private List<String> filterFields;
  private OffersMap offersMap;
  private ModulesMap modulesMap;
  private LoyaltyProgramsMap loyaltyProgramsMap;
  private DeliverablesMap deliverablesMap;
  private JourneysMap journeysMap;
  private SubscriberMessageTemplatesMap subscriberMessageTemplatesMap;

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
  public MDRDatacubeGenerator(String datacubeName, ElasticsearchClientAPI elasticsearch, DatacubeWriter datacubeWriter, OfferService offerService, OfferObjectiveService offerObjectiveService, LoyaltyProgramService loyaltyProgramService, JourneyService journeyService, SubscriberMessageTemplateService subscriberMessageTemplateService)  
  {
    super(datacubeName, elasticsearch, datacubeWriter);

    this.offersMap = new OffersMap(offerService);
    this.modulesMap = new ModulesMap();
    this.loyaltyProgramsMap = new LoyaltyProgramsMap(loyaltyProgramService);
    this.deliverablesMap = new DeliverablesMap();
    this.journeysMap = new JourneysMap(journeyService);
    this.subscriberMessageTemplatesMap = new SubscriberMessageTemplatesMap(subscriberMessageTemplateService);
    
    //
    // Filter fields
    //

    this.filterFields = new ArrayList<String>();
    this.filterFields.add("moduleID");
    this.filterFields.add("featureID");
    this.filterFields.add("language");
    this.filterFields.add("templateID");
    this.filterFields.add("returnCode");
    this.filterFields.add("channelID");
    this.filterFields.add("contactType");
    
  }

  /*****************************************
  *
  * Elasticsearch indices settings
  *
  *****************************************/
  @Override protected String getDatacubeESIndex() { return DATACUBE_ES_INDEX; }
  @Override protected String getDataESIndex() { return (DATA_ES_INDEX_PREFIX+targetWeek); }
  
  //
  // Target day
  //
  @Override
  protected QueryBuilder getSubsetQuery() 
  {
    return QueryBuilders.boolQuery().must(QueryBuilders
        .rangeQuery("eventDatetime")
        .gte(RLMDateUtils.printTimestamp(this.targetWindowStart))
        .lt(RLMDateUtils.printTimestamp(this.targetWindowEnd))); // End not included
  }

  /*****************************************
  *
  * Filters settings
  *
  *****************************************/
  @Override protected List<String> getFilterFields() { return filterFields; }
  
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
    
    offersMap.update();
    modulesMap.updateFromElasticsearch(elasticsearch);
    loyaltyProgramsMap.update();
    deliverablesMap.updateFromElasticsearch(elasticsearch);
    journeysMap.update();
    subscriberMessageTemplatesMap.update();
    
    return true;
  }
  
  @Override
  protected void embellishFilters(Map<String, Object> filters)
  {
    String moduleID = (String) filters.remove("moduleID");
    filters.put("module", modulesMap.getDisplay(moduleID, "module"));

    DatacubeUtils.embelishFeature(filters, moduleID, modulesMap, loyaltyProgramsMap, deliverablesMap, offersMap, journeysMap);

    String templateID = (String) filters.remove("templateID");
    filters.put("template", subscriberMessageTemplatesMap.getDisplay(templateID, "template"));
    
    String channelID = (String) filters.remove("channelID");
    filters.put("channel", com.evolving.nglm.evolution.Deployment.getCommunicationChannels().get(channelID).getDisplay());
    
    DatacubeUtils.embelishReturnCode(filters);
    
    // Specials for timestamp (will not be display in filter but extracted later in DatacubeGenerator)
    if(this.hourlyMode) {
      Long time = (Long) filters.remove("timestamp");
      // The timestamp retrieve from the Date Histogram is the START of the interval. 
      // But here we want to timestamp at the END (+ 1hour -1ms)
      Date date = new Date(time + 3600*1000 - 1);
      filters.put("timestamp", RLMDateUtils.printTimestamp(date));
    }
  }

  /*****************************************
  *
  * Metrics settings
  *
  *****************************************/
  @Override protected List<AggregationBuilder> getMetricAggregations() { return Collections.emptyList(); }
  @Override protected Map<String, Long> extractMetrics(ParsedBucket compositeBucket) throws ClassCastException { return Collections.emptyMap(); }
  
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
    Date yesterday = RLMDateUtils.addDays(now, -1, Deployment.getBaseTimeZone());
    Date beginningOfYesterday = RLMDateUtils.truncate(yesterday, Calendar.DATE, Deployment.getBaseTimeZone());
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, Deployment.getBaseTimeZone());        // 00:00:00.000
    Date endOfYesterday = RLMDateUtils.addMilliseconds(beginningOfToday, -1);                               // 23:59:59.999
    
    //
    // Run configurations
    //
    this.hourlyMode = false;
    this.targetWeek = RLMDateUtils.printISOWeek(yesterday);
    this.targetWindowStart = beginningOfYesterday;
    this.targetWindowEnd = beginningOfToday;
    this.targetTimestamp = RLMDateUtils.printTimestamp(endOfYesterday);

    //
    // Timestamp & period
    //
    String timestamp = RLMDateUtils.printTimestamp(endOfYesterday);
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
    Date tomorrow = RLMDateUtils.addDays(now, 1, Deployment.getBaseTimeZone());
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, Deployment.getBaseTimeZone());
    Date beginningOfTomorrow = RLMDateUtils.truncate(tomorrow, Calendar.DATE, Deployment.getBaseTimeZone());        // 00:00:00.000
    Date endOfToday = RLMDateUtils.addMilliseconds(beginningOfTomorrow, -1);                                        // 23:59:59.999
    
    //
    // Run configurations
    //
    this.hourlyMode = false;
    this.targetWeek = RLMDateUtils.printISOWeek(now);
    this.targetWindowStart = beginningOfToday;
    this.targetWindowEnd = beginningOfTomorrow;
    this.targetTimestamp = RLMDateUtils.printTimestamp(endOfToday);

    //
    // Timestamp & period
    //
    String timestamp = RLMDateUtils.printTimestamp(now);
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
    Date yesterday = RLMDateUtils.addDays(now, -1, Deployment.getBaseTimeZone());
    Date beginningOfYesterday = RLMDateUtils.truncate(yesterday, Calendar.DATE, Deployment.getBaseTimeZone());
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, Deployment.getBaseTimeZone());        // 00:00:00.000
    Date endOfYesterday = RLMDateUtils.addMilliseconds(beginningOfToday, -1);                               // 23:59:59.999
    
    //
    // Run configurations
    //
    this.hourlyMode = true;
    this.targetWeek = RLMDateUtils.printISOWeek(yesterday);
    this.targetWindowStart = beginningOfYesterday;
    this.targetWindowEnd = beginningOfToday;
    this.targetTimestamp = RLMDateUtils.printTimestamp(endOfYesterday);

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
    Date tomorrow = RLMDateUtils.addDays(now, 1, Deployment.getBaseTimeZone());
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, Deployment.getBaseTimeZone());
    Date beginningOfTomorrow = RLMDateUtils.truncate(tomorrow, Calendar.DATE, Deployment.getBaseTimeZone());        // 00:00:00.000
    Date endOfToday = RLMDateUtils.addMilliseconds(beginningOfTomorrow, -1);                                        // 23:59:59.999
    
    //
    // Run configurations
    //
    this.hourlyMode = true;
    this.targetWeek = RLMDateUtils.printISOWeek(now);
    this.targetWindowStart = beginningOfToday;
    this.targetWindowEnd = beginningOfTomorrow;
    this.targetTimestamp = RLMDateUtils.printTimestamp(endOfToday);

    //
    // Timestamp & period
    //
    String timestamp = null; // Should not be used, will be override.
    long targetPeriod = 3600*1000; // 1 hour
    
    this.run(timestamp, targetPeriod);
  }
}
