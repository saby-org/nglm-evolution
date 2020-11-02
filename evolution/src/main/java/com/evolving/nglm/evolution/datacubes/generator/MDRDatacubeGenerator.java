package com.evolving.nglm.evolution.datacubes.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite.ParsedBucket;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.OfferObjectiveService;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.PaymentMeanService;
import com.evolving.nglm.evolution.RESTAPIGenericReturnCodes;
import com.evolving.nglm.evolution.SalesChannelService;
import com.evolving.nglm.evolution.SubscriberMessageTemplateService;
import com.evolving.nglm.evolution.datacubes.DatacubeUtils;
import com.evolving.nglm.evolution.datacubes.SimpleDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.mapping.DeliverablesMap;
import com.evolving.nglm.evolution.datacubes.mapping.JourneysMap;
import com.evolving.nglm.evolution.datacubes.mapping.LoyaltyProgramsMap;
import com.evolving.nglm.evolution.datacubes.mapping.ModulesMap;
import com.evolving.nglm.evolution.datacubes.mapping.OfferObjectivesMap;
import com.evolving.nglm.evolution.datacubes.mapping.OffersMap;
import com.evolving.nglm.evolution.datacubes.mapping.PaymentMeansMap;
import com.evolving.nglm.evolution.datacubes.mapping.SalesChannelsMap;
import com.evolving.nglm.evolution.datacubes.mapping.SubscriberMessageTemplatesMap;

public class MDRDatacubeGenerator extends SimpleDatacubeGenerator
{
  private static final String DATACUBE_ES_INDEX = "datacube_messages";
  private static final String DATA_ES_INDEX_PREFIX = "detailedrecords_messages-";
  private static final String METRIC_TOTAL_AMOUNT = "totalAmount";

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private List<String> filterFields;
  private List<AggregationBuilder> metricAggregations;
  private OffersMap offersMap;
  private ModulesMap modulesMap;
  private SalesChannelsMap salesChannelsMap;
  private PaymentMeansMap paymentMeansMap;
  private OfferObjectivesMap offerObjectivesMap;
  private LoyaltyProgramsMap loyaltyProgramsMap;
  private DeliverablesMap deliverablesMap;
  private JourneysMap journeysMap;
  private SubscriberMessageTemplatesMap subscriberMessageTemplatesMap;

  private boolean previewMode;
  private String targetDay;

  /*****************************************
  *
  * Constructors
  *
  *****************************************/
  public MDRDatacubeGenerator(String datacubeName, RestHighLevelClient elasticsearch, OfferService offerService, SalesChannelService salesChannelService, PaymentMeanService paymentMeanService, OfferObjectiveService offerObjectiveService, LoyaltyProgramService loyaltyProgramService, JourneyService journeyService, SubscriberMessageTemplateService subscriberMessageTemplateService)  
  {
    super(datacubeName, elasticsearch);

    this.offersMap = new OffersMap(offerService);
    this.modulesMap = new ModulesMap();
    this.salesChannelsMap = new SalesChannelsMap(salesChannelService);
    this.paymentMeansMap = new PaymentMeansMap(paymentMeanService);
    this.offerObjectivesMap = new OfferObjectivesMap(offerObjectiveService);
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
    
    //
    // Data Aggregations
    //
    this.metricAggregations = new ArrayList<AggregationBuilder>();
  }

  /*****************************************
  *
  * Elasticsearch indices settings
  *
  *****************************************/
  @Override protected String getDatacubeESIndex() { return DATACUBE_ES_INDEX; }
  @Override protected String getDataESIndex() { return (DATA_ES_INDEX_PREFIX+targetDay); }

  /*****************************************
  *
  * Filters settings
  *
  *****************************************/
  @Override protected List<String> getFilterFields() { return filterFields; }
  
  @Override
  protected boolean runPreGenerationPhase() throws ElasticsearchException, IOException, ClassCastException
  {
    if(!isESIndexAvailable(getDataESIndex())) {
      log.info("Elasticsearch index [" + getDataESIndex() + "] does not exist.");
      return false;
    }
    
    offersMap.update();
    modulesMap.updateFromElasticsearch(elasticsearch);
    salesChannelsMap.update();
    paymentMeansMap.update();
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
  }

  /*****************************************
  *
  * Metrics settings
  *
  *****************************************/
  @Override protected List<AggregationBuilder> getMetricAggregations() { return this.metricAggregations; }
    
  @Override
  protected Map<String, Object> extractMetrics(ParsedBucket compositeBucket) throws ClassCastException
  {
    HashMap<String, Object> metrics = new HashMap<String,Object>();
    if (compositeBucket.getAggregations() == null) {
      log.error("Unable to extract metrics, aggregation is missing.");
      return metrics;
    }
    
    ParsedSum dataTotalAmountBucket = compositeBucket.getAggregations().get(METRIC_TOTAL_AMOUNT);
    if (dataTotalAmountBucket == null) {
      log.error("Unable to extract totalAmount metric, aggregation is missing.");
      return metrics;
    }
    metrics.put(METRIC_TOTAL_AMOUNT, (int) dataTotalAmountBucket.getValue());
    
    return metrics;
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
    return this.extractDocumentIDFromFilter(filters, this.targetDay);
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
   * The definitive datacube is generated on yesterday, for a period of 1 day (~24 hours except for some special days)
   * Rows will be timestamped at yesterday_23:59:59.999+ZZZZ
   * Timestamp is the last millisecond of the period (therefore included).
   * This way it shows that *count* is computed for this day (yesterday) but at the very end of the day.
   */
  public void definitive()
  {
    Date now = SystemTime.getCurrentTime();
    Date yesterday = RLMDateUtils.addDays(now, -1, Deployment.getBaseTimeZone());
    Date beginningOfYesterday = RLMDateUtils.truncate(yesterday, Calendar.DATE, Deployment.getBaseTimeZone());
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, Deployment.getBaseTimeZone());        // 00:00:00.000
    Date endOfYesterday = RLMDateUtils.addMilliseconds(beginningOfToday, -1);                               // 23:59:59.999

    this.previewMode = false;
    this.targetDay = RLMDateUtils.printDay(yesterday);

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
  public void preview()
  {
    Date now = SystemTime.getCurrentTime();
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, Deployment.getBaseTimeZone());

    this.previewMode = true;
    this.targetDay = RLMDateUtils.printDay(now);

    //
    // Timestamp & period
    //
    String timestamp = RLMDateUtils.printTimestamp(now);
    long targetPeriod = now.getTime() - beginningOfToday.getTime() + 1; // +1 !
    
    this.run(timestamp, targetPeriod);
  }
}
