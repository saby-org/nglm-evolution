package com.evolving.nglm.evolution.datacubes.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite.ParsedBucket;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.SubscriberProfileDatacubeMetric;
import com.evolving.nglm.evolution.datacubes.mapping.LoyaltyProgramsMap;

public class ProgramsHistoryDatacubeGenerator extends DatacubeGenerator
{
  private static final String DATACUBE_ES_INDEX = "datacube_loyaltyprogramshistory";
  private static final String DATA_ES_INDEX = "subscriberprofile";
  private static final String DATA_ES_INDEX_SNAPSHOT_PREFIX = "subscriberprofile_snapshot-";
  private static final String FILTER_LOYALTY_PROGRAM_TIER = "loyaltyProgramTier";
  private static final String DATA_POINT_EARNED = "_Earned";
  private static final String DATA_POINT_REDEEMED = "_Redeemed";
  private static final String DATA_POINT_EXPIRED = "_Expired";
  private static final String DATA_METRIC_PREFIX = "metric_";
  private static final Pattern LOYALTY_TIER_PATTERN = Pattern.compile("\\[(.*), (.*), (.*)\\]");

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private List<String> filterFields;
  private LoyaltyProgramsMap loyaltyProgramsMap;

  private boolean previewMode;
  private boolean snapshotsAvailable;
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
  public ProgramsHistoryDatacubeGenerator(String datacubeName, RestHighLevelClient elasticsearch, LoyaltyProgramService loyaltyProgramService)
  {
    super(datacubeName, elasticsearch);

    this.loyaltyProgramsMap = new LoyaltyProgramsMap(loyaltyProgramService);
    this.snapshotsAvailable = true;
    //TODO: this.subscriberStatusDisplayMapping = new SubscriberStatusMap();
    
    //
    // Filter fields
    //
    this.filterFields = new ArrayList<String>();
    this.filterFields.add("evolutionSubscriberStatus");
  }

  /*****************************************
  *
  * Elasticsearch indices settings
  *
  *****************************************/
  @Override protected String getDataESIndex() { 
    if (this.previewMode || !this.snapshotsAvailable) {
      return DATA_ES_INDEX;
    } else {
      return DATA_ES_INDEX_SNAPSHOT_PREFIX + metricTargetDay; 
    }
  }
  
  @Override protected String getDatacubeESIndex() { return DATACUBE_ES_INDEX; }

  /*****************************************
  *
  * Filters settings
  *
  *****************************************/
  @Override protected List<String> getFilterFields() { return filterFields; }
  
  @Override 
  protected List<CompositeValuesSourceBuilder<?>> getFilterComplexSources() {
    List<CompositeValuesSourceBuilder<?>> result = new ArrayList<CompositeValuesSourceBuilder<?>>();
    
    String painlessProgramsMap = "[";
    String painlessRewardsMap = "[";
    
    // Warning, if programID or rewardID contain special characters (', ", \) it will break the script !
    boolean first = true;
    for(String programID : loyaltyProgramsMap.keySet()) 
      {
        if(!first) {
          painlessProgramsMap += ", ";
          painlessRewardsMap += ", ";
        }
        painlessProgramsMap += "'"+ programID +"'";
        painlessRewardsMap += "'"+ loyaltyProgramsMap.getRewardPointsID(programID, "painlessRewardsMap") +"'";
        first = false;
      }
    painlessProgramsMap += "]";
    painlessRewardsMap += "]";

    String targetDayBeginningIncluded = metricTargetDayStartTime + "L";
    String targetDayEndExcluded = (metricTargetDayStartTime+metricTargetDayDuration) + "L";
    String targetDayAfterBeginningIncluded = metricTargetDayAfterStartTime + "L";
    String targetDayAfterEndExcluded = (metricTargetDayAfterStartTime+metricTargetDayAfterDuration) + "L";
        
    TermsValuesSourceBuilder loyaltyProgramTier = new TermsValuesSourceBuilder(FILTER_LOYALTY_PROGRAM_TIER)
        .script(new Script(ScriptType.INLINE, "painless", "def left = [];"
            + " def programs_map = "+ painlessProgramsMap +";"
            + " def rewards_map = "+ painlessRewardsMap +";"
            + " for (int i = 0; i < params._source['loyaltyPrograms'].length; i++) {"
              + " def tuple = [0,0,0];"
              + " tuple[0] = params._source['loyaltyPrograms'][i]['programID'];"
              + " tuple[1] = params._source['loyaltyPrograms'][i]['tierName']?.toString();"
              + " tuple[2] = false;"
              + " if(tuple[0] == null){ continue; }"
              + " def pointID = '';"
              + " for (int j = 0; j < programs_map.length; j++) {"
                + " if(programs_map[j] == tuple[0]){ pointID = rewards_map[j]; break; }"
              + " }"
              + " if(params._source['pointFluctuations'][pointID]?.toString() != null){"
                + " def updateTime = doc['lastUpdateDate'].value.toInstant().toEpochMilli();"
                + " if( (updateTime >= "+ targetDayAfterBeginningIncluded +" && updateTime < "+ targetDayAfterEndExcluded +"  && params._source['pointFluctuations'][pointID]['yesterday']['redeemed'] > 0 ) "
                + "   || (updateTime >= "+ targetDayBeginningIncluded +" && updateTime < "+ targetDayEndExcluded +" && params._source['pointFluctuations'][pointID]['today']['redeemed'] > 0) ) {"
                + "   tuple[2] = true;"
                + " }"
              + " }"
              + " left.add(tuple);"
            + " }"
            + " return left;", Collections.emptyMap()));
    result.add(loyaltyProgramTier);
    
    return result;
  }

  @Override
  protected boolean runPreGenerationPhase() throws ElasticsearchException, IOException, ClassCastException
  {
    loyaltyProgramsMap.update();
    //TODO: subscriberStatusDisplayMapping.updateFromElasticsearch(elasticsearch);
    
    this.snapshotsAvailable = true;
    String initialDataIndex = getDataESIndex();
    this.snapshotsAvailable = isESIndexAvailable(initialDataIndex);
    if(!this.snapshotsAvailable) {
      log.warn("Elasticsearch index [" + initialDataIndex + "] does not exist. We will execute request on [" + getDataESIndex() + "] instead.");
    }
    
    return true;
  }

  @Override
  protected void embellishFilters(Map<String, Object> filters)
  {
    // TODO: retrieve display for evolutionSubscriberStatus
    
    String loyaltyTier = (String) filters.remove(FILTER_LOYALTY_PROGRAM_TIER);
    String loyaltyProgramID = "undefined";
    String tierName = "undefined";
    String redeemer = "false";
    Matcher m = LOYALTY_TIER_PATTERN.matcher(loyaltyTier);
    if(m.matches()) 
      {
        loyaltyProgramID = m.group(1);
        tierName = m.group(2);
        redeemer = m.group(3);
        if(tierName.equals("null")) 
          {
            // rename
            tierName = "None";
          }
      }
    else 
      {
        log.warn("Unable to parse "+ FILTER_LOYALTY_PROGRAM_TIER + " field.");
      }
    
    filters.put("tier", tierName);
    filters.put("redeemer", Boolean.parseBoolean(redeemer));
    filters.put("loyaltyProgramID", loyaltyProgramID); // @rl: Hack, this will be removed in extractMetrics function. Really ugly and not consistent with the pattern...
    filters.put("loyaltyProgram", loyaltyProgramsMap.getDisplay(loyaltyProgramID, "loyaltyProgram"));
  }

  /*****************************************
  *
  * Metrics settings
  *
  *****************************************/
  @Override
  protected List<AggregationBuilder> getMetricAggregations()
  {
    // Those aggregations need to be recomputed with the requested date !
    List<AggregationBuilder> metricAggregations = new ArrayList<AggregationBuilder>();
    
    List<String> pointIDs = new ArrayList<String>();
    
    for(String programID : loyaltyProgramsMap.keySet()) 
      {
        String newPointID = loyaltyProgramsMap.getRewardPointsID(programID, "loyaltyProgram");
        boolean found = false;
        for(String pointID: pointIDs) 
          {
            if(pointID.equals(newPointID)) 
              {
                found = true;
                break;
              }
          }
        if(!found && newPointID != null) 
          {
            pointIDs.add(newPointID);
          }
      }

    String targetDayBeginningIncluded = metricTargetDayStartTime + "L";
    String targetDayEndExcluded = (metricTargetDayStartTime+metricTargetDayDuration) + "L";
    String targetDayAfterBeginningIncluded = metricTargetDayAfterStartTime + "L";
    String targetDayAfterEndExcluded = (metricTargetDayAfterStartTime+metricTargetDayAfterDuration) + "L";
    
    for(String pointID: pointIDs) 
      {
        AggregationBuilder pointEarned = AggregationBuilders.sum(pointID + DATA_POINT_EARNED)
            .script(new Script(ScriptType.INLINE, "painless", "def left = 0;"
            + " if(params._source['pointFluctuations']['"+pointID+"']?.toString() != null){"
              + " def updateTime = doc['lastUpdateDate'].value.toInstant().toEpochMilli();"
              + " if(updateTime >= "+ targetDayAfterBeginningIncluded +" && updateTime < "+ targetDayAfterEndExcluded +"){"
                + " left = params._source['pointFluctuations']['"+pointID+"']['yesterday']['earned'];"
              + " } else if(updateTime >= "+ targetDayBeginningIncluded +" && updateTime < "+ targetDayEndExcluded +"){"
                + " left = params._source['pointFluctuations']['"+pointID+"']['today']['earned'];"
              + " }"
            + " } return left;", Collections.emptyMap()));
        metricAggregations.add(pointEarned);
        
        AggregationBuilder pointRedeemed = AggregationBuilders.sum(pointID + DATA_POINT_REDEEMED)
            .script(new Script(ScriptType.INLINE, "painless", "def left = 0;"
            + " if(params._source['pointFluctuations']['"+pointID+"']?.toString() != null){"
              + " def updateTime = doc['lastUpdateDate'].value.toInstant().toEpochMilli();"
              + " if(updateTime >= "+ targetDayAfterBeginningIncluded +" && updateTime < "+ targetDayAfterEndExcluded +"){"
                + " left = params._source['pointFluctuations']['"+pointID+"']['yesterday']['redeemed'];"
                    + " } else if(updateTime >= "+ targetDayBeginningIncluded +" && updateTime < "+ targetDayEndExcluded +"){"
                + " left = params._source['pointFluctuations']['"+pointID+"']['today']['redeemed'];"
              + " }"
            + " } return left;", Collections.emptyMap()));
        metricAggregations.add(pointRedeemed);
        
        AggregationBuilder pointExpired = AggregationBuilders.sum(pointID + DATA_POINT_EXPIRED)
            .script(new Script(ScriptType.INLINE, "painless", "def left = 0;"
            + " if(params._source['pointFluctuations']['"+pointID+"']?.toString() != null){"
              + " def updateTime = doc['lastUpdateDate'].value.toInstant().toEpochMilli();"
              + " if(updateTime >= "+ targetDayAfterBeginningIncluded +" && updateTime < "+ targetDayAfterEndExcluded +"){"
                + " left = params._source['pointFluctuations']['"+pointID+"']['yesterday']['expired'];"
                    + " } else if(updateTime >= "+ targetDayBeginningIncluded +" && updateTime < "+ targetDayEndExcluded +"){"
                + " left = params._source['pointFluctuations']['"+pointID+"']['today']['expired'];"
              + " }"
            + " } return left;", Collections.emptyMap()));
        metricAggregations.add(pointExpired);
      }
    
    Map<String, SubscriberProfileDatacubeMetric> customMetrics = Deployment.getSubscriberProfileDatacubeMetrics();
    for(String metricID: customMetrics.keySet()) {
      SubscriberProfileDatacubeMetric metric = customMetrics.get(metricID);
      AggregationBuilder customMetricAgg = AggregationBuilders.sum(DATA_METRIC_PREFIX+metricID)
          .script(new Script(ScriptType.INLINE, "painless", "def left = 0;"
          + " def updateTime = doc['lastUpdateDate'].value.toInstant().toEpochMilli();"
          + " if(updateTime >= "+ targetDayAfterBeginningIncluded +" && updateTime < "+ targetDayAfterEndExcluded +"){"
            + " left = params._source['"+ metric.getYesterdayESField() +"'];"
          + " } else if(updateTime >= "+ targetDayBeginningIncluded +" && updateTime < "+ targetDayEndExcluded +"){"
            + " left = params._source['"+ metric.getTodayESField() +"'];"
          + " } return left;", Collections.emptyMap()));
      metricAggregations.add(customMetricAgg);
    }
    
    return metricAggregations;
  }

  @Override
  protected Map<String, Object> extractMetrics(ParsedBucket compositeBucket, Map<String, Object> contextFilters) throws ClassCastException
  {    
    HashMap<String, Object> metrics = new HashMap<String,Object>();
    if (compositeBucket.getAggregations() == null) {
      log.error("Unable to extract data, aggregation is missing.");
      return metrics;
    }

    String programID = (String) contextFilters.get("loyaltyProgramID");
    contextFilters.remove("loyaltyProgramID"); // @rl: it was just kept for that purpose, do not push it at the end.
    String pointID = loyaltyProgramsMap.getRewardPointsID(programID, "loyaltyProgram");
    if (pointID == null) {
      log.error("Unable to extract "+programID+" points information from loyalty programs mapping.");
      return metrics;
    }
    
    ParsedSum dataPointEarnedBucket = compositeBucket.getAggregations().get(pointID+DATA_POINT_EARNED);
    if (dataPointEarnedBucket == null) {
      log.error("Unable to extract "+pointID+" points earned data, aggregation is missing.");
      return metrics;
    }
    metrics.put("rewards.earned", (int) dataPointEarnedBucket.getValue());
    
    ParsedSum dataPointRedeemedBucket = compositeBucket.getAggregations().get(pointID+DATA_POINT_REDEEMED);
    if (dataPointRedeemedBucket == null) {
      log.error("Unable to extract "+pointID+" points redeemed data, aggregation is missing.");
      return metrics;
    }
    metrics.put("rewards.redeemed", (int) dataPointRedeemedBucket.getValue());
    
    ParsedSum dataPointExpiredBucket = compositeBucket.getAggregations().get(pointID+DATA_POINT_EXPIRED);
    if (dataPointExpiredBucket == null) {
      log.error("Unable to extract "+pointID+" points expired data, aggregation is missing.");
      return metrics;
    }
    metrics.put("rewards.expired", (int) dataPointExpiredBucket.getValue());
    
    Map<String, SubscriberProfileDatacubeMetric> customMetrics = Deployment.getSubscriberProfileDatacubeMetrics();
    for(String metricID: customMetrics.keySet()) {
      SubscriberProfileDatacubeMetric customMetric = customMetrics.get(metricID);
      
      ParsedSum metricBucket = compositeBucket.getAggregations().get(DATA_METRIC_PREFIX+metricID);
      if (metricBucket == null) {
        log.error("Unable to extract "+metricID+" metric data, aggregation is missing.");
        return metrics;
      }
      metrics.put("custom." + customMetric.getDisplay(), (int) metricBucket.getValue());
    }
    
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
    Date tomorrow = RLMDateUtils.addDays(now, 1, Deployment.getBaseTimeZone());
    
    // Dates: YYYY-MM-dd 00:00:00.000
    Date beginningOfYesterday = RLMDateUtils.truncate(yesterday, Calendar.DATE, Deployment.getBaseTimeZone());
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, Deployment.getBaseTimeZone());
    Date beginningOfTomorrow = RLMDateUtils.truncate(tomorrow, Calendar.DATE, Deployment.getBaseTimeZone());

    this.previewMode = false;
    this.metricTargetDay = DAY_FORMAT.format(yesterday);
    this.metricTargetDayStartTime = beginningOfYesterday.getTime();
    this.metricTargetDayDuration = beginningOfToday.getTime() - beginningOfYesterday.getTime();
    this.metricTargetDayAfterStartTime = beginningOfToday.getTime();
    this.metricTargetDayAfterDuration = beginningOfTomorrow.getTime() - beginningOfToday.getTime();

    //
    // Timestamp & period
    //
    Date endOfYesterday = RLMDateUtils.addMilliseconds(beginningOfToday, -1);                               // 23:59:59.999
    String timestamp = TIMESTAMP_FORMAT.format(endOfYesterday);
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
    Date twodaysafter = RLMDateUtils.addDays(now, 2, Deployment.getBaseTimeZone());
    
    // Dates: YYYY-MM-dd 00:00:00.000
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, Deployment.getBaseTimeZone());
    Date beginningOfTomorrow = RLMDateUtils.truncate(tomorrow, Calendar.DATE, Deployment.getBaseTimeZone());
    Date beginningOfTwodaysafter = RLMDateUtils.truncate(twodaysafter, Calendar.DATE, Deployment.getBaseTimeZone());
    
    this.previewMode = true;
    this.metricTargetDay = DAY_FORMAT.format(now);
    this.metricTargetDayStartTime = beginningOfToday.getTime();
    this.metricTargetDayDuration = beginningOfTomorrow.getTime() - beginningOfToday.getTime();
    this.metricTargetDayAfterStartTime = beginningOfTomorrow.getTime();
    this.metricTargetDayAfterDuration = beginningOfTwodaysafter.getTime() - beginningOfTomorrow.getTime();

    //
    // Timestamp & period
    //
    String timestamp = TIMESTAMP_FORMAT.format(now);
    long targetPeriod = now.getTime() - beginningOfToday.getTime() + 1; // +1 !
    
    this.run(timestamp, targetPeriod);
  }
}
