package com.evolving.nglm.evolution.datacubes.loyalty;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
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
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.mapping.LoyaltyProgramsMap;
import com.evolving.nglm.evolution.datacubes.mapping.SubscriberStatusMap;
import com.evolving.nglm.evolution.datacubes.subscriber.SubscriberProfileDatacubeMetric;

public class ProgramsHistoryDatacubeGenerator extends DatacubeGenerator
{
  private static final DateFormat DATE_FORMAT;
  static
  {
    DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
  }
  private static final String DATACUBE_ES_INDEX = "datacube_loyaltyprogramshistory";
  private static final String DATA_ES_INDEX = "subscriberprofile";
  private static final String FILTER_LOYALTY_PROGRAM_TIER = "loyaltyProgramTier";
  private static final String DATA_POINT_EARNED = "_Earned";
  private static final String DATA_POINT_REDEEMED = "_Redeemed";
  private static final String DATA_POINT_EXPIRED = "_Expired";
  private static final String DATA_METRIC_PREFIX = "metric_";
  private static final Pattern LOYALTY_TIER_PATTERN = Pattern.compile("\\[(.*), (.*), (.*)\\]");

  private List<String> filterFields;
  private LoyaltyProgramsMap loyaltyProgramsMap;
  private SubscriberStatusMap subscriberStatusDisplayMapping;
  
  private String generationDate;
  private String oneDayAfter; // Temporary variable for request generation
  
  public ProgramsHistoryDatacubeGenerator(String datacubeName, RestHighLevelClient elasticsearch, LoyaltyProgramService loyaltyProgramService)
  {
    super(datacubeName, elasticsearch);

    this.loyaltyProgramsMap = new LoyaltyProgramsMap(loyaltyProgramService);
    this.subscriberStatusDisplayMapping = new SubscriberStatusMap();
    
    //
    // Filter fields
    //
    
    this.filterFields = new ArrayList<String>();
    this.filterFields.add("evolutionSubscriberStatus");
  }

  @Override protected String getDatacubeESIndex() { return DATACUBE_ES_INDEX; }
  @Override protected String getDataESIndex() { return DATA_ES_INDEX; }
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
                + " if( (doc['lastUpdateDate'].value.toString('YYYY-MM-dd') == '"+ oneDayAfter +"' && params._source['pointFluctuations'][pointID]['yesterday']['redeemed'] > 0 ) "
                + "   || (doc['lastUpdateDate'].value.toString('YYYY-MM-dd') == '"+ generationDate +"' && params._source['pointFluctuations'][pointID]['today']['redeemed'] > 0) ) {"
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
    subscriberStatusDisplayMapping.updateFromElasticsearch(elasticsearch);
    
    try {
      oneDayAfter = DATE_FORMAT.format(RLMDateUtils.addDays(DATE_FORMAT.parse(generationDate), 1, Deployment.getBaseTimeZone()));
    } 
    catch (ParseException e) {
      log.error("Unable to build some part of the ES request due to date formatting error.");
      return false;
    }
    
    return true;
  }

  @Override
  protected void addStaticFilters(Map<String, Object> filters)
  {
    filters.put("dataDate", generationDate);
  }

  @Override
  protected void embellishFilters(Map<String, Object> filters)
  {
    String status = (String) filters.remove("evolutionSubscriberStatus");
    filters.put("evolutionSubscriberStatus.id", status);
    filters.put("evolutionSubscriberStatus.display", subscriberStatusDisplayMapping.getDisplay(status));
    
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
    
    filters.put("tierName", tierName);
    filters.put("redeemer", Boolean.parseBoolean(redeemer));
    
    filters.put("loyaltyProgram.id", loyaltyProgramID);
    filters.put("loyaltyProgram.display", loyaltyProgramsMap.getDisplay(loyaltyProgramID, "loyaltyProgram"));
  }

  @Override
  protected List<AggregationBuilder> getDataAggregations()
  {
    // Those aggregations need to be recomputed with the requested date !
    List<AggregationBuilder> dataAggregations = new ArrayList<AggregationBuilder>();
    
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
    
    for(String pointID: pointIDs) 
      {
        AggregationBuilder pointEarned = AggregationBuilders.sum(pointID + DATA_POINT_EARNED)
            .script(new Script(ScriptType.INLINE, "painless", "def left = 0; if(params._source['pointFluctuations']['"+pointID+"']?.toString() != null){ if(doc['lastUpdateDate'].value.toString('YYYY-MM-dd') == '"+ oneDayAfter +"'){ left = params._source['pointFluctuations']['"+pointID+"']['yesterday']['earned']; } else if(doc['lastUpdateDate'].value.toString('YYYY-MM-dd') == '"+ generationDate +"') {left = params._source['pointFluctuations']['"+pointID+"']['today']['earned']; } } return left;", Collections.emptyMap()));
        dataAggregations.add(pointEarned);
        
        AggregationBuilder pointRedeemed = AggregationBuilders.sum(pointID + DATA_POINT_REDEEMED)
            .script(new Script(ScriptType.INLINE, "painless", "def left = 0; if(params._source['pointFluctuations']['"+pointID+"']?.toString() != null){ if(doc['lastUpdateDate'].value.toString('YYYY-MM-dd') == '"+ oneDayAfter +"'){ left = params._source['pointFluctuations']['"+pointID+"']['yesterday']['redeemed']; } else if(doc['lastUpdateDate'].value.toString('YYYY-MM-dd') == '"+ generationDate +"') {left = params._source['pointFluctuations']['"+pointID+"']['today']['redeemed']; } } return left;", Collections.emptyMap()));
        dataAggregations.add(pointRedeemed);
        
        AggregationBuilder pointExpired = AggregationBuilders.sum(pointID + DATA_POINT_EXPIRED)
            .script(new Script(ScriptType.INLINE, "painless", "def left = 0; if(params._source['pointFluctuations']['"+pointID+"']?.toString() != null){ if(doc['lastUpdateDate'].value.toString('YYYY-MM-dd') == '"+ oneDayAfter +"'){ left = params._source['pointFluctuations']['"+pointID+"']['yesterday']['expired']; } else if(doc['lastUpdateDate'].value.toString('YYYY-MM-dd') == '"+ generationDate +"') {left = params._source['pointFluctuations']['"+pointID+"']['today']['expired']; } } return left;", Collections.emptyMap()));
        dataAggregations.add(pointExpired);
      }
    
    Map<String, SubscriberProfileDatacubeMetric> metrics = Deployment.getSubscriberProfileDatacubeMetrics();
    for(String metricID: metrics.keySet()) {
      SubscriberProfileDatacubeMetric metric = metrics.get(metricID);
      AggregationBuilder metricAgg = AggregationBuilders.sum(DATA_METRIC_PREFIX+metricID)
          .script(new Script(ScriptType.INLINE, "painless", "def left = 0;"
          + " if(doc['lastUpdateDate'].value.toString('YYYY-MM-dd') == '"+ oneDayAfter +"') { left = params._source['"+ metric.getYesterdayESField() +"']; }"
          + " else if(doc['lastUpdateDate'].value.toString('YYYY-MM-dd') == '"+ generationDate +"') { left = params._source['"+ metric.getTodayESField() +"']; }"
          + " return left;", Collections.emptyMap()));
      dataAggregations.add(metricAgg);
    }
    
    return dataAggregations;
  }

  @Override
  protected Map<String, Object> extractData(ParsedBucket compositeBucket, Map<String, Object> contextFilters) throws ClassCastException
  {    
    HashMap<String, Object> data = new HashMap<String,Object>();
    if (compositeBucket.getAggregations() == null) {
      log.error("Unable to extract data, aggregation is missing.");
      return data;
    }

    String programID = (String) contextFilters.get("loyaltyProgram.id");
    String pointID = loyaltyProgramsMap.getRewardPointsID(programID, "loyaltyProgram");
    if (pointID == null) {
      log.error("Unable to extract "+programID+" points information from loyalty programs mapping.");
      return data;
    }
    
    ParsedSum dataPointEarnedBucket = compositeBucket.getAggregations().get(pointID+DATA_POINT_EARNED);
    if (dataPointEarnedBucket == null) {
      log.error("Unable to extract "+pointID+" points earned data, aggregation is missing.");
      return data;
    }
    data.put("rewardPointEarned", (int) dataPointEarnedBucket.getValue());
    
    ParsedSum dataPointRedeemedBucket = compositeBucket.getAggregations().get(pointID+DATA_POINT_REDEEMED);
    if (dataPointRedeemedBucket == null) {
      log.error("Unable to extract "+pointID+" points redeemed data, aggregation is missing.");
      return data;
    }
    data.put("rewardPointRedeemed", (int) dataPointRedeemedBucket.getValue());
    
    ParsedSum dataPointExpiredBucket = compositeBucket.getAggregations().get(pointID+DATA_POINT_EXPIRED);
    if (dataPointExpiredBucket == null) {
      log.error("Unable to extract "+pointID+" points expired data, aggregation is missing.");
      return data;
    }
    data.put("rewardPointExpired", (int) dataPointExpiredBucket.getValue());
    
    Map<String, SubscriberProfileDatacubeMetric> metrics = Deployment.getSubscriberProfileDatacubeMetrics();
    for(String metricID: metrics.keySet()) {
      SubscriberProfileDatacubeMetric metric = metrics.get(metricID);
      
      ParsedSum metricBucket = compositeBucket.getAggregations().get(DATA_METRIC_PREFIX+metricID);
      if (metricBucket == null) {
        log.error("Unable to extract "+metricID+" metric data, aggregation is missing.");
        return data;
      }
      data.put(metric.getDisplay(), (int) metricBucket.getValue());
    }
    
    return data;
  }

  /*****************************************
  *
  *  run
  *
  *****************************************/
  
  public void run(Date generationDate)
  {
    this.generationDate = DATE_FORMAT.format(generationDate);
    this.run();
  }
}
