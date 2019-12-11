package com.evolving.nglm.evolution.datacubes.loyalty;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
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
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.mapping.LoyaltyProgramDisplayMapping;
import com.evolving.nglm.evolution.datacubes.mapping.SubscriberStatusDisplayMapping;

public class LoyaltyDatacubeGenerator extends DatacubeGenerator
{
  private static final String filterLoyaltyProgramTier = "loyaltyProgramTier";
  private static final String dataPointEarned = "_Earned";
  private static final String dataPointRedeemed = "_Redeemed";
  private static final String dataPointExpired = "_Expired";
  private static final String dataPointRedeemerCount = "_RedeemerCount";
  private static final String datacubeESIndex = "datacube_loyaltyprogramshistory";
  private static final String dataESIndexPrefix = "subscriberprofile";
  private static final Pattern loyaltyTierPattern = Pattern.compile("\\[(.*), (.*)\\]");

  private List<String> filterFields;
  private List<CompositeValuesSourceBuilder<?>> filterComplexSources;
  private LoyaltyProgramDisplayMapping loyaltyProgramDisplayMapping;
  private SubscriberStatusDisplayMapping subscriberStatusDisplayMapping;
  
  public LoyaltyDatacubeGenerator(String datacubeName, RestHighLevelClient elasticsearch)  
  {
    super(datacubeName, elasticsearch);

    this.loyaltyProgramDisplayMapping = new LoyaltyProgramDisplayMapping();
    this.subscriberStatusDisplayMapping = new SubscriberStatusDisplayMapping();
    
    //
    // Filter fields
    //
    
    this.filterFields = new ArrayList<String>();
    this.filterFields.add("evolutionSubscriberStatus");
    
    //
    // Filter Complex Sources
    // - LoyaltyProgram x Tier ...
    //
    
    this.filterComplexSources = new ArrayList<CompositeValuesSourceBuilder<?>>();

    TermsValuesSourceBuilder loyaltyProgramTier = new TermsValuesSourceBuilder(filterLoyaltyProgramTier)
        .script(new Script(ScriptType.INLINE, "painless", "def left = []; for (int i = 0; i < params._source['loyaltyPrograms'].length; i++) { def pair = [0,0]; pair[0] = params._source['loyaltyPrograms'][i]['programID']; pair[1] = params._source['loyaltyPrograms'][i]['tierName']?.toString();  left.add(pair); } return left;", Collections.emptyMap()));
    this.filterComplexSources.add(loyaltyProgramTier);
    
    //
    // Data Aggregations
    // - nothing ...
    //
  }

  @Override
  protected void runPreGenerationPhase(RestHighLevelClient elasticsearch) throws ElasticsearchException, IOException, ClassCastException
  {
    loyaltyProgramDisplayMapping.updateFromElasticsearch(elasticsearch);
    subscriberStatusDisplayMapping.updateFromElasticsearch(elasticsearch);
  }

  @Override
  protected void embellishFilters(Map<String, Object> filters)
  {
    String status = (String) filters.remove("evolutionSubscriberStatus");
    filters.put("evolutionSubscriberStatus.id", status);
    filters.put("evolutionSubscriberStatus.display", subscriberStatusDisplayMapping.getDisplay(status));
    
    String loyaltyTier = (String) filters.remove(filterLoyaltyProgramTier);
    String loyaltyProgramID = "undefined";
    String tierName = "undefined";
    Matcher m = loyaltyTierPattern.matcher(loyaltyTier);
    if(m.matches()) 
      {
        loyaltyProgramID = m.group(1);
        tierName = m.group(2);
        if(tierName.equals("null")) 
          {
            // rename
            tierName = "None";
          }
      }
    else 
      {
        log.warn("Unable to parse "+ filterLoyaltyProgramTier + " field.");
      }
    
    filters.put("tierName", tierName);
    
    filters.put("loyaltyProgram.id", loyaltyProgramID);
    filters.put("loyaltyProgram.display", loyaltyProgramDisplayMapping.getDisplay(loyaltyProgramID));
  }

  @Override
  protected List<String> getFilterFields()
  {
    return filterFields;
  }

  @Override
  protected List<CompositeValuesSourceBuilder<?>> getFilterComplexSources(String date)
  {
    return this.filterComplexSources;
  }

  @Override
  protected List<AggregationBuilder> getDataAggregations(String date)
  {
    // Those aggregations need to be recomputed with the requested date !
    List<AggregationBuilder> dataAggregations = new ArrayList<AggregationBuilder>();
    String requestedDate = date;
    String oneDayAfter;
    
    try
      {
        oneDayAfter = DATE_FORMAT.format(RLMDateUtils.addDays(DATE_FORMAT.parse(date), 1, Deployment.getBaseTimeZone()));
      } 
    catch (ParseException e)
      {
        log.error("Unable to build some part of the ES request due to date formatting error.");
        return dataAggregations;
      }
    
    List<String> pointIDs = new ArrayList<String>();
    
    for(String programID : loyaltyProgramDisplayMapping.getLoyaltyPrograms()) 
      {
        String newPointID = loyaltyProgramDisplayMapping.getRewardPointsID(programID);
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
        AggregationBuilder pointEarned = AggregationBuilders.sum(pointID + dataPointEarned)
            .script(new Script(ScriptType.INLINE, "painless", "def left = 0; if(params._source['pointFluctuations']['"+pointID+"']?.toString() != null){ if(doc['lastUpdateDate'].value.toString('YYYY-MM-dd') == '"+ oneDayAfter +"'){ left = params._source['pointFluctuations']['"+pointID+"']['yesterday']['earned']; } else if(doc['lastUpdateDate'].value.toString('YYYY-MM-dd') == '"+ requestedDate +"') {left = params._source['pointFluctuations']['"+pointID+"']['today']['earned']; } } return left;", Collections.emptyMap()));
        dataAggregations.add(pointEarned);
        
        AggregationBuilder pointRedeemed = AggregationBuilders.sum(pointID + dataPointRedeemed)
            .script(new Script(ScriptType.INLINE, "painless", "def left = 0; if(params._source['pointFluctuations']['"+pointID+"']?.toString() != null){ if(doc['lastUpdateDate'].value.toString('YYYY-MM-dd') == '"+ oneDayAfter +"'){ left = params._source['pointFluctuations']['"+pointID+"']['yesterday']['redeemed']; } else if(doc['lastUpdateDate'].value.toString('YYYY-MM-dd') == '"+ requestedDate +"') {left = params._source['pointFluctuations']['"+pointID+"']['today']['redeemed']; } } return left;", Collections.emptyMap()));
        dataAggregations.add(pointRedeemed);
        
        AggregationBuilder pointExpired = AggregationBuilders.sum(pointID + dataPointExpired)
            .script(new Script(ScriptType.INLINE, "painless", "def left = 0; if(params._source['pointFluctuations']['"+pointID+"']?.toString() != null){ if(doc['lastUpdateDate'].value.toString('YYYY-MM-dd') == '"+ oneDayAfter +"'){ left = params._source['pointFluctuations']['"+pointID+"']['yesterday']['expired']; } else if(doc['lastUpdateDate'].value.toString('YYYY-MM-dd') == '"+ requestedDate +"') {left = params._source['pointFluctuations']['"+pointID+"']['today']['expired']; } } return left;", Collections.emptyMap()));
        dataAggregations.add(pointExpired);
        
        AggregationBuilder redeemerCount = AggregationBuilders.sum(pointID + dataPointRedeemerCount)
            .script(new Script(ScriptType.INLINE, "painless", "def left = 0; if(params._source['pointFluctuations']['"+pointID+"']?.toString() != null){ if( (doc['lastUpdateDate'].value.toString('YYYY-MM-dd') == '"+ oneDayAfter +"' && params._source['pointFluctuations']['"+pointID+"']['yesterday']['redeemed'] > 0 ) || (doc['lastUpdateDate'].value.toString('YYYY-MM-dd') == '"+ requestedDate +"' && params._source['pointFluctuations']['"+pointID+"']['today']['redeemed'] > 0) ) {left = 1;} } return left;", Collections.emptyMap()));
        dataAggregations.add(redeemerCount);
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
    String pointID = loyaltyProgramDisplayMapping.getRewardPointsID(programID);
    if (pointID == null) {
      log.error("Unable to extract "+programID+" points information from loyalty programs mapping.");
      return data;
    }
    
    ParsedSum dataPointEarnedBucket = compositeBucket.getAggregations().get(pointID+dataPointEarned);
    if (dataPointEarnedBucket == null) {
      log.error("Unable to extract "+pointID+" points earned data, aggregation is missing.");
      return data;
    }
    data.put("rewardPointEarned", (int) dataPointEarnedBucket.getValue());
    
    ParsedSum dataPointRedeemedBucket = compositeBucket.getAggregations().get(pointID+dataPointRedeemed);
    if (dataPointRedeemedBucket == null) {
      log.error("Unable to extract "+pointID+" points redeemed data, aggregation is missing.");
      return data;
    }
    data.put("rewardPointRedeemed", (int) dataPointRedeemedBucket.getValue());
    
    ParsedSum dataPointExpiredBucket = compositeBucket.getAggregations().get(pointID+dataPointExpired);
    if (dataPointExpiredBucket == null) {
      log.error("Unable to extract "+pointID+" points expired data, aggregation is missing.");
      return data;
    }
    data.put("rewardPointExpired", (int) dataPointExpiredBucket.getValue());
    
    ParsedSum dataPointRedeemerCountBucket = compositeBucket.getAggregations().get(pointID+dataPointRedeemerCount);
    if (dataPointRedeemerCountBucket == null) {
      log.error("Unable to extract "+pointID+" redeemer count data, aggregation is missing.");
      return data;
    }
    data.put("redeemerCount", (int) dataPointRedeemerCountBucket.getValue());
    
    return data;
  }

  @Override
  protected String getDataESIndex(String date)
  {
    return dataESIndexPrefix;
  }

  @Override
  protected String getDatacubeESIndex()
  {
    return datacubeESIndex;
  }
}
