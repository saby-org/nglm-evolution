package com.evolving.nglm.evolution.datacubes.tiers;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite.ParsedBucket;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.mapping.GUIManagerClient;
import com.evolving.nglm.evolution.datacubes.mapping.LoyaltyProgramsMap;

public class TiersDatacubeGenerator extends DatacubeGenerator
{
  private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
  private static final String DATACUBE_ES_INDEX = "datacube_loyaltyprogramschanges";
  private static final String DATA_ES_INDEX = "subscriberprofile";
  private static final String FILTER_ALL = "filters";
  private static final Pattern LOYALTY_TIERS_PATTERN = Pattern.compile("\\[(.*), (.*), (.*), (.*)\\]");

  private GUIManagerClient guiClient;
  private LoyaltyProgramsMap loyaltyProgramsMap;
  
  private String generationDate;
  
  public TiersDatacubeGenerator(String datacubeName, RestHighLevelClient elasticsearch, GUIManagerClient guiClient)
  {
    super(datacubeName, elasticsearch);
    this.guiClient = guiClient;
    this.loyaltyProgramsMap = new LoyaltyProgramsMap();
  }

  @Override protected String getDatacubeESIndex() { return DATACUBE_ES_INDEX; }
  @Override protected String getDataESIndex() { return DATA_ES_INDEX; }
  @Override protected List<String> getFilterFields() { return Collections.emptyList(); }
  @Override protected List<AggregationBuilder> getDataAggregations() { return Collections.emptyList(); }
  @Override protected Map<String, Object> extractData(ParsedBucket compositeBucket, Map<String, Object> contextFilters) throws ClassCastException { return Collections.emptyMap(); }

  @Override
  protected void runPreGenerationPhase() throws ElasticsearchException, IOException, ClassCastException
  {
    loyaltyProgramsMap.updateFromGUIManager(guiClient);
  }

  @Override
  protected void addStaticFilters(Map<String, Object> filters)
  {
    filters.put("dataDate", generationDate);
  }

  @Override
  protected void embellishFilters(Map<String, Object> filters)
  {
    String date = (String) filters.remove("dataDate");
    filters.put("tierChangeDate", date);
    
    String loyaltyTiers = (String) filters.remove(FILTER_ALL);
    String loyaltyProgramID = "undefined";
    String newTierName = "undefined";
    String previousTierName = "undefined";
    String tierChangeType = "undefined";
    Matcher m = LOYALTY_TIERS_PATTERN.matcher(loyaltyTiers);
    if(m.matches()) 
      {
        loyaltyProgramID = m.group(1);
        newTierName = m.group(2);
        previousTierName = m.group(3);
        tierChangeType = m.group(4);
        
        //
        // rename
        // 
        
        if(newTierName.equals("null")) { newTierName = "None"; }
        if(previousTierName.equals("null")) { previousTierName = "None"; }
      }
    else 
      {
        log.warn("Unable to parse " + FILTER_ALL + " field.");
      }
    
    filters.put("newTierName", newTierName);
    filters.put("previousTierName", previousTierName);
    filters.put("tierChangeType", tierChangeType);
    
    filters.put("loyaltyProgram.id", loyaltyProgramID);
    filters.put("loyaltyProgram.display", loyaltyProgramsMap.getDisplay(loyaltyProgramID, "loyaltyProgram"));
  }

  @Override
  protected List<CompositeValuesSourceBuilder<?>> getFilterComplexSources()
  {
    //
    // LoyaltyProgram x New Tier x Previous Tier x Type ...
    //
    
    List<CompositeValuesSourceBuilder<?>> filterComplexSources = new ArrayList<CompositeValuesSourceBuilder<?>>();
    
    Long requestedDate;
    Long oneDayAfter;
    
    try
      {
        requestedDate  = DATE_FORMAT.parse(generationDate).getTime();
        oneDayAfter = RLMDateUtils.addDays(DATE_FORMAT.parse(generationDate), 1, Deployment.getBaseTimeZone()).getTime();
      } 
    catch (ParseException e)
      {
        log.error("Unable to build some part of the ES request due to date formatting error.");
        return filterComplexSources;
      }
    
    String dateBeginningIncluded = requestedDate.toString() + "L";
    String dateEndExcluded = oneDayAfter.toString() + "L";

    TermsValuesSourceBuilder loyaltyProgramTier = new TermsValuesSourceBuilder(FILTER_ALL)
        .script(new Script(ScriptType.INLINE, "painless", "def left = []; for (int i = 0; i < params._source['loyaltyPrograms'].length; i++) { if(params._source['loyaltyPrograms'][i]['tierUpdateDate']?.toString() != null && params._source['loyaltyPrograms'][i]['tierUpdateDate'] >= "+dateBeginningIncluded+" && params._source['loyaltyPrograms'][i]['tierUpdateDate'] < "+dateEndExcluded+"){ def filter = [0,0,0,0]; filter[0] = params._source['loyaltyPrograms'][i]['programID']; filter[1] = params._source['loyaltyPrograms'][i]['tierName']?.toString(); filter[2] = params._source['loyaltyPrograms'][i]['previousTierName']?.toString(); filter[3] = params._source['loyaltyPrograms'][i]['tierChangeType']?.toString(); left.add(filter); } } return left;", Collections.emptyMap()));
    filterComplexSources.add(loyaltyProgramTier);
    
    return filterComplexSources;
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
