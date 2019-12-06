package com.evolving.nglm.evolution.datacubes.tiers;

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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite.ParsedBucket;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;

public class TiersDatacubeGenerator extends DatacubeGenerator
{
  private List<String> filterFields;
  private final String allFilters = "filters";
  
  private final Pattern loyaltyTiersPattern = Pattern.compile("\\[(.*), (.*), (.*), (.*)\\]");
  
  private Map<String,String> loyaltyProgramDisplayMapping = new HashMap<>();    // Mapping (LoyaltyProgramID,loyaltyProgramName)
  
  //
  // Elasticsearch indexes
  //
  
  private final String datacubeESIndex = "datacube_loyaltyprogramschanges";
  private final String dataESIndexPrefix = "subscriberprofile";
  private final String mappingLoyalty = "mapping_loyaltyprograms";
  
  public TiersDatacubeGenerator(String datacubeName, RestHighLevelClient elasticsearch)  
  {
    super(datacubeName, elasticsearch);
    
    //
    // Filter fields
    //
    
    this.filterFields = new ArrayList<String>();
    
    //
    // Filter Complex Sources
    // - nothing ...
    
    //
    // Data Aggregations
    // - nothing ...
    //
  }

  @Override
  protected void runPreGenerationPhase(RestHighLevelClient elasticsearch) throws ElasticsearchException, IOException, ClassCastException
  {
    // 
    // Retrieve (LoyaltyProgramID, loyaltyProgramName) mapping
    //
    
    this.loyaltyProgramDisplayMapping = new HashMap<String, String>();

    SearchSourceBuilder request = new SearchSourceBuilder()
        .sort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
        .query(QueryBuilders.matchAllQuery())
        .size(BUCKETS_MAX_NBR);
    
    SearchResponse response = executeESSearchRequest(new SearchRequest(mappingLoyalty).source(request), elasticsearch);
    if(response == null) { return; }
    if(response.isTimedOut()
        || response.getFailedShards() > 0
        || response.getSkippedShards() > 0
        || response.status() != RestStatus.OK) {
      log.error("Elasticsearch search response return with bad status in {}", this.datacubeName);
      return;
    }
    
    SearchHits hits = response.getHits();
    if(hits == null) { return; }
    
    for(SearchHit hit: hits) {
      Map<String, Object> source = hit.getSourceAsMap();
      this.loyaltyProgramDisplayMapping.put((String) source.get("loyaltyProgramID"), (String) source.get("loyaltyProgramName"));
    }
  }

  @Override
  protected void embellishFilters(Map<String, Object> filters)
  {
    String date = (String) filters.remove("dataDate");
    filters.put("tierChangeDate", date);
    
    String loyaltyTiers = (String) filters.remove(allFilters);
    String loyaltyProgramID = "undefined";
    String newTierName = "undefined";
    String previousTierName = "undefined";
    String tierChangeType = "undefined";
    Matcher m = loyaltyTiersPattern.matcher(loyaltyTiers);
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
        log.warn("Unable to parse " + allFilters + " field.");
      }
    filters.put("newTierName", newTierName);
    filters.put("previousTierName", previousTierName);
    filters.put("tierChangeType", tierChangeType);
    
    filters.put("loyaltyProgram.id", loyaltyProgramID);
    String loyaltyProgramDisplay = this.loyaltyProgramDisplayMapping.get(loyaltyProgramID);
    filters.put("loyaltyProgram.display", (loyaltyProgramDisplay != null)? loyaltyProgramDisplay : loyaltyProgramID);
    if(loyaltyProgramDisplay == null)
      {
        log.warn("Unable to retrieve loyaltyProgram.display for loyaltyProgram.id: "+ loyaltyProgramID);
      }
  }

  @Override
  protected List<String> getFilterFields()
  {
    return filterFields;
  }

  @Override
  protected List<CompositeValuesSourceBuilder<?>> getFilterComplexSources(String date)
  {
    //
    // LoyaltyProgram x New Tier x Previous Tier x Type ...
    //
    
    List<CompositeValuesSourceBuilder<?>> filterComplexSources = new ArrayList<CompositeValuesSourceBuilder<?>>();
    
    Long requestedDate;
    Long oneDayAfter;
    
    try
      {
        requestedDate  = DATE_FORMAT.parse(date).getTime();
        oneDayAfter = RLMDateUtils.addDays(DATE_FORMAT.parse(date), 1, Deployment.getBaseTimeZone()).getTime();
      } 
    catch (ParseException e)
      {
        log.error("Unable to build some part of the ES request due to date formatting error.");
        return filterComplexSources;
      }
    
    String dateBeginningIncluded = requestedDate.toString() + "L";
    String dateEndExcluded = oneDayAfter.toString() + "L";

    TermsValuesSourceBuilder loyaltyProgramTier = new TermsValuesSourceBuilder(allFilters)
        .script(new Script(ScriptType.INLINE, "painless", "def left = []; for (int i = 0; i < params._source['loyaltyPrograms'].length; i++) { if(params._source['loyaltyPrograms'][i]['tierUpdateDate']?.toString() != null && params._source['loyaltyPrograms'][i]['tierUpdateDate'] >= "+dateBeginningIncluded+" && params._source['loyaltyPrograms'][i]['tierUpdateDate'] < "+dateEndExcluded+"){ def filter = [0,0,0,0]; filter[0] = params._source['loyaltyPrograms'][i]['programID']; filter[1] = params._source['loyaltyPrograms'][i]['tierName']?.toString(); filter[2] = params._source['loyaltyPrograms'][i]['previousTierName']?.toString(); filter[3] = params._source['loyaltyPrograms'][i]['tierChangeType']?.toString(); left.add(filter); } } return left;", Collections.emptyMap()));
    filterComplexSources.add(loyaltyProgramTier);
    
    return filterComplexSources;
  }

  @Override
  protected List<AggregationBuilder> getDataAggregations(String date)
  {
    return new ArrayList<AggregationBuilder>();
  }

  @Override
  protected Map<String, Object> extractData(ParsedBucket compositeBucket, Map<String, Object> contextFilters) throws ClassCastException
  {
    return new HashMap<String,Object>();
  }

  @Override
  protected String getDataESIndex(String date)
  {
    return this.dataESIndexPrefix;
  }

  @Override
  protected String getDatacubeESIndex()
  {
    return this.datacubeESIndex;
  }
}
