package com.evolving.nglm.evolution.datacubes.mapping;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This map is special and will not be retrieve from GUIService or from a mapping index.
 * It will be retrieve directly from journeystatistic index by inspecting the `rewards` field of every documents.
 */
public class JourneyRewardsMap
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/
  
  private static final Logger log = LoggerFactory.getLogger(JourneyRewardsMap.class);
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private List<String> rewardIDs;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public JourneyRewardsMap() 
  {
    this.rewardIDs = Collections.emptyList();
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/
  
  public List<String> getRewardIDs() { return this.rewardIDs; }
  
  /*****************************************
  *
  *  updateFromElasticsearch
  *
  *****************************************/
  
  public void updateFromElasticsearch(RestHighLevelClient elasticsearch, String journeyStatisticESindex) throws ElasticsearchException, IOException, ClassCastException 
  {
    // reset list
    this.rewardIDs = new LinkedList<String>();
    
    String termsAggregationName = "ALL_REWARDS";
    SearchSourceBuilder request = new SearchSourceBuilder()
        .query(QueryBuilders.matchAllQuery())
        .aggregation(AggregationBuilders.terms(termsAggregationName).script(new Script(ScriptType.INLINE, "painless", "return params._source.rewards.keySet();", Collections.emptyMap())))
        .size(0);
    
    SearchResponse response = null;
    try 
      {
        response = elasticsearch.search(new SearchRequest(journeyStatisticESindex).source(request), RequestOptions.DEFAULT);
      } 
    catch(ElasticsearchException e)
      {
        if (e.status() == RestStatus.NOT_FOUND) {
          log.warn("Elasticsearch index {} does not exist.", journeyStatisticESindex);
          return;
        } else {
          throw e;
        }
      }
    
    if(response.isTimedOut()
        || response.getFailedShards() > 0
        || response.getSkippedShards() > 0
        || response.status() != RestStatus.OK) 
      {
        log.error("Elasticsearch index {} search response returned with bad status.", journeyStatisticESindex);
        return;
      }

    if(response.getAggregations() == null) {
      log.error("Main aggregation is missing in JourneyRewardsMap search response.");
      return;
    }
    
    ParsedTerms termsBuckets = response.getAggregations().get(termsAggregationName);
    if(termsBuckets == null) {
      log.error("Terms buckets are missing in JourneyRewardsMap search response.");
      return;
    }
    
    for(Bucket bucket: termsBuckets.getBuckets()) {
      if (bucket instanceof ParsedStringTerms.ParsedBucket) {
        this.rewardIDs.add(((ParsedStringTerms.ParsedBucket) bucket).getKeyAsString());
      } else {
        log.warn("Unsupported bucket in JourneyRewardsMap, discarding this rewardID.");
      }
    }
  }
}
