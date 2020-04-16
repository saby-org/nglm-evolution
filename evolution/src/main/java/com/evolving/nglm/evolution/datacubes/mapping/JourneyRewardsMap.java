package com.evolving.nglm.evolution.datacubes.mapping;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
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

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;

/**
 * This map is special and will not be retrieve from GUIService or from a mapping index.
 * It will be retrieve directly from journeystatistic index by inspecting the `rewards` field of every documents.
 * Then, it will push the result in Elasticsearch in mapping_journeyrewards index for Grafana.
 */
public class JourneyRewardsMap
{
  private static final Logger log = LoggerFactory.getLogger(JourneyRewardsMap.class);

  private static final String MAPPING_INDEX = "mapping_journeyrewards";
  
  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private RestHighLevelClient elasticsearch;
  private List<String> rewards;

  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public JourneyRewardsMap(RestHighLevelClient elasticsearch) 
  {
    this.elasticsearch = elasticsearch;
    this.rewards = Collections.emptyList();
  }

  /*****************************************
  *
  * Getters
  *
  *****************************************/
  public List<String> getRewards() { return this.rewards; }
  
  /*****************************************
  *
  * Update
  *
  *****************************************/
  private void updateFromElasticsearch(String journeyStatisticESindex) throws ElasticsearchException, IOException, ClassCastException 
  {
    // reset list
    this.rewards = new LinkedList<String>();
    
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
        this.rewards.add(((ParsedStringTerms.ParsedBucket) bucket).getKeyAsString());
      } else {
        log.warn("Unsupported bucket in JourneyRewardsMap, discarding this rewardID.");
      }
    }
  }
  
  private void pushInMapping(String journeyID) throws ElasticsearchException, IOException {
    String timestamp = DatacubeGenerator.TIMESTAMP_FORMAT.format(SystemTime.getCurrentTime());      // @rl: TODO timestamp in more generic class ? Elasticsearch client ?
    for(String reward: this.rewards) {
      Map<String,Object> mappingRow = new HashMap<String,Object>();
      mappingRow.put("timestamp", timestamp);
      mappingRow.put("journeyID", journeyID);
      mappingRow.put("reward", reward);
      
      String documentID = journeyID + '-' + reward; // We need to use update with docID in order to avoid pushing several time the same mapping !

      UpdateRequest request = new UpdateRequest(MAPPING_INDEX, documentID);
      request.doc(mappingRow);
      request.docAsUpsert(true);
      request.retryOnConflict(4);
      
      this.elasticsearch.update(request, RequestOptions.DEFAULT);
    }
  }
  
  public void update(String journeyID, String journeyStatisticESindex) throws ElasticsearchException, IOException {
    this.updateFromElasticsearch(journeyStatisticESindex);
    this.pushInMapping(journeyID);
  }
}
