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
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
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

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.JourneyStatisticESSinkConnector;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.DatacubeWriter;
import com.evolving.nglm.evolution.datacubes.generator.JourneyRewardsDatacubeGenerator;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

/**
 * This map is special and will not be retrieve from GUIService or from a mapping index.
 * It will be retrieve directly from journeystatistic index by inspecting the `rewards` fields from the mapping API.
 * Then, it will push the result in Elasticsearch in mapping_journeyrewards index for Grafana.
 */
public class JourneyRewardsMap
{
  private static final Logger log = LoggerFactory.getLogger(JourneyRewardsMap.class);

  private static final String FROM_INDEX_DEFAULT = "journeystatistic";
  private static final String MAPPING_INDEX = "mapping_journeyrewards";
  
  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private JourneyService journeyService;
  private ElasticsearchClientAPI elasticsearch;
  private Map<String, List<String>> rewardsMap;  // Map<JourneyID, List<RewardName>>

  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public JourneyRewardsMap(JourneyService journeyService, ElasticsearchClientAPI elasticsearch) 
  {
    this.journeyService = journeyService;
    this.elasticsearch = elasticsearch;
    this.rewardsMap = Collections.emptyMap();
  }

  /*****************************************
  *
  * Getters
  *
  *****************************************/
  public List<String> getRewards(String journeyID) 
  { 
    List<String> result = this.rewardsMap.get(journeyID);
    return (result != null)? result: Collections.emptyList(); 
  }
  
  /*****************************************
  *
  * Update
  *
  *****************************************/
  private void updateFromElasticsearch() throws ElasticsearchException, IOException, ClassCastException 
  {
    // reset list
    this.rewardsMap = new HashMap<String, List<String>>();
    
    //
    // Init search map(index, JourneyID)
    //
    Map<String, String> reverseJourneyIDMap = new HashMap<String, String>();
    for(GUIManagedObject object : this.journeyService.getStoredJourneys(true, 0)) { // @rl Also retrieve archived? - not sure if necessary TODO EVPRO-99 check
      String indexName = JourneyStatisticESSinkConnector.getJourneyStatisticIndex(object.getGUIManagedObjectID(), FROM_INDEX_DEFAULT); 
      reverseJourneyIDMap.put(indexName, object.getGUIManagedObjectID());
    }
    
    //
    // Create GET _mapping request
    //
    GetMappingsRequest request = new GetMappingsRequest();
    request.indices("journeystatistic-*");
    
    GetMappingsResponse getMappingResponse = elasticsearch.syncMappingWithRetry(request, RequestOptions.DEFAULT);

    for(String journeystatisticIndex: getMappingResponse.mappings().keySet()) {
      //
      // Extract rewards
      //
      MappingMetadata metadata = getMappingResponse.mappings().get(journeystatisticIndex);
      if(metadata == null) {
        continue;
      }
      
      Map<String, Object> sources = metadata.sourceAsMap();
      if(sources == null) {
        continue;
      }
      
      Map<String, Object> sourcesProperties = (Map<String, Object>) sources.get("properties");
      if(sourcesProperties == null) {
        continue;
      }

      Map<String, Object> sourcesRewards = (Map<String, Object>) sourcesProperties.get("rewards");
      if(sourcesRewards == null) {
        continue;
      }

      Map<String, Object> sourcesRewardsProperties = (Map<String, Object>) sourcesRewards.get("properties");
      if(sourcesRewardsProperties == null) {
        continue;
      }
      
      List<String> rewards = new LinkedList<String>();
      for(Object rewardName: sourcesRewardsProperties.keySet()) {
        rewards.add(rewardName.toString());
      }

      //
      // Extract JourneyID
      //
      String journeyID = reverseJourneyIDMap.get(journeystatisticIndex);
      if(journeyID != null) {
        rewardsMap.put(journeyID, rewards);
      }
    }
  }
  
  private void pushInMapping(DatacubeWriter datacubeWriter) throws ElasticsearchException, IOException {
    String timestamp = RLMDateUtils.formatDateForElasticsearchDefault(SystemTime.getCurrentTime());
    
    List<UpdateRequest> list = new LinkedList<UpdateRequest>();
    for(String journeyID : this.rewardsMap.keySet()) {
      for(String reward : this.rewardsMap.get(journeyID)) {
        Map<String,Object> mappingRow = new HashMap<String,Object>();
        mappingRow.put("timestamp", timestamp);
        mappingRow.put("journeyID", journeyID);
        mappingRow.put("reward", reward);
        
        String documentID = journeyID + '-' + reward; // We need to use update with docID in order to avoid pushing several time the same mapping !
  
        UpdateRequest request = new UpdateRequest(MAPPING_INDEX, documentID);
        request.doc(mappingRow);
        request.docAsUpsert(true);
        request.retryOnConflict(4);
  
        list.add(request);
      }
    }
    
    datacubeWriter.getDatacubeBulkProcessor().add(list);
  }
  
  public void updateAndPush(DatacubeWriter datacubeWriter) throws ElasticsearchException, IOException {
    this.updateFromElasticsearch();
    this.pushInMapping(datacubeWriter);
  }
}
