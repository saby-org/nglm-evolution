package com.evolving.nglm.evolution.datacubes.mapping;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ESObjectList<T>
{  
  /*****************************************
  *
  *  configuration
  *
  *****************************************/
  
  protected static final Logger log = LoggerFactory.getLogger(ESObjectList.class);
  protected static final int BUCKETS_MAX_NBR = 10000;  // TODO: factorize in ES client later (with some generic ES calls)
  
  /*****************************************
  *
  *  data
  *  The purpose of the missing map is to raise only one warning message by missing IDs.
  *
  *****************************************/
  
  protected final String mappingEsIndex;
  protected Map<String, T> mapping;             // (objectID,object)
  protected Set<String> warnings; // TODO: factorize with GUIManagerObjectList later 

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public ESObjectList(String mappingEsIndex) 
  {
    this.mappingEsIndex = mappingEsIndex;
    this.mapping = Collections.emptyMap();
    this.warnings = Collections.emptySet();
  }

  /*****************************************
  *
  *  abstract
  *
  *****************************************/
  
  protected abstract void updateMapping(Map<String, Object> row);
  
  /*****************************************
  *
  *  reset mapping and warnings
  *
  *****************************************/
  
  private void reset() 
  {
    this.mapping = new HashMap<String, T>();
    this.warnings = new HashSet<String>();
  }
  
  /*****************************************
  *
  *  logWarningOnlyOnce
  *
  *****************************************/

  protected void logWarningOnlyOnce(String msg)
  {
    if(!this.warnings.contains(msg))
      {
        this.warnings.add(msg);
        log.warn(msg);
      }
  }

  /*****************************************
  *
  *  updateFromElasticsearch
  *
  *****************************************/
  
  public void updateFromElasticsearch(RestHighLevelClient elasticsearch) throws ElasticsearchException, IOException, ClassCastException 
  {
    this.reset();
    
    SearchSourceBuilder request = new SearchSourceBuilder()
        .sort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
        .query(QueryBuilders.matchAllQuery())
        .size(BUCKETS_MAX_NBR);
    
    SearchResponse response = null;
    try 
      {
        response = elasticsearch.search(new SearchRequest(mappingEsIndex).source(request), RequestOptions.DEFAULT);
      } 
    catch(ElasticsearchException e)
      {
        if (e.status() == RestStatus.NOT_FOUND) {
          log.warn("Elasticsearch index {} does not exist.", mappingEsIndex);
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
        log.error("Elasticsearch index {} search response returned with bad status.", mappingEsIndex);
        return;
      }
    
    SearchHits hits = response.getHits();
    if(hits == null) { return; }
    
    for(SearchHit hit: hits) 
      {
        Map<String, Object> source = hit.getSourceAsMap();
        this.updateMapping(source);
      }
  }
}
