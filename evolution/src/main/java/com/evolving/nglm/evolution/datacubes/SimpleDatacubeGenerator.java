package com.evolving.nglm.evolution.datacubes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite.ParsedBucket;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

/**
 * Simple datacube generator where the structure of the ES request is basically:
 * - a list of term aggregations inside a composite aggregations that represent all filters 
 * - a list of sub-aggregations for all metrics
 * 
 * Filters are then, simply retrieve with the key of the bucket.
 * Metrics are directly available inside the bucket.
 * Every bucket represent a row
 */
public abstract class SimpleDatacubeGenerator extends DatacubeGenerator
{
  /*****************************************
  *
  * Properties
  *
  *****************************************/
  protected String compositeAggregationName = "DATACUBE";

  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public SimpleDatacubeGenerator(String datacubeName, ElasticsearchClientAPI elasticsearch, DatacubeWriter datacubeWriter) 
  {
    super(datacubeName, elasticsearch, datacubeWriter);
  }
  
  /*****************************************
  *
  *  abstract functions
  *
  *****************************************/
  //
  // Filters settings
  //
  protected abstract List<String> getFilterFields();

  //
  // Metrics settings
  //
  protected abstract List<AggregationBuilder> getMetricAggregations();
  protected abstract Map<String, Object> extractMetrics(ParsedBucket compositeBucket) throws ClassCastException;
  
  /*****************************************
  *
  *  overridable functions
  *
  *****************************************/
  // Target every documents of the index by default, can be overriden.
  protected QueryBuilder getSubsetQuery() 
  {
    return QueryBuilders.matchAllQuery();
  }
  
  /*****************************************
  *
  *  getElasticsearchRequest
  *
  *****************************************/
  @Override
  protected SearchRequest getElasticsearchRequest() 
  {
    String ESIndex = getDataESIndex();
    
    List<String> datacubeFilterFields = getFilterFields();
    List<AggregationBuilder> datacubeMetricAggregations = getMetricAggregations();
    
    //
    // Composite sources are created from datacube filters
    //
    List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
    for(String datacubeFilter: datacubeFilterFields) {
      TermsValuesSourceBuilder sourceTerms = new TermsValuesSourceBuilder(datacubeFilter).field(datacubeFilter).missingBucket(true);
      sources.add(sourceTerms);
    }
    CompositeAggregationBuilder compositeAggregation = AggregationBuilders.composite(compositeAggregationName, sources).size(ElasticsearchClientAPI.MAX_BUCKETS);

    //
    // Metric aggregations
    //
    for(AggregationBuilder subaggregation : datacubeMetricAggregations) {
      compositeAggregation = compositeAggregation.subAggregation(subaggregation);
    }
    
    //
    // Datacube request
    //
    SearchSourceBuilder datacubeRequest = new SearchSourceBuilder()
        .sort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
        .query(getSubsetQuery())
        .aggregation(compositeAggregation)
        .size(0);
    
    return new SearchRequest(ESIndex).source(datacubeRequest);
  }

  /*****************************************
  *
  *  extractDatacubeRows
  *
  *****************************************/
  @Override
  protected List<Map<String,Object>> extractDatacubeRows(SearchResponse response, String timestamp, long period) throws ClassCastException 
  {
    List<Map<String,Object>> result = new ArrayList<Map<String,Object>>();
    
    if (response.isTimedOut()
        || response.getFailedShards() > 0
        || response.getSkippedShards() > 0
        || response.status() != RestStatus.OK) {
      log.error("Elasticsearch search response return with bad status.");
      return result;
    }
    
    if(response.getAggregations() == null) {
      log.error("Main aggregation is missing in search response.");
      return result;
    }
    
    ParsedComposite compositeBuckets = response.getAggregations().get(compositeAggregationName);
    if(compositeBuckets == null) {
      log.error("Composite buckets are missing in search response.");
      return result;
    }
    
    for(ParsedBucket bucket: compositeBuckets.getBuckets()) {
      long docCount = bucket.getDocCount();

      //
      // Extract filter, replace null
      //
      Map<String, Object> filters = bucket.getKey();
      for(String key: filters.keySet()) {
        if(filters.get(key) == null) {
          filters.replace(key, UNDEFINED_BUCKET_VALUE);
        }
      }
      
      //
      // Extract metrics
      //
      Map<String, Object> metrics = extractMetrics(bucket);
      
      //
      // Build row
      //
      Map<String, Object> row = extractRow(filters, docCount, timestamp, period, metrics);
      result.add(row);
    }
    
    
    return result;
  }
}
