package com.evolving.nglm.evolution.datacubes.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedNested;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedReverseNested;
import org.elasticsearch.search.aggregations.bucket.range.ParsedRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.SegmentationDimension;
import com.evolving.nglm.evolution.SegmentationDimensionService;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.DatacubeManager;
import com.evolving.nglm.evolution.datacubes.DatacubeWriter;
import com.evolving.nglm.evolution.datacubes.mapping.LoyaltyProgramsMap;
import com.evolving.nglm.evolution.datacubes.mapping.SegmentationDimensionsMap;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

public class ProgramsChangesDatacubeGenerator extends DatacubeGenerator
{
  private static final String DATACUBE_ES_INDEX_SUFFIX = "_datacube_loyaltyprogramschanges";
  public static final String DATACUBE_ES_INDEX(int tenantID) { return "t" + tenantID + DATACUBE_ES_INDEX_SUFFIX; }
  private static final String DATA_ES_INDEX = "subscriberprofile";
  private static final String DATA_FILTER_STRATUM_PREFIX = "stratum."; // from subscriberprofile index
  private static final String DATACUBE_FILTER_STRATUM_PREFIX = "stratum."; // pushed in datacube index - same as SubscriberProfileDatacube

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private LoyaltyProgramsMap loyaltyProgramsMap;
  private SegmentationDimensionsMap segmentationDimensionList;

  private long targetPeriod;
  private long targetPeriodStartIncluded;
  private String targetDay;

  /*****************************************
  *
  * Constructors
  *
  *****************************************/
  public ProgramsChangesDatacubeGenerator(String datacubeName, ElasticsearchClientAPI elasticsearch, DatacubeWriter datacubeWriter, LoyaltyProgramService loyaltyProgramService, int tenantID, String timeZone, SegmentationDimensionService segmentationDimensionService)
  {
    super(datacubeName, elasticsearch, datacubeWriter, tenantID, timeZone);
    
    this.loyaltyProgramsMap = new LoyaltyProgramsMap(loyaltyProgramService);
    this.segmentationDimensionList = new SegmentationDimensionsMap(segmentationDimensionService);
  }
  
  public ProgramsChangesDatacubeGenerator(String datacubeName, int tenantID, DatacubeManager datacubeManager) {
    this(datacubeName,
        datacubeManager.getElasticsearchClientAPI(),
        datacubeManager.getDatacubeWriter(),
        datacubeManager.getLoyaltyProgramService(),
        tenantID,
        Deployment.getDeployment(tenantID).getTimeZone(),
        datacubeManager.getSegmentationDimensionService());
  }

  /*****************************************
  *
  * Elasticsearch indices settings
  *
  *****************************************/
  @Override protected String getDataESIndex() { return DATA_ES_INDEX; }
  @Override protected String getDatacubeESIndex() { return DATACUBE_ES_INDEX(this.tenantID); }

  /*****************************************
  *
  * Datacube generation phases
  *
  *****************************************/
  @Override
  protected boolean runPreGenerationPhase() throws ElasticsearchException, IOException, ClassCastException
  {
    loyaltyProgramsMap.update();
    this.segmentationDimensionList.update();
    return true;
  }

  @Override
  protected SearchRequest getElasticsearchRequest()
  {
    //
    // Target index
    //
    String ESIndex = getDataESIndex();
    
    //
    // Filter query
    //
    // Hack: When a newly created subscriber in Elasticsearch comes first by ExtendedSubscriberProfile sink connector,
    // it has not yet any of the "product" main (& mandatory) fields.
    // Those comes when the SubscriberProfile sink connector push them.
    // For a while, it is possible a document in subscriberprofile index miss many product fields required by datacube generation.
    // Therefore, we filter out those subscribers with missing data by looking for lastUpdateDate
    BoolQueryBuilder query = QueryBuilders.boolQuery();
    query.filter().add(QueryBuilders.existsQuery("lastUpdateDate"));
    query.filter().add(QueryBuilders.termQuery("tenantID", this.tenantID)); // filter to keep only tenant related items !
    
    //
    // Aggregations
    //
    List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
    sources.add(new TermsValuesSourceBuilder("loyaltyProgramID").field("loyaltyPrograms.programID"));
    sources.add(new TermsValuesSourceBuilder("newTier").field("loyaltyPrograms.tierName").missingBucket(true)); // Missing means opt-out. We need to catch them !
    sources.add(new TermsValuesSourceBuilder("previousTier").field("loyaltyPrograms.previousTierName").missingBucket(true)); // Missing means opt-in. We need to catch them !
    sources.add(new TermsValuesSourceBuilder("tierChangeType").field("loyaltyPrograms.tierChangeType"));
    
    //
    // Sub Aggregation DATE - It is a filter (only one bucket), but for a field of the nested object.
    //
    
    RangeAggregationBuilder dateAgg = AggregationBuilders.range("DATE")
        .field("loyaltyPrograms.tierUpdateDate")
        .addRange(targetPeriodStartIncluded, targetPeriodStartIncluded + targetPeriod); // Reminder: from is included, to is excluded
    
    //
    // Sub Aggregations dimensions (only statistic dimensions)
    //
    TermsAggregationBuilder rootStratumBuilder = null; // first aggregation
    TermsAggregationBuilder termStratumBuilder = null; // last aggregation
    
    for (String dimensionID : segmentationDimensionList.keySet()) {
      GUIManagedObject segmentationObject = segmentationDimensionList.get(dimensionID);
      if (segmentationObject != null && segmentationObject instanceof SegmentationDimension && ((SegmentationDimension) segmentationObject).getStatistics()) {
        if (termStratumBuilder != null) {
          TermsAggregationBuilder temp = AggregationBuilders.terms(DATA_FILTER_STRATUM_PREFIX + dimensionID)
              .field(DATA_FILTER_STRATUM_PREFIX + dimensionID).missing("undefined");
          termStratumBuilder = termStratumBuilder.subAggregation(temp);
          termStratumBuilder = temp;
        }
        else {
          termStratumBuilder = AggregationBuilders.terms(DATA_FILTER_STRATUM_PREFIX + dimensionID)
              .field(DATA_FILTER_STRATUM_PREFIX + dimensionID).missing("undefined");
          rootStratumBuilder = termStratumBuilder;
        }
      }
    }
    
    //
    // Final
    //
    AggregationBuilder aggregation = AggregationBuilders.nested("DATACUBE", "loyaltyPrograms").subAggregation(
        AggregationBuilders.composite("LOYALTY-COMPOSITE", sources).size(ElasticsearchClientAPI.MAX_BUCKETS).subAggregation(
            dateAgg.subAggregation(
              AggregationBuilders.reverseNested("REVERSE").subAggregation(
                  rootStratumBuilder))));
    
    //
    // Datacube request
    //
    SearchSourceBuilder datacubeRequest = new SearchSourceBuilder()
        .sort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
        .query(query)
        .aggregation(aggregation)
        .size(0);
    
    return new SearchRequest(ESIndex).source(datacubeRequest);
  }

  @Override
  protected void embellishFilters(Map<String, Object> filters)
  {
    String loyaltyProgramID = (String) filters.remove("loyaltyProgramID");
    filters.put("loyaltyProgram", loyaltyProgramsMap.getDisplay(loyaltyProgramID, "loyaltyProgram"));
    
    //
    // Dimensions
    //
    for (String dimensionID : segmentationDimensionList.keySet()) {
      GUIManagedObject segmentationObject = segmentationDimensionList.get(dimensionID);
      if (segmentationObject != null && segmentationObject instanceof SegmentationDimension && ((SegmentationDimension) segmentationObject).getStatistics()) {
        String segmentID = (String) filters.remove(DATA_FILTER_STRATUM_PREFIX + dimensionID);
        String dimensionDisplay = segmentationDimensionList.getDimensionDisplay(dimensionID, DATA_FILTER_STRATUM_PREFIX + dimensionID);
        String fieldName = DATACUBE_FILTER_STRATUM_PREFIX + dimensionDisplay;
        filters.put(fieldName, segmentationDimensionList.getSegmentDisplay(dimensionID, segmentID, fieldName));
      }
    }
    
    // "newTier" stay the same (None is managed in extractDatacubeRows)
    // "previousTier" stay the same (None is managed in extractDatacubeRows)
    // "tierChangeType" stay the same
  }
  
  /**
   * Auxiliary recursive function for row extraction
   * This function will explore the combination tree built from buckets.
   * Each leaf of the tree represent a final combination. 
   * The doc count of the combination will be retrieve in their leaf.
   * 
   * This recursive function will return every combination created from this 
   * node as it was the root of the tree.
   * 
   * @return List[ Combination(Dimension, Segment) -> Count ]
   */
  private List<Pair<Map<String, String>, Long>> extractSegmentationStratum(ParsedTerms parsedTerms)
  {
    if (parsedTerms == null || parsedTerms.getBuckets() == null) {
      log.error("stratum buckets are missing in search response.");
      return Collections.emptyList();
    }
    
    List<Pair<Map<String, String>, Long>> result = new LinkedList<Pair<Map<String, String>, Long>>();
    
    String dimensionID = parsedTerms.getName();
    for (Terms.Bucket stratumBucket : parsedTerms.getBuckets()) { // Explore each segment for this dimension.
      String segmentID = stratumBucket.getKeyAsString();
      
      Map<String, Aggregation> stratumBucketAggregation = stratumBucket.getAggregations().getAsMap();
      if (stratumBucketAggregation == null || stratumBucketAggregation.isEmpty())  {
        //
        // Leaf - extract count
        //
        long count = stratumBucket.getDocCount();
        Map<String, String> combination = new HashMap<String,String>();
        combination.put(dimensionID, segmentID);                             // Add new dimension
        result.add(new Pair<Map<String, String>, Long>(combination, count)); // Add this combination to the result
      }
      else {
        //
        // Node - recursive call
        // 
        for(Aggregation subAggregation :  stratumBucketAggregation.values()) {
          List<Pair<Map<String, String>, Long>> childResults = extractSegmentationStratum((ParsedTerms) subAggregation);
  
          for (Pair<Map<String, String>, Long> stratum : childResults) {
            stratum.getFirstElement().put(dimensionID, segmentID);           // Add new dimension
            result.add(stratum);                                             // Add this combination to the result
          }
        }
      }
    }
    
    return result;
  }
  
  @Override
  protected List<Map<String, Object>> extractDatacubeRows(SearchResponse response, String timestamp, long period) throws ClassCastException
  {
    List<Map<String,Object>> result = new ArrayList<Map<String,Object>>();
    
    if (response.isTimedOut()
        || response.getFailedShards() > 0
        || response.status() != RestStatus.OK) {
      log.error("Elasticsearch search response return with bad status.");
      log.error(response.toString());
      return result;
    }
    
    if(response.getAggregations() == null) {
      log.error("Main aggregation is missing in search response.");
      return result;
    }
    
    ParsedNested parsedNested = response.getAggregations().get("DATACUBE");
    if(parsedNested == null || parsedNested.getAggregations() == null) {
      log.error("Nested aggregation is missing in search response.");
      return result;
    }
    
    ParsedComposite parsedComposite = parsedNested.getAggregations().get("LOYALTY-COMPOSITE");
    if(parsedComposite == null || parsedComposite.getBuckets() == null) {
      log.error("Composite buckets are missing in search response.");
      return result;
    }
   
    for(ParsedComposite.ParsedBucket bucket: parsedComposite.getBuckets()) {
      //
      // Extract the filter
      //
      Map<String, Object> filters = bucket.getKey();
      for(String key: filters.keySet()) {
        if(filters.get(key) == null) {
          filters.replace(key, "None"); // for newTier & previousTier
        }
      }
      
      // Special filter: tenantID 
      filters.put("tenantID", this.tenantID);

      //
      // Extract only the change of the day
      //
      if(bucket.getAggregations() == null) {
        log.error("Aggregations in bucket is missing in search response.");
        continue;
      }
      
      ParsedRange parsedRange = bucket.getAggregations().get("DATE");
      if(parsedRange == null || parsedRange.getBuckets() == null) {
        log.error("Composite buckets are missing in search response.");
        continue;
      }

      // There must be only one bucket ! - It is a filter
      for(org.elasticsearch.search.aggregations.bucket.range.Range.Bucket dateBucket: parsedRange.getBuckets()) {
        if(dateBucket.getAggregations() == null) {
          log.error("Aggregations in bucket is missing in search response.");
          continue;
        }
        
        ParsedReverseNested parsedReverseNested = dateBucket.getAggregations().get("REVERSE");
        if(parsedReverseNested == null || parsedReverseNested.getAggregations() == null) {
          log.error("Reverse nested aggregation is missing in search response.");
          continue;
        }
        
        for(Aggregation parsedTerms : parsedReverseNested.getAggregations()) {
          List<Pair<Map<String, String>, Long>> childResults = extractSegmentationStratum((ParsedTerms) parsedTerms);
          
          for (Pair<Map<String, String>, Long> stratum : childResults) {
            Map<String, Object> filtersCopy = new HashMap<String, Object>(filters);
            Long docCount = stratum.getSecondElement();
            
            for (String dimensionID : stratum.getFirstElement().keySet()) {
              filtersCopy.put(dimensionID, stratum.getFirstElement().get(dimensionID));
            }
            
            //
            // Build row
            //
            Map<String, Object> row = extractRow(filtersCopy, docCount, timestamp, period, Collections.emptyMap());
            result.add(row);
          }
        }
      }
    }
    
    return result;
  }
  
  /*****************************************
  *
  * DocumentID settings
  *
  *****************************************/
  /**
   * In order to override preview documents, we use the following trick: the timestamp used in the document ID must be 
   * the timestamp of the definitive push (and not the time we publish it).
   * This way, preview documents will override each other till be overriden by the definitive one running the day after.
   * 
   * Be careful, it only works if we ensure to publish the definitive one. 
   * Already existing combination of filters must be published even if there is 0 count inside, in order to 
   * override potential previews.
   */
  @Override
  protected String getDocumentID(Map<String,Object> filters, String timestamp) {
    return this.extractDocumentIDFromFilter(filters, this.targetDay, "default");
  }
  
  /*****************************************
  *
  * Run
  *
  *****************************************/
  /**
   * The definitive datacube is generated on yesterday, for a period of 1 day (~24 hours except for some special days)
   * Rows will be timestamped at yesterday_23:59:59.999+ZZZZ
   * Timestamp is the last millisecond of the period (therefore included).
   * This way it shows that *count* is computed for this day (yesterday) but at the very end of the day.
   */
  public void definitive()
  {
    Date now = SystemTime.getCurrentTime();
    Date yesterday = RLMDateUtils.addDays(now, -1, this.getTimeZone()); 
    Date beginningOfYesterday = RLMDateUtils.truncate(yesterday, Calendar.DATE, this.getTimeZone());
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, this.getTimeZone());        // 00:00:00.000
    Date endOfYesterday = RLMDateUtils.addMilliseconds(beginningOfToday, -1);                     // 23:59:59.999
    this.targetPeriod = beginningOfToday.getTime() - beginningOfYesterday.getTime();    // most of the time 86400000ms (24 hours)
    this.targetPeriodStartIncluded = beginningOfYesterday.getTime();

    this.targetDay = this.printDay(yesterday);

    //
    // Timestamp & period
    //
    String timestamp = this.printTimestamp(endOfYesterday);
    
    this.run(timestamp, targetPeriod);
  }
  
  /**
   * A preview is a datacube generation on the today's day. 
   * Timestamp is the last millisecond of the period (therefore included).
   * Because the day is still not ended, it won't be the definitive value of *count*.
   */
  public void preview()
  {
    Date now = SystemTime.getCurrentTime();
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, this.getTimeZone()); 
    this.targetPeriod = now.getTime() - beginningOfToday.getTime() + 1; // +1 !
    this.targetPeriodStartIncluded = beginningOfToday.getTime();

    this.targetDay = this.printDay(now);

    //
    // Timestamp & period
    //
    String timestamp = this.printTimestamp(now);
    
    this.run(timestamp, targetPeriod);
  }
}
