package com.evolving.nglm.evolution.datacubes.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite.ParsedBucket;
import org.elasticsearch.search.aggregations.bucket.range.ParsedDateRange;
import org.elasticsearch.search.aggregations.bucket.range.ParsedRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.evolution.Journey;
import com.evolving.nglm.evolution.JourneyMetricDeclaration;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.JourneyStatisticESSinkConnector;
import com.evolving.nglm.evolution.SegmentationDimensionService;
import com.evolving.nglm.evolution.datacubes.DatacubeManager;
import com.evolving.nglm.evolution.datacubes.DatacubeUtils;
import com.evolving.nglm.evolution.datacubes.DatacubeWriter;
import com.evolving.nglm.evolution.datacubes.SimpleDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.mapping.JourneysMap;
import com.evolving.nglm.evolution.datacubes.mapping.SegmentationDimensionsMap;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

public class JourneyTrafficDatacubeGenerator extends SimpleDatacubeGenerator
{
  private static final String DATACUBE_ES_INDEX_SUFFIX = "_datacube_journeytraffic-";
  public static final String DATACUBE_ES_INDEX_PREFIX(int tenantID) { return "t" + tenantID + DATACUBE_ES_INDEX_SUFFIX; }
  private static final String DATA_ES_INDEX_PREFIX = "journeystatistic-";
  private static final String FILTER_STRATUM_PREFIX = "subscriberStratum.";
  private static final String METRIC_CONVERSION_COUNT = "metricConversionCount";
  private static final String METRIC_CONVERTED_TODAY = "metricConvertedToday";
  private static final String METRIC_COUNT_RANGE = "POSITIVE_VALUES";
  private static final double METRIC_COUNT_FROM = 0.000001d; // Hacky: see range aggregation, because "from" is included. Here we want positive values only.

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private SegmentationDimensionsMap segmentationDimensionList;
  private JourneysMap journeysMap;
  private JourneyService journeyService;
  
  private String journeyID;
  private Date publishDate;
  private Date metricTargetDayStart;
  private Date metricTargetDayAfterStart;

  /*****************************************
  *
  * Constructors
  *
  *****************************************/
  public JourneyTrafficDatacubeGenerator(String datacubeName, ElasticsearchClientAPI elasticsearch, DatacubeWriter datacubeWriter, SegmentationDimensionService segmentationDimensionService, JourneyService journeyService, int tenantID, String timeZone)  
  {
    super(datacubeName, elasticsearch, datacubeWriter, tenantID, timeZone);

    this.segmentationDimensionList = new SegmentationDimensionsMap(segmentationDimensionService);
    this.journeysMap = new JourneysMap(journeyService);
    this.journeyService = journeyService;
  }
  
  public JourneyTrafficDatacubeGenerator(String datacubeName, int tenantID, DatacubeManager datacubeManager) {
    this(datacubeName,
        datacubeManager.getElasticsearchClientAPI(),
        datacubeManager.getDatacubeWriter(),
        datacubeManager.getSegmentationDimensionService(),
        datacubeManager.getJourneyService(),
        tenantID,
        Deployment.getDeployment(tenantID).getTimeZone());
  }

  /*****************************************
  *
  * Elasticsearch indices settings
  *
  *****************************************/
  @Override protected String getDataESIndex() 
  { 
    return DATA_ES_INDEX_PREFIX + JourneyStatisticESSinkConnector.journeyIDFormatterForESIndex(this.journeyID); 
  }

  @Override protected String getDatacubeESIndex() 
  { 
    return DATACUBE_ES_INDEX_PREFIX(this.tenantID) + DatacubeUtils.retrieveJourneyEndWeek(this.journeyID, this.journeyService);
  }

  /*****************************************
  *
  * Filters settings
  *
  *****************************************/
  @Override 
  protected List<String> getFilterFields() {
    //
    // Build filter fields
    //
    List<String> filterFields = new ArrayList<String>();
    filterFields.add("status");
    filterFields.add("nodeID");
    filterFields.add("sample");

    // getFilterFields is called after runPreGenerationPhase. It safe to assume segmentationDimensionList is up to date.
    // Regarding dimensions, in journeystatistic ES indices, there is already only the "statistics" ones (see JourneyStatisticESSinkConnector.java)
    // Nonetheless, to avoid having plenty of "null" dimensions in the datacube (and pollute the index settings) we also 
    // filter out "non statistics" dimensions here.
    for(String dimensionID: segmentationDimensionList.keySet()) {
      if (segmentationDimensionList.isFlaggedStatistics(dimensionID)) {
        filterFields.add(FILTER_STRATUM_PREFIX + dimensionID);
      }
    }
    
    return filterFields; 
  }
  
  @Override
  protected boolean runPreGenerationPhase() throws ElasticsearchException, IOException, ClassCastException
  {
    if(!isESIndexAvailable(getDataESIndex())) {
      log.info("Elasticsearch index [" + getDataESIndex() + "] does not exist.");
      return false;
    }
    
    this.segmentationDimensionList.update();
    this.journeysMap.update();
    
    //
    // Should we keep pushing datacube for this journey ?
    //
    Journey journey = this.journeysMap.get(this.journeyID);
    if(journey == null) {
      log.error("Error, unable to retrieve info for JourneyID=" + this.journeyID);
      return false;
    }
    // Number of day needed after EndDate for JourneyMetrics to be pushed. - Add 24 hours to be sure (due to truncation, see populateMetricsPost)
    int maximumPostPeriod = Deployment.getJourneyMetricConfiguration().getPostPeriodDays() + 1;
    Date stopDate = RLMDateUtils.addDays(journey.getEffectiveEndDate(), maximumPostPeriod, this.getTimeZone());
    if(publishDate.after(stopDate)) {
      log.info("JourneyID=" + this.journeyID + " has ended more than " + maximumPostPeriod + " days ago. No data will be published anymore.");
      return false;
    }
    
    return true;
  }
  
  @Override 
  protected void embellishFilters(Map<String, Object> filters) 
  {
    //
    // Journey (already in index name, added for Kibana/Grafana)
    //
    filters.put("journey", journeysMap.getDisplay(journeyID, "journey"));
    
    //
    // nodeID
    //
    String nodeID = (String) filters.remove("nodeID");
    filters.put("node", journeysMap.getNodeDisplay(journeyID, nodeID, "node"));
    
    //
    // Special dimension with all, for Grafana 
    //
    filters.put(FILTER_STRATUM_PREFIX + "Global", " ");
    
    //
    // subscriberStratum dimensions
    //
    for(String dimensionID: segmentationDimensionList.keySet()) {
      if (segmentationDimensionList.isFlaggedStatistics(dimensionID)) {
        String fieldName = FILTER_STRATUM_PREFIX + dimensionID;
        String segmentID = (String) filters.remove(fieldName);
        
        String newFieldName = FILTER_STRATUM_PREFIX + segmentationDimensionList.getDimensionDisplay(dimensionID, fieldName);
        filters.put(newFieldName, segmentationDimensionList.getSegmentDisplay(dimensionID, segmentID, fieldName));
      }
    }
  }
  
  /*****************************************
  *
  * Metrics settings
  *
  *****************************************/
  @Override
  protected List<AggregationBuilder> getMetricAggregations()
  {
    // Those aggregations need to be recomputed with the update of journeyTrafficList
    List<AggregationBuilder> metricAggregations = new ArrayList<AggregationBuilder>();
    
    metricAggregations.add(AggregationBuilders.sum(METRIC_CONVERSION_COUNT).field("conversionCount"));
    metricAggregations.add(AggregationBuilders.dateRange(METRIC_CONVERTED_TODAY).field("lastConversionDate")
        .addRange(this.printTimestamp(metricTargetDayStart), this.printTimestamp(metricTargetDayAfterStart)));

    Map<String, JourneyMetricDeclaration> journeyMetricsMap = Deployment.getJourneyMetricConfiguration().getMetrics();
    if(! journeyMetricsMap.entrySet().isEmpty()) {
      JourneyMetricDeclaration valJournMetric = journeyMetricsMap.entrySet().iterator().next().getValue();
      
      metricAggregations.add(AggregationBuilders.filter("subsWithPriorMetrics", QueryBuilders.existsQuery(valJournMetric.getESFieldPrior())));
      metricAggregations.add(AggregationBuilders.filter("subsWithDuringMetrics", QueryBuilders.existsQuery(valJournMetric.getESFieldDuring())));
      metricAggregations.add(AggregationBuilders.filter("subsWithPostMetrics", QueryBuilders.existsQuery(valJournMetric.getESFieldPost())));
      
      for(Entry<String, JourneyMetricDeclaration> entry : journeyMetricsMap.entrySet())   {
        metricAggregations.add(AggregationBuilders.sum(entry.getValue().getID() + "_" + entry.getValue().getESFieldPrior()).field(entry.getValue().getESFieldPrior()));
        metricAggregations.add(AggregationBuilders.sum(entry.getValue().getID() + "_" + entry.getValue().getESFieldDuring()).field(entry.getValue().getESFieldDuring()));
        metricAggregations.add(AggregationBuilders.sum(entry.getValue().getID() + "_" + entry.getValue().getESFieldPost()).field(entry.getValue().getESFieldPost()));
        
        if(entry.getValue().isCustomerCount()) {
          // Also add the 3 "count" metrics
          metricAggregations.add(AggregationBuilders.range(entry.getValue().getID() + "_" + entry.getValue().getESFieldPrior() + "_COUNT")
              .field(entry.getValue().getESFieldPrior()).addRange(new Range(METRIC_COUNT_RANGE, METRIC_COUNT_FROM, null)));
          metricAggregations.add(AggregationBuilders.range(entry.getValue().getID() + "_" + entry.getValue().getESFieldDuring() + "_COUNT")
              .field(entry.getValue().getESFieldDuring()).addRange(new Range(METRIC_COUNT_RANGE, METRIC_COUNT_FROM, null)));
          metricAggregations.add(AggregationBuilders.range(entry.getValue().getID() + "_" + entry.getValue().getESFieldPost() + "_COUNT")
              .field(entry.getValue().getESFieldPost()).addRange(new Range(METRIC_COUNT_RANGE, METRIC_COUNT_FROM, null)));
          
        }
      }
    }

    return metricAggregations;
  }
  
  @Override
  protected Map<String, Long> extractMetrics(ParsedBucket compositeBucket) throws ClassCastException
  {
    HashMap<String, Long> metrics = new HashMap<String, Long>();
    
    if (compositeBucket.getAggregations() == null) {
      log.error("Unable to extract metrics, aggregation is missing.");
      return metrics;
    }

    // ConversionCount
    ParsedSum conversionCountAggregation = compositeBucket.getAggregations().get(METRIC_CONVERSION_COUNT);
    if (conversionCountAggregation == null) {
      log.error("Unable to extract conversionCount in journeystatistics, aggregation is missing.");
      return metrics;
    } else {
      metrics.put("conversions", new Long((long) conversionCountAggregation.getValue()));
    }
    
    // ConvertedToday
    ParsedDateRange convertedTodayAggregation = compositeBucket.getAggregations().get(METRIC_CONVERTED_TODAY);
    if(convertedTodayAggregation == null || convertedTodayAggregation.getBuckets() == null) {
      log.error("Date Range buckets are missing in search response, unable to retrieve convertedToday.");
      return metrics;
    }
    
    for(org.elasticsearch.search.aggregations.bucket.range.Range.Bucket bucket: convertedTodayAggregation.getBuckets()) {
      // WARNING: we should only enter this loop once because there is only one bucket defined !
      metrics.put("converted.today", new Long(bucket.getDocCount()));
    }

    Map<String, JourneyMetricDeclaration> journeyMetricsMap = Deployment.getJourneyMetricConfiguration().getMetrics();
    if(! journeyMetricsMap.entrySet().isEmpty()) {
      ParsedFilter countAggregation = compositeBucket.getAggregations().get("subsWithPriorMetrics");
      if (countAggregation != null ) {
        metrics.put("custom.subsWithPriorMetrics", countAggregation.getDocCount());
      }
  
      countAggregation = compositeBucket.getAggregations().get("subsWithDuringMetrics");
      if (countAggregation != null ) {
        metrics.put("custom.subsWithDuringMetrics", countAggregation.getDocCount());
      }
  
      countAggregation = compositeBucket.getAggregations().get("subsWithPostMetrics");
      if (countAggregation != null ) {
        metrics.put("custom.subsWithPostMetrics", countAggregation.getDocCount());
      }
  
      for(Entry<String, JourneyMetricDeclaration> entry : journeyMetricsMap.entrySet()) {
        ParsedSum priorAggregation = compositeBucket.getAggregations().get(entry.getValue().getID() + "_" + entry.getValue().getESFieldPrior());
        ParsedSum duringAggregation = compositeBucket.getAggregations().get(entry.getValue().getID() + "_" + entry.getValue().getESFieldDuring());
        ParsedSum postAggregation = compositeBucket.getAggregations().get(entry.getValue().getID() + "_" + entry.getValue().getESFieldPost());
  
        if (priorAggregation != null ) {
          metrics.put("custom." + entry.getValue().getESFieldPrior(), (long) priorAggregation.getValue());
        }
  
        if (duringAggregation != null) {
          metrics.put("custom." + entry.getValue().getESFieldDuring(), (long) duringAggregation.getValue());
        }
  
        if(postAggregation != null) {
          metrics.put("custom." + entry.getValue().getESFieldPost(), (long) postAggregation.getValue());
        }
        

        if(entry.getValue().isCustomerCount()) {
          // Also extract the 3 "count" metrics
          ParsedRange priorAggregationCount = compositeBucket.getAggregations().get(entry.getValue().getID() + "_" + entry.getValue().getESFieldPrior() + "_COUNT");
          ParsedRange duringAggregationCount = compositeBucket.getAggregations().get(entry.getValue().getID() + "_" + entry.getValue().getESFieldDuring() + "_COUNT");
          ParsedRange postAggregationCount = compositeBucket.getAggregations().get(entry.getValue().getID() + "_" + entry.getValue().getESFieldPost( )+ "_COUNT");
          
          if (priorAggregationCount != null ) {
            // This list should only contain ONE bucket (the METRIC_COUNT_RANGE one)
            for(org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation.Bucket bucket: priorAggregationCount.getBuckets()) {
              if(bucket.getKeyAsString().equals(METRIC_COUNT_RANGE)) {
                metrics.put("custom.customerCount." + entry.getValue().getESFieldPrior(), bucket.getDocCount());
                break;
              }
            }
          }
          
          if (duringAggregationCount != null ) {
            // This list should only contain ONE bucket (the METRIC_COUNT_RANGE one)
            for(org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation.Bucket bucket: duringAggregationCount.getBuckets()) {
              if(bucket.getKeyAsString().equals(METRIC_COUNT_RANGE)) {
                metrics.put("custom.customerCount." + entry.getValue().getESFieldDuring(), bucket.getDocCount());
                break;
              }
            }
          }
          
          if (postAggregationCount != null ) {
            // This list should only contain ONE bucket (the METRIC_COUNT_RANGE one)
            for(org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation.Bucket bucket: postAggregationCount.getBuckets()) {
              if(bucket.getKeyAsString().equals(METRIC_COUNT_RANGE)) {
                metrics.put("custom.customerCount." + entry.getValue().getESFieldPost(), bucket.getDocCount());
                break;
              }
            }
          }
          
        }
      }
    }
        
    return metrics;
  }

  /*****************************************
  *
  * Run
  *
  *****************************************/
  public void definitive(String journeyID, long journeyStartDateTime, Date publishDate)
  {
    this.journeyID = journeyID;
    this.publishDate = publishDate;

    Date tomorrow = RLMDateUtils.addDays(publishDate, 1, this.getTimeZone());
    // Dates: yyyy-MM-dd 00:00:00.000
    Date beginningOfToday = RLMDateUtils.truncate(publishDate, Calendar.DATE, this.getTimeZone());
    Date beginningOfTomorrow = RLMDateUtils.truncate(tomorrow, Calendar.DATE, this.getTimeZone());
    this.metricTargetDayStart = beginningOfToday;
    this.metricTargetDayAfterStart = beginningOfTomorrow;
    
    String timestamp = this.printTimestamp(publishDate);
    long targetPeriod = publishDate.getTime() - journeyStartDateTime;
    this.run(timestamp, targetPeriod);
  }
}
