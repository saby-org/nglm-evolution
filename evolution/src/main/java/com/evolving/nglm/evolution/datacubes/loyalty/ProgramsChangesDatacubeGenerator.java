package com.evolving.nglm.evolution.datacubes.loyalty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
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
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.mapping.LoyaltyProgramsMap;

public class ProgramsChangesDatacubeGenerator extends DatacubeGenerator
{
  private static final String DATACUBE_ES_INDEX = "datacube_loyaltyprogramschanges";
  private static final String DATA_ES_INDEX = "subscriberprofile";
  private static final String DATA_ES_INDEX_SNAPSHOT_PREFIX = "subscriberprofile_snapshot-";
  private static final String FILTER_ALL = "filters";
  private static final Pattern LOYALTY_TIERS_PATTERN = Pattern.compile("\\[(.*), (.*), (.*), (.*)\\]");

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private LoyaltyProgramsMap loyaltyProgramsMap;

  private boolean previewMode;
  private boolean snapshotsAvailable;
  private long targetPeriod;
  private long targetPeriodStartIncluded;
  private String targetDay;

  /*****************************************
  *
  * Constructors
  *
  *****************************************/
  public ProgramsChangesDatacubeGenerator(String datacubeName, RestHighLevelClient elasticsearch, LoyaltyProgramService loyaltyProgramService)
  {
    super(datacubeName, elasticsearch);
    this.loyaltyProgramsMap = new LoyaltyProgramsMap(loyaltyProgramService);
    this.snapshotsAvailable = true;
  }

  /*****************************************
  *
  * Elasticsearch indices settings
  *
  *****************************************/
  @Override protected String getDataESIndex() { 
    if (this.previewMode || !this.snapshotsAvailable) {
      return DATA_ES_INDEX;
    } else {
      return DATA_ES_INDEX_SNAPSHOT_PREFIX + targetDay; 
    }
  }
  
  @Override protected String getDatacubeESIndex() { return DATACUBE_ES_INDEX; }

  /*****************************************
  *
  * Filters settings
  *
  *****************************************/
  @Override protected List<String> getFilterFields() { return Collections.emptyList(); }
  
  @Override
  protected List<CompositeValuesSourceBuilder<?>> getFilterComplexSources()
  {
    //
    // LoyaltyProgram x New Tier x Previous Tier x Type ...
    //
    List<CompositeValuesSourceBuilder<?>> filterComplexSources = new ArrayList<CompositeValuesSourceBuilder<?>>();
    
    String dateBeginningIncluded = targetPeriodStartIncluded + "L";
    String dateEndExcluded = (targetPeriodStartIncluded+targetPeriod) + "L";

    TermsValuesSourceBuilder loyaltyProgramTier = new TermsValuesSourceBuilder(FILTER_ALL)
        .script(new Script(ScriptType.INLINE, "painless", "def left = [];"
            + " for (int i = 0; i < params._source['loyaltyPrograms'].length; i++) {"
              + " if(params._source['loyaltyPrograms'][i]['tierUpdateDate']?.toString() != null && params._source['loyaltyPrograms'][i]['tierUpdateDate'] >= "+dateBeginningIncluded+" && params._source['loyaltyPrograms'][i]['tierUpdateDate'] < "+dateEndExcluded+"){"
                + " def filter = [0,0,0,0];"
                + " filter[0] = params._source['loyaltyPrograms'][i]['programID'];"
                + " filter[1] = params._source['loyaltyPrograms'][i]['tierName']?.toString();"
                + " filter[2] = params._source['loyaltyPrograms'][i]['previousTierName']?.toString();"
                + " filter[3] = params._source['loyaltyPrograms'][i]['tierChangeType']?.toString();"
                + " left.add(filter);"
              + " }"
            + " }"
            + " return left;", Collections.emptyMap()));
    filterComplexSources.add(loyaltyProgramTier);
    
    return filterComplexSources;
  }
  
  @Override
  protected boolean runPreGenerationPhase() throws ElasticsearchException, IOException, ClassCastException
  {
    loyaltyProgramsMap.update();
    
    this.snapshotsAvailable = true;
    String initialDataIndex = getDataESIndex();
    this.snapshotsAvailable = isESIndexAvailable(initialDataIndex);
    if(!this.snapshotsAvailable) {
      log.warn("Elasticsearch index [" + initialDataIndex + "] does not exist. We will execute request on [" + getDataESIndex() + "] instead.");
    }
    
    return true;
  }

  @Override
  protected void embellishFilters(Map<String, Object> filters)
  {
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
    
    filters.put("newTier", newTierName);
    filters.put("previousTier", previousTierName);
    filters.put("tierChangeType", tierChangeType);
    filters.put("loyaltyProgram", loyaltyProgramsMap.getDisplay(loyaltyProgramID, "loyaltyProgram"));
  }

  /*****************************************
  *
  * Metrics settings
  *
  *****************************************/
  @Override protected List<AggregationBuilder> getMetricAggregations() { return Collections.emptyList(); }
  @Override protected Map<String, Object> extractMetrics(ParsedBucket compositeBucket, Map<String, Object> contextFilters) throws ClassCastException { return Collections.emptyMap(); }

  /*****************************************
  *
  * DocumentID settings
  *
  *****************************************/
  /**
   * For the moment, we only publish with a period of the day (except for preview)
   * In order to keep only one document per day (for each combination of filters), we use the following trick:
   * We only use the day as a timestamp (without the hour) in the document ID definition.
   * This way, preview documents will override each other till be overriden by the definitive one at 23:59:59.999 
   * 
   * Be careful, it only works if we ensure to publish the definitive one. 
   * Already existing combination of filters must be published even if there is 0 count inside, in order to 
   * override potential previews.
   */
  @Override
  protected String getDocumentID(Map<String,Object> filters, String timestamp) {
    return this.extractDocumentIDFromFilter(filters, this.targetDay);
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
    Date yesterday = RLMDateUtils.addDays(now, -1, Deployment.getBaseTimeZone());
    Date beginningOfYesterday = RLMDateUtils.truncate(yesterday, Calendar.DATE, Deployment.getBaseTimeZone());
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, Deployment.getBaseTimeZone());        // 00:00:00.000
    Date endOfYesterday = RLMDateUtils.addMilliseconds(beginningOfToday, -1);                               // 23:59:59.999
    this.targetPeriod = beginningOfToday.getTime() - beginningOfYesterday.getTime();    // most of the time 86400000ms (24 hours)
    this.targetPeriodStartIncluded = beginningOfYesterday.getTime();

    this.previewMode = false;
    this.targetDay = DAY_FORMAT.format(yesterday);

    //
    // Timestamp & period
    //
    String timestamp = TIMESTAMP_FORMAT.format(endOfYesterday);
    
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
    Date beginningOfToday = RLMDateUtils.truncate(now, Calendar.DATE, Deployment.getBaseTimeZone());
    this.targetPeriod = now.getTime() - beginningOfToday.getTime() + 1; // +1 !
    this.targetPeriodStartIncluded = beginningOfToday.getTime();

    this.previewMode = true;
    this.targetDay = DAY_FORMAT.format(now);

    //
    // Timestamp & period
    //
    String timestamp = TIMESTAMP_FORMAT.format(now);
    
    this.run(timestamp, targetPeriod);
  }
}
