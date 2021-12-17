/****************************************************************************
 *
 *  ROIReportMonoPhase.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports.ROI;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Map.Entry;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.DeliveryRequest;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.datacubes.SubscriberProfileDatacubeMetric;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.PresentationStrategyService;
import com.evolving.nglm.evolution.ScoringStrategyService;
import com.evolving.nglm.evolution.SubscriberProfile.EvolutionSubscriberStatus;
import com.evolving.nglm.evolution.TokenTypeService;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportMonoPhase;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.evolution.reports.subscriber.SubscriberReportMonoPhase;

public class ROIReportMonoPhase implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(ROIReportMonoPhase.class);
  final private static String CSV_SEPARATOR = ReportUtils.getSeparator();
  
//EVPRO-1172
  private static final String FILTER_PREFIX = "filter.";
  private static final String STATUS_PREVIOUS_EVOLUTION = "status_previous_evolutionSubscriberStatus";
  private static final String STATUS_PREVIOUS_UCG = "status_previous_universalControlGroup";
  private static final String EVOLUTION_STATUS = "evolutionSubscriberStatus";
  private static final String UCG = "universalControlGroup";
  private static final String EVOLUTION_STATUS_PREVIOUS = "previousEvolutionSubscriberStatus";
  private static final String UCG_PREVIOUS = "universalControlGroupPrevious";
  private static final String NOT_IN_GROUP = "not.in.group";
  private static final String IN_GROUP = "in.group";
  
  private static final String dateTime = "dateTime";
  private static final String metric = "metric";
  private static final String nbCustomerUCG = "nbCustomerUCG";
  private static final String nbCustomerTarget = "nbCustomerTarget";
  private static final String averageMetricUCG = "averageMetricUCG";
  private static final String averageMetricTarget = "averageMetricTarget";
  private static final String metricROI = "metricROI";
  private static final String nbRetainedCustomers = "nbRetainedCustomers";
  private static final String churnRate = "churnRate";
  
  private static final String churnMetricROI = "churnMetricROI";
  
  
  
  static List<String> headerFieldsOrder = new ArrayList<String>();
  static
  {
	headerFieldsOrder.add(dateTime);
    headerFieldsOrder.add(metric);
    headerFieldsOrder.add(nbCustomerUCG);
    headerFieldsOrder.add(nbCustomerTarget);
    headerFieldsOrder.add(averageMetricUCG);
    headerFieldsOrder.add(averageMetricTarget);
    headerFieldsOrder.add(metricROI);
    headerFieldsOrder.add(nbRetainedCustomers);
    headerFieldsOrder.add(churnRate);
    headerFieldsOrder.add(churnMetricROI);
  }

  private int tenantID = 0;
  private LinkedHashMap<String, Object> result = new LinkedHashMap<>();
  private String timestampES = null;
  private Integer nbCustomerUCGByDate = 0;
  private Integer nbCustomerTargetByDate = 0;
  private Map<String,Integer> averageMetricsUCG = null;
  private Map<String,Integer> averageMetricsTarget = null;
  private Integer nonChurnerTarget = 0;
  private Integer churnerTarget = 0;
  private Integer nonChurnerUCG = 0;
  private Integer churnerUCG = 0;
  
  
  private void init() {
	nbCustomerUCGByDate = 0;
    nbCustomerTargetByDate = 0;
    averageMetricsUCG = new HashMap<String,Integer>();
    averageMetricsTarget = new HashMap<String,Integer>();;
    nonChurnerTarget = 0;
    churnerTarget = 0;
    nonChurnerUCG = 0;
    churnerUCG = 0;
    //initialize averageMetricsUCG and averageMetricsTarget
	Map<String, SubscriberProfileDatacubeMetric> customMetrics = Deployment.getSubscriberProfileDatacubeConfiguration().getMetrics();
    for(String metricID: customMetrics.keySet()) {
      SubscriberProfileDatacubeMetric customMetric = customMetrics.get(metricID);
      if(customMetric.isMetricROI()) {
      	averageMetricsUCG.put(metricID,0);
        averageMetricsTarget.put(metricID,0);		
      }
    } 
  }
  
  private void createResultMap(String metricID){
    result.clear();
	result.put(dateTime,timestampES);
	result.put(nbCustomerUCG,nbCustomerUCGByDate==null?0:nbCustomerUCGByDate);
	result.put(nbCustomerTarget,nbCustomerTargetByDate==null?0:nbCustomerTargetByDate);
	result.put(metric,metricID);
	result.put(averageMetricUCG,averageMetricsUCG.get(metricID));
	result.put(averageMetricTarget,averageMetricsTarget.get(metricID));
	result.put(metricROI,(averageMetricsTarget.get(metricID)-averageMetricsUCG.get(metricID))*nbCustomerTargetByDate);
      
    if((nonChurnerUCG+churnerUCG)!=0){
  	  //nbRetainedCustomers
  	  result.put(nbRetainedCustomers,nonChurnerTarget-(nonChurnerTarget+churnerTarget)*nonChurnerUCG/(nonChurnerUCG+churnerUCG));
    } else {
      result.put(nbRetainedCustomers,0);
    }
	if((nonChurnerTarget+churnerTarget)!=0) {
	  //churnRate
	  result.put(churnRate,nonChurnerTarget/(nonChurnerTarget+churnerTarget)-1);
	} else {
	  result.put(churnRate,0);
	}
	//churnRate
	if(result.get(churnRate)!=null && result.get("average_"+metricID+"_Target")!=null && (int)result.get(churnRate)!=0) {
	  result.put(churnMetricROI,(nonChurnerTarget-(nonChurnerTarget+churnerTarget)*nonChurnerUCG/(nonChurnerUCG+churnerUCG))*(int)result.get("average_"+metricID+"_Target")/(int)result.get(churnRate));
	}
	    
  }
  
  
  /****************************************
  *
  * dumpElementToCsv
  *
    ****************************************/
  public boolean dumpElementToCsvMono(Map<String,Object> map, ZipOutputStream writer, boolean addHeaders) throws IOException
  {
	Map<String, Object> elasticFields = map;
    if(timestampES == null) {
  	  timestampES = (String)elasticFields.get("timestamp");
    }
  	
    //nbCustomerUCGbyDate (filter.stratum.Subscriber Universal Control Group = in.group && filter.evolutionSubscriberStatus!=terminated)
    if(elasticFields.get(FILTER_PREFIX+UCG).toString().equalsIgnoreCase(IN_GROUP) && !elasticFields.get(FILTER_PREFIX+EVOLUTION_STATUS).toString().equalsIgnoreCase(EvolutionSubscriberStatus.Terminated.getExternalRepresentation())) {
  	nbCustomerUCGByDate = nbCustomerUCGByDate + (int)elasticFields.get("count");
    }
    //nbCustomerTargetbyDate (filter.stratum.Subscriber Universal Control Group = not.in.group && filter.evolutionSubscriberStatus!=terminated)
    if(elasticFields.get(FILTER_PREFIX+UCG).toString().equalsIgnoreCase(NOT_IN_GROUP) && !elasticFields.get(FILTER_PREFIX+EVOLUTION_STATUS).toString().equalsIgnoreCase(EvolutionSubscriberStatus.Terminated.getExternalRepresentation())) {
  	nbCustomerTargetByDate = nbCustomerTargetByDate + (int)elasticFields.get("count");
    }
    //average_metricUCG and average_metricTarget: average value of a metric over a period for customers in/not in UCG
    Map<String, SubscriberProfileDatacubeMetric> customMetrics = Deployment.getSubscriberProfileDatacubeConfiguration().getMetrics();
    for(String metricID: customMetrics.keySet()) {
      SubscriberProfileDatacubeMetric customMetric = customMetrics.get(metricID);
      if(customMetric.isMetricROI()) {
      	//headerFieldsOrder.add("average_"+metricID+"_UCG");
      	if(elasticFields.get(FILTER_PREFIX+UCG).toString().equalsIgnoreCase(IN_GROUP)){
      		averageMetricsUCG.put(metricID,averageMetricsUCG.get(metricID)+(int)elasticFields.get("metric.custom."+customMetric.getDisplay()));
      	}	
          //headerFieldsOrder.add("average_"+metricID+"_Target");
      	if(elasticFields.get(FILTER_PREFIX+UCG).toString().equalsIgnoreCase(NOT_IN_GROUP)){
      		averageMetricsTarget.put(metricID,averageMetricsTarget.get(metricID)+(int)elasticFields.get("metric.custom."+customMetric.getDisplay()));
      	}
      }
    } 
  //nbRetainedCustomers
    if(!elasticFields.get(FILTER_PREFIX+EVOLUTION_STATUS_PREVIOUS).toString().equalsIgnoreCase(EvolutionSubscriberStatus.Terminated.getExternalRepresentation()) && !elasticFields.get(FILTER_PREFIX+EVOLUTION_STATUS).toString().equalsIgnoreCase(EvolutionSubscriberStatus.Terminated.getExternalRepresentation())) {
	      if(elasticFields.get(FILTER_PREFIX+UCG_PREVIOUS).toString().equalsIgnoreCase(NOT_IN_GROUP) && elasticFields.get(FILTER_PREFIX+UCG).toString().equalsIgnoreCase(NOT_IN_GROUP)){
	    	  nonChurnerTarget = nonChurnerTarget + (int)elasticFields.get("custom."+STATUS_PREVIOUS_EVOLUTION);
	      }
	      if(elasticFields.get(FILTER_PREFIX+UCG_PREVIOUS).toString().equalsIgnoreCase(IN_GROUP) && elasticFields.get(FILTER_PREFIX+UCG).toString().equalsIgnoreCase(IN_GROUP)){
	    	  nonChurnerUCG = nonChurnerUCG + (int)elasticFields.get("custom."+STATUS_PREVIOUS_UCG);
	      }
    }
    if(!elasticFields.get(FILTER_PREFIX+EVOLUTION_STATUS_PREVIOUS).toString().equalsIgnoreCase(EvolutionSubscriberStatus.Terminated.getExternalRepresentation()) && elasticFields.get(FILTER_PREFIX+EVOLUTION_STATUS).toString().equalsIgnoreCase(EvolutionSubscriberStatus.Terminated.getExternalRepresentation())) {
	      if(elasticFields.get(FILTER_PREFIX+UCG_PREVIOUS).toString().equalsIgnoreCase(NOT_IN_GROUP) && elasticFields.get(FILTER_PREFIX+UCG).toString().equalsIgnoreCase(NOT_IN_GROUP)){
	    	  churnerTarget = churnerTarget + (int)elasticFields.get("custom."+STATUS_PREVIOUS_EVOLUTION);
	      }
	      if(elasticFields.get(FILTER_PREFIX+UCG_PREVIOUS).toString().equalsIgnoreCase(IN_GROUP) && elasticFields.get(FILTER_PREFIX+UCG).toString().equalsIgnoreCase(IN_GROUP)){
	    	  churnerUCG = churnerUCG + (int)elasticFields.get("custom."+STATUS_PREVIOUS_UCG);
	      }
    }
     
    return addHeaders;
    }
    
    /****************************************
    *
    *  addHeaders
    *
    ****************************************/

    private void addHeaders(ZipOutputStream writer, List<String> headers, int offset) throws IOException
    {
      if (headers != null && !headers.isEmpty())
        {
          String header = "";
          for (String field : headers)
            {
              header += field + CSV_SEPARATOR;
            }
          header = header.substring(0, header.length() - offset);
          writer.write(header.getBytes());
          if (offset == 1)
            {
              writer.write("\n".getBytes());
            }
        }
    }
    
    /*************************************
     * 
     * Add headers for empty file   * 
     * 
     *****************************************/
    
    @Override public void dumpHeaderToCsv(ZipOutputStream writer, boolean addHeaders)
    {
      try
        {
          if (addHeaders)
            {
              List<String> headers = new ArrayList<String>(headerFieldsOrder);
              addHeaders(writer, headers, 1);
            }
        } 
      catch (IOException e)
        {
          e.printStackTrace();
        }
    }
    


  /****************************************
   *
   * main
   *
   ****************************************/

  public static void main(String[] args, final Date reportGenerationDate)
  {
    ROIReportMonoPhase roiReportMonoPhase = new ROIReportMonoPhase();
    roiReportMonoPhase.start(args, reportGenerationDate);
  }
  
  private void start(String[] args, final Date reportGenerationDate)
  {
    log.info("received " + args.length + " args");
    for (String arg : args)
      {
        log.info("ROIReportESReader: arg " + arg);
      }

    if (args.length < 3) {
      log.warn(
          "Usage : ROIReportMonoPhase <ESNode> <ES customer index> <csvfile>");
      return;
    }
    String esNode          = args[0];
    String esIndexCustomer = args[1];
    String csvfile         = args[2];
    if (args.length > 3) tenantID = Integer.parseInt(args[3]);

    log.info("Reading data from ES in "+esIndexCustomer+"  index and writing to "+csvfile+" file.");  

    String timeZone =  Deployment.getDefault().getTimeZone(); // TODO EVPRO-99 refactor with tenant
    Date yesterday = RLMDateUtils.addDays(reportGenerationDate, -1, timeZone);
    Date beginningOfYesterday = RLMDateUtils.truncate(yesterday, Calendar.DATE, timeZone);
    Date beginningOfToday = RLMDateUtils.truncate(reportGenerationDate, Calendar.DATE, timeZone);        // 00:00:00.000
    Date endOfYesterday = RLMDateUtils.addMilliseconds(beginningOfToday, -1);                     // 23:59:59.999
  
    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    
    esIndexWithQuery.put(esIndexCustomer, QueryBuilders.boolQuery()
            .filter(QueryBuilders.rangeQuery("timestamp").gte(RLMDateUtils.formatDateForElasticsearchDefault(beginningOfYesterday)).lte(RLMDateUtils.formatDateForElasticsearchDefault(endOfYesterday))));

    
    init();
    
    ReportMonoPhase reportMonoPhase = new ReportMonoPhase(
            esNode,
            esIndexWithQuery,
            this,
            csvfile
            );
    

    if (!reportMonoPhase.startOneToOne())
    {
      log.warn("An error occured, the report might be corrupted");
      throw new RuntimeException("An error occurred, report must be restarted");
    }
    if (csvfile == null)
    {
      log.info("csvfile is null !");
      return;
    }

  File file = new File(csvfile + ReportUtils.ZIP_EXTENSION);
  if (file.exists())
    {
      log.info(csvfile + " already exists, do nothing");
    }
	FileOutputStream fos = null;
    ZipOutputStream writer = null;
    try {
        fos = new FileOutputStream(file);
        writer = new ZipOutputStream(fos);
        ZipEntry entry = new ZipEntry(new File(csvfile).getName());
        writer.putNextEntry(entry);
        writer.setLevel(Deflater.BEST_SPEED);
    	addHeaders(writer, headerFieldsOrder, 1);
    	Map<String, SubscriberProfileDatacubeMetric> customMetrics = Deployment.getSubscriberProfileDatacubeConfiguration().getMetrics();
	    for(String metricID: customMetrics.keySet()) {
	  	  SubscriberProfileDatacubeMetric customMetric = customMetrics.get(metricID);
	      if(customMetric.isMetricROI()) {
	    	createResultMap(metricID);
	    	if(result!= null && !result.isEmpty()) {
		    	String line = ReportUtils.formatResult(headerFieldsOrder, result);
		        if (log.isTraceEnabled()) log.trace("Writing to csv file : " + line);
		        writer.write(line.getBytes());
	    	}
	      }
	    }
    }
    catch (IOException e1)
    {
      StringWriter stackTraceWriter = new StringWriter();
      e1.printStackTrace(new PrintWriter(stackTraceWriter, true));
      log.info("Error when creating " + csvfile + " : " + e1.getLocalizedMessage() + " stack : " + stackTraceWriter.toString());
    }
    finally {
      log.info("Finished ROIESReader");
      if (writer != null)
        {
          try
          {
            writer.flush();
            writer.closeEntry();
            writer.close();
          }
          catch (IOException e)
          {
            log.info("Exception " + e.getLocalizedMessage());
          }
        }
      if (fos != null)
        {
          try
          {
            fos.close();
          }
          catch (IOException e)
          {
            log.info("Exception " + e.getLocalizedMessage());
          }
        }
    }
    
  
  }

}
