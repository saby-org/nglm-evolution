/****************************************************************************
 *
 *  TokenReportMonoPhase.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports.ROI;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Map.Entry;
import java.util.zip.ZipOutputStream;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.DeliveryRequest;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.PresentationStrategyService;
import com.evolving.nglm.evolution.ScoringStrategyService;
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
  
  private static final String dateTime = "dateTime";
  private static final String nbCustomerUCG = "nbCustomerUCG";
  private static final String nbCustomerTarget = "nbCustomerTarget";
  private static final String average_metric_UCG = "average_metric_UCG";
  private static final String average_metric_Target = "average_metric_Target";
  private static final String metric_ROI = "metric_ROI";
  private static final String nbRetainedCustomers = "nbRetainedCustomers";
  private static final String churnRate = "churnRate";
  private static final String churnROI = "churnROI";
  
  
  
  
  static List<String> headerFieldsOrder = new ArrayList<String>();
  static
  {
    headerFieldsOrder.add(dateTime);
    headerFieldsOrder.add(nbCustomerUCG);
    headerFieldsOrder.add(nbCustomerTarget);
    headerFieldsOrder.add(average_metric_UCG);
    headerFieldsOrder.add(average_metric_Target);
    headerFieldsOrder.add(metric_ROI);
    headerFieldsOrder.add(nbRetainedCustomers);
    headerFieldsOrder.add(churnRate);
    headerFieldsOrder.add(churnROI);
  }

  private int tenantID = 0;

  /****************************************
  *
  * dumpElementToCsv
  *
    ****************************************/
  public boolean dumpElementToCsvMono(Map<String,Object> map, ZipOutputStream writer, boolean addHeaders) throws IOException
  {
    final int tenantID;
    LinkedHashMap<String, Object> result = new LinkedHashMap<>();
    Map<String, Object> elasticFields = map;
    if (elasticFields != null)
    {
      if (elasticFields.get("tenantID") != null)
        tenantID = (Integer) elasticFields.get("tenantID");
      else
        tenantID = 0;
      
      if (elasticFields.containsKey("activationDate") && elasticFields.get("activationDate") != null)
      {
        Object activationDateObj = elasticFields.get("activationDate");
        if (activationDateObj instanceof String)
        {
          String activationDateStr = (String) activationDateObj;
          // TEMP fix for BLK : reformat date with correct template.
          // current format comes from ES and is : 2020-04-20T09:51:38.953Z
          try
          {
            Date date = parseSDF1.parse(activationDateStr);
            // replace with new value
            result.put(activationDate, ReportsCommonCode.getDateString(date)); 
          }
          catch (ParseException e1)
          {
            // Could also be 2019-11-27 15:39:30.276+0100
            try
            {
              Date date = parseSDF2.parse(activationDateStr);
              // replace with new value
              result.put(activationDate, ReportsCommonCode.getDateString(date));
            }
            catch (ParseException e2)
            {
              log.info("Unable to parse " + activationDateStr);
            }
          }
        }
        else
        {
          log.info("activationDate is of wrong type : " + activationDateObj.getClass().getName());
        }
      }
      else
      {
        result.put(activationDate, "");
      }
      for (String field : allProfileFields)
      {
        if (!field.equals(segments))
        {
          result.put(field, elasticFields.get(field));
        }
        else if (field.equals(evolutionSubscriberStatusChangeDate))
        {

          // TEMP fix for BLK : reformat date with correct template.

          result.put(evolutionSubscriberStatusChangeDate, ReportsCommonCode.parseDate((String) elasticFields.get(evolutionSubscriberStatusChangeDate)));

          // END TEMP fix for BLK
        }
      }
      if (elasticFields.containsKey("relationships"))
      {
        if (elasticFields.get("relationships") != null)
        {
          Object relationshipObject = elasticFields.get("relationships");
          result.put(relationships, relationshipObject);
        }
        else
        {
          result.put(relationships, "");
        }
      }
      else
      {
        result.put(relationships, "");
      }
      result.putAll(allDimensionsMapPerTenant.get(tenantID)); // all dimensions have empty segments
      for (String field : allProfileFields)
      {
        if (elasticFields.containsKey(segments))
        {
          String s = "" + elasticFields.get(segments);
          String removeBrackets = s.substring(1, s.length() - 1); // "[ seg1, seg2, ...]"
          String segmentIDs[] = removeBrackets.split(",");
          Arrays.stream(segmentIDs).forEach(
              segmentID -> {
                String[] couple = segmentsNamesPerTenant.get(tenantID).get(segmentID.trim());
                if (couple != null)
                {
                  String dimName = couple[INDEX_DIMENSION_NAME];
                  String dimDisplay = dimNameDisplayMappingPerTenant.get(tenantID).get(dimName);
                  if (dimDisplay == null || dimDisplay.isEmpty()) dimDisplay = dimName;
                  result.put(dimDisplay, couple[INDEX_SEGMENT_NAME]);
                }
                else
                {
                  log.trace("Unknown segment ID : " + segmentID);
                }
              });
        }
      }

      if (addHeaders)
      {
        addHeaders(writer, result.keySet(), 1);
        addHeaders = false;
      }
      String line = ReportUtils.formatResult(result);
      if (log.isTraceEnabled()) log.trace("Writing to csv file : " + line);
      writer.write(line.getBytes());
    }
    return addHeaders;
 
    /****************************************
    *
    *  addHeaders
    *
    ****************************************/

    private void addHeaders(ZipOutputStream writer, Set<String> headers, int offset) throws IOException
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

    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    esIndexWithQuery.put(esIndexCustomer, QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("tenantID", tenantID)));
      
    ReportMonoPhase reportMonoPhase = new ReportMonoPhase(
            esNode,
            esIndexWithQuery,
            this,
            csvfile
            );
    
    
    
  }

}
