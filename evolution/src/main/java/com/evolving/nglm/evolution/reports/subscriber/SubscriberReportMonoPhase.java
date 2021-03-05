/****************************************************************************
 *
 *  SubscriberReportMonoPhase.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports.subscriber;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipOutputStream;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.evolution.CriterionField;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.Segment;
import com.evolving.nglm.evolution.SegmentationDimension;
import com.evolving.nglm.evolution.SegmentationDimensionService;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportMonoPhase;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;
import com.evolving.nglm.evolution.reports.odr.ODRReportMonoPhase;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;

public class SubscriberReportMonoPhase implements ReportCsvFactory {

	private static final Logger log = LoggerFactory.getLogger(SubscriberReportMonoPhase.class);
  final private static String CSV_SEPARATOR = ReportUtils.getSeparator();
  private static SegmentationDimensionService segmentationDimensionService = null;
  private final static String subscriberID = "subscriberID";
  private final static String customerID = "customerID";
  private final static String segments = "segments";
  private final static String evolutionSubscriberStatusChangeDate = "evolutionSubscriberStatusChangeDate";
  private static Map<String, String[]> segmentsNames = new HashMap<>(); // segmentID -> [dimensionName, segmentName]
  private final static int INDEX_DIMENSION_NAME = 0;
  private final static int INDEX_SEGMENT_NAME = 1;
  private Map<String, String> allDimensionsMap = new HashMap<>();
  private List<String> allProfileFields = new ArrayList<>();
  private static SimpleDateFormat parseSDF1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
  private static SimpleDateFormat parseSDF2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSXX");
  private Map<String, String> dimNameDisplayMapping = new HashMap<String, String>();

  /****************************************
  *
  *  dumpElementToCsv
  *
  ****************************************/
  public boolean dumpElementToCsvMono(Map<String,Object> map, ZipOutputStream writer, boolean addHeaders) throws IOException
  {

    LinkedHashMap<String, Object> result = new LinkedHashMap<>();
    Map<String, Object> elasticFields = map;

    if (elasticFields != null)
      {
        if (elasticFields.get(subscriberID) != null)
          {
            result.put(customerID, elasticFields.get(subscriberID));
            for (AlternateID alternateID : Deployment.getAlternateIDs().values())
              {
                Object alternateId = elasticFields.get(alternateID.getESField());
                result.put(alternateID.getName(), alternateId);
              }
          } 
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
                        result.put("activationDate", ReportsCommonCode.getDateString(date)); 
                      }
                    catch (ParseException e1)
                      {
                        // Could also be 2019-11-27 15:39:30.276+0100
                        try
                          {
                            Date date = parseSDF2.parse(activationDateStr);
                            // replace with new value
                            result.put("activationDate", ReportsCommonCode.getDateString(date));
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
                result.put("activationDate", "");
              }
        
        if (elasticFields.containsKey("relationships"))
          {
            if (elasticFields.get("relationships") != null)
              {
                Object relationshipObject = elasticFields.get("relationships");
                result.put("relationships", relationshipObject);
              }
            else
              {
                result.put("relationships", "");
              }
          }
        else
          {
            result.put("relationships", "");
          }

        result.putAll(allDimensionsMap); // all dimensions have empty segments
        for (String field : allProfileFields)
          {
            if (field.equals(segments))
              {
                if (elasticFields.containsKey(segments))
                  {
                    String s = "" + elasticFields.get(segments);
                    String removeBrackets = s.substring(1, s.length() - 1); // "[ seg1, seg2, ...]"
                    String segmentIDs[] = removeBrackets.split(",");
                    Arrays.stream(segmentIDs).forEach(
                        segmentID -> {
                          String[] couple = segmentsNames.get(segmentID.trim());
                          if (couple != null)
                            {
                              String dimName = couple[INDEX_DIMENSION_NAME];
                              String dimDisplay = dimNameDisplayMapping.get(dimName);
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
            else if (field.equals(evolutionSubscriberStatusChangeDate))
              {

                // TEMP fix for BLK : reformat date with correct template.

                result.put(evolutionSubscriberStatusChangeDate, ReportsCommonCode.parseDate((String) elasticFields.get(evolutionSubscriberStatusChangeDate)));

                // END TEMP fix for BLK
              }
            else
              {
                result.put(field, elasticFields.get(field));
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
  }
  
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
  *  initSegmentationData
  *
  ****************************************/
  
  private void initSegmentationData()
  {
    // segmentID -> [dimensionName, segmentName]
    for (GUIManagedObject dimension : segmentationDimensionService.getStoredSegmentationDimensions())
      {
        if (dimension instanceof SegmentationDimension)
          {
            SegmentationDimension segmentation = (SegmentationDimension) dimension;
            //allDimensions.add(segmentation.getSegmentationDimensionName());
            allDimensionsMap.put(segmentation.getGUIManagedObjectDisplay(), "");
            dimNameDisplayMapping.put(segmentation.getSegmentationDimensionName(), dimension.getGUIManagedObjectDisplay());
            if (segmentation.getSegments() != null)
              {
                for (Segment segment : segmentation.getSegments())
                  {
                    String[] segmentInfo = new String[2];
                    segmentInfo[INDEX_DIMENSION_NAME] = segmentation.getSegmentationDimensionName();
                    segmentInfo[INDEX_SEGMENT_NAME] = segment.getName();
                    segmentsNames.put(segment.getID(), segmentInfo);
                  }
              }
          }
      }
  }

	public static void main(String[] args, final Date reportGenerationDate)
	{
    SubscriberReportMonoPhase subscriberReportMonoPhase = new SubscriberReportMonoPhase();
    subscriberReportMonoPhase.start(args, reportGenerationDate);
  }
  
  private void start(String[] args, final Date reportGenerationDate)
  {
	  log.info("** received " + args.length + " args");
	  for(String arg : args){
	    log.info("SubscriberReportMonoPhase: arg " + arg);
	  }

	  if (args.length < 4) {
	    log.warn(
	        "Usage : SubscriberReportMonoPhase <KafkaNodeList> <ESNode> <ES customer index> <csvfile>");
	    return;
	  }
	  String kafkaNodeList   = args[0];
	  String esNode          = args[1];
	  String esIndexCustomer = args[2];
    String csvfile         = args[3];

	  log.info("Reading data from ES in "+esIndexCustomer+"  index and writing to "+csvfile+" file.");	

    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    esIndexWithQuery.put(esIndexCustomer, QueryBuilders.matchAllQuery());

    ReportMonoPhase reportMonoPhase = new ReportMonoPhase(
        esNode,
        esIndexWithQuery,
        this,
        csvfile
        );

    synchronized (log) // why not, this is a static object that always exists
    {
      if (segmentationDimensionService == null) // do it only once, because we can't stop it fully
        {
          segmentationDimensionService = new SegmentationDimensionService(kafkaNodeList, "report-segmentationDimensionservice-subscriberReportMonoPhase", Deployment.getSegmentationDimensionTopic(), false);
          segmentationDimensionService.start(); // never stop it
        }
    }
      
    try {
      //
      // build map of segmentID -> [dimensionName, segmentName] once for all
      //

      synchronized (allDimensionsMap)
      {
        allDimensionsMap.clear();
        segmentsNames.clear();
        dimNameDisplayMapping.clear();
        initSegmentationData();
      }

      synchronized (allProfileFields)
      {
        allProfileFields.clear();
        initProfileFields();
      }

      if (allProfileFields.isEmpty())
        {
          log.warn("Cannot find any profile field in configuration, no report produced");
          return;
        }

      if (!reportMonoPhase.startOneToOne())
        {
          log.warn("An error occured, the report might be corrupted");
        }
    } finally {
      log.info("Finished SubscriberReportESReader");
    }
  }
	
  private void initProfileFields()
  {
    for (CriterionField field : Deployment.getBaseProfileCriterionFields().values())
      {
        if (field.getESField() != null)
          {
            allProfileFields.add(field.getESField());
          }
      }
  }
}
