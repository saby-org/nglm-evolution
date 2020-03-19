/****************************************************************************
 *
 *  SubscriberReportCsvWriter.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports.subscriber;

import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.evolution.*;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportCsvWriter;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.ZipOutputStream;

/**
 * This implements the phase 3 for the Subscriber report.
 *
 */
public class SubscriberReportCsvWriter implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(SubscriberReportCsvWriter.class);
  final private static String CSV_SEPARATOR = ReportUtils.getSeparator();
  private static SegmentationDimensionService segmentationDimensionService = null;
  private final static String subscriberID = "subscriberID";
  private final static String customerID = "customerID";
  private final static String segments = "segments";
  private final static String evolutionSubscriberStatusChangeDate = "evolutionSubscriberStatusChangeDate";
  private static Map<String, String[]> segmentsNames = new HashMap<>(); // segmentID -> [dimensionName, segmentName]
  private final static int INDEX_DIMENSION_NAME = 0;
  private final static int INDEX_SEGMENT_NAME = 1;
  //private static List<String> allDimensions = new ArrayList<>();
  private static Map<String, String> allDimensionsMap = new HashMap<>();
  private static List<String> allProfileFields = new ArrayList<>();

  /****************************************
  *
  *  dumpElementToCsv
  *
  ****************************************/

  /**
   * This methods writes a single {@link ReportElement} to the report (csv
   * file).
   * 
   * @throws IOException in case anything goes wrong while writing to the
   * report.
   */
  public void dumpElementToCsv(String key, ReportElement re, ZipOutputStream writer, boolean addHeaders) throws IOException
  {
    if (re.type == ReportElement.MARKER) // We will find markers in the topic
      return;
    log.trace("We got " + key + " " + re);

    LinkedHashMap<String, Object> result = new LinkedHashMap<>();
    Map<String, Object> elasticFields = re.fields.get(SubscriberReportObjects.index_Subscriber);

    if (elasticFields != null)
      {
        if (elasticFields.get(subscriberID) != null)
          {
            result.put(customerID, elasticFields.get(subscriberID));
            for (AlternateID alternateID : Deployment.getAlternateIDs().values())
              {
                if (elasticFields.get(alternateID.getESField()) != null)
                  {
                    Object alternateId = elasticFields.get(alternateID.getESField());
                    result.put(alternateID.getName(), alternateId);
                  }
              }
          } 
        if (elasticFields.containsKey("activationDate"))
          {

            if (elasticFields.get("activationDate") != null)
              {
                Object activationDateObj = elasticFields.get("activationDate");
                if (activationDateObj instanceof String)
                  {
                    String activationDateStr = (String) activationDateObj;
                    // TEMP fix for BLK : reformat date with correct
                    // template.
                    // current format comes from ES and is :
                    // 2020-04-20T09:51:38.953Z
                    SimpleDateFormat parseSDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
                    try
                      {
                        Date date = parseSDF.parse(activationDateStr);
                        result.put("activationDate", ReportsCommonCode.getDateString(date)); // replace
                                                                                               // with
                                                                                               // new
                                                                                               // value
                      }
                    catch (ParseException e1)
                      {
                        // Could also be 2019-11-27 15:39:30.276+0100
                        SimpleDateFormat parseSDF2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSXX");
                        try
                          {
                            Date date = parseSDF2.parse(activationDateStr);
                            result.put("activationDate", ReportsCommonCode.getDateString(date)); // replace
                                                                                                   // with
                                                                                                   // new
                                                                                                   // value
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
          }

        for (String field : allProfileFields)
          {
            if (field.equals(segments))
              {
                if (elasticFields.containsKey(segments))
                  {
                    //allDimensions.stream().forEach(dimensionName -> result.put(dimensionName,""));
                    result.putAll(allDimensionsMap);
                    String s = "" + elasticFields.get(segments);
                    String removeBrackets = s.substring(1, s.length() - 1); // "[ seg1, seg2, ...]"
                    String segmentIDs[] = removeBrackets.split(",");
                    Arrays.stream(segmentIDs).forEach(
                        segmentID -> {
                          String[] couple = segmentsNames.get(segmentID.trim());
                          if (couple != null)
                            {
                              result.put(couple[INDEX_DIMENSION_NAME], couple[INDEX_SEGMENT_NAME]);
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

                List<SimpleDateFormat> standardDateFormats = ReportsCommonCode.initializeDateFormats();
                result.put(evolutionSubscriberStatusChangeDate, ReportsCommonCode.parseDate(standardDateFormats, (String) elasticFields.get(evolutionSubscriberStatusChangeDate)));

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
          }
        String line = ReportUtils.formatResult(result);
        log.trace("Writing to csv file : " + line);
        writer.write(line.getBytes());
        writer.write("\n".getBytes());
      }
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
  
  private static void initSegmentationData()
  {
    // segmentID -> [dimensionName, segmentName]
    for (GUIManagedObject dimension : segmentationDimensionService.getStoredSegmentationDimensions())
      {
        if (dimension instanceof SegmentationDimension)
          {
            SegmentationDimension segmentation = (SegmentationDimension) dimension;
            //allDimensions.add(segmentation.getSegmentationDimensionName());
            allDimensionsMap.put(segmentation.getSegmentationDimensionName(), "");
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
  
  /****************************************
  *
  *  main
  *
  ****************************************/
  
  public static void main(String[] args)
  {
    log.info("received " + args.length + " args");
    for (String arg : args)
      {
        log.info("SubscriberReportESReader: arg " + arg);
      }

    if (args.length < 3)
      {
        log.warn("Usage : SubscriberReportCsvWriter <KafkaNode> <topic in> <csvfile>");
        return;
      }
    String kafkaNode = args[0];
    String topic = args[1];
    String csvfile = args[2];
    log.info("Reading data from " + topic + " topic on broker " + kafkaNode + " producing " + csvfile + " with '" + CSV_SEPARATOR + "' separator");

    segmentationDimensionService = new SegmentationDimensionService(kafkaNode, "guimanager-segmentationDimensionservice-" + topic, Deployment.getSegmentationDimensionTopic(), false);
    segmentationDimensionService.start();
    
    //
    // build map of segmentID -> [dimensionName, segmentName] once for all
    //
    
    initSegmentationData();
    initProfileFields();
    if (allProfileFields.isEmpty())
      {
        log.warn("Cannot find any profile field in configuration, no report produced");
        return;
      }
    
    ReportCsvFactory reportFactory = new SubscriberReportCsvWriter();
    ReportCsvWriter reportWriter = new ReportCsvWriter(reportFactory, kafkaNode, topic);
    if (!reportWriter.produceReport(csvfile))
      {
        log.warn("An error occured, the report might be corrupted");
        return;
      }
  }

  private static void initProfileFields()
  {
    for (CriterionField field : Deployment.getProfileCriterionFields().values())
      {
        if (field.getESField() != null)
          {
            allProfileFields.add(field.getESField());
          }
      }
  }
  
}
