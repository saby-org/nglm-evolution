/****************************************************************************
 *
 *  SubscriberReportCsvWriter.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.extracts;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.zip.ZipOutputStream;

/**
 * This implements the phase 3 for the extract generation.
 */
public class ExtractCsvWriter implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(ExtractCsvWriter.class);
  final private static String CSV_SEPARATOR = Deployment.getExtractManagerCsvSeparator();
  private final static String subscriberID = "subscriberID";
  private final static String customerID = "customerID";
  private final static String segments = "segments";
  private final static String evolutionSubscriberStatusChangeDate = "evolutionSubscriberStatusChangeDate";
  private final static int INDEX_DIMENSION_NAME = 0;
  private final static int INDEX_SEGMENT_NAME = 1;
  private Map<String, String> dimNameDisplayMapping = new HashMap<String, String>();
  private Map<String, String[]> segmentsNames = new HashMap<>();

  private int noOfRecords;
  private int currentCountOfRecordsWrited;
  private List<String> returnFields;

  public ExtractCsvWriter(int noOfRecords)
  {
    this.noOfRecords = noOfRecords;
    if(this.noOfRecords == 0)
    {
      this.noOfRecords = Integer.MAX_VALUE;
    }
  }

  public ExtractCsvWriter(int noOfRecords, List<String> returnFields)
  {
    this(noOfRecords);
    this.returnFields = returnFields;
  }

  /****************************************
   *
   *  dumpElementToCsv
   *
   ****************************************/
  public boolean dumpElementToCsvMono(Map<String,Object> map, ZipOutputStream writer, boolean addHeaders) throws IOException
  {
    //this was done here to not perform any change in ReportMonoPhase class
    //when the number of desired records was writen nothing else will be writen in file. The method will do nothing
    if(currentCountOfRecordsWrited >= noOfRecords) return false;

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
      //this is not working fine yet. 201001
      if(returnFields != null)
      {
        for (String field : returnFields)
        {
          if (field.equals(segments))
          {
            if (elasticFields.containsKey(segments))
            {
              String s = "" + elasticFields.get(segments);
              String removeBrackets = s.substring(1, s.length() - 1); // "[ seg1, seg2, ...]"
              String segmentIDs[] = removeBrackets.split(",");
              Arrays.stream(segmentIDs).forEach(segmentID -> {
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
      }

      if (addHeaders)
      {
        addHeaders(writer, result.keySet(), 1);
        addHeaders = false;
      }
      String line = ReportUtils.formatResult(result);
      log.trace("Writing to csv file : " + line);
      writer.write(line.getBytes());
    }
    currentCountOfRecordsWrited++;
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

}
