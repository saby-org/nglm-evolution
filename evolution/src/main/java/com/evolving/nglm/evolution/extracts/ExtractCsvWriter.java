/****************************************************************************
 *
 *  SubscriberReportCsvWriter.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.extracts;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.evolution.*;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportCsvWriter;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;
import com.evolving.nglm.evolution.reports.subscriber.SubscriberReportObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.zip.ZipOutputStream;

/**
 * This implements the phase 3 for the Subscriber report.
 *
 */
public class ExtractCsvWriter implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(ExtractCsvWriter.class);
  final private static String CSV_SEPARATOR = Deployment.getExtractManagerCsvSeparator();
  private final static String subscriberID = "subscriberID";
  private final static String customerID = "customerID";

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

        if (addHeaders)
          {
            addHeaders(writer, result.keySet(), 1);
            addHeaders = false;
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
    
    ReportCsvFactory reportFactory = new ExtractCsvWriter();
    ReportCsvWriter reportWriter = new ReportCsvWriter(reportFactory, kafkaNode, topic);
    if (!reportWriter.produceReport(csvfile))
      {
        log.warn("An error occured, the report might be corrupted");
        return;
      }
  }
  
}
