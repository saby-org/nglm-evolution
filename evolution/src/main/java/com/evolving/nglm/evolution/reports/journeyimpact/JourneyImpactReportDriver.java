package com.evolving.nglm.evolution.reports.journeyimpact;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Scanner;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.Journey;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientException;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.evolution.reports.bdr.BDRReportMonoPhase;

public class JourneyImpactReportDriver extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(JourneyImpactReportDriver.class);
  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();
  private static JourneyService journeyService;
  
  private static final String journeyID = "journeyID";
  private static final String journeyName = "journeyName";
  private static final String journeyType = "journeyType";
  private static final String customerStatus = "customerStatus";
  private static final String qty_customers = "qty_customers";
  private static final String dateTime = "dateTime";
  private static final String startDate = "startDate";
  private static final String endDate = "endDate";
  private static final String rewards = "rewards";
  
  static List<String> headerFieldsOrder = new ArrayList<String>();
  static
  {
    headerFieldsOrder.add(journeyID);
    headerFieldsOrder.add(journeyName);
    headerFieldsOrder.add(journeyType);
    headerFieldsOrder.add(customerStatus);
    headerFieldsOrder.add(qty_customers);
    headerFieldsOrder.add(dateTime);
    headerFieldsOrder.add(startDate);
    headerFieldsOrder.add(endDate);
    headerFieldsOrder.add(rewards);
  }
  
  /****************************************
   *
   *  produceReport
   *
   ****************************************/

  @Override public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafkaNode, String elasticSearch, String csvFilename, String[] params, int tenantID)
  {
    log.debug("Processing Journey Impact Report with "+report+" and "+params);

    Random r = new Random();
    int apiProcessKey = r.nextInt(999);

    String journeyTopic = Deployment.getJourneyTopic();

    journeyService = new JourneyService(kafkaNode, "journeysreportcsvwriter-journeyservice-" + apiProcessKey, journeyTopic, false);
    journeyService.start();


    if (csvFilename == null)
      {
        log.info("csvfile is null !");
        return;
      }

    File file = new File(csvFilename + ReportUtils.ZIP_EXTENSION);
    if (file.exists())
      {
        log.info(csvFilename + " already exists, do nothing");
        return;
      }

    // holding the zip writers of tmp files
    Map<String,ZipOutputStream> tmpZipFiles = new HashMap<>();

    ElasticsearchClientAPI elasticsearchReaderClient = new ElasticsearchClientAPI("ReportManager");
    ReportsCommonCode.initializeDateFormats();

    try
    {
      Collection<GUIManagedObject> journeys = journeyService.getStoredJourneys(tenantID);
      int nbJourneys = journeys.size();
      log.info("journeys list size : " + nbJourneys);

      for (GUIManagedObject guiManagedObject : journeys)
        {
          if (guiManagedObject != null && guiManagedObject instanceof Journey && !((Journey) guiManagedObject).isWorkflow()) {
            Journey journey = (Journey) guiManagedObject;
            Map<String, Object> journeyInfo1 = new LinkedHashMap<String, Object>(); // to preserve order
            String journeyID = journey.getJourneyID();
            journeyInfo1.put("journeyID", journeyID);
            journeyInfo1.put(journeyName, journey.getGUIManagedObjectDisplay());
            journeyInfo1.put(journeyType, journey.getGUIManagedObjectType().getExternalRepresentation());
            
            Map<String, Object> journeyInfo2 = new LinkedHashMap<String, Object>(); // to preserve order
            
            journeyInfo2.put(dateTime, ReportsCommonCode.getDateString(SystemTime.getCurrentTime()));
            journeyInfo2.put(startDate, ReportsCommonCode.getDateString(journey.getEffectiveStartDate()));
            journeyInfo2.put(endDate, ReportsCommonCode.getDateString(journey.getEffectiveEndDate()));

            String journeyRewards = "";
            String displayForDatacubes = journey.getGUIManagedObjectDisplay() != null ? journey.getGUIManagedObjectDisplay() : journeyID;
            try
            {
              Map<String, Long> distributedRewards = elasticsearchReaderClient.getDistributedRewards(journeyID, displayForDatacubes);
              StringBuilder sbRewards = new StringBuilder();
              for (Entry<String, Long> rewards : distributedRewards.entrySet())
                {
                  sbRewards.append(rewards.getKey()).append("=").append(rewards.getValue()).append(",");
                }
              if (sbRewards.toString().length() > 0)
                {
                  journeyRewards = sbRewards.toString().substring(0, sbRewards.toString().length() - 1);
                }
            }
            catch (ElasticsearchClientException e)
            {
              log.info("Exception processing "+journey.getGUIManagedObjectDisplay(), e);
            }
            journeyInfo2.put(rewards, journeyRewards); // added each time for order

            try
            {
              Map<String, Long> journeyStatusCount = elasticsearchReaderClient.getJourneyStatusCount(journeyID);
              // Fill with missing statuses
//              for (SubscriberJourneyStatus states : SubscriberJourneyStatus.values())
//                {
//                  if (!journeyStatusCount.containsKey(states.getDisplay()))
//                      {
//                        journeyStatusCount.put(states.getDisplay(), 0L);
//                      }
//                }
              
              Map<String, Map<String, Long>> metricsPerStatus = elasticsearchReaderClient.getMetricsPerStatus(journeyID, journey.getTenantID());
              
              // We have data for this journey, write it to tmp file

              String tmpFileName = file+"."+journeyID+".tmp";
              FileOutputStream fos = null;
              ZipOutputStream writer = null;
              try
              {
                fos = new FileOutputStream(tmpFileName);
                writer = new ZipOutputStream(fos);
                String dataFile[] = csvFilename.split("[.]");
                String dataFileName = dataFile[0] + "_" + journeyID;
                String zipEntryName = new File(dataFileName + "." + dataFile[1]).getName();
                ZipEntry zipEntry = new ZipEntry(zipEntryName);
                writer.putNextEntry(zipEntry);
                writer.setLevel(Deflater.BEST_SPEED);
                tmpZipFiles.put(tmpFileName,writer); // to add it later to final ZIP file
                boolean addHeader = true;
                for (String status : journeyStatusCount.keySet())
                  {
                    Map<String, Object> mapPerStatus = new LinkedHashMap<>();
                    mapPerStatus.putAll(journeyInfo1);
                    mapPerStatus.put(customerStatus, status);
                    mapPerStatus.put(qty_customers, journeyStatusCount.get(status));
                    mapPerStatus.putAll(journeyInfo2);
                    // add metrics
                    for (Entry<String, Long> entry : metricsPerStatus.get(status).entrySet()) {
                      mapPerStatus.put(entry.getKey(), entry.getValue());
                    }
                    dumpLineToCsv(mapPerStatus, writer, addHeader);
                    addHeader = false;
                  }
              } catch (IOException e) {
                log.error("Error writing to " + tmpFileName, e);
              } finally {
                try {
                  if (writer != null) {
                    writer.flush();
                    writer.closeEntry();
                    writer.close();
                  }
                } catch (IOException e) {
                  log.info("Exception generating "+tmpFileName, e);
                } finally {
                  try {
                    if (fos != null) {
                      fos.close();
                    }
                  } catch (IOException e) {
                    log.info("Exception generating "+tmpFileName, e);
                  }
                }
              }
            }
            catch (ElasticsearchClientException e)
            {
              log.info("Exception processing "+journey.getGUIManagedObjectDisplay(), e);
            }
          }
        }

      // write final file from tmp
      FileOutputStream fos = null;
      ZipOutputStream writer = null;
      try {
        fos = new FileOutputStream(file);
        writer = new ZipOutputStream(fos);
        for (String tmpFile : tmpZipFiles.keySet()){
          // open tmp file
          FileInputStream fis = null;
          ZipInputStream reader = null;
          try {
            fis = new FileInputStream(tmpFile);
            reader = new ZipInputStream(fis);
            writer.putNextEntry(reader.getNextEntry());
            writer.setLevel(Deflater.BEST_SPEED);
            int length;
            byte[] bytes = new byte[5*1024*1024];//5M buffer
            while ((length=reader.read(bytes))!=-1) writer.write(bytes,0,length); // copy to final file
          } catch (IOException e) {
            log.error("Error writing to " + file.getAbsolutePath() + " : " + e.getLocalizedMessage());
          } finally {
            try {
              if (reader != null) {
                reader.closeEntry();
                reader.close();
              }
            } catch (IOException e) {
              log.info("Exception generating "+file.getAbsolutePath(), e.getLocalizedMessage());
            } finally {
              try {
                if (fis != null) {
                  fis.close();
                }
              } catch (IOException e) {
                log.info("Exception generating "+file.getAbsolutePath(), e.getLocalizedMessage());
              } finally {
                new File(tmpFile).delete();
              }
            }
          } 
        }
      } catch (IOException e) {
        log.error("Error writing to " + file.getAbsolutePath() + " : " + e.getLocalizedMessage());
      } finally {
        try {
          if (writer != null) {
            writer.flush();
            writer.closeEntry();
            writer.close();
          }
        } catch (IOException e) {
          log.info("Exception generating "+file.getAbsolutePath(), e.getLocalizedMessage());
        } finally {
          try {
            if (fos != null) {
              fos.close();
            }
          } catch (IOException e) {
            log.info("Exception generating "+file.getAbsolutePath(), e.getLocalizedMessage());
          }
        }
      }
    } finally {
      try {
        if (elasticsearchReaderClient != null) {
          elasticsearchReaderClient.closeCleanly();
        }
      } catch (IOException e) {
        log.info("Exception generating "+csvFilename, e);
      } finally {
        journeyService.stop();
        log.debug("Finished with Journey Impact Report");
      }
    }
  }

  private void addHeaders(ZipOutputStream writer, Map<String,Object> values) throws IOException {
    if (values != null && !values.isEmpty()) {
      String headers="";
      StringBuilder sbHeader = new StringBuilder();
      for (String fields : values.keySet()) {
        headerFieldsOrder.add(fields);
        sbHeader.append(fields).append(CSV_SEPARATOR);
      }
      if (sbHeader.toString().length() > 0) {
        headers = sbHeader.toString().substring(0, sbHeader.toString().length() - 1);
      }
      writer.write(headers.getBytes());
      writer.write("\n".getBytes());
    }
  }

  public void dumpLineToCsv(Map<String, Object> lineMap, ZipOutputStream writer, boolean addHeaders)
  {
    try
    {
      if (addHeaders)
        {
          addHeaders(writer, lineMap);
        }
      String line = ReportUtils.formatResult(lineMap);
      if (log.isTraceEnabled()) log.trace("Writing to csv file : " + line);
      writer.write(line.getBytes());
    } 
    catch (IOException e)
    {
      log.info("Exception", e);
    }
  }
  
  @Override
  public List<FilterObject> reportFilters() {
    return null;
  }

  @Override
  public List<String> reportHeader() {
    List<String> result = JourneyImpactReportDriver.headerFieldsOrder;
    return result;
  }
}
