package com.evolving.nglm.evolution.reports.loyaltyprogramcustomerstate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.zip.ZipOutputStream;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.LoyaltyProgram;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportMonoPhase;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;

public class LoyaltyProgramCustomerStatesMonoPhase implements ReportCsvFactory
{
  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(LoyaltyProgramCustomerStatesMonoPhase.class);

  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();
  List<String> headerFieldsOrder = new ArrayList<String>();
  private LoyaltyProgramService loyaltyProgramService;
  private final static String customerID = "customerID";

  @Override
  public boolean dumpElementToCsvMono(Map<String, Object> map, ZipOutputStream writer, boolean addHeaders) throws IOException
  {
    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    Map<String, Object> subscriberFields = map;    
    LinkedHashMap<String, Object> subscriberComputedFields = new LinkedHashMap<String, Object>();
    LinkedHashMap<String, Object> fullFields = new LinkedHashMap<String, Object>();
    if (subscriberFields != null && !subscriberFields.isEmpty())
      {
        String subscriberID = subscriberFields.get("subscriberID").toString();
        Date now = SystemTime.getCurrentTime();
        if (subscriberID != null)
          {
            try
              {
                if (subscriberFields.get("loyaltyPrograms") == null)
                  {
                    return true;
                  }
                List<Map<String, Object>> loyaltyProgramsArray = (List<Map<String, Object>>) subscriberFields.get("loyaltyPrograms");
                if (loyaltyProgramsArray.isEmpty())
                  {
                    return true;
                  }
                subscriberComputedFields.put(customerID, subscriberID);
                for (AlternateID alternateID : Deployment.getAlternateIDs().values())
                  {
                    if (subscriberFields.get(alternateID.getESField()) != null)
                      {
                        Object alternateId = subscriberFields.get(alternateID.getESField());
                        subscriberComputedFields.put(alternateID.getName(), alternateId);
                      }
                  }
                subscriberComputedFields.put("dateTime", ReportsCommonCode.getDateString(now));
                for (int i = 0; i < loyaltyProgramsArray.size(); i++)
                  {
                    fullFields = new LinkedHashMap<String, Object>();
                    fullFields.putAll(subscriberComputedFields);
                    Map<String, Object> obj = (Map<String, Object>) loyaltyProgramsArray.get(i);
                    Object programID = obj.get("programID");
                    if (programID != null && programID instanceof String)
                      {
                        GUIManagedObject guiManagedObject = loyaltyProgramService.getStoredLoyaltyProgram((String) programID);
                        if (guiManagedObject instanceof LoyaltyProgram)
                          {
                            LoyaltyProgram loyaltyProgram = (LoyaltyProgram) guiManagedObject;
                            fullFields.put("programName", loyaltyProgram.getJSONRepresentation().get("display"));
                            Object loyaltyProgramEnrollmentDate = obj.get("loyaltyProgramEnrollmentDate");
                            if (loyaltyProgramEnrollmentDate == null)
                              {
                                fullFields.put("programEnrolmentDate", "");
                              }
                            else if (loyaltyProgramEnrollmentDate instanceof Long)
                              {
                                fullFields.put("programEnrolmentDate", ReportsCommonCode.getDateString(new Date((Long) loyaltyProgramEnrollmentDate)));
                              }
                            else
                              {
                                log.info("loyaltyProgramEnrollmentDate is not a Long : "
                                    + loyaltyProgramEnrollmentDate.getClass().getName());
                                fullFields.put("programEnrolmentDate", "");
                              }

                            if (obj.get("tierName") != null)
                              {
                                fullFields.put("tierName", obj.get("tierName"));
                              }
                            else
                              {
                                fullFields.put("tierName", "");
                              }

                            Object tierUpdateDate = obj.get("tierUpdateDate");
                            if (tierUpdateDate != null)
                              {
                                if (tierUpdateDate instanceof Long)
                                  {
                                    fullFields.put("tierUpdateDate", ReportsCommonCode.getDateString(new Date((Long) tierUpdateDate)));
                                  }
                                else
                                  {
                                    log.info("tierUpdateDate is not a Long : " + tierUpdateDate.getClass().getName());
                                  }
                              }
                            else
                              {
                                fullFields.put("tierUpdateDate", "");
                              }

                            if (obj.get("previousTierName") != null)
                              {
                                fullFields.put("previousTierName", obj.get("previousTierName"));
                              }
                            else
                              {
                                fullFields.put("previousTierName", "");
                              }

                            if (obj.get("statusPointName") != null)
                              {
                                fullFields.put("statusPointsName", obj.get("statusPointName"));
                              }
                            else
                              {
                                fullFields.put("statusPointsName", "");
                              }

                            if (obj.get("statusPointBalance") != null)
                              {
                                fullFields.put("statusPointsBalance", obj.get("statusPointBalance"));
                              }
                            else
                              {
                                fullFields.put("statusPointsBalance", "");
                              }

                            if (obj.get("rewardPointName") != null)
                              {
                                fullFields.put("rewardPointsName", obj.get("rewardPointName"));
                              }
                            else
                              {
                                fullFields.put("rewardPointsName", "");
                              }

                            if (obj.get("rewardPointBalance") != null)
                              {
                                fullFields.put("rewardPointsBalance", obj.get("rewardPointBalance"));
                              }
                            else
                              {
                                fullFields.put("rewardPointsBalance", "");
                              }
                          }
                      }
                    records.add(fullFields);
                  }
              }
            catch (Exception e)
              {
                log.warn("LoyaltyProgramCsvWriter.dumpElementToCsv(subscriber not found) skipping subscriberID=" + subscriberID, e);
              }
          }
      }

    if (!records.isEmpty())
      {

        if (addHeaders)
          {
            headerFieldsOrder.clear();
            addHeaders(writer, records.get(0), 1);
            addHeaders = false;
          }

        for (Map<String, Object> record : records)
          {
            String line = ReportUtils.formatResult(headerFieldsOrder, record);
            if (!line.isEmpty())
              {
                if (log.isTraceEnabled()) log.trace("Writing to csv file : " + line);
                writer.write(line.getBytes());
              }
            else
              {
                log.trace("Empty line => not writing");
              }
          }
      }
    return addHeaders;
  }

  private void addHeaders(ZipOutputStream writer, Map<String,Object> values, int offset) throws IOException {
    if(values != null && !values.isEmpty()) {
      String headers="";
      for(String fields : values.keySet()){
        headerFieldsOrder.add(fields);
        headers += fields + CSV_SEPARATOR;
      }
      headers = headers.substring(0, headers.length() - offset);
      writer.write(headers.getBytes());
      if(offset == 1) {
        writer.write("\n".getBytes());
      }
    }
  }
  
  
  public static void main(String[] args, final Date reportGenerationDate)
  {
    LoyaltyProgramCustomerStatesMonoPhase loyaltyProgramCustomerStatesMonoPhase = new LoyaltyProgramCustomerStatesMonoPhase();
    loyaltyProgramCustomerStatesMonoPhase.start(args, reportGenerationDate);
  }
  
  private void start(String[] args, final Date reportGenerationDate)
  {
    log.info("received " + args.length + " args");
    for(String arg : args){
      log.info("LoyaltyProgramESReader: arg " + arg);
    }

    if (args.length < 5) {
      log.warn(
          "Usage : LoyaltyProgramCustomerStatesMonoPhase <ESNode> <ES customer index> <csvfile> <defaultReportPeriodQuantity> <defaultReportPeriodUnit>");
      return;
    }

    String esNode            = args[0];
    String esIndexSubscriber = args[1];
    String csvfile           = args[2];

    log.info("Reading data from ES in "+esIndexSubscriber+" index and writing to "+csvfile);   

    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    esIndexWithQuery.put(esIndexSubscriber, QueryBuilders.matchAllQuery());
    
    List<String> subscriberFields = new ArrayList<>();
    subscriberFields.add("subscriberID");
    for (AlternateID alternateID : Deployment.getAlternateIDs().values()){
      subscriberFields.add(alternateID.getESField());
    }
    subscriberFields.add("loyaltyPrograms");
    
    loyaltyProgramService = new LoyaltyProgramService(Deployment.getBrokerServers(), "loyaltyprogramcustomerstatereport-" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getLoyaltyProgramTopic(), false);
    loyaltyProgramService.start();

    try {
      ReportMonoPhase reportMonoPhase = new ReportMonoPhase(
          esNode,
          esIndexWithQuery,
          this,
          csvfile, true, true, subscriberFields
          );

      if (!reportMonoPhase.startOneToOne())
        {
          log.warn("An error occured, the report might be corrupted");
          throw new RuntimeException("An error occureed, report must be restarted");
        }
    } finally {
      loyaltyProgramService.stop();
      log.info("Finished LoyaltyProgramESReader");
    }
  }
}
