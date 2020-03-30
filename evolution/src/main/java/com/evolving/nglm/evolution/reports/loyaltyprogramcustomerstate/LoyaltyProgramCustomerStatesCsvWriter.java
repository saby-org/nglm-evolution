package com.evolving.nglm.evolution.reports.loyaltyprogramcustomerstate;

import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.LoyaltyProgram;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportCsvWriter;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.zip.ZipOutputStream;

public class LoyaltyProgramCustomerStatesCsvWriter implements ReportCsvFactory
{

  private static final Logger log = LoggerFactory.getLogger(LoyaltyProgramCustomerStatesCsvWriter.class);
  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();
  List<String> headerFieldsOrder = new ArrayList<String>();
  private static LoyaltyProgramService loyaltyProgramService;
  private final static String customerID = "customerID";

  @Override
  public void dumpElementToCsv(String key, ReportElement re, ZipOutputStream writer, boolean addHeaders) throws IOException
  {
    if (re.type == ReportElement.MARKER) // We will find markers in the topic
      return;

    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();

    log.trace("We got " + key + " " + re);
    Map<String, Object> subscriberFields = re.fields.get(0);    

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
                    return;
                  }
                List<Map<String, Object>> loyaltyProgramsArray = (List<Map<String, Object>>) subscriberFields.get("loyaltyPrograms");
                if (loyaltyProgramsArray.isEmpty())
                  {
                    return;
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
                                fullFields.put("tierUpdateDate", obj.get("tierUpdateDate"));
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
          }

        for (Map<String, Object> record : records)
          {
            String line = ReportUtils.formatResult(headerFieldsOrder, record);
            if (!line.isEmpty())
              {
                log.trace("Writing to csv file : " + line);
                writer.write(line.getBytes());
                writer.write("\n".getBytes());
              }
            else
              {
                log.trace("Empty line => not writing");
              }
          }
      }
  }

  public static void main(String[] args) {
    log.info("received " + args.length + " args");
    for(String arg : args){
      log.info("LoyaltyProgramCsvWriter: arg " + arg);
    }

    if (args.length < 3) {
      log.warn("Usage : LoyaltyProgramCsvWriter <KafkaNode> <topic in> <csvfile>");
      return;
    }
    String kafkaNode = args[0];
    String topic     = args[1];
    String csvfile   = args[2];
    log.info("Reading data from "+topic+" topic on broker "+kafkaNode
        + " producing "+csvfile+" with '"+CSV_SEPARATOR+"' separator");
    ReportCsvFactory reportFactory = new LoyaltyProgramCustomerStatesCsvWriter();
    ReportCsvWriter reportWriter = new ReportCsvWriter(reportFactory, kafkaNode, topic);
    
    loyaltyProgramService = new LoyaltyProgramService(Deployment.getBrokerServers(), "loyaltyprogramcustomerstatereport-" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getLoyaltyProgramTopic(), false);
    loyaltyProgramService.start();
    
    if (!reportWriter.produceReport(csvfile)) {
      log.warn("An error occured, the report might be corrupted");
      return;
    }
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
  
}
