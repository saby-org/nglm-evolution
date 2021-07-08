package com.evolving.nglm.evolution.reports.loyaltychallengecustomerstate;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.ZipOutputStream;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.LoyaltyProgram;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportMonoPhase;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;

public class LoyaltyChallengeCustomerStatesMonoPhase implements ReportCsvFactory
{
  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(LoyaltyChallengeCustomerStatesMonoPhase.class);
  
  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();
  List<String> headerFieldsOrder = new ArrayList<String>();
  private final static String customerID = "customerID";
  
  /****************************************************************
   * 
   * main
   * 
   ***************************************************************/
  
  public static void main(String[] args, final Date reportGenerationDate)
  {
    LoyaltyChallengeCustomerStatesMonoPhase loyaltyChallengeCustomerStatesMonoPhase = new LoyaltyChallengeCustomerStatesMonoPhase();
    loyaltyChallengeCustomerStatesMonoPhase.start(args, reportGenerationDate);
  }
  
  /****************************************************************
   * 
   * start
   * 
   ***************************************************************/
  
  private void start(String[] args, final Date reportGenerationDate)
  {
    log.info("received " + args.length + " args");
    for (String arg : args)
      {
        log.info("LoyaltyChallengeESReader: arg " + arg);
      }

    if (args.length < 5)
      {
        log.warn("Usage : LoyaltyChallengeCustomerStatesMonoPhase <ESNode> <ES customer index> <csvfile> <defaultReportPeriodQuantity> <defaultReportPeriodUnit>");
        return;
      }

    String esNode = args[0];
    String esIndexSubscriber = args[1];
    String csvfile = args[2];

    log.info("Reading data from ES in " + esIndexSubscriber + " index and writing to " + csvfile);

    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    esIndexWithQuery.put(esIndexSubscriber, QueryBuilders.matchAllQuery());

    List<String> subscriberFields = new ArrayList<>();
    subscriberFields.add("subscriberID");
    for (AlternateID alternateID : Deployment.getAlternateIDs().values())
      {
        subscriberFields.add(alternateID.getESField());
      }
    subscriberFields.add("loyaltyPrograms");


    try
      {
        ReportMonoPhase reportMonoPhase = new ReportMonoPhase(esNode, esIndexWithQuery, this, csvfile, true, true, subscriberFields);
        if (!reportMonoPhase.startOneToOne())
          {
            log.warn("An error occured, the report might be corrupted");
            throw new RuntimeException("An error occurred, report must be restarted");
          }
      } 
    finally
      {
        log.info("Finished LoyaltyChallengeESReader");
      }
  }
  
  /****************************************************************
   * 
   * dumpElementToCsvMono
   * 
   ***************************************************************/
  
  @Override public boolean dumpElementToCsvMono(Map<String, Object> map, ZipOutputStream writer, boolean addHeaders) throws IOException
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

                //
                // filter CHALLENGE
                //

                if (loyaltyProgramsArray != null) loyaltyProgramsArray = loyaltyProgramsArray.stream().filter(loyaltyProgramMap -> LoyaltyProgram.LoyaltyProgramType.CHALLENGE.getExternalRepresentation().equals(loyaltyProgramMap.get("loyaltyProgramType"))).collect(Collectors.toList());
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
                    
                    //
                    //  read ES fields
                    //
                    
                    Object loyaltyProgramDisplay = obj.get("loyaltyProgramDisplay");
                    Object loyaltyProgramEnrollmentDate = obj.get("loyaltyProgramEnrollmentDate");
                    Object loyaltyProgramExitDate = obj.get("loyaltyProgramExitDate");
                    Object levelName = obj.get("levelName");
                    Object currentScore = obj.get("score");
                    Object levelUpdateDate = obj.get("levelUpdateDate");
                    Object previousLevelName = obj.get("previousLevelName");
                    Object occurrenceNumber = obj.get("occurrenceNumber");
                    Object previousOccurrenceLevel = obj.get("previousPeriodLevel");
                    Object previousOccurrenceScore = obj.get("previousPeriodScore");
                    
                    //
                    //  report fields
                    //
                    
                    fullFields.put("programName", loyaltyProgramDisplay.toString());
                    fullFields.put("programEnrolmentDate", getReportFormattedDate(loyaltyProgramEnrollmentDate));
                    fullFields.put("programExitDate", getReportFormattedDate(loyaltyProgramExitDate));
                    fullFields.put("levelName", levelName);
                    fullFields.put("currentScore", currentScore);
                    fullFields.put("levelUpdateDate", getReportFormattedDate(levelUpdateDate));
                    fullFields.put("previousLevelName", previousLevelName == null ? "" : previousLevelName);
                    fullFields.put("challengeOccurrence", occurrenceNumber);
                    fullFields.put("previousOccurrenceLevel", previousOccurrenceLevel == null ? "" : previousOccurrenceLevel);
                    fullFields.put("previousOccurrenceScore", previousOccurrenceScore);
                    
                    //
                    //  add
                    //
                    
                    records.add(fullFields);
                  }
              } catch (Exception e)
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
                if (log.isTraceEnabled())
                  log.trace("Writing to csv file : " + line);
                writer.write(line.getBytes());
              } else
              {
                log.trace("Empty line => not writing");
              }
          }
      }
    return addHeaders;
  }
  
  /****************************************************************
   * 
   * addHeaders
   * 
   ***************************************************************/
  
  private void addHeaders(ZipOutputStream writer, Map<String, Object> values, int offset) throws IOException
  {
    if (values != null && !values.isEmpty())
      {
        String headers = "";
        for (String fields : values.keySet())
          {
            headerFieldsOrder.add(fields);
            headers += fields + CSV_SEPARATOR;
          }
        headers = headers.substring(0, headers.length() - offset);
        writer.write(headers.getBytes());
        if (offset == 1)
          {
            writer.write("\n".getBytes());
          }
      }
  }
  
  /****************************************************************
   * 
   * getReportFormattedDate
   * 
   ***************************************************************/
  
  public String getReportFormattedDate(Object unknownDateObj)
  {
    String result = "";
    if (unknownDateObj instanceof Long)
      {
        result = ReportsCommonCode.getDateString(new Date((Long) unknownDateObj));
      }
    else if (unknownDateObj instanceof String)
      {
        try
          {
            Date esDate = RLMDateUtils.parseDateFromElasticsearch((String) unknownDateObj);
            result = ReportsCommonCode.getDateString(esDate);
          } 
        catch (ParseException e)
          {
            if (log.isErrorEnabled()) log.error("unbale to parse ES date String {}", unknownDateObj.toString());
          }
      }
    return result;
  }

}
