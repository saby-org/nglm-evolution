package com.evolving.nglm.evolution.reports.loyaltymissioncustomerstate;

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

public class LoyaltyMissionCustomerStatesMonoPhase implements ReportCsvFactory
{
  
  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(LoyaltyMissionCustomerStatesMonoPhase.class);
  
  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();
  
  private final static String customerID = "customerID";
  private final static String dateTime = "dateTime";
  private final static String programName = "programName";
  private final static String programEnrolmentDate = "programEnrolmentDate";
  private final static String programExitDate = "programExitDate";
  private final static String stepName = "stepName";
  private final static String stepUpdateDate = "stepUpdateDate";
  private final static String currentProgression = "currentProgression";
  private final static String completedSteps = "completedSteps";
  
  static List<String> headerFieldsOrder = new ArrayList<String>();
  static
  {
    headerFieldsOrder.add(customerID);
    for (AlternateID alternateID : Deployment.getAlternateIDs().values())
    {
      headerFieldsOrder.add(alternateID.getName());
    }
    headerFieldsOrder.add(dateTime);
    headerFieldsOrder.add(programName);
    headerFieldsOrder.add(programEnrolmentDate);
    headerFieldsOrder.add(programExitDate);
    headerFieldsOrder.add(stepName);
    headerFieldsOrder.add(stepUpdateDate);
    headerFieldsOrder.add(currentProgression);
    headerFieldsOrder.add(completedSteps);
  }
  /****************************************************************
   * 
   * main
   * 
   ***************************************************************/
  
  public static void main(String[] args, final Date reportGenerationDate)
  {
    LoyaltyMissionCustomerStatesMonoPhase loyaltyMissionCustomerStatesMonoPhase = new LoyaltyMissionCustomerStatesMonoPhase();
    loyaltyMissionCustomerStatesMonoPhase.start(args, reportGenerationDate);
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
        log.info("LoyaltyMissionESReader: arg " + arg);
      }

    if (args.length < 5)
      {
        log.warn("Usage : LoyaltyMissionCustomerStatesMonoPhase <ESNode> <ES customer index> <csvfile> <defaultReportPeriodQuantity> <defaultReportPeriodUnit>");
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
        log.info("Finished LoyaltyMissionESReader");
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
                // filter MISSION
                //

                if (loyaltyProgramsArray != null) loyaltyProgramsArray = loyaltyProgramsArray.stream().filter(loyaltyProgramMap -> LoyaltyProgram.LoyaltyProgramType.MISSION.getExternalRepresentation().equals(loyaltyProgramMap.get("loyaltyProgramType"))).collect(Collectors.toList());
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
                    else
                    {
                      subscriberComputedFields.put(alternateID.getName(), "");
                    }
                  }
                subscriberComputedFields.put("dateTime", ReportsCommonCode.getDateString(now));
                for (int i = 0; i < loyaltyProgramsArray.size(); i++)
                  {
                    LinkedHashMap<String, Object> fullFields = new LinkedHashMap<String, Object>();
                    
                    //
                    //  subscriberFields
                    //
                    
                    fullFields.putAll(subscriberComputedFields);
                    
                    //
                    //  read ES fields
                    //
                    
                    Map<String, Object> obj = (Map<String, Object>) loyaltyProgramsArray.get(i);
                    Object loyaltyProgramDisplay = obj.get("loyaltyProgramDisplay");
                    Object loyaltyProgramEnrollmentDate = obj.get("loyaltyProgramEnrollmentDate");
                    Object loyaltyProgramExitDate = obj.get("loyaltyProgramExitDate");
                    Object stepName = obj.get("stepName");
                    Object stepUpdateDate = obj.get("stepUpdateDate");
                    Object currentProgression = obj.get("currentProgression");
                    List<Map<String, String>> completedSteps = (List<Map<String, String>>) obj.get("completedSteps");
                    
                    //
                    //  report fields
                    //
                    
                    fullFields.put("programName", loyaltyProgramDisplay.toString());
                    fullFields.put("programEnrolmentDate", getReportFormattedDate(loyaltyProgramEnrollmentDate));
                    fullFields.put("programExitDate", getReportFormattedDate(loyaltyProgramExitDate));
                    fullFields.put("stepName", stepName == null ? "" : stepName);
                    fullFields.put("stepUpdateDate", getReportFormattedDate(stepUpdateDate));
                    fullFields.put("currentProgression", currentProgression);
                    fullFields.put("completedSteps", prepareCompletedSteps(completedSteps));
                    
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
   * prepareCompletedSteps
   * 
   ***************************************************************/
  
  private String prepareCompletedSteps(List<Map<String, String>> completedSteps)
  {
    StringBuilder completedStepsBuilder = new StringBuilder();
    if (completedSteps != null && !completedSteps.isEmpty())
      {
        boolean firstStep = true;
        for (Map<String, String> completedStep : completedSteps)
          {
            if (!firstStep) completedStepsBuilder.append(",");
            completedStepsBuilder.append("(").append(completedStep.get("completedStep")).append(",").append(getReportFormattedDate(completedStep.get("completionDate"))).append(")");
            firstStep = false;
          }
      }
    return completedStepsBuilder.toString();
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
            if (log.isErrorEnabled()) log.error("unable to parse date String {}", unknownDateObj.toString());
          }
      }
    return result;
  }

}
