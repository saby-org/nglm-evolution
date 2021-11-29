/****************************************************************************
 *
 *  BadgeCustomerReportMonoPhase.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports.badgeCustomer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.zip.ZipOutputStream;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Badge;
import com.evolving.nglm.evolution.BadgeObjectiveInstance;
import com.evolving.nglm.evolution.BadgeObjectiveService;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportMonoPhase;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;

public class BadgeCustomerReportMonoPhase implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(BadgeCustomerReportMonoPhase.class);
  final private static String CSV_SEPARATOR = ReportUtils.getSeparator();

  private final static String customerID = "customerID";
  private static final String badgeID = "badgeID";
  private static final String badgeDisplay = "badgeDisplay";
  private static final String badgeAwardDate = "badgeAwardDate";
  private static final String badgeStatus = "badgeStatus";
  private static final String badgeType = "badgeType";
  private static final String badgeObjective = "badgeObjective";

  static List<String> headerFieldsOrder = new ArrayList<String>();
  static
    {
      headerFieldsOrder.add(customerID);
      for (AlternateID alternateID : Deployment.getAlternateIDs().values())
        {
          headerFieldsOrder.add(alternateID.getName());
        }
      headerFieldsOrder.add(badgeID);
      headerFieldsOrder.add(badgeDisplay);
      headerFieldsOrder.add(badgeAwardDate);
      headerFieldsOrder.add(badgeStatus);
      headerFieldsOrder.add(badgeType);
      headerFieldsOrder.add(badgeObjective);
    }

  private LoyaltyProgramService loyaltyProgramService = null;
  private BadgeObjectiveService badgeObjectiveService = null;
  private int tenantID = 0;

  /****************************************
   *
   * dumpElementToCsv
   *
   ****************************************/

  public boolean dumpElementToCsvMono(Map<String, Object> map, ZipOutputStream writer, boolean addHeaders) throws IOException
  {
    LinkedHashMap<String, Object> result = new LinkedHashMap<>();
    LinkedHashMap<String, Object> commonFields = new LinkedHashMap<>();
    Map<String, Object> subscriberFields = map;

    if (subscriberFields != null)
      {
        String subscriberID = Objects.toString(subscriberFields.get("subscriberID"));
        Date now = SystemTime.getCurrentTime();
        if (subscriberID != null)
          {
            if (subscriberFields.get("badges") != null)
              {
                Map<String, Object> badgesMap = (Map<String, Object>) subscriberFields.get("badges");
                List<Map<String, Object>> badgesArray = (List<Map<String, Object>>) badgesMap.get("badges");
                if (!badgesArray.isEmpty())
                  {
                    //
                    // customerID
                    //

                    commonFields.put(customerID, subscriberID);
                    for (AlternateID alternateID : Deployment.getAlternateIDs().values())
                      {
                        if (subscriberFields.get(alternateID.getESField()) != null)
                          {
                            Object alternateId = subscriberFields.get(alternateID.getESField());
                            commonFields.put(alternateID.getName(), alternateId);
                          }
                        else
                          {
                            commonFields.put(alternateID.getName(), "");
                          }
                      }

                    for (int i = 0; i < badgesArray.size(); i++)
                      {
                        result.clear();
                        result.putAll(commonFields);
                        Map<String, Object> badgeESFieldValue = (Map<String, Object>) badgesArray.get(i);

                        //
                        //  ESVal
                        //
                        
                        String badgeIDESVal = (String) badgeESFieldValue.get(badgeID);
                        String awardedDateESVal = (String) badgeESFieldValue.get(badgeAwardDate);
                        String badgeStatusESval = (String) badgeESFieldValue.get(badgeStatus);
                        String badgeTypeESval = (String) badgeESFieldValue.get(badgeType);
                        
                        //
                        //  derived
                        //
                        
                        String badgeDisplayVal = "";
                        StringBuilder badgeObjectives = new StringBuilder();
                        if (badgeIDESVal != null)
                          {
                            GUIManagedObject guiManagedObject = loyaltyProgramService.getStoredLoyaltyProgram(badgeIDESVal, true);
                            if (guiManagedObject != null)
                              badgeDisplayVal = guiManagedObject.getGUIManagedObjectDisplay();
                            if (guiManagedObject != null && guiManagedObject.getAccepted())
                              {
                                Set<BadgeObjectiveInstance> badgeObjectiveValues = ((Badge) guiManagedObject).getBadgeObjectives();
                                if (badgeObjectiveValues != null && !badgeObjectiveValues.isEmpty())
                                  {
                                    for (BadgeObjectiveInstance badgeObjective : badgeObjectiveValues)
                                      {
                                        GUIManagedObject guiManagedObjectBadge = badgeObjectiveService.getStoredBadgeObjective(badgeObjective.getBadgeObjectiveID(), true);
                                        if (guiManagedObjectBadge != null)
                                          {
                                            badgeObjectives.append(guiManagedObjectBadge.getGUIManagedObjectDisplay()).append(",");
                                          }
                                      }
                                    
                                  }
                              }

                          }
                        String badgeObjectivesValue = null;
                        if (badgeObjectives.length() > 0)
                          {
                            badgeObjectivesValue = badgeObjectives.toString().substring(0, badgeObjectives.toString().length() - 1);
                          }

                        //
                        //  add
                        //
                        
                        result.put(badgeID, badgeIDESVal);
                        result.put(badgeDisplay, badgeDisplayVal);
                        result.put(badgeAwardDate, ReportsCommonCode.parseDate((String) awardedDateESVal));
                        result.put(badgeStatus, badgeStatusESval);
                        result.put(badgeType, badgeTypeESval);
                        result.put(badgeObjective, badgeObjectivesValue);

                        if (addHeaders)
                          {
                            addHeaders(writer, result.keySet(), 1);
                            addHeaders = false;
                          }
                        String line = ReportUtils.formatResult(result);
                        if (log.isTraceEnabled()) log.trace("Writing to csv file : " + line);
                        writer.write(line.getBytes());
                      }
                  }
              }
          }
      }
    return addHeaders;
  }

  private String dateOrEmptyString(Object time)
  {
    return (time == null) ? "" : ReportsCommonCode.getDateString(new Date((long) time));
  }

  /****************************************
   *
   * addHeaders
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
    BadgeCustomerReportMonoPhase badgeCustomerReportMonoPhase = new BadgeCustomerReportMonoPhase();
    badgeCustomerReportMonoPhase.start(args, reportGenerationDate);
  }

  private void start(String[] args, final Date reportGenerationDate)
  {
    if (log.isInfoEnabled()) log.info("received " + args.length + " args");
    for (String arg : args)
      {
        if (log.isInfoEnabled()) log.info("BadgeCustomerESReader: arg " + arg);
      }

    if (args.length < 3)
      {
        if (log.isWarnEnabled()) log.warn("Usage : BadgeCustomerMonoPhase <ESNode> <ES customer index> <csvfile>");
        return;
      }
    String esNode = args[0];
    String esIndexCustomer = args[1];
    String csvfile = args[2];
    if (args.length > 3) tenantID = Integer.parseInt(args[3]);

    if (log.isInfoEnabled()) log.info("Reading data from ES in " + esIndexCustomer + "  index and writing to " + csvfile + " file.");
    ReportCsvFactory reportFactory = new BadgeCustomerReportMonoPhase();
    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    esIndexWithQuery.put(esIndexCustomer, QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("tenantID", tenantID)));
    ReportMonoPhase reportMonoPhase = new ReportMonoPhase(esNode, esIndexWithQuery, this, csvfile);
    loyaltyProgramService = new LoyaltyProgramService(Deployment.getBrokerServers(), "report-loyaltyProgramService-BadgeCustomerReportMonoPhase", Deployment.getLoyaltyProgramTopic(), false);
    badgeObjectiveService = new BadgeObjectiveService(Deployment.getBrokerServers(), "report-badgeObjectiveService-BadgeCustomerReportMonoPhase", Deployment.getBadgeObjectiveTopic(), false);
    loyaltyProgramService.start();
    badgeObjectiveService.start();
    try
      {
        if (!reportMonoPhase.startOneToOne())
          {
            if (log.isWarnEnabled()) log.warn("An error occured, the report might be corrupted");
            throw new RuntimeException("An error occurred, report must be restarted");
          }
      } 
    finally
      {
        loyaltyProgramService.stop();
        badgeObjectiveService.stop();
      }
  }

}
