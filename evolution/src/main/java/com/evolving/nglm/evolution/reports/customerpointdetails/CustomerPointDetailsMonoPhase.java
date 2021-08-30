package com.evolving.nglm.evolution.reports.customerpointdetails;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.zip.ZipOutputStream;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.Point;
import com.evolving.nglm.evolution.PointService;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportMonoPhase;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;

public class CustomerPointDetailsMonoPhase implements ReportCsvFactory
{

  private static final Logger log = LoggerFactory.getLogger(CustomerPointDetailsMonoPhase.class);
  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();
  
  private static final String customerID = "customerID";
  private static final String dateTime = "dateTime";
  private static final String pointName = "pointName";
  private static final String pointBalance = "pointBalance";
  private static final String pointValidity = "pointValidity";
  
  
  
  static List<String> headerFieldsOrder = new ArrayList<String>();
  static
  {
    headerFieldsOrder.add(customerID);
    for (AlternateID alternateID : Deployment.getAlternateIDs().values())
    {
      if(alternateID.getName().equals("msisdn")) {
      headerFieldsOrder.add(alternateID.getName());}
    }
    headerFieldsOrder.add(dateTime);
    headerFieldsOrder.add(pointName);
    headerFieldsOrder.add(pointBalance);
    headerFieldsOrder.add(pointValidity);
  }
  
  private PointService pointService;

  @Override
  public boolean dumpElementToCsvMono(Map<String,Object> map, ZipOutputStream writer, boolean addHeaders) throws IOException
  {
    Map<String, Object> subscriberFields = map;
    LinkedHashMap<String, Object> subscriberComputedFields = new LinkedHashMap<String, Object>();
    LinkedHashMap<String, Object> loyaltyComputedFields = new LinkedHashMap<String, Object>();
    List<Map<String, Object>> elementsToBeDump = new ArrayList<Map<String, Object>>();
    if (subscriberFields != null && !subscriberFields.isEmpty()) {

      String subscriberID = Objects.toString(subscriberFields.get("subscriberID"));
      Date now = SystemTime.getCurrentTime();
      if (subscriberID != null){
        try{
          if(subscriberFields.get("pointBalances") != null) {

            //
            //  get subscriber information
            //
            
            subscriberComputedFields.put(customerID, subscriberID);
            for(AlternateID alternateID : Deployment.getAlternateIDs().values()){
              if(subscriberFields.get(alternateID.getESField()) != null){
                Object alternateId = subscriberFields.get(alternateID.getESField());
                subscriberComputedFields.put(alternateID.getName(),alternateId);   
              }
            }
            subscriberComputedFields.put(dateTime, ReportsCommonCode.getDateString(now));  

            List<Map<String, Object>> pointsArray = (List<Map<String, Object>>) subscriberFields.get("pointBalances");
            if (!pointsArray.isEmpty())
              {
                //
                //  get subscriber points and generate 1 line per point
                //

                for (int i = 0; i < pointsArray.size(); i++)
                  {
                    Map<String, Object> obj = (Map<String, Object>) pointsArray.get(i);
                    GUIManagedObject guiManagedObject = pointService.getStoredPoint((String) obj.get("pointID"));
                    if (guiManagedObject != null && guiManagedObject instanceof Point)
                      {
                        Point storedPoint = (Point) guiManagedObject;
                        SortedMap<Date, Integer> balances = new TreeMap<Date, Integer>();

                        List<Object> expirationDateArray = (List<Object>) obj.get("expirationDates");
                        for (int j = 0; j < expirationDateArray.size(); j++)
                          {
                            Map<String, Object> insideExpiration = (Map<String, Object>) expirationDateArray.get(j);
                            int amount = 0;
                            try
                            {
                              amount = (int) insideExpiration.get("amount");
                            }
                            catch (ClassCastException e)
                            {
                              log.trace("amount is not an integer : " + insideExpiration.get("amount") + ", using " + amount);
                            }
                            balances.put(new Date((Long) insideExpiration.get("date")), amount);
                          }

                        loyaltyComputedFields.put(pointName, storedPoint.getDisplay());
                        loyaltyComputedFields.put(pointBalance, getBalance(now, balances));

                        List<Map<String, Object>> outputJSON = new ArrayList<>();
                        for (Entry<Date, Integer> balance : balances.entrySet())
                          {
                            Map<String, Object> validityJSON = new LinkedHashMap<>(); // to preserve order when displaying
                            validityJSON.put("quantity", balance.getValue());
                            validityJSON.put("validityDate", ReportsCommonCode.getDateString(balance.getKey()));
                            outputJSON.add(validityJSON);
                          }
                        loyaltyComputedFields.put(pointValidity, ReportUtils.formatJSON(outputJSON));

                        // store subscriber information + point information
                        LinkedHashMap<String, Object> fullFields = new LinkedHashMap<String, Object>();
                        fullFields.putAll(subscriberComputedFields);
                        fullFields.putAll(loyaltyComputedFields);
                        elementsToBeDump.add(fullFields);
                      }
                  }
              }
            }
          else
            {
              return true;
            }
        }
        catch (Exception e){
          log.warn("CustomerPointDetailsCsvWriter.dumpElementToCsv(subscriber not found) skipping subscriberID="+subscriberID, e);
        }
      }
    }

    if(!elementsToBeDump.isEmpty()){
      if(addHeaders){
        headerFieldsOrder.clear();
        addHeaders(writer, elementsToBeDump.get(0), 1);
        addHeaders = false;
      }

      for(Map<String, Object> oneElement : elementsToBeDump){
        String line = ReportUtils.formatResult(oneElement);
        if(!line.isEmpty()){
          if (log.isTraceEnabled()) log.trace("Writing to csv file : "+line);
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

  public static void main(String[] args, final Date reportGenerationDate)
  {
    CustomerPointDetailsMonoPhase customerPointDetailsMonoPhase = new CustomerPointDetailsMonoPhase();
    customerPointDetailsMonoPhase.start(args, reportGenerationDate);
  }
  
  private void start(String[] args, final Date reportGenerationDate)
  {
    log.info("received " + args.length + " args");
    for(String arg : args){
      log.info("CustomerPointDetailsMonoPhase: arg " + arg);
    }

    if (args.length < 3) {
      log.warn("Usage : CustomerPointDetailsMonoPhase <EsNode> <IndexSubscriber> <csvfile> <defaultReportPeriodQuantity> <defaultReportPeriodUnit>");
      return;
    }

    String esNode             = args[0];
    String esIndexSubscriber  = args[1];
    String csvfile            = args[2];
    // Other arguments are ignored in original report

    log.info("Reading data from "+esIndexSubscriber + " producing "+csvfile+" with '"+CSV_SEPARATOR+"' separator");

    pointService = new PointService(Deployment.getBrokerServers(), "customerpointdetailsreport-" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getPointTopic(), false);
    pointService.start();

    try {
      LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();

      esIndexWithQuery.put(esIndexSubscriber, QueryBuilders.matchAllQuery());

      List<String> subscriberFields = new ArrayList<>();
      subscriberFields.add("subscriberID");
      for (AlternateID alternateID : Deployment.getAlternateIDs().values()){
        subscriberFields.add(alternateID.getESField());
      }
      subscriberFields.add("pointBalances");

      ReportMonoPhase reportMonoPhase = new ReportMonoPhase(
          esNode,
          esIndexWithQuery,
          this,
          csvfile,
          true,
          true,
          subscriberFields
          );

      if (!reportMonoPhase.startOneToOne())
        {
          log.warn("An error occured, the report might be corrupted");
          throw new RuntimeException("An error occurred, report must be restarted");
        }
    } finally {
      pointService.stop();
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

  /*****************************************
   *
   *  getBalance
   *
   *****************************************/

  public int getBalance(Date evaluationDate, SortedMap<Date,Integer> balances)
  {
    int result = 0;
    for (Date expirationDate : balances.keySet())
      {
        if (evaluationDate.compareTo(expirationDate) <= 0)
          {
            result += balances.get(expirationDate);
          }
      }
    return result;
  }
 
 
}
