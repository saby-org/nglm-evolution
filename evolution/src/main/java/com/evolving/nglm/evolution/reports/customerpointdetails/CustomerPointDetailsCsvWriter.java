package com.evolving.nglm.evolution.reports.customerpointdetails;

import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.Point;
import com.evolving.nglm.evolution.PointService;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportCsvWriter;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.zip.ZipOutputStream;

public class CustomerPointDetailsCsvWriter implements ReportCsvFactory
{

  private static final Logger log = LoggerFactory.getLogger(CustomerPointDetailsCsvWriter.class);
  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();
  List<String> headerFieldsOrder = new ArrayList<String>();
  private static PointService pointService;

  @Override
  public void dumpElementToCsv(String key, ReportElement re, ZipOutputStream writer, boolean addHeaders) throws IOException
  {
    if (re.type == ReportElement.MARKER) // We will find markers in the topic
      return;

    log.trace("We got "+key+" "+re);
    Map<String, Object> subscriberFields = re.fields.get(0);
    LinkedHashMap<String, Object> subscriberComputedFields = new LinkedHashMap<String, Object>();
    LinkedHashMap<String, Object> loyaltyComputedFields = new LinkedHashMap<String, Object>();
    List<Map<String, Object>> elementsToBeDump = new ArrayList<Map<String, Object>>();
    if (subscriberFields != null && !subscriberFields.isEmpty()) {

      String subscriberID = subscriberFields.get("subscriberID").toString();
      Date now = SystemTime.getCurrentTime();
      if (subscriberID != null){
        try{
          if(subscriberFields.get("pointBalances") != null) {

            //
            //  get subscriber information
            //
            
            subscriberComputedFields.put("customerID", subscriberID);
            for(AlternateID alternateID : Deployment.getAlternateIDs().values()){
              if(subscriberFields.get(alternateID.getESField()) != null){
                Object alternateId = subscriberFields.get(alternateID.getESField());
                subscriberComputedFields.put(alternateID.getName(),alternateId);   
              }
            }
            subscriberComputedFields.put("dateTime", ReportsCommonCode.getDateString(now));  

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

                        loyaltyComputedFields.put("pointName", storedPoint.getDisplay());
                        loyaltyComputedFields.put("pointBalance", getBalance(now, balances));

                        List<Map<String, Object>> outputJSON = new ArrayList<>();
                        for (Entry<Date, Integer> balance : balances.entrySet())
                          {
                            Map<String, Object> validityJSON = new LinkedHashMap<>(); // to preserve order when displaying
                            validityJSON.put("quantity", balance.getValue());
                            validityJSON.put("validityDate", ReportsCommonCode.getDateString(balance.getKey()));
                            outputJSON.add(validityJSON);
                          }
                        loyaltyComputedFields.put("pointValidity", ReportUtils.formatJSON(outputJSON));

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
              return;
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
          log.trace("Writing to csv file : "+line);
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
      log.info("CustomerPointDetailsCsvWriter: arg " + arg);
    }

    if (args.length < 3) {
      log.warn("Usage : CustomerPointDetailsCsvWriter <KafkaNode> <topic in> <csvfile>");
      return;
    }
    String kafkaNode = args[0];
    String topic     = args[1];
    String csvfile   = args[2];
    log.info("Reading data from "+topic+" topic on broker "+kafkaNode
        + " producing "+csvfile+" with '"+CSV_SEPARATOR+"' separator");
    ReportCsvFactory reportFactory = new CustomerPointDetailsCsvWriter();
    ReportCsvWriter reportWriter = new ReportCsvWriter(reportFactory, kafkaNode, topic);

    pointService = new PointService(Deployment.getBrokerServers(), "customerpointdetailsreport-" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getPointTopic(), false);
    pointService.start();

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
