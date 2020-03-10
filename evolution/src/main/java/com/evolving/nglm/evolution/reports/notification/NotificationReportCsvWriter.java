package com.evolving.nglm.evolution.reports.notification;

import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.evolution.*;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportCsvWriter;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.ZipOutputStream;

public class NotificationReportCsvWriter implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(NotificationReportCsvWriter.class);
  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();
  
  private static JourneyService journeyService;
  private static OfferService offerService;
  private static LoyaltyProgramService loyaltyProgramService;
  
  List<String> headerFieldsOrder = new ArrayList<String>();
  private boolean addHeaders = true;

  private static final String moduleId = "moduleID";
  private static final String featureId = "featureID";
  private static final String moduleName = "moduleName";
  private static final String featureDisplay = "featureName";
  private static final String subscriberID = "subscriberID";
  private static final String customerID = "customerID";  
  private static final String creationDate = "creationDate";  
  private static final String deliveryDate = "deliveryDate";  
  private static final String originatingDeliveryRequestID = "originatingDeliveryRequestID";  
  private static final String deliveryRequestID = "deliveryRequestID"; 
  private static final String deliveryStatus = "deliveryStatus"; 
  private static final String eventID = "eventID"; 
  private static final String returnCode = "returnCode"; 
  private static final String returnCodeDetails = "returnCodeDetails"; 
  private static final String source = "source"; 
  
  
  
  /**
   * This methods writes a single {@link ReportElement} to the report (csv file).
   * @throws IOException in case anything goes wrong while writing to the report.
   */
  public void dumpElementToCsv(String key, ReportElement re, ZipOutputStream writer) throws IOException {
      if (re.type == ReportElement.MARKER) // We will find markers in the topic
          return;

      log.trace("We got "+key+" "+re);
      Map<String, Object> notifFieldsMap = re.fields.get(0);
      Map<String, Object> subscriberFields = re.fields.get(1);
      LinkedHashMap<String, Object> result = new LinkedHashMap<>();
      for (Object notifFieldsObj : notifFieldsMap.values()) // we don't care about the keys
      {
        Map<String, Object> notifFields = (Map<String, Object>) notifFieldsObj;
        if (notifFields != null && !notifFields.isEmpty() && subscriberFields != null && !subscriberFields.isEmpty()) {
          
          if(notifFields.get(subscriberID) != null) {
            Object subscriberIDField = notifFields.get(subscriberID);
            result.put(customerID, subscriberIDField);
            notifFields.remove(subscriberID);
          }
          for (AlternateID alternateID : Deployment.getAlternateIDs().values())
            {
              if (subscriberFields.get(alternateID.getESField()) != null)
                {
                  Object alternateId = subscriberFields.get(alternateID.getESField());
                  result.put(alternateID.getName(), alternateId);
                }
            }

          //Compute featureName and ModuleName from ID
        
          if (notifFields.containsKey(creationDate))
            {
              if (notifFields.get(creationDate) != null)
                {
                  Object creationDateObj = notifFields.get(creationDate);
                  if (creationDateObj instanceof String)
                    {
                      String creationDateStr = (String) creationDateObj;
                      // TEMP fix for BLK : reformat date with correct
                      // template.
                      // current format comes from ES and is :
                      // 2020-04-20T09:51:38.953Z
                      SimpleDateFormat parseSDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
                      try
                        {
                          Date date = parseSDF.parse(creationDateStr);
                          result.put(creationDate, ReportsCommonCode.getDateString(date)); // replace
                                                                                                        // with
                                                                                                        // new
                                                                                                        // value
                        }
                      catch (ParseException e1)
                        {
                          // Could also be 2019-11-27 15:39:30.276+0100
                          SimpleDateFormat parseSDF2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSXX");
                          try
                            {
                              Date date = parseSDF2.parse(creationDateStr);
                              result.put(creationDate, ReportsCommonCode.getDateString(date)); // replace
                                                                                                            // with
                                                                                                            // new
                                                                                                            // value
                            }
                          catch (ParseException e2)
                            {
                              log.info("Unable to parse " + creationDateStr);
                            }
                        }

                    }
                  else
                    {
                      log.info(creationDate + " is of wrong type : "
                          + creationDateObj.getClass().getName());
                    }
                }
              else {
                result.put(creationDate, "");
              }
          }          
        
          if (notifFields.containsKey(deliveryDate))
            {
              if (notifFields.get(deliveryDate) != null)
                {
                  Object deliveryDateObj = notifFields.get(deliveryDate);
                  if (deliveryDateObj instanceof String)
                    {
                      String deliveryDateStr = (String) deliveryDateObj;
                      // TEMP fix for BLK : reformat date with correct
                      // template.
                      // current format comes from ES and is :
                      // 2020-04-20T09:51:38.953Z
                      SimpleDateFormat parseSDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
                      try
                        {
                          Date date = parseSDF.parse(deliveryDateStr);
                          result.put(deliveryDate, ReportsCommonCode.getDateString(date)); // replace
                                                                                                        // with
                                                                                                        // new
                                                                                                        // value
                        }
                      catch (ParseException e1)
                        {
                          // Could also be 2019-11-27 15:39:30.276+0100
                          SimpleDateFormat parseSDF2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSXX");
                          try
                            {
                              Date date = parseSDF2.parse(deliveryDateStr);
                              result.put(deliveryDate, ReportsCommonCode.getDateString(date)); // replace
                                                                                                            // with
                                                                                                            // new
                                                                                                            // value
                            }
                          catch (ParseException e2)
                            {
                              log.info("Unable to parse " + deliveryDateStr);
                            }
                        }

                    }
                  else
                    {
                      log.info(deliveryDate + " is of wrong type : "
                          + deliveryDateObj.getClass().getName());
                    }
                }
              else {
                result.put(deliveryDate, "");
              }
          }
          
          if(notifFields.containsKey(originatingDeliveryRequestID)) {
            result.put(originatingDeliveryRequestID, notifFields.get(originatingDeliveryRequestID));
          }
          if(notifFields.containsKey(deliveryRequestID)) {
            result.put(deliveryRequestID, notifFields.get(deliveryRequestID));
          }
          if(notifFields.containsKey(deliveryStatus)) {
            result.put(deliveryStatus, notifFields.get(deliveryStatus));
          }
          if(notifFields.containsKey(eventID)) {
            result.put(eventID, notifFields.get(eventID));
          }         
          if(notifFields.containsKey(moduleId) && notifFields.containsKey(featureId)){
            String moduleID = (String) notifFields.get(moduleId);
            Module module = null;
            if(moduleID != null) {
              module = Module.fromExternalRepresentation(moduleID);
            }else {
              module = Module.Unknown;
            }
            String feature = DeliveryRequest.getFeatureDisplay(module, String.valueOf(notifFields.get(featureId).toString()), journeyService, offerService, loyaltyProgramService);
            result.put(featureDisplay, feature);
            result.put(moduleName, module.toString());          

            notifFields.remove(featureId);
            notifFields.remove(moduleId);
          }
          if(notifFields.containsKey(returnCode)) {
            result.put(returnCode, notifFields.get(returnCode));
          } 
          if(notifFields.get(returnCodeDetails) != null) {
            result.put(returnCodeDetails, notifFields.get(returnCodeDetails));
          } 
          if(notifFields.containsKey(source)) {
            result.put(source, notifFields.get(source));
          } 
          
          if (addHeaders)
            {
              addHeaders(writer, result.keySet(), 1);
            }
          String line = ReportUtils.formatResult(result);
          log.trace("Writing to csv file : "+line);
          writer.write(line.getBytes());
          writer.write("\n".getBytes());
        }
      }
  }

  public static void main(String[] args) {
      log.info("received " + args.length + " args");
      for(String arg : args){
          log.info("NotificationReportCsvWriter: arg " + arg);
      }

      if (args.length < 3) {
          log.warn("Usage : NotificationReportCsvWriter <KafkaNode> <topic in> <csvfile>");
          return;
      }
      String kafkaNode = args[0];
      String topic     = args[1];
      String csvfile   = args[2];
      log.info("Reading data from "+topic+" topic on broker "+kafkaNode
              + " producing "+csvfile+" with '"+CSV_SEPARATOR+"' separator");
      ReportCsvFactory reportFactory = new NotificationReportCsvWriter();
      ReportCsvWriter reportWriter = new ReportCsvWriter(reportFactory, kafkaNode, topic);
      
      String journeyTopic = Deployment.getJourneyTopic();
      String offerTopic = Deployment.getOfferTopic();
      String loyaltyProgramTopic = Deployment.getLoyaltyProgramTopic();

      journeyService = new JourneyService(kafkaNode, "notifreportcsvwriter-journeyservice-" + topic, journeyTopic, false);
      offerService = new OfferService(kafkaNode, "notifreportcsvwriter-offerservice-" + topic, offerTopic, false);
      loyaltyProgramService = new LoyaltyProgramService(kafkaNode, "notifreportcsvwriter-loyaltyprogramservice-" + topic, loyaltyProgramTopic, false);
      offerService.start();
      journeyService.start();
      loyaltyProgramService.start();
      
      if (!reportWriter.produceReport(csvfile)) {
          log.warn("An error occured, the report might be corrupted");
          return;
      }
  }

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
        addHeaders = false;
      }
  }

}

