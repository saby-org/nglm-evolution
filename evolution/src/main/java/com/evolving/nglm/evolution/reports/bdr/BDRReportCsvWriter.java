package com.evolving.nglm.evolution.reports.bdr;

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

public class BDRReportCsvWriter implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(BDRReportCsvWriter.class);
  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();
  private static DeliverableService deliverableService;
  private static JourneyService journeyService;
  private static OfferService offerService;
  private static LoyaltyProgramService loyaltyProgramService;

  private static final String moduleId = "moduleID";
  private static final String featureId = "featureID";
  private static final String deliverableID = "deliverableID";
  private static final String deliverableQty = "deliverableQty";
  private static final String deliveryStatus = "deliveryStatus";
  private static final String moduleName = "moduleName";
  private static final String featureDisplay = "featureName";
  private static final String deliverableDisplay = "deliverableName";
  private static final String subscriberID = "subscriberID";
  private static final String customerID = "customerID";
  private static final String deliverableExpirationDate = "deliverableExpirationDate";
  private static final String eventDatetime = "eventDatetime";
  private static final String operation = "operation";
  private static final String orgin = "origin";
  private static final String providerId = "providerID";
  private static final String returnCode = "returnCode";
  private static final String returnCodeDetails = "returnCodeDetails";
  private static final String deliveryRequestID = "deliveryRequestID";
  private static final String originatingDeliveryRequestID = "originatingDeliveryRequestID";
  private static final String eventID = "eventID";

  List<String> headerFieldsOrder = new ArrayList<String>();

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
    LinkedHashMap<String, Object> subscriberFieldsResult = new LinkedHashMap<>();
    Map<String, Object> bdrFieldsMap = re.fields.get(0);
    Map<String, Object> subscriberFields = re.fields.get(1);
    for (Object bdrFieldsObj : bdrFieldsMap.values()) // we don't care about the
                                                      // keys
      {
        Map<String, Object> bdrFields = (Map<String, Object>) bdrFieldsObj;
        if (bdrFields != null && !bdrFields.isEmpty() && subscriberFields != null && !subscriberFields.isEmpty())
          {
            // rename subscriberID
            if(bdrFields.get(subscriberID) != null) {
              Object subscriberIDField = bdrFields.get(subscriberID);
              result.put(customerID, subscriberIDField);
              //bdrFields.remove(subscriberID);
            }
            for (AlternateID alternateID : Deployment.getAlternateIDs().values())
              {
                if (subscriberFields.get(alternateID.getESField()) != null)
                  {
                    Object alternateId = subscriberFields.get(alternateID.getESField());
                    result.put(alternateID.getName(), alternateId);
                  }
              } 
            if (bdrFields.containsKey(eventID)) {
              result.put(eventID, bdrFields.get(eventID));
              
            }
            if (bdrFields.containsKey(deliverableID)) {
              result.put(deliverableID, bdrFields.get(deliverableID));
              
            }
           
            if (bdrFields.containsKey(deliverableID))
              {               
                GUIManagedObject deliverableObject = deliverableService
                    .getStoredDeliverable(String.valueOf(bdrFields.get(deliverableID)));
                if (deliverableObject instanceof Deliverable)
                  {
                    result.put(deliverableDisplay, ((Deliverable) deliverableObject).getDeliverableDisplay());                   
                  }
                else {
                  result.put(deliverableDisplay, "");
                }
              }
            if (bdrFields.containsKey(deliverableQty))
              {
                result.put(deliverableQty, bdrFields.get(deliverableQty));
              }
            if (bdrFields.containsKey(deliverableExpirationDate))
              {
                if (bdrFields.get(deliverableExpirationDate) != null)
                  {
                    Object deliverableExpirationDateObj = bdrFields.get(deliverableExpirationDate);
                    if (deliverableExpirationDateObj instanceof String)
                      {
                        String deliverableExpirationDateStr = (String) deliverableExpirationDateObj;
                        // TEMP fix for BLK : reformat date with correct
                        // template.
                        // current format comes from ES and is :
                        // 2020-04-20T09:51:38.953Z
                        SimpleDateFormat parseSDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
                        try
                          {
                            Date date = parseSDF.parse(deliverableExpirationDateStr);
                            result.put(deliverableExpirationDate, ReportsCommonCode.getDateString(date)); // replace
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
                                Date date = parseSDF2.parse(deliverableExpirationDateStr);
                                result.put(deliverableExpirationDate, ReportsCommonCode.getDateString(date)); // replace
                                                                                                              // with
                                                                                                              // new
                                                                                                              // value
                              }
                            catch (ParseException e2)
                              {
                                log.info("Unable to parse " + deliverableExpirationDateStr);
                              }
                          }

                      }
                    else
                      {
                        log.info(deliverableExpirationDate + " is of wrong type : "
                            + deliverableExpirationDateObj.getClass().getName());
                      }
                  }
                else {
                  result.put(deliverableExpirationDate, "");
                }
            }
            
            if (bdrFields.containsKey(deliveryRequestID))
              {
                result.put(deliveryRequestID, bdrFields.get(deliveryRequestID));
              }
            if (bdrFields.containsKey(originatingDeliveryRequestID))
              {
                result.put(originatingDeliveryRequestID, bdrFields.get(originatingDeliveryRequestID));
              }
            if (bdrFields.containsKey(deliveryStatus))
              {
                result.put(deliveryStatus, bdrFields.get(deliveryStatus));
              }
            if (bdrFields.containsKey(eventDatetime))
              {
                if (bdrFields.get(eventDatetime) != null)
                  {
                    Object eventDatetimeObj = bdrFields.get(eventDatetime);
                    if (eventDatetimeObj instanceof String)
                      {
                        // TEMP fix for BLK : reformat date with correct
                        // template.

                        List<SimpleDateFormat> standardDateFormats = ReportsCommonCode.initializeDateFormats();
                        result.put(eventDatetime,
                            ReportsCommonCode.parseDate(standardDateFormats, (String) eventDatetimeObj));

                        // END TEMP fix for BLK
                      }
                    else
                      {
                        log.info(eventDatetime + " is of wrong type : " + eventDatetimeObj.getClass().getName());
                      }

                  }
                else {
                  result.put(eventDatetime, "");
                }
              }

            // Compute featureName and ModuleName from ID
            if (bdrFields.containsKey(moduleId) && bdrFields.containsKey(featureId))
              {
                Module module = Module
                    .fromExternalRepresentation(String.valueOf(bdrFields.get(moduleId)));
                String feature_display = DeliveryRequest.getFeatureDisplay(module,
                    String.valueOf(bdrFields.get(featureId).toString()), journeyService, offerService,
                    loyaltyProgramService);                
                result.put(featureDisplay, feature_display);
                result.put(moduleName, module.toString());

                // bdrFields.remove(featureId);
                // bdrFields.remove(moduleId);
              }

            if (bdrFields.containsKey(operation))
              {
                result.put(operation, bdrFields.get(operation));
              }
            if (bdrFields.containsKey(orgin))
              {
                result.put(orgin, bdrFields.get(orgin));
              }
            if (bdrFields.containsKey(providerId))
              {
                result.put(providerId, bdrFields.get(providerId));
              }
            if (bdrFields.containsKey(returnCode))
              {
                result.put(returnCode, bdrFields.get(returnCode));
              }
            if (bdrFields.containsKey(returnCodeDetails))
              {
                result.put(returnCodeDetails, bdrFields.get(returnCodeDetails));
              } 
           

          }

       /*if (addHeaders)
          {
            headerFieldsOrder.clear();
           // addHeaders(writer, subscriberFieldsResult, 0); 
            //addHeaders(writer, subscriberFields, 1); 
            addHeaders(writer, result, 2);
          }*/
        if (addHeaders)
          {
            addHeaders(writer, result.keySet(), 1);
          }
        String line = ReportUtils.formatResult(result);
        log.trace("Writing to csv file : " + line);
        writer.write(line.getBytes());
        writer.write("\n".getBytes());
      }  

  }

  public static void main(String[] args)
  {
    log.info("received " + args.length + " args");
    for (String arg : args)
      {
        log.info("BDRReportCsvWriter: arg " + arg);
      }

    if (args.length < 3)
      {
        log.warn("Usage : BDRReportCsvWriter <KafkaNode> <topic in> <csvfile>");
        return;
      }
    String kafkaNode = args[0];
    String topic = args[1];
    String csvfile = args[2];
    log.info("Reading data from " + topic + " topic on broker " + kafkaNode + " producing " + csvfile + " with '"
        + CSV_SEPARATOR + "' separator");
    ReportCsvFactory reportFactory = new BDRReportCsvWriter();
    ReportCsvWriter reportWriter = new ReportCsvWriter(reportFactory, kafkaNode, topic);

    String deliverableServiceTopic = Deployment.getDeliverableTopic();
    String offerTopic = Deployment.getOfferTopic();
    String journeyTopic = Deployment.getJourneyTopic();
    String loyaltyProgramTopic = Deployment.getLoyaltyProgramTopic();

    deliverableService = new DeliverableService(kafkaNode, "bdrreportcsvwriter-deliverableserviceservice-" + topic,
        deliverableServiceTopic, false);
    journeyService = new JourneyService(kafkaNode, "bdrreportcsvwriter-journeyservice-" + topic, journeyTopic, false);
    offerService = new OfferService(kafkaNode, "bdrreportcsvwriter-offerservice-" + topic, offerTopic, false);
    loyaltyProgramService = new LoyaltyProgramService(kafkaNode, "bdrreportcsvwriter-loyaltyprogramservice-" + topic,
        loyaltyProgramTopic, false);
    deliverableService.start();
    journeyService.start();
    offerService.start();
    loyaltyProgramService.start();

    if (!reportWriter.produceReport(csvfile))
      {
        log.warn("An error occured, the report might be corrupted");
        return;
      }
  }

  /*private void addHeaders(ZipOutputStream writer, Map<String, Object> values, int offset) throws IOException
  {
    if (values != null && !values.isEmpty())
      {
        String[] allFields = values.keySet().toArray(new String[0]);
        // Arrays.sort(allFields);
        String headers = "";
        for (String fields : allFields)
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
        addHeaders = false;
      }
  }*/
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


}
