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
  private static final String returnCodeDescription = "returnCodeDescription";
  private static final String source = "source";
  private static SimpleDateFormat parseSDF1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
  private static SimpleDateFormat parseSDF2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSXX");

  private static List<String> headerFieldsOrder = new LinkedList<String>();
  static
  {
    headerFieldsOrder.add(moduleId);
    headerFieldsOrder.add(featureId);
    headerFieldsOrder.add(moduleName);
    headerFieldsOrder.add(featureDisplay);
    headerFieldsOrder.add(customerID);
    for (AlternateID alternateID : Deployment.getAlternateIDs().values())
      {
        headerFieldsOrder.add(alternateID.getName());
      }
    headerFieldsOrder.add(creationDate);
    headerFieldsOrder.add(deliveryDate);
    headerFieldsOrder.add(originatingDeliveryRequestID);
    headerFieldsOrder.add(deliveryRequestID);
    headerFieldsOrder.add(deliveryStatus);
    headerFieldsOrder.add(eventID);
    headerFieldsOrder.add(returnCode);
    headerFieldsOrder.add(returnCodeDescription);
    headerFieldsOrder.add(returnCodeDetails);
    headerFieldsOrder.add(source);
  }

  @Override public void dumpLineToCsv(Map<String, Object> lineMap, ZipOutputStream writer, boolean addHeaders)
  {
    try
      {
        if (addHeaders)
          {
            addHeaders(writer, headerFieldsOrder, 1);
          }
        String line = ReportUtils.formatResult(headerFieldsOrder, lineMap);
        log.trace("Writing to csv file : " + line);
        writer.write(line.getBytes());
        writer.write("\n".getBytes());
      } 
    catch (IOException e)
      {
        e.printStackTrace();
      }
  }

  public Map<String, List<Map<String, Object>>> getSplittedReportElementsForFile(ReportElement reportElement)
  {
    Map<String, List<Map<String, Object>>> result = new LinkedHashMap<String, List<Map<String, Object>>>();
    Map<String, Object> notifFields = reportElement.fields.get(0);
    LinkedHashMap<String, Object> notifRecs = new LinkedHashMap<>();
    if (notifFields != null && !notifFields.isEmpty())
      {

        if (notifFields.get(subscriberID) != null)
          {
            Object subscriberIDField = notifFields.get(subscriberID);
            notifRecs.put(customerID, subscriberIDField);
            notifFields.remove(subscriberID);
          }
        for (AlternateID alternateID : Deployment.getAlternateIDs().values())
          {
            if (notifFields.get(alternateID.getID()) != null)
              {
                Object alternateId = notifFields.get(alternateID.getID());
                notifRecs.put(alternateID.getName(), alternateId);
              }
          }

        // Compute featureName and ModuleName from ID

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
                    try
                    {
                      Date date = parseSDF1.parse(creationDateStr);
                      notifRecs.put(creationDate, ReportsCommonCode.getDateString(date)); // replace with new value
                    } catch (ParseException e1)
                    {
                      // Could also be 2019-11-27 15:39:30.276+0100
                      try
                      {
                        Date date = parseSDF2.parse(creationDateStr);
                        notifRecs.put(creationDate, ReportsCommonCode.getDateString(date)); // replace with new value
                      } catch (ParseException e2)
                      {
                        log.info("Unable to parse " + creationDateStr);
                      }
                    }

                  } else
                    {
                      log.info(creationDate + " is of wrong type : " + creationDateObj.getClass().getName());
                    }
              } else
                {
                  notifRecs.put(creationDate, "");
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
                    try
                    {
                      Date date = parseSDF1.parse(deliveryDateStr);
                      notifRecs.put(deliveryDate, ReportsCommonCode.getDateString(date)); // replace with new value
                    } catch (ParseException e1)
                    {
                      // Could also be 2019-11-27 15:39:30.276+0100
                      try
                      {
                        Date date = parseSDF2.parse(deliveryDateStr);
                        notifRecs.put(deliveryDate, ReportsCommonCode.getDateString(date)); // replace with new value
                      } catch (ParseException e2)
                      {
                        log.info("Unable to parse " + deliveryDateStr);
                      }
                    }

                  } else
                    {
                      log.info(deliveryDate + " is of wrong type : " + deliveryDateObj.getClass().getName());
                    }
              } else
                {
                  notifRecs.put(deliveryDate, "");
                }
          }

        if (notifFields.containsKey(originatingDeliveryRequestID))
          {
            notifRecs.put(originatingDeliveryRequestID, notifFields.get(originatingDeliveryRequestID));
          }
        if (notifFields.containsKey(deliveryRequestID))
          {
            notifRecs.put(deliveryRequestID, notifFields.get(deliveryRequestID));
          }
        if (notifFields.containsKey(deliveryStatus))
          {
            notifRecs.put(deliveryStatus, notifFields.get(deliveryStatus));
          }
        if (notifFields.containsKey(eventID))
          {
            notifRecs.put(eventID, notifFields.get(eventID));
          }
        if (notifFields.containsKey(moduleId) && notifFields.containsKey(featureId))
          {
            String moduleID = (String) notifFields.get(moduleId);
            Module module = null;
            if (moduleID != null)
              {
                module = Module.fromExternalRepresentation(moduleID);
              } else
              {
                module = Module.Unknown;
              }
            String feature = DeliveryRequest.getFeatureDisplay(module, String.valueOf(notifFields.get(featureId).toString()), journeyService, offerService, loyaltyProgramService);
            notifRecs.put(featureDisplay, feature);
            notifRecs.put(moduleName, module.toString());
            notifRecs.put(featureId, notifFields.get(featureId));
            notifRecs.put(moduleId, notifFields.get(moduleId));

            notifFields.remove(featureId);
            notifFields.remove(moduleId);
          }
        if (notifFields.containsKey(returnCode))
          {
            Object code = notifFields.get(returnCode);
            notifRecs.put(returnCode, code);
            notifRecs.put(returnCodeDescription, (code != null && code instanceof Integer) ? RESTAPIGenericReturnCodes.fromGenericResponseCode((int) code).getGenericResponseMessage() : "");
            notifRecs.put(returnCodeDetails, notifFields.get(returnCodeDetails));
          }
        if (notifFields.containsKey(source))
          {
            notifRecs.put(source, notifFields.get(source));
          }

        //
        // result
        //

        String rawEventDateTime = notifRecs.get(creationDate) == null ? null : notifRecs.get(creationDate).toString();
        if (rawEventDateTime == null) log.warn("bad EventDateTime -- report will be generated in 'null' file name -- for record {} ", notifFields);
        String evntDate = getEventDate(rawEventDateTime);
        if (result.containsKey(evntDate))
          {
            result.get(evntDate).add(notifRecs);
          } 
        else
          {
            List<Map<String, Object>> elements = new ArrayList<Map<String, Object>>();
            elements.add(notifRecs);
            result.put(evntDate, elements);
          }
      }
    return result;
  }
  
  private String getEventDate(String rawEventDateTime)
  {
    String result = "null";
    if (rawEventDateTime == null || rawEventDateTime.trim().isEmpty()) return result;
    String eventDateTimeFormat = "yyyy-MM-dd";
    result = rawEventDateTime.substring(0, eventDateTimeFormat.length());
    return result;
  }

  public static void main(String[] args)
  {
    log.info("received " + args.length + " args");
    for (String arg : args)
      {
        log.info("NotificationReportCsvWriter: arg " + arg);
      }

    if (args.length < 3)
      {
        log.warn("Usage : NotificationReportCsvWriter <KafkaNode> <topic in> <csvfile>");
        return;
      }
    String kafkaNode = args[0];
    String topic = args[1];
    String csvfile = args[2];
    log.info("Reading data from " + topic + " topic on broker " + kafkaNode + " producing " + csvfile + " with '" + CSV_SEPARATOR + "' separator");
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

    if (!reportWriter.produceReport(csvfile, true))
      {
        log.warn("An error occured, the report might be corrupted");
        return;
      }
  }

  private void addHeaders(ZipOutputStream writer, List<String> headers, int offset) throws IOException
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
