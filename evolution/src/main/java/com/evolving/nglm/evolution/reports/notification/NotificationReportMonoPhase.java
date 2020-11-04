package com.evolving.nglm.evolution.reports.notification;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.DeliveryRequest;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.DialogTemplate;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.MailTemplate;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.PushTemplate;
import com.evolving.nglm.evolution.RESTAPIGenericReturnCodes;
import com.evolving.nglm.evolution.SMSTemplate;
import com.evolving.nglm.evolution.SubscriberMessageTemplate;
import com.evolving.nglm.evolution.SubscriberMessageTemplateService;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.MailNotificationManager.MailNotificationManagerRequest;
import com.evolving.nglm.evolution.NotificationManager.NotificationManagerRequest;
import com.evolving.nglm.evolution.SMSNotificationManager.SMSNotificationManagerRequest;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportCsvWriter;
import com.evolving.nglm.evolution.reports.ReportEsReader;
import com.evolving.nglm.evolution.reports.ReportMonoPhase;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.evolution.reports.ReportEsReader.PERIOD;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;
import com.evolving.nglm.evolution.reports.loyaltyprogramcustomerstate.LoyaltyProgramCustomerStatesMonoPhase;
import com.evolving.nglm.evolution.reports.odr.ODRReportMonoPhase;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.zip.ZipOutputStream;

public class NotificationReportMonoPhase implements ReportCsvFactory
{

  private static final Logger log = LoggerFactory.getLogger(NotificationReportMonoPhase.class);
  private static final DateFormat DATE_FORMAT;
  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();

  private JourneyService journeyService;
  private OfferService offerService;
  private LoyaltyProgramService loyaltyProgramService;
  private SubscriberMessageTemplateService subscriberMessageTemplateService;

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
  private static final String communicationChannel = "communicationChannel";
  
  private static SimpleDateFormat parseSDF1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
  private static SimpleDateFormat parseSDF2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSXX");

  private static final String messageContent = "messageContent";
  private static final String templateID = "templateID";
  private static final String language = "language";
  
  private static List<String> headerFieldsOrder = new LinkedList<String>();
  static
  {
    DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));

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
    headerFieldsOrder.add(messageContent);
    headerFieldsOrder.add(communicationChannel);
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
      } 
    catch (IOException e)
      {
        e.printStackTrace();
      }
  }

  public Map<String, List<Map<String, Object>>> getSplittedReportElementsForFileMono(Map<String, Object> map)
  {
    Map<String, List<Map<String, Object>>> result = new LinkedHashMap<String, List<Map<String, Object>>>();
    Map<String, Object> notifFields = map;
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
        if (notifFields.containsKey(templateID))
          {
            Map<String, Object> msgContentJSON = new LinkedHashMap<>(); // to preserve order when displaying
            String tempID = notifFields.get(templateID).toString();
            String lang = notifFields.containsKey(language) ? notifFields.get(language).toString() : "";
            SubscriberMessageTemplate templateObject = subscriberMessageTemplateService.getActiveSubscriberMessageTemplate(tempID, SystemTime.getCurrentTime());
            // get actual text based on the template kind
            if (templateObject instanceof SMSTemplate)
              {
                Map<String,Object> tags = getAllTags(notifFields);
                List<String> tagsList = getTags(tags, "tags");
                SMSNotificationManagerRequest req = new SMSNotificationManagerRequest(tempID, lang, tagsList);
                String actualMessage = req.getText(subscriberMessageTemplateService);
                msgContentJSON.put("sms", truncateIfNecessary(actualMessage));
              }
            else if (templateObject instanceof MailTemplate)
              {
                Map<String,Object> tags = getAllTags(notifFields);
                List<String> subjectTagsList = getTags(tags, "subjectTags");
                List<String> textBodyTagsList = getTags(tags, "textBodyTags");
                List<String> htmlBodyTagsList = getTags(tags, "htmlBodyTags");
                MailNotificationManagerRequest req = new MailNotificationManagerRequest(tempID, lang, subjectTagsList, textBodyTagsList, htmlBodyTagsList);
                String actualSubject = req.getSubject(subscriberMessageTemplateService);
                String actualTextBody = req.getTextBody(subscriberMessageTemplateService);
                String actualHtmlBody = req.getHtmlBody(subscriberMessageTemplateService);
                msgContentJSON.put("subject", truncateIfNecessary(actualSubject));
                msgContentJSON.put("textBody", truncateIfNecessary(actualTextBody));
                msgContentJSON.put("htmlBody", truncateIfNecessary(actualHtmlBody));
              }
            else if (templateObject instanceof PushTemplate)
              {
                // TODO
                log.info("Not Yet Implemented, template class : " + templateObject.getClass().getCanonicalName());
              }
            else // GenericTemplate
              {
                Map<String, List<String>> tags = getAllTagsList(notifFields);
                NotificationManagerRequest req = new NotificationManagerRequest(tempID, lang, tags);
                Map<String, String> resolvedParameters = req.getResolvedParameters(subscriberMessageTemplateService);
                msgContentJSON.putAll(resolvedParameters);
              }
            for (String key : msgContentJSON.keySet())
              {
                String value = (String) msgContentJSON.get(key);

                boolean hasNewline = value.contains("\n");
                if (hasNewline)
                  {
                    value = value.replace("\n", " ");
                  }
                msgContentJSON.put(key, value);
              }          
            notifRecs.put(messageContent, ReportUtils.formatJSON(msgContentJSON));
          }
        
        if (notifFields.containsKey(templateID))
          {
            String tempID = notifFields.get(templateID).toString();
            SubscriberMessageTemplate templateObject = subscriberMessageTemplateService
                .getActiveSubscriberMessageTemplate(tempID, SystemTime.getCurrentTime());
            
            if (templateObject instanceof SMSTemplate)
              {
                notifRecs.put("communicationChannel", "SMS");
              }
            else if (templateObject instanceof MailTemplate)
              {
                notifRecs.put("communicationChannel", "EMAIL");
              }
            else if (templateObject instanceof PushTemplate)
              {
                PushTemplate template = (PushTemplate) subscriberMessageTemplateService
                    .getActiveSubscriberMessageTemplate(tempID, SystemTime.getCurrentTime());
                notifRecs.put("communicationChannel",
                    Deployment.getCommunicationChannels().get(template.getCommunicationChannelID()).getDisplay());

              }
            else if (templateObject instanceof DialogTemplate)// GenericTemplate
              {
                DialogTemplate template = (DialogTemplate) subscriberMessageTemplateService
                    .getActiveSubscriberMessageTemplate(tempID, SystemTime.getCurrentTime());
                String channelID = template.getCommunicationChannelID();
                notifRecs.put("communicationChannel",
                    Deployment.getCommunicationChannels().get(channelID).getDisplay());
              }

          }
        else
          {
            notifRecs.put("communicationChannel", "");
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
  
  private String truncateIfNecessary(String actualMessage)
  {
    String res = actualMessage;
    int messageMaxLength = Deployment.getReportManagerMaxMessageLength();
    if (actualMessage.length() > messageMaxLength)
      {
        res = actualMessage.substring(0, messageMaxLength) + "...";
      }
    return res;
  }

  @SuppressWarnings("unchecked")
  private List<String> getTags(Map<String, Object> notifFields, String whichTag)
  {
    Object tagsObj = notifFields.get(whichTag);
    List<String> tagsList = null;
    if (tagsObj instanceof List<?>)
      {
        tagsList = (List<String>) tagsObj;
      }
    else
      {
        tagsList = new ArrayList<>();
      }
    return tagsList;
  }

  @SuppressWarnings("unchecked")
  private Map<String,Object> getAllTags(Map<String, Object> notifFields)
  {
    Object tagsObj = notifFields.get("tags");
    Map<String,Object> tagsList = null;
    if (tagsObj instanceof Map<?,?>)
      {
        tagsList = (Map<String,Object>) tagsObj;
      }
    else
      {
        tagsList = new HashMap<>();
      }
    return tagsList;
  }

  @SuppressWarnings("unchecked")
  private Map<String,List<String>> getAllTagsList(Map<String, Object> notifFields)
  {
    Object tagsObj = notifFields.get("tags");
    Map<String,List<String>> tagsList = null;
    if (tagsObj instanceof Map<?,?>)
      {
        tagsList = (Map<String,List<String>>) tagsObj;
      }
    else
      {
        tagsList = new HashMap<>();
      }
    return tagsList;
  }

  private String getEventDate(String rawEventDateTime)
  {
    String result = "null";
    if (rawEventDateTime == null || rawEventDateTime.trim().isEmpty()) return result;
    String eventDateTimeFormat = "yyyy-MM-dd";
    result = rawEventDateTime.substring(0, eventDateTimeFormat.length());
    return result;
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

  public static void main(String[] args, final Date reportGenerationDate)
  {
    NotificationReportMonoPhase notificationReportMonoPhase = new NotificationReportMonoPhase();
    notificationReportMonoPhase.start(args, reportGenerationDate);
  }
  
  private void start(String[] args, final Date reportGenerationDate)
  {
    log.info("received " + args.length + " args");
    for (String arg : args)
      {
        log.info("NotificationReportMonoPhase: arg " + arg);
      }

    if (args.length < 3)
      {
        log.warn("Usage : NotificationReportMonoPhase <ESNode> <ES journey index> <csvfile> <defaultReportPeriodQuantity> <defaultReportPeriodUnit>");
        return;
      }
    String esNode       = args[0];
    String esIndexNotif = args[1];
    String csvfile      = args[2];

    Integer reportPeriodQuantity = 0;
    String reportPeriodUnit = null;
    if (args.length > 4 && args[3] != null && args[4] != null)
      {
        reportPeriodQuantity = Integer.parseInt(args[3]);
        reportPeriodUnit = args[4];
      }
    
    Date fromDate = getFromDate(reportGenerationDate, reportPeriodUnit, reportPeriodQuantity);
    Date toDate = reportGenerationDate;
    
    List<String> esIndexDates = getEsIndexDates(fromDate, toDate);
    StringBuilder esIndexNotifList = new StringBuilder();
    boolean firstEntry = true;
    for (String esIndexDate : esIndexDates)
      {
        if (!firstEntry) esIndexNotifList.append(",");
        String indexName = esIndexNotif + esIndexDate;
        esIndexNotifList.append(indexName);
        firstEntry = false;
      }

    log.info("Reading data from ES in (" + esIndexNotifList.toString() + ") indexes and writing to " + csvfile);

    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    esIndexWithQuery.put(esIndexNotifList.toString(), QueryBuilders.matchAllQuery());
    ReportCsvFactory reportFactory = new NotificationReportMonoPhase();


    String journeyTopic = Deployment.getJourneyTopic();
    String offerTopic = Deployment.getOfferTopic();
    String loyaltyProgramTopic = Deployment.getLoyaltyProgramTopic();
    String subscriberMessageTemplateTopic = Deployment.getSubscriberMessageTemplateTopic();
    
    journeyService = new JourneyService(Deployment.getBrokerServers(), "notifreportcsvwriter-journeyservice-NotificationReportMonoPhase", journeyTopic, false);
    offerService = new OfferService(Deployment.getBrokerServers(), "notifreportcsvwriter-offerservice-NotificationReportMonoPhase", offerTopic, false);
    loyaltyProgramService = new LoyaltyProgramService(Deployment.getBrokerServers(), "notifreportcsvwriter-loyaltyprogramservice-NotificationReportMonoPhase", loyaltyProgramTopic, false);
    subscriberMessageTemplateService = new SubscriberMessageTemplateService(Deployment.getBrokerServers(), "notifreportcsvwriter-subscribermessagetemplateservice-NotificationReportMonoPhase", subscriberMessageTemplateTopic, false);
    
    offerService.start();
    journeyService.start();
    loyaltyProgramService.start();
    subscriberMessageTemplateService.start();
    
    ReportMonoPhase reportMonoPhase = new ReportMonoPhase(
        esNode,
        esIndexWithQuery,
        reportFactory,
        csvfile
    );

    if (!reportMonoPhase.startOneToOne(true))
      {
        log.warn("An error occured, the report might be corrupted");
        return;
      }
    offerService.stop();
    journeyService.stop();
    loyaltyProgramService.stop();
    subscriberMessageTemplateService.stop();
    log.info("Finished NotificationReport");
  }
  
  private static List<String> getEsIndexDates(final Date fromDate, Date toDate)
  {
    Date tempfromDate = fromDate;
    List<String> esIndexOdrList = new ArrayList<String>();
    while(tempfromDate.getTime() <= toDate.getTime())
      {
        esIndexOdrList.add(DATE_FORMAT.format(tempfromDate));
        tempfromDate = RLMDateUtils.addDays(tempfromDate, 1, Deployment.getBaseTimeZone());
      }
    return esIndexOdrList;
  }
  
  private static Date getFromDate(final Date reportGenerationDate, String reportPeriodUnit, Integer reportPeriodQuantity)
  {
    reportPeriodQuantity = reportPeriodQuantity == null || reportPeriodQuantity == 0 ? new Integer(1) : reportPeriodQuantity;
    if (reportPeriodUnit == null) reportPeriodUnit = PERIOD.DAYS.getExternalRepresentation();

    //
    //
    //

    Date now = reportGenerationDate;
    Date fromDate = null;
    switch (reportPeriodUnit.toUpperCase())
    {
      case "DAYS":
        fromDate = RLMDateUtils.addDays(now, -reportPeriodQuantity, com.evolving.nglm.core.Deployment.getBaseTimeZone());
        break;

      case "WEEKS":
        fromDate = RLMDateUtils.addWeeks(now, -reportPeriodQuantity, com.evolving.nglm.core.Deployment.getBaseTimeZone());
        break;

      case "MONTHS":
        fromDate = RLMDateUtils.addMonths(now, -reportPeriodQuantity, com.evolving.nglm.core.Deployment.getBaseTimeZone());
        break;

      default:
        break;
    }
    return fromDate;
  }
}
