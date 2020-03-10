/****************************************************************************
 *
 *  TokenReportCsvWriter.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports.tokenOffer;

import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.*;
import com.evolving.nglm.evolution.SubscriberProfileService.EngineSubscriberProfileService;
import com.evolving.nglm.evolution.Token.TokenStatus;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportCsvWriter;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.zip.ZipOutputStream;

/**
 * This implements the phase 3 for the Subscriber report.
 *
 */
public class TokenOfferReportCsvWriter implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(TokenOfferReportCsvWriter.class);
  final private static String CSV_SEPARATOR = ReportUtils.getSeparator();
  private boolean addHeaders = true;

  private final static String subscriberID = "subscriberID";
  private final static String customerID = "customerID";

  // private static List<String> allDimensions = new ArrayList<>();

  private static SubscriberProfileService subscriberProfileService;
  private static SalesChannelService salesChannelService;
  private static OfferService offerService = null;
  private static String subscriberProfileEndpoints = com.evolving.nglm.evolution.Deployment
      .getSubscriberProfileEndpoints();

  public TokenOfferReportCsvWriter()
  {

  }

  /****************************************
   *
   * dumpElementToCsv
   *
   ****************************************/

  /**
   * This methods writes a single {@link ReportElement} to the report (csv
   * file).
   * 
   * @throws IOException in case anything goes wrong while writing to the
   * report.
   */
  public void dumpElementToCsv(String key, ReportElement re, ZipOutputStream writer) throws IOException
  {
    if (re.type == ReportElement.MARKER) // We will find markers in the topic
      return;
    log.trace("We got " + key + " " + re);

    LinkedHashMap<String, Object> result = new LinkedHashMap<>();
    Map<String, Object> elasticFields = re.fields.get(TokenOfferReportObjects.index_Subscriber);

    /*
     * Map<String,CriterionField> presentationCriterionFields = new
     * LinkedHashMap<String,CriterionField>(); presentationCriterionFields =
     * com.evolving.nglm.evolution.Deployment.getPresentationCriterionFields();
     * for(String keySet : presentationCriterionFields.keySet() ) {
     * log.info("The key set" + keySet); }
     */

    if (elasticFields != null)
      {
        /*
         * if (elasticFields.get(subscriberID) != null) { result.put(customerID,
         * elasticFields.get(subscriberID)); for (AlternateID alternateID :
         * Deployment.getAlternateIDs().values()) { if
         * (elasticFields.get(alternateID.getESField()) != null) { Object
         * alternateId = elasticFields.get(alternateID.getESField());
         * result.put(alternateID.getName(), alternateId); } } }
         */

        try
          {

            String subscriber = (String) elasticFields.get(subscriberID);

            Date now = SystemTime.getCurrentTime();
            SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriber, false,
                false);

            if (subscriberProfile == null)
              {
                result.put("responseCode", "CustomerNotFound");
              }
            else
              {
                // List<JSONObject> tokensJson;
                List<Token> tokens = subscriberProfile.getTokens();

                if (tokens.size() != 0)
                  {
                    for (Token token : tokens)
                      {

                        Token subscriberToken = token;
                        DNBOToken subscriberStoredToken = (DNBOToken) subscriberToken;
                        List<String> presentedOfferIds = subscriberStoredToken.getPresentedOfferIDs();
                        if (token.getTokenStatus() == TokenStatus.New)
                          {
                            result.put(customerID, elasticFields.get(subscriberID));
                            for (AlternateID alternateID : Deployment.getAlternateIDs().values())
                              {
                                if (elasticFields.get(alternateID.getESField()) != null)
                                  {
                                    Object alternateId = elasticFields.get(alternateID.getESField());

                                    result.put(alternateID.getName(), alternateId);

                                  }
                              }
                            result.put("tokenCode", token.getTokenCode());
                            result.put("voucherCode", "");
                            result.put("offerName", "");
                            result.put("offerStatus", "");                            
                            result.put("offerRank", "");
                            result.put("allocationDate", "");
                            result.put("redeemedDate", "");  
                            result.put("salesChannel", "");
                            if (addHeaders)
                              {
                                addHeaders(writer, result.keySet(), 1);
                              }

                            String line = ReportUtils.formatResult(result);
                            log.trace("Writing to csv file : " + line);
                            writer.write(line.getBytes());
                            writer.write("\n".getBytes());

                          }
                        else if (token.getTokenStatus() == TokenStatus.Bound)
                          {
                            String salesChannel = subscriberStoredToken.getPresentedOffersSalesChannel();
                            if (presentedOfferIds.size() == 0)
                              {
                                result.put(customerID, elasticFields.get(subscriberID));
                                for (AlternateID alternateID : Deployment.getAlternateIDs().values())
                                  {
                                    if (elasticFields.get(alternateID.getESField()) != null)
                                      {
                                        Object alternateId = elasticFields.get(alternateID.getESField());

                                        result.put(alternateID.getName(), alternateId);

                                      }
                                  }
                                result.put("tokenCode", token.getTokenCode());
                                result.put("voucherCode", "");
                                result.put("offerName", "");
                                result.put("offerStatus", "");                                
                                result.put("offerRank", "");
                                result.put("allocationDate", "");
                                result.put("redeemedDate", "");
                                result.put("salesChannel", "");
                                
                                if (addHeaders)
                                  {
                                    addHeaders(writer, result.keySet(), 1);
                                  }
                                String line = ReportUtils.formatResult(result);
                                log.trace("Writing to csv file : " + line);
                                writer.write(line.getBytes());
                                writer.write("\n".getBytes());
                              }
                            else
                              {
                                for (String presentedOfferId : presentedOfferIds)
                                  {
                                    result.put(customerID, elasticFields.get(subscriberID));
                                    for (AlternateID alternateID : Deployment.getAlternateIDs().values())
                                      {
                                        if (elasticFields.get(alternateID.getESField()) != null)
                                          {
                                            Object alternateId = elasticFields.get(alternateID.getESField());

                                            result.put(alternateID.getName(), alternateId);

                                          }
                                      }
                                    result.put("tokenCode", token.getTokenCode());
                                    result.put("voucherCode", "");
                                    if (offerService.getStoredOffer(presentedOfferId) != null)
                                      {
                                        result.put("offerName",
                                            offerService.getStoredOffer(presentedOfferId).getGUIManagedObjectDisplay());
                                      }
                                    else
                                      {
                                        result.put("offerName", "");
                                      }
                                    result.put("offerStatus", "Allocated");
                                   
                                    result.put("offerRank", presentedOfferIds.indexOf(presentedOfferId) + 1);
                                    if (ReportsCommonCode.getDateString(
                                        offerService.getStoredOffer(presentedOfferId).getCreatedDate()) != null)
                                      {
                                        result.put("allocationDate", ReportsCommonCode.getDateString(
                                            offerService.getStoredOffer(presentedOfferId).getCreatedDate()));
                                      }
                                    else
                                      {
                                        result.put("allocationDate", "");
                                      }
                                    if (ReportsCommonCode.getDateString(
                                        offerService.getStoredOffer(presentedOfferId).getEffectiveStartDate()) != null)
                                      {
                                        result.put("redeemed Date", ReportsCommonCode.getDateString(
                                            offerService.getStoredOffer(presentedOfferId).getEffectiveStartDate()));
                                      }
                                    else
                                      {
                                        result.put("redeemedDate", "");
                                      }
                                    if (salesChannel == null)
                                      {
                                        result.put("salesChannel", "");
                                      }
                                    else
                                      {
                                        if (salesChannelService.getStoredSalesChannel(salesChannel) != null)
                                          {
                                            result.put("salesChannel", salesChannelService
                                                .getStoredSalesChannel(salesChannel).getGUIManagedObjectDisplay());
                                          }
                                        else
                                          {
                                            result.put("salesChannel", "");
                                          }
                                      }
                                  
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
                          }
                        else if (token.getTokenStatus() == TokenStatus.Redeemed)
                          {
                            result.put(customerID, elasticFields.get(subscriberID));
                            for (AlternateID alternateID : Deployment.getAlternateIDs().values())
                              {
                                if (elasticFields.get(alternateID.getESField()) != null)
                                  {
                                    Object alternateId = elasticFields.get(alternateID.getESField());

                                    result.put(alternateID.getName(), alternateId);

                                  }
                              }
                            result.put("tokenCode", token.getTokenCode());
                            result.put("voucherCode", "");
                            String acceptedOfferId = subscriberStoredToken.getAcceptedOfferID();
                            String salesChannel = subscriberStoredToken.getPresentedOffersSalesChannel();
                            if (acceptedOfferId == null)
                              {

                                result.put("offerName", "");
                                result.put("offerStatus", "");                               
                                result.put("offerRank", "");
                                result.put("allocationDate", "");
                                result.put("redeemedDate", "");
                                result.put("salesChannel", "");
                                
                              }
                            else
                              {
                                if (offerService.getStoredOffer(acceptedOfferId) != null)
                                  {
                                    result.put("offerName",
                                        offerService.getStoredOffer(acceptedOfferId).getGUIManagedObjectDisplay());
                                  }
                                else
                                  {
                                    result.put("offerName", "");
                                  }
                                result.put("offerStatus", "Accepted");
                               
                                result.put("offerRank", presentedOfferIds.indexOf(acceptedOfferId) + 1);
                                if (ReportsCommonCode.getDateString(
                                    offerService.getStoredOffer(acceptedOfferId).getCreatedDate()) != null)
                                  {
                                    result.put("allocationDate", ReportsCommonCode
                                        .getDateString(offerService.getStoredOffer(acceptedOfferId).getCreatedDate()));
                                  }
                                else
                                  {
                                    result.put("allocationDate", "");
                                  }
                                if (ReportsCommonCode.getDateString(
                                    offerService.getStoredOffer(acceptedOfferId).getEffectiveStartDate()) != null)
                                  {
                                    result.put("redeemedDate", ReportsCommonCode.getDateString(
                                        offerService.getStoredOffer(acceptedOfferId).getEffectiveStartDate()));
                                  }
                                else
                                  {
                                    result.put("redeemedDate", "");
                                  }
                                if (salesChannel == null)
                                  {
                                    result.put("salesChannel", "");
                                  }
                                else
                                  {
                                    if (salesChannelService.getStoredSalesChannel(salesChannel) != null)
                                      {
                                        result.put("salesChannel", salesChannelService
                                            .getStoredSalesChannel(salesChannel).getGUIManagedObjectDisplay());
                                      }
                                    else
                                      {
                                        result.put("salesChannel", "");
                                      }
                                  }
                                

                              }
                            if (addHeaders)
                              {
                                addHeaders(writer, result.keySet(), 1);
                              }
                            String line = ReportUtils.formatResult(result);
                            log.trace("Writing to csv file : " + line);
                            writer.write(line.getBytes());
                            writer.write("\n".getBytes());

                          }
                        else if (token.getTokenStatus() == TokenStatus.Expired)
                          {
                            result.put(customerID, elasticFields.get(subscriberID));
                            for (AlternateID alternateID : Deployment.getAlternateIDs().values())
                              {
                                if (elasticFields.get(alternateID.getESField()) != null)
                                  {
                                    Object alternateId = elasticFields.get(alternateID.getESField());

                                    result.put(alternateID.getName(), alternateId);

                                  }
                              }
                            result.put("tokenCode", token.getTokenCode());
                            result.put("vocherCode", "");
                            result.put("offerName", "");
                            result.put("offerStatus", "");                           
                            result.put("offerRank", "");
                            result.put("allocationDate", "");
                            result.put("redeemedDate", "");
                            result.put("salesChannel", "");
                            
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
                  }
              }
          }
        catch (Exception e)
          {
            log.error("unable to process request", e.getMessage());
          }

      }
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
        addHeaders = false;
      }
  }

  /****************************************
   *
   * initSegmentationData
   *
   ****************************************/

  /*
   * private static void initSegmentationData() { // segmentID ->
   * [dimensionName, segmentName] for (GUIManagedObject dimension :
   * segmentationDimensionService.getStoredSegmentationDimensions()) { if
   * (dimension instanceof SegmentationDimension) { SegmentationDimension
   * segmentation = (SegmentationDimension) dimension; //
   * allDimensions.add(segmentation.getSegmentationDimensionName());
   * allDimensionsMap.put(segmentation.getSegmentationDimensionName(), ""); if
   * (segmentation.getSegments() != null) { for (Segment segment :
   * segmentation.getSegments()) { String[] segmentInfo = new String[2];
   * segmentInfo[INDEX_DIMENSION_NAME] =
   * segmentation.getSegmentationDimensionName();
   * segmentInfo[INDEX_SEGMENT_NAME] = segment.getName();
   * segmentsNames.put(segment.getID(), segmentInfo); } } } } }
   */

  /****************************************
   *
   * main
   *
   ****************************************/

  public static void main(String[] args)
  {
    log.info("received " + args.length + " args");
    for (String arg : args)
      {
        log.info("TokenReportESReader: arg " + arg);
      }

    if (args.length < 3)
      {
        log.warn("Usage : TokenReportCsvWriter <KafkaNode> <topic in> <csvfile>");
        return;
      }
    String kafkaNode = args[0];
    String topic = args[1];
    String csvfile = args[2];
    log.info("Reading data from " + topic + " topic on broker " + kafkaNode + " producing " + csvfile + " with '"
        + CSV_SEPARATOR + "' separator");

    /*
     * segmentationDimensionService = new
     * SegmentationDimensionService(kafkaNode,
     * "guimanager-segmentationDimensionservice-" + topic,
     * com.evolving.nglm.evolution.Deployment.getSegmentationDimensionTopic(),
     * false); segmentationDimensionService.start();
     */

    subscriberProfileService = new EngineSubscriberProfileService(subscriberProfileEndpoints);
    subscriberProfileService.start();

    offerService = new OfferService(kafkaNode, "offerReportDriver-offerService-" + topic,
        com.evolving.nglm.evolution.Deployment.getOfferTopic(), false);
    offerService.start();

    salesChannelService = new SalesChannelService(com.evolving.nglm.evolution.Deployment.getBrokerServers(),
        "offerReportDriver-saleschannelservice-" + topic, com.evolving.nglm.evolution.Deployment.getSalesChannelTopic(),
        false);
    salesChannelService.start();

    //
    // build map of segmentID -> [dimensionName, segmentName] once for all
    //

    // initSegmentationData();
    // initProfileFields();
    /*
     * if (allProfileFields.isEmpty()) { log.
     * warn("Cannot find any profile field in configuration, no report produced"
     * ); return; }
     */

    ReportCsvFactory reportFactory = new TokenOfferReportCsvWriter();
    ReportCsvWriter reportWriter = new ReportCsvWriter(reportFactory, kafkaNode, topic);
    if (!reportWriter.produceReport(csvfile))
      {
        log.warn("An error occured, the report might be corrupted");
        return;
      }
  }

  /*
   * private static void initProfileFields() { for (CriterionField field :
   * Deployment.getProfileCriterionFields().values()) { if (field.getESField()
   * != null) { allProfileFields.add(field.getESField()); } } }
   */

}
