/****************************************************************************
 *
 *  TokenReportCsvWriter.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports.token;

import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.*;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.SubscriberProfileService.EngineSubscriberProfileService;
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
public class TokenReportCsvWriter implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(TokenReportCsvWriter.class);
  final private static String CSV_SEPARATOR = ReportUtils.getSeparator();

  private final static String subscriberID = "subscriberID";
  private final static String customerID = "customerID";

  // private static List<String> allDimensions = new ArrayList<>();

  private static SubscriberProfileService subscriberProfileService;
  private static OfferService offerService = null;
  private static TokenTypeService tokenTypeservice = null;
  private static PresentationStrategyService presentationStrategyService = null;
  private static ScoringStrategyService scoringStrategyService = null;
  private static JourneyService journeyService = null;
  private static LoyaltyProgramService loyaltyProgramService = null;
  private static String subscriberProfileEndpoints = com.evolving.nglm.evolution.Deployment
      .getSubscriberProfileEndpoints();

  public TokenReportCsvWriter()
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
  public boolean dumpElementToCsv(String key, ReportElement re, ZipOutputStream writer, boolean addHeaders) throws IOException
  {
    if (re.type == ReportElement.MARKER) // We will find markers in the topic
      return true;
    log.trace("We got " + key + " " + re);

    LinkedHashMap<String, Object> result = new LinkedHashMap<>();
    Map<String, Object> elasticFields = re.fields.get(TokenReportObjects.index_Subscriber);

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
                List<Token> tokens = subscriberProfile.getTokens();
                if (tokens.size() != 0)
                  {
                    for (Token token : tokens)
                      {
                        Token subscriberToken = token;
                        DNBOToken subscriberStoredToken = (DNBOToken) subscriberToken;
                        String presentationStrategyId = subscriberStoredToken.getPresentationStrategyID();
                        String tokenTypeId = token.getTokenTypeID();
                        String acceptedOfferId = subscriberStoredToken.getAcceptedOfferID();
                        List<String> scoringStrategyList = subscriberStoredToken.getScoringStrategyIDs();
                        List<String> scoringStrategy = new ArrayList<>();
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
                        result.put("tokenType", tokenTypeId);
                        if (presentationStrategyId == null)
                          {
                            result.put("presentationStrategy", "");
                          }
                        else
                          {
                            if (presentationStrategyService
                                .getStoredPresentationStrategy(presentationStrategyId) != null)
                              result.put("presentationStrategy", presentationStrategyService
                                  .getStoredPresentationStrategy(presentationStrategyId).getGUIManagedObjectDisplay());
                          }
                        if (scoringStrategyList.size() == 0)
                          {
                            result.put("scoringStrategy", "");
                          }
                        else
                          {

                            for (String scoringStrategyId : scoringStrategyList)

                              {
                                if (scoringStrategyService.getStoredScoringStrategy(scoringStrategyId) != null)
                                  {

                                    scoringStrategy.add(scoringStrategyService
                                        .getStoredScoringStrategy(scoringStrategyId).getGUIManagedObjectDisplay());

                                  }
                              }
                            if (scoringStrategy.size() != 0)
                              {
                                result.put("scoringStrategy", scoringStrategy);
                              }
                            else
                              {
                                result.put("scoringStrategy", "");
                              }
                          }
                        if (ReportsCommonCode.getDateString(token.getCreationDate()) == null)
                          {
                            result.put("creationDate", "");
                          }
                        else
                          {
                            result.put("creationDate", ReportsCommonCode.getDateString(token.getCreationDate()));
                          }

                        if (ReportsCommonCode.getDateString(subscriberStoredToken.getTokenExpirationDate()) == null)
                          {
                            result.put("expirationDate", "");
                          }
                        else
                          {
                            result.put("expirationDate",
                                ReportsCommonCode.getDateString(token.getTokenExpirationDate()));
                          }

                        if (token.getTokenStatus() == null)
                          {
                            result.put("tokenStatus", "");
                          }
                        else
                          {
                            result.put("tokenStatus", token.getTokenStatus());
                          }
                        if (ReportsCommonCode.getDateString(subscriberStoredToken.getBoundDate()) == null)
                          {
                            result.put("lastAllocationDate", "");
                          }
                        else
                          {
                            result.put("lastAllocationDate",
                                ReportsCommonCode.getDateString(subscriberStoredToken.getBoundDate()));
                          }
                        if (subscriberStoredToken.getBoundCount() == 0)
                          {
                            result.put("qtyAllocations", "");
                          }
                        else
                          {
                            result.put("qtyAllocations", subscriberStoredToken.getBoundCount());
                          }
                        if (subscriberStoredToken.getPresentedOfferIDs().size() == 0)
                          {
                            result.put("qtyAllocatedOffers", "");
                          }
                        else
                          {
                            result.put("qtyAllocatedOffers", subscriberStoredToken.getPresentedOfferIDs().size());
                          }

                        if (ReportsCommonCode.getDateString(subscriberStoredToken.getRedeemedDate()) == null)
                          {
                            result.put("redeemedDate", "");
                          }
                        else
                          {
                            result.put("redeemedDate", ReportsCommonCode.getDateString(token.getRedeemedDate()));
                          }

                        if (acceptedOfferId == null)
                          {
                            result.put("acceptedOffer", "");
                          }
                        else
                          {
                            if (offerService.getStoredOffer(acceptedOfferId) != null)
                              {
                                result.put("acceptedOffer",
                                    offerService.getStoredOffer(acceptedOfferId).getGUIManagedObjectDisplay());
                              }
                            else
                              {
                                result.put("acceptedOffer", "");
                              }
                          }
                                     
                        if (subscriberStoredToken.getModuleID() == null)
                          {
                            result.put("Module", "");
                          }
                        else
                          {
                            Module module = Module
                                .fromExternalRepresentation(String.valueOf(subscriberStoredToken.getModuleID()));
                            result.put("module", module);
                          }
                        if (subscriberStoredToken.getFeatureID() == null || subscriberStoredToken.getModuleID() == null)
                          {
                            result.put("featureName", "");
                          }
                        else
                          {
                            Module module = Module
                                .fromExternalRepresentation(String.valueOf(subscriberStoredToken.getModuleID()));
                            String feature_display = DeliveryRequest.getFeatureDisplay(module,
                                String.valueOf(subscriberStoredToken.getFeatureID()), journeyService, offerService,
                                loyaltyProgramService);   
                            result.put("featureName", feature_display);
                          }
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
        catch (Exception e)
          {
            log.error("unable to process request", e.getMessage());
          }
      }
    return addHeaders;
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

    scoringStrategyService = new ScoringStrategyService(Deployment.getBrokerServers(),
        "offerReport-scoringstrategyservice-" + topic, com.evolving.nglm.evolution.Deployment.getScoringStrategyTopic(),
        false);
    scoringStrategyService.start();

    presentationStrategyService = new PresentationStrategyService(kafkaNode,
        "offerReport-presentationstrategyservice-" + topic,
        com.evolving.nglm.evolution.Deployment.getPresentationStrategyTopic(), false);
    presentationStrategyService.start();

    tokenTypeservice = new TokenTypeService(kafkaNode, "offerReport-tokentypeservice-" + topic,
        com.evolving.nglm.evolution.Deployment.getTokenTypeTopic(), false);
    tokenTypeservice.start();
    journeyService = new JourneyService(kafkaNode, "bdrreportcsvwriter-journeyservice-" + topic, com.evolving.nglm.evolution.Deployment.getJourneyTopic(), false);
    loyaltyProgramService = new LoyaltyProgramService(kafkaNode, "bdrreportcsvwriter-loyaltyprogramservice-" + topic,
        com.evolving.nglm.evolution.Deployment.getLoyaltyProgramTopic(), false);
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

    ReportCsvFactory reportFactory = new TokenReportCsvWriter();
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
