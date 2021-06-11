/****************************************************************************
 *
 *  TokenReportMonoPhase.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports.token;

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
import com.evolving.nglm.evolution.DeliveryRequest;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.PresentationStrategyService;
import com.evolving.nglm.evolution.ScoringStrategyService;
import com.evolving.nglm.evolution.TokenTypeService;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportMonoPhase;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.evolution.reports.subscriber.SubscriberReportMonoPhase;

public class TokenReportMonoPhase implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(TokenReportMonoPhase.class);
  final private static String CSV_SEPARATOR = ReportUtils.getSeparator();

  private final static String subscriberID = "subscriberID";
  private final static String customerID = "customerID";
  
  private static final String tokenCode = "tokenCode";
  private static final String tokenType = "tokenType";
  private static final String creationDate = "creationDate";
  private static final String expirationDate = "expirationDate";
  private static final String redeemedDate = "redeemedDate";
  private static final String lastAllocationDate = "lastAllocationDate";
  private static final String tokenStatus = "tokenStatus";
  private static final String qtyAllocations = "qtyAllocations";
  private static final String qtyAllocatedOffers = "qtyAllocatedOffers";
  private static final String presentationStrategy = "presentationStrategy";
  private static final String scoringStrategy = "scoringStrategy";
  private static final String acceptedOffer = "acceptedOffer";
  private static final String module = "module";
  private static final String featureName = "featureName";
  
  
  
  static List<String> headerFieldsOrder = new ArrayList<String>();
  static
  {
    headerFieldsOrder.add(customerID);
    for (AlternateID alternateID : Deployment.getAlternateIDs().values())
    {
      if(alternateID.getName().equals("msisdn")) {
      headerFieldsOrder.add(alternateID.getName());}
    }
    headerFieldsOrder.add(tokenCode);
    headerFieldsOrder.add(tokenType);
    headerFieldsOrder.add(creationDate);
    headerFieldsOrder.add(expirationDate);
    headerFieldsOrder.add(redeemedDate);
    headerFieldsOrder.add(lastAllocationDate);
    headerFieldsOrder.add(tokenStatus);
    headerFieldsOrder.add(qtyAllocations);
    headerFieldsOrder.add(qtyAllocatedOffers);
    headerFieldsOrder.add(presentationStrategy);
    headerFieldsOrder.add(scoringStrategy);
    headerFieldsOrder.add(acceptedOffer);
    headerFieldsOrder.add(module);
    headerFieldsOrder.add(featureName);
  }

  private OfferService offerService = null;
  private TokenTypeService tokenTypeService = null;
  private PresentationStrategyService presentationStrategyService = null;
  private ScoringStrategyService scoringStrategyService = null;
  private JourneyService journeyService = null;
  private LoyaltyProgramService loyaltyProgramService = null;
  private int tenantID = 0;

  /****************************************
   *
   * dumpElementToCsv
   *
   ****************************************/

 
  public boolean dumpElementToCsvMono(Map<String,Object> map, ZipOutputStream writer, boolean addHeaders) throws IOException
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
            if (subscriberFields.get("tokens") != null)
              {
                List<Map<String, Object>> tokensArray = (List<Map<String, Object>>) subscriberFields.get("tokens");
                if (!tokensArray.isEmpty())
                  {
                    commonFields.put(customerID, subscriberID);

                    for (AlternateID alternateID : Deployment.getAlternateIDs().values())
                      {
                        if (subscriberFields.get(alternateID.getESField()) != null)
                          {
                            Object alternateId = subscriberFields.get(alternateID.getESField());
                            commonFields.put(alternateID.getName(), alternateId);
                          }
                      }

                    for (int i = 0; i < tokensArray.size(); i++)
                      {
                        result.clear();
                        result.putAll(commonFields);
                        Map<String, Object> token = (Map<String, Object>) tokensArray.get(i);

                        result.put(tokenCode, token.get("tokenCode"));
                        result.put(tokenType, token.get("tokenType"));
                        result.put(creationDate, dateOrEmptyString(token.get("creationDate")));
                        result.put(expirationDate, dateOrEmptyString(token.get("expirationDate")));
                        result.put(redeemedDate, dateOrEmptyString(token.get("redeemedDate")));
                        result.put(lastAllocationDate, dateOrEmptyString(token.get("lastAllocationDate")));
                        result.put(tokenStatus, token.get("tokenStatus"));
                        result.put(qtyAllocations, token.get("qtyAllocations"));
                        result.put(qtyAllocatedOffers, token.get("qtyAllocatedOffers"));

                        GUIManagedObject presentationStrategy = null;
                        if (token.get("presentationStrategyID") != null)
                          {
                            presentationStrategy = presentationStrategyService
                                .getStoredPresentationStrategy((String) token.get("presentationStrategyID"));
                          }
                        if (presentationStrategy != null)
                          {
                            result.put("presentationStrategy", presentationStrategy.getGUIManagedObjectDisplay());
                          }
                        else
                          {
                            result.put("presentationStrategy", "");
                          }

                        List<String> scoringStrategyArray = (List<String>) subscriberFields.get("scoringStrategyIDs");
                        if (scoringStrategyArray != null)
                          {
                            List<String> scoringStrategy = new ArrayList<>();
                            for (int j = 0; j < scoringStrategyArray.size(); j++)
                              {
                                String ssID = (String) scoringStrategyArray.get(j);
                                GUIManagedObject ss = scoringStrategyService.getStoredScoringStrategy(ssID);
                                if (ss != null)
                                  {
                                    scoringStrategy.add(ss.getGUIManagedObjectDisplay());
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
                        else
                          {
                            result.put("scoringStrategy", "");
                          }

                        GUIManagedObject acceptedOffer = null;
                        if (token.get("acceptedOfferID") != null)
                          {
                            acceptedOffer = offerService.getStoredOffer((String) token.get("acceptedOfferID"));
                          }
                        if (acceptedOffer != null)
                          {
                            result.put("acceptedOffer", acceptedOffer.getGUIManagedObjectDisplay());
                          }
                        else
                          {
                            result.put("acceptedOffer", "");
                          }

                        String moduleID = (String) token.get("moduleID");
                        if (moduleID != null)
                          {
                            Module module = Module.fromExternalRepresentation(moduleID);
                            result.put("module", module.toString());
                            String featureID = (String) token.get("featureID");
                            if (featureID != null)
                              {
                                String featureDisplay = DeliveryRequest.getFeatureDisplay(module, featureID, journeyService, offerService, loyaltyProgramService);
                                result.put(featureName, featureDisplay);
                              }
                            else
                              {
                                result.put("featureName", "");
                              }
                          }
                        else
                          {
                            result.put(module, "");
                            result.put(featureName, "");
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
    TokenReportMonoPhase tokenReportMonoPhase = new TokenReportMonoPhase();
    tokenReportMonoPhase.start(args, reportGenerationDate);
  }
  
  private void start(String[] args, final Date reportGenerationDate)
  {
    log.info("received " + args.length + " args");
    for (String arg : args)
      {
        log.info("TokenReportESReader: arg " + arg);
      }

    if (args.length < 3) {
      log.warn(
          "Usage : TokenReportMonoPhase <ESNode> <ES customer index> <csvfile>");
      return;
    }
    String esNode          = args[0];
    String esIndexCustomer = args[1];
    String csvfile         = args[2];
    if (args.length > 3) tenantID = Integer.parseInt(args[3]);

    log.info("Reading data from ES in "+esIndexCustomer+"  index and writing to "+csvfile+" file.");  

    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    esIndexWithQuery.put(esIndexCustomer, QueryBuilders.matchAllQuery());
      
    ReportMonoPhase reportMonoPhase = new ReportMonoPhase(
              esNode,
              esIndexWithQuery,
              this,
              csvfile
          );

    offerService = new OfferService(Deployment.getBrokerServers(), "report-offerService-tokenReportMonoPhase", Deployment.getOfferTopic(), false);
    scoringStrategyService = new ScoringStrategyService(Deployment.getBrokerServers(), "report-scoringstrategyservice-tokenReportMonoPhase", Deployment.getScoringStrategyTopic(), false);
    presentationStrategyService = new PresentationStrategyService(Deployment.getBrokerServers(), "report-presentationstrategyservice-tokenReportMonoPhase", Deployment.getPresentationStrategyTopic(), false);
    tokenTypeService = new TokenTypeService(Deployment.getBrokerServers(), "report-tokentypeservice-tokenReportMonoPhase", Deployment.getTokenTypeTopic(), false);
    journeyService = new JourneyService(Deployment.getBrokerServers(), "report-journeyservice-tokenReportMonoPhase",Deployment.getJourneyTopic(), false);
    loyaltyProgramService = new LoyaltyProgramService(Deployment.getBrokerServers(), "report-loyaltyprogramservice-tokenReportMonoPhase", Deployment.getLoyaltyProgramTopic(), false);

    offerService.start();
    scoringStrategyService.start();
    presentationStrategyService.start();
    tokenTypeService.start();
    journeyService.start();
    loyaltyProgramService.start();

    try {
      if (!reportMonoPhase.startOneToOne())
        {
          log.warn("An error occured, the report might be corrupted");
          throw new RuntimeException("An error occurred, report must be restarted");
        }
    } finally {
      offerService.stop();
      scoringStrategyService.stop();
      presentationStrategyService.stop();
      tokenTypeService.stop();
      journeyService.stop();
      loyaltyProgramService.stop();
    }
  }

}
