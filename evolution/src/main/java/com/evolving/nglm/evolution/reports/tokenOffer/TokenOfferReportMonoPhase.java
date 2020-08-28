/****************************************************************************
 *
 *  TokenReportCsvWriter.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports.tokenOffer;

import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.*;
import com.evolving.nglm.evolution.SubscriberProfileService.EngineSubscriberProfileService;
import com.evolving.nglm.evolution.Token.TokenStatus;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportCsvWriter;
import com.evolving.nglm.evolution.reports.ReportMonoPhase;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;
import com.evolving.nglm.evolution.reports.token.TokenReportMonoPhase;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.zip.ZipOutputStream;

public class TokenOfferReportMonoPhase implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(TokenOfferReportMonoPhase.class);
  final private static String CSV_SEPARATOR = ReportUtils.getSeparator();

  private final static String subscriberID = "subscriberID";
  private final static String customerID = "customerID";

  private static SalesChannelService salesChannelService;
  private static OfferService offerService = null;

  public TokenOfferReportMonoPhase()
  {

  }


  public boolean dumpElementToCsvMono(Map<String,Object> map, ZipOutputStream writer, boolean addHeaders) throws IOException
  {

    LinkedHashMap<String, Object> result = new LinkedHashMap<>();
    LinkedHashMap<String, Object> commonFields = new LinkedHashMap<>();
    Map<String, Object> elasticFields = map;

    if (elasticFields != null)
      {
        String subscriberID = Objects.toString(elasticFields.get("subscriberID"));
        Date now = SystemTime.getCurrentTime();
        if (subscriberID != null)
          {
            List<Map<String, Object>> tokensArray = (List<Map<String, Object>>) elasticFields.get("tokens");
            if (tokensArray != null && !tokensArray.isEmpty())
              {
                commonFields.put(customerID, subscriberID);
                for (AlternateID alternateID : Deployment.getAlternateIDs().values())
                  {
                    if (elasticFields.get(alternateID.getESField()) != null)
                      {
                        Object alternateId = elasticFields.get(alternateID.getESField());
                        commonFields.put(alternateID.getName(), alternateId);
                      }
                  }
                for (int i = 0; i < tokensArray.size(); i++)
                  {
                    result.clear();
                    result.putAll(commonFields);
                    Map<String, Object> token = (Map<String, Object>) tokensArray.get(i);
                    String tokenStatus = (String) token.get("tokenStatus");
                    result.put("tokenCode", token.get("tokenCode"));
                    String salesChannel = (String) token.get("presentedOffersSalesChannel");
                    if (salesChannel != null && salesChannelService.getStoredSalesChannel(salesChannel) != null)
                      {
                        result.put("salesChannel", salesChannelService.getStoredSalesChannel(salesChannel).getGUIManagedObjectDisplay());
                      }
                    else
                      {
                        result.put("salesChannel", "");
                      }
                    if (TokenStatus.New.getExternalRepresentation().equals(tokenStatus))
                      {
                        result.put("voucherCode", "");
                        result.put("offerName", "");
                        result.put("offerStatus", "");                            
                        result.put("offerRank", "");
                        result.put("allocationDate", "");
                        result.put("redeemedDate", "");  
                        result.put("salesChannel", "");
                      }
                    else if (TokenStatus.Bound.getExternalRepresentation().equals(tokenStatus))
                      {
                        String qtyAllocatedOffers = (String) token.get("tokenStatus");
                        if ("0".equals(qtyAllocatedOffers))
                          {
                            result.put("voucherCode", "");
                            result.put("offerName", "");
                            result.put("offerStatus", "");                                
                            result.put("offerRank", "");
                            result.put("allocationDate", "");
                            result.put("redeemedDate", "");
                            result.put("salesChannel", "");
                          }
                        else
                          {
                            result.put("voucherCode", "");

                            /*
                            List<String> presentedOfferIDsArray = (List<String>) elasticFields.get("presentedOfferIDs");
                            if (presentedOfferIDsArray != null && !presentedOfferIDsArray.isEmpty())
                              {
                                for (int j = 0; j < presentedOfferIDsArray.size(); j++)
                                  {
                                    String presentedOfferId = (String) presentedOfferIDsArray.get(j);
                                    
                                    // TODO : array ??
                                    
                                    GUIManagedObject presentedOffer = offerService.getStoredOffer(presentedOfferId);
                                    if (presentedOffer != null)
                                      {
                                        result.put("offerName", presentedOffer.getGUIManagedObjectDisplay());
                                      }
                                    else
                                      {
                                        result.put("offerName", "");
                                      }
                                    result.put("offerStatus", "Allocated");
                                    result.put("offerRank", j + 1);
                                    longDateToReport("lastAllocationDate", "allocationDate", result, token);
                                  }
                              }
                              */
                          }
                      }
                    else if (TokenStatus.Redeemed.getExternalRepresentation().equals(tokenStatus))
                      {
                        result.put("voucherCode", "");
                        String acceptedOfferId = (String) elasticFields.get("acceptedOfferID");
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
                            GUIManagedObject acceptedOffer = offerService.getStoredOffer(acceptedOfferId);
                            if (acceptedOffer != null)
                              {
                                result.put("offerName", acceptedOffer.getGUIManagedObjectDisplay());
                              }
                            else
                              {
                                result.put("offerName", "");
                              }
                            result.put("offerStatus", "Allocated"); // TODO Redeemed ?
                            int offerRank = 0;
                            List<String> presentedOfferIDsArray = (List<String>) elasticFields.get("presentedOfferIDs");
                            if (presentedOfferIDsArray != null && !presentedOfferIDsArray.isEmpty())
                              {
                                for (int j = 0; j < presentedOfferIDsArray.size(); j++)
                                  {
                                    String presentedOfferId = (String) presentedOfferIDsArray.get(j);
                                    if (acceptedOfferId.equals(presentedOfferId))
                                      {
                                        offerRank = j+1;
                                        break;
                                      }
                                  }
                              }
                            result.put("offerRank", offerRank);
                            longDateToReport("lastAllocationDate", "allocationDate", result, token);
                            longDateToReport("redeemedDate", "redeemedDate", result, token);
                          }
                      }
                    else if (TokenStatus.Expired.getExternalRepresentation().equals(tokenStatus))
                      {
                        result.put("vocherCode", "");
                        result.put("offerName", "");
                        result.put("offerStatus", "");                           
                        result.put("offerRank", "");
                        result.put("allocationDate", "");
                        result.put("redeemedDate", "");
                        result.put("salesChannel", "");
                      }
                    if (addHeaders)
                      {
                        addHeaders(writer, result.keySet(), 1);
                        addHeaders = false;
                      }
                    String line = ReportUtils.formatResult(result);
                    log.trace("Writing to csv file : " + line);
                    writer.write(line.getBytes());
                    writer.write("\n".getBytes());
                  }
              }
          }
      }
    return addHeaders;
  }


  public void longDateToReport(String inField, String outField, LinkedHashMap<String, Object> result, Map<String, Object> token)
  {
    Long redeemedDate = (Long) token.get(inField);
    if (redeemedDate == null)
      {
        result.put(outField, "");
      }
    else
      {
        result.put(outField, ReportsCommonCode.getDateString(new Date(redeemedDate)));
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
        log.info("TokenOfferReportCsvWriter: arg " + arg);
      }

    if (args.length < 3)
      {
        log.warn("Usage : TokenOfferReportCsvWriter <ESNode> <ES customer index> <csvfile>");
        return;
      }
    String esNode          = args[0];
    String esIndexCustomer = args[1];
    String csvfile         = args[2];

    log.info("Reading data from ES in "+esIndexCustomer+"  index and writing to "+csvfile+" file.");  
    ReportCsvFactory reportFactory = new TokenReportMonoPhase();

    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    esIndexWithQuery.put(esIndexCustomer, QueryBuilders.matchAllQuery());
      
    ReportMonoPhase reportMonoPhase = new ReportMonoPhase(
              esNode,
              esIndexWithQuery,
              reportFactory,
              csvfile
          );

    offerService = new OfferService(Deployment.getBrokerServers(), "tokenOfferReportDriver-offerService-tokenOfferReportMonoPhase", Deployment.getOfferTopic(), false);
    offerService.start();

    salesChannelService = new SalesChannelService(Deployment.getBrokerServers(), "tokenOfferReportDriver-saleschannelservice-tokenOfferReportMonoPhase", Deployment.getSalesChannelTopic(), false);
    salesChannelService.start();

    if (!reportMonoPhase.startOneToOne())
      {
        log.warn("An error occured, the report might be corrupted");
        return;
      }
    
  }

}
