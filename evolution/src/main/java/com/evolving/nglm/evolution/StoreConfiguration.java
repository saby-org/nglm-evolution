/*****************************************************************************
*
*  SubscriberGroupLoader.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SystemTime;

import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
   
public class StoreConfiguration
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  StoreType
  //

  public enum StoreType
  {
    Store("store"),
    Load("load"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private StoreType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static StoreType fromExternalRepresentation(String externalRepresentation) { for (StoreType enumeratedValue : StoreType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  private static final Logger log = LoggerFactory.getLogger(StoreConfiguration.class);

  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args) throws Exception
  {
    /****************************************
    *
    *  configuration
    *
    ****************************************/
    
    //
    //  validate
    //

    if (args.length < 4 || StoreType.fromExternalRepresentation(args[0]) == StoreType.Unknown)
      {
        log.error("usage: StoreConfiguration <store|load> <guimanager-host> <guimanager-file> <file>");
        System.exit(-1);
      }

    //
    //  arguments
    //

    StoreType operation = StoreType.fromExternalRepresentation(args[0]);
    String guiManagerHost = args[1];
    String guiManagerPort = args[2];
    String configurationFile = args[3];

    //
    //  operation
    //

    switch (operation)
      {
        case Store:
          storeConfiguration(guiManagerHost, guiManagerPort, configurationFile);
          break;

        case Load:
          loadConfiguration(guiManagerHost, guiManagerPort, configurationFile);
          break;
      }
  }

  /*****************************************
  *
  *  storeConfiguration
  *
  *****************************************/

  private static void storeConfiguration(String guiManagerHost, String guiManagerPort, String configurationFile)
  {
    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("storeConfiguration {} {} {}", guiManagerHost, guiManagerPort, configurationFile);

    /*****************************************
    *
    *  printStream
    *
    *****************************************/

    PrintStream printStream = null;
    try
      {
        printStream = new PrintStream(new FileOutputStream(configurationFile), false, StandardCharsets.UTF_8.name());
      }
    catch (FileNotFoundException | UnsupportedEncodingException e)
      {
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("Exception creating configuration file: {}", stackTraceWriter.toString());
        throw new RuntimeException(e);
      }

    /*****************************************
    *
    *  process
    *
    *****************************************/
    
    List<JSONObject> records = new ArrayList<JSONObject>();
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getSegmentationDimensionList", "segmentationDimensions", "putSegmentationDimension"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getPointList", "points", "putPoint"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getCallingChannelList", "callingChannels", "putCallingChannel"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getSalesChannelList", "salesChannels", "putSalesChannel"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getSupplierList", "suppliers", "putSupplier"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getProductList", "products", "putProduct"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getCatalogCharacteristicList", "catalogCharacteristics", "putCatalogCharacteristic"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getContactPolicyList", "contactPolicies", "putContactPolicy"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getJourneyObjectiveList", "journeyObjectives", "putJourneyObjective"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getOfferObjectiveList", "offerObjectives", "putOfferObjective"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getProductTypeList", "productTypes", "putProductType"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getUCGRuleList", "ucgRules", "putUCGRule"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getTokenTypeList", "tokenTypes", "putTokenType"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getPaymentMeanList", "paymentMeans", "putPaymentMean"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getCommunicationChannelsList", "communicationChannels", "putCommunicationChannel"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getBlackoutPeriodsList", "blackoutPeriods", "putBlackoutPeriods"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getPartnerList", "partners", "putPartner"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getMailTemplateList", "templates", "putMailTemplate"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getSMSTemplateList", "templates", "putSMSTemplate"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getPushTemplateList", "templates", "putPushTemplate"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getOfferList", "offers", "putOffer"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getLoyaltyProgramsList", "loyaltyPrograms", "putLoyaltyProgram"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getExclusionInclusionTargetList", "exclusionInclusionTargets", "putExclusionInclusionTarget"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getTargetList", "targets", "putTarget"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getJourneyTemplateList", "journeyTemplates", "putJourneyTemplate"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getJourneyList", "journeys", "putJourney"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getCampaignList", "campaigns", "putCampaign"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getBulkCampaignList", "bulkCampaigns", "putBulkCampaign"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getScoringStrategyList", "scoringStrategies", "putScoringStrategy"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getPresentationStrategyList", "presentationStrategies", "putPresentationStrategy"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getReportList", "reports", "putReport"));
    records.addAll(retrieveConfiguration(guiManagerHost, guiManagerPort, "getDNBOMatrixList", "dnboMatrixes", "putDNBOMatrix"));

    /*****************************************
    *
    *  log
    *
    *****************************************/

    for (JSONObject configurationItem : records)
      {
        log.info("storeConfigurationItem {} {} {}", JSONUtilities.decodeString(configurationItem, "putAPI", true), JSONUtilities.decodeString(JSONUtilities.decodeJSONObject(configurationItem, "configuration", true), "id", true), JSONUtilities.decodeBoolean(JSONUtilities.decodeJSONObject(configurationItem, "configuration", true), "accepted", true));
      }

    /*****************************************
    *
    *  format result
    *
    *****************************************/

    Map<String,Object> resultJSON = new HashMap<String,Object>();
    resultJSON.put("storeDate", SystemTime.getCurrentTime().toString());
    resultJSON.put("guiManager", guiManagerHost + ":" + guiManagerPort);
    resultJSON.put("configuration", JSONUtilities.encodeArray(records));
    JSONObject result = JSONUtilities.encodeObject(resultJSON);

    /*****************************************
    *
    *  write configuration file
    *
    *****************************************/

    printStream.println(result.toString());
    if (printStream.checkError())
      {
        log.error("Error writing configuration file");
        throw new RuntimeException("configuration file");
      }

    /*****************************************
    *
    *  close
    *
    *****************************************/

    printStream.close();
    if (printStream.checkError())
      {
        log.error("Error closing configuration file");
        throw new RuntimeException("configuration file");
      }
  }

  /*****************************************
  *
  *  retrieveConfiguration
  *
  *****************************************/

  private static List<JSONObject> retrieveConfiguration(String guiManagerHost, String guiManagerPort, String api, String field, String putAPI)
  {
    /*****************************************
    *
    *  getRequestBody
    *
    *****************************************/

    Map<String,Object> getRequestBodyJSON = new HashMap<String,Object>();
    getRequestBodyJSON.put("apiVersion",1);
    JSONObject getRequestBody = JSONUtilities.encodeObject(getRequestBodyJSON);

    /*****************************************
    *
    *  call
    *
    *****************************************/

    JSONObject response = callGM(guiManagerHost, guiManagerPort, api, getRequestBody);

    /*****************************************
    *
    *  validate
    *
    *****************************************/

    if (! Objects.equals(JSONUtilities.decodeString(response, "responseCode", false), "ok"))
      {
        log.error("Error on guimanager call {}: {}", api, JSONUtilities.decodeString(response, "responseCode", false));
        throw new RuntimeException("error on guimanager call");
      }

    /*****************************************
    *
    *  process
    *
    *****************************************/

    List<JSONObject> result = new ArrayList<JSONObject>();
    for (JSONObject configurationItem : (List<JSONObject>) JSONUtilities.decodeJSONArray(response, field, true))
      {
        if (Objects.equals(JSONUtilities.decodeBoolean(configurationItem, "readOnly", Boolean.FALSE), Boolean.FALSE))
          {
            Map<String,Object> configurationJSON = new HashMap<String,Object>();
            configurationJSON.put("putAPI", putAPI);
            configurationJSON.put("configuration", configurationItem);
            result.add(JSONUtilities.encodeObject(configurationJSON));
          }
      }

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return result;
  }

  /*****************************************
  *
  *  loadConfiguration
  *
  *****************************************/

  private static void loadConfiguration(String guiManagerHost, String guiManagerPort, String configurationFile)
  {
    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("loadConfiguration {} {} {}", guiManagerHost, guiManagerPort, configurationFile);

    /*****************************************
    *
    *  read configuration
    *
    *****************************************/

    JSONObject jsonRoot = null;
    try
      {
        //
        //  read
        //

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(configurationFile), StandardCharsets.UTF_8));
        StringBuilder configurationBuffer = new StringBuilder();
        boolean eofReached = false;
        while (! eofReached)
          {
            String record = reader.readLine();
            if (record != null)
              configurationBuffer.append(record);
            else
              eofReached = true;
          }
        reader.close();

        //
        //  parse
        //

        jsonRoot = (JSONObject) (new JSONParser()).parse(configurationBuffer.toString());
      }
    catch (org.json.simple.parser.ParseException | IOException e )
      {
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("Exception reading configuration: {}", stackTraceWriter.toString());
        throw new RuntimeException(e);
      }

    /*****************************************
    *
    *  load
    *
    *****************************************/

    for (JSONObject configurationItem : (List<JSONObject>) JSONUtilities.decodeJSONArray(jsonRoot, "configuration", true))
      {
        /*****************************************
        *
        *  item
        *
        *****************************************/

        String putAPI = JSONUtilities.decodeString(configurationItem, "putAPI", true);
        JSONObject configuration = JSONUtilities.decodeJSONObject(configurationItem, "configuration", true);
        configuration.put("apiVersion", 1);

        /*****************************************
        *
        *  call
        *
        *****************************************/

        JSONObject response = callGM(guiManagerHost, guiManagerPort, putAPI, configuration);

        /*****************************************
        *
        *  log
        *
        *****************************************/

        log.info("loadConfigurationItem {} {} {}", putAPI, JSONUtilities.decodeString(configuration, "id", true), JSONUtilities.decodeString(response, "responseCode", false));
      }

    /*****************************************
    *
    *  log
    *
    *****************************************/
    
    log.info("loadConfiguration complete");
  }

  /*****************************************
  *
  *  callGM
  *
  *****************************************/

  private static JSONObject callGM(String guiManagerHost, String guiManagerPort, String api, JSONObject requestJSON)
  {
    /*****************************************
    *
    *  call
    *
    *****************************************/

    HttpResponse httpResponse = null;
    try
      {
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost httpPost = new HttpPost("http://" + guiManagerHost + ":" + guiManagerPort + "/nglm-guimanager/" + api);
        httpPost.setEntity(new StringEntity(requestJSON.toString(), ContentType.create("application/json")));
        httpResponse = httpClient.execute(httpPost);
        if (httpResponse == null || httpResponse.getStatusLine() == null || httpResponse.getStatusLine().getStatusCode() != 200 || httpResponse.getEntity() == null)
          {
            log.error("Error processing REST api: {}", httpResponse.toString());
            throw new RuntimeException("REST error");
          }

      }
    catch (IOException e)
      {
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("Exception processing REST api: {}", stackTraceWriter.toString());
        throw new RuntimeException(e);
      }

    /*****************************************
    *
    *  process response
    *
    *****************************************/

    JSONObject jsonResponse = null;
    try
      {
        jsonResponse = (JSONObject) (new JSONParser()).parse(EntityUtils.toString(httpResponse.getEntity(), "UTF-8"));
      }
    catch (IOException|ParseException e)
      {
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("Exception processing REST api: {}", stackTraceWriter.toString());
        throw new RuntimeException(e);
      }

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return jsonResponse;
  }
}
