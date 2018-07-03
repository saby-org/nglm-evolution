/*****************************************************************************
*
*  GUIManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Alarm;
import com.evolving.nglm.core.Alarm.AlarmLevel;
import com.evolving.nglm.core.Alarm.AlarmType;
import com.evolving.nglm.core.LicenseChecker;
import com.evolving.nglm.core.LicenseChecker.LicenseState;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
import com.rii.utilities.SystemTime;
import com.rii.utilities.UniqueKeyServer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
   
public class GUIManager
{
  /*****************************************
  *
  *  ProductID
  *
  *****************************************/
  
  public static String ProductID = "Evolution-GUIManager";
  
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  private enum API
  {
    getStaticConfiguration,
    getSupportedLanguages,
    getSupportedCurrencies,
    getSupportedTimeUnits,
    getSalesChannels,
    getSupportedDataTypes,
    getProfileCriterionFields,
    getProfileCriterionFieldNames,
    getProfileCriterionField,
    getOfferTypes,
    getProductTypes,
    getJourneyList,
    putJourney,
    removeJourney,
    getOfferList,
    putOffer,
    removeOffer;
  }

  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(GUIManager.class);

  //
  //  license
  //

  private LicenseChecker licenseChecker = null;
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private static final int RESTAPIVersion = 1;
  private HttpServer restServer;
  private JourneyService journeyService;
  private OfferService offerService;
  private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;

  /*****************************************
  *
  *  epochServer
  *
  *****************************************/
  
  private static UniqueKeyServer epochServer = new UniqueKeyServer();

  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args) throws Exception
  {
    NGLMRuntime.initialize();
    GUIManager guiManager = new GUIManager();
    guiManager.start(args);
  }

  /****************************************
  *
  *  start
  *
  *****************************************/

  private void start(String[] args)
  {
    /*****************************************
    *
    *  configuration
    *
    *****************************************/

    String apiProcessKey = args[0];
    String bootstrapServers = args[1];
    int apiRestPort = parseInteger("apiRestPort", args[2]);
    String nodeID = System.getProperty("nglm.license.nodeid");
    String journeyTopic = Deployment.getJourneyTopic();
    String offerTopic = Deployment.getOfferTopic();
    String subscriberGroupEpochTopic = Deployment.getSubscriberGroupEpochTopic();
    
    //
    //  log
    //

    log.info("main START: {} {} {} {} {} {} {}", apiProcessKey, bootstrapServers, apiRestPort, nodeID, journeyTopic, offerTopic, subscriberGroupEpochTopic);

    //
    //  license
    //

    licenseChecker = new LicenseChecker(ProductID, nodeID, Deployment.getZookeeperRoot(), Deployment.getZookeeperConnect());
    
    /*****************************************
    *
    *  services
    *
    *****************************************/

    //
    //  construct
    //

    journeyService = new JourneyService(bootstrapServers, "guimanager-journeyservice-" + apiProcessKey, journeyTopic, true);
    offerService = new OfferService(bootstrapServers, "guimanager-offerservice-" + apiProcessKey, offerTopic, true);
    subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("guimanager-subscribergroupepoch", apiProcessKey, bootstrapServers, subscriberGroupEpochTopic, SubscriberGroupEpoch::unpack);

    //
    //  start
    //

    journeyService.start();
    offerService.start();

    /*****************************************
    *
    *  REST interface -- server and handlers
    *
    *****************************************/

    try
      {
        InetSocketAddress addr = new InetSocketAddress(apiRestPort);
        restServer = HttpServer.create(addr, 0);
        restServer.createContext("/nglm-guimanager/getStaticConfiguration", new APIHandler(API.getStaticConfiguration));
        restServer.createContext("/nglm-guimanager/getSupportedLanguages", new APIHandler(API.getSupportedLanguages));
        restServer.createContext("/nglm-guimanager/getSupportedCurrencies", new APIHandler(API.getSupportedCurrencies));
        restServer.createContext("/nglm-guimanager/getSupportedTimeUnits", new APIHandler(API.getSupportedTimeUnits));
        restServer.createContext("/nglm-guimanager/getSalesChannels", new APIHandler(API.getSalesChannels));
        restServer.createContext("/nglm-guimanager/getSupportedDataTypes", new APIHandler(API.getSupportedDataTypes));
        restServer.createContext("/nglm-guimanager/getProfileCriterionFields", new APIHandler(API.getProfileCriterionFields));
        restServer.createContext("/nglm-guimanager/getProfileCriterionFieldNames", new APIHandler(API.getProfileCriterionFieldNames));
        restServer.createContext("/nglm-guimanager/getProfileCriterionField", new APIHandler(API.getProfileCriterionField));
        restServer.createContext("/nglm-guimanager/getOfferTypes", new APIHandler(API.getOfferTypes));
        restServer.createContext("/nglm-guimanager/getProductTypes", new APIHandler(API.getProductTypes));
        restServer.createContext("/nglm-guimanager/getJourneyList", new APIHandler(API.getJourneyList));
        restServer.createContext("/nglm-guimanager/putJourney", new APIHandler(API.putJourney));
        restServer.createContext("/nglm-guimanager/removeJourney", new APIHandler(API.removeJourney));
        restServer.createContext("/nglm-guimanager/getOfferList", new APIHandler(API.getOfferList));
        restServer.createContext("/nglm-guimanager/putOffer", new APIHandler(API.putOffer));
        restServer.createContext("/nglm-guimanager/removeOffer", new APIHandler(API.removeOffer));
        restServer.setExecutor(Executors.newFixedThreadPool(10));
        restServer.start();
      }
    catch (IOException e)
      {
        throw new ServerRuntimeException("could not initialize REST server", e);
      }

    /*****************************************
    *
    *  log restServerStarted
    *
    *****************************************/

    log.info("main restServerStarted");
  }

  /*****************************************
  *
  *  parseInteger
  *
  *****************************************/

  private int parseInteger(String field, String stringValue)
  {
    int result = 0;
    try
      {
        result = Integer.parseInt(stringValue);
      }
    catch (NumberFormatException e)
      {
        throw new ServerRuntimeException("bad " + field + " argument", e);
      }
    return result;
  }

  /*****************************************
  *
  *  handleAPI
  *
  *****************************************/

  private synchronized void handleAPI(API api, HttpExchange exchange) throws IOException
  {
    try
      {
        /*****************************************
        *
        *  get the body
        *
        *****************************************/

        StringBuilder requestBodyStringBuilder = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()));
        while (true)
          {
            String line = reader.readLine();
            if (line == null) break;
            requestBodyStringBuilder.append(line);
          }
        reader.close();
        log.debug("API (raw request): {} {}",api,requestBodyStringBuilder.toString());
        JSONObject jsonRoot = (JSONObject) (new JSONParser()).parse(requestBodyStringBuilder.toString());

        /*****************************************
        *
        *  validate
        *
        *****************************************/

        int apiVersion = JSONUtilities.decodeInteger(jsonRoot, "apiVersion", true);
        if (apiVersion > RESTAPIVersion)
          {
            throw new ServerRuntimeException("unknown api version " + apiVersion);
          }
        jsonRoot.remove("apiVersion");

        /*****************************************
        *
        *  license state
        *
        *****************************************/

        LicenseState licenseState = licenseChecker.checkLicense();
        Alarm licenseAlarm = licenseState.getHighestAlarm();
        boolean allowAccess = true;
        switch (licenseAlarm.getLevel())
          {
            case None:
            case Alert:
            case Alarm:
              allowAccess = true;
              break;

            case Limit:
            case Block:
              allowAccess = false;
              break;
          }
        
        /*****************************************
        *
        *  process
        *
        *****************************************/

        JSONObject jsonResponse = null;
        if (licenseState.isValid() && allowAccess)
          {
            switch (api)
              {
                case getStaticConfiguration:
                  jsonResponse = processGetStaticConfiguration(jsonRoot);
                  break;

                case getSupportedLanguages:
                  jsonResponse = processGetSupportedLanguages(jsonRoot);
                  break;

                case getSupportedCurrencies:
                  jsonResponse = processGetSupportedCurrencies(jsonRoot);
                  break;

                case getSupportedTimeUnits:
                  jsonResponse = processGetSupportedTimeUnits(jsonRoot);
                  break;

                case getSalesChannels:
                  jsonResponse = processGetSalesChannels(jsonRoot);
                  break;

                case getSupportedDataTypes:
                  jsonResponse = processGetSupportedDataTypes(jsonRoot);
                  break;

                case getProfileCriterionFields:
                  jsonResponse = processGetProfileCriterionFields(jsonRoot);
                  break;

                case getProfileCriterionFieldNames:
                  jsonResponse = processGetProfileCriterionFieldNames(jsonRoot);
                  break;

                case getProfileCriterionField:
                  jsonResponse = processGetProfileCriterionField(jsonRoot);
                  break;

                case getOfferTypes:
                  jsonResponse = processGetOfferTypes(jsonRoot);
                  break;

                case getProductTypes:
                  jsonResponse = processGetProductTypes(jsonRoot);
                  break;

                case getJourneyList:
                  jsonResponse = processGetJourneyList(jsonRoot);
                  break;

                case putJourney:
                  jsonResponse = processPutJourney(jsonRoot);
                  break;

                case removeJourney:
                  jsonResponse = processRemoveJourney(jsonRoot);
                  break;

                case getOfferList:
                  jsonResponse = processGetOfferList(jsonRoot);
                  break;

                case putOffer:
                  jsonResponse = processPutOffer(jsonRoot);
                  break;

                case removeOffer:
                  jsonResponse = processRemoveOffer(jsonRoot);
                  break;
              }
          }
        else
          {
            jsonResponse = processFailedLicenseCheck(licenseState);
            log.warn("Failed license check {} ", licenseState);
          }

        //
        //  validate
        //

        if (jsonResponse == null)
          {
            throw new ServerException("no handler for " + api);
          }

        /*****************************************
        *
        *  send response
        *
        *****************************************/

        //
        //  standard response fields
        //

        jsonResponse.put("apiVersion", RESTAPIVersion);
        jsonResponse.put("licenseCheck", licenseAlarm.getJSONRepresentation());

        //
        //  log
        //

        log.debug("API (raw response): {}", jsonResponse.toString());

        //
        //  send
        //

        exchange.sendResponseHeaders(200, 0);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
        writer.write(jsonResponse.toString());
        writer.close();
        exchange.close();
      }
    catch (org.json.simple.parser.ParseException | IOException | ServerException | RuntimeException e )
      {
        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  send error response
        //

        HashMap<String,Object> response = new HashMap<String,Object>();
        response.put("responseCode", "systemError");
        response.put("responseMessage", e.getMessage());
        JSONObject jsonResponse = JSONUtilities.encodeObject(response);
        exchange.sendResponseHeaders(200, 0);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
        writer.write(jsonResponse.toString());
        writer.close();
        exchange.close();
      }
  }

  /*****************************************
  *
  *  processFailedLicenseCheck
  *
  *****************************************/

  private JSONObject processFailedLicenseCheck(LicenseState licenseState)
  {
    HashMap<String,Object> response = new HashMap<String,Object>();
    
    response.put("responseCode", "failedLicenseCheck");
    response.put("responseMessage", licenseState.getOutcome().name());

    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  getStaticConfiguration
  *
  *****************************************/

  private JSONObject processGetStaticConfiguration(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve supportedLanguages
    *
    *****************************************/

    List<JSONObject> supportedLanguages = new ArrayList<JSONObject>();
    for (SupportedLanguage supportedLanguage : Deployment.getSupportedLanguages().values())
      {
        JSONObject supportedLanguageJSON = supportedLanguage.getJSONRepresentation();
        supportedLanguages.add(supportedLanguageJSON);
      }

    /*****************************************
    *
    *  retrieve supportedCurrencies
    *
    *****************************************/

    List<JSONObject> supportedCurrencies = new ArrayList<JSONObject>();
    for (SupportedCurrency supportedCurrency : Deployment.getSupportedCurrencies().values())
      {
        JSONObject supportedCurrencyJSON = supportedCurrency.getJSONRepresentation();
        supportedCurrencies.add(supportedCurrencyJSON);
      }

    /*****************************************
    *
    *  retrieve supportedTimeUnits
    *
    *****************************************/

    List<JSONObject> supportedTimeUnits = new ArrayList<JSONObject>();
    for (SupportedTimeUnit supportedTimeUnit : Deployment.getSupportedTimeUnits().values())
      {
        JSONObject supportedTimeUnitJSON = supportedTimeUnit.getJSONRepresentation();
        supportedTimeUnits.add(supportedTimeUnitJSON);
      }

    /*****************************************
    *
    *  retrieve salesChannels
    *
    *****************************************/

    List<JSONObject> salesChannels = new ArrayList<JSONObject>();
    for (SalesChannel salesChannel : Deployment.getSalesChannels().values())
      {
        JSONObject salesChannelJSON = salesChannel.getJSONRepresentation();
        salesChannels.add(salesChannelJSON);
      }

    /*****************************************
    *
    *  retrieve supported data types
    *
    *****************************************/

    List<JSONObject> supportedDataTypes = new ArrayList<JSONObject>();
    for (SupportedDataType supportedDataType : Deployment.getSupportedDataTypes().values())
      {
        JSONObject supportedDataTypeJSON = supportedDataType.getJSONRepresentation();
        supportedDataTypes.add(supportedDataTypeJSON);
      }

    /*****************************************
    *
    *  retrieve profile criterion fields
    *
    *****************************************/

    List<JSONObject> profileCriterionFields = processCriterionFields(Deployment.getProfileCriterionFields());

    /*****************************************
    *
    *  retrieve offerTypes
    *
    *****************************************/

    List<JSONObject> offerTypes = new ArrayList<JSONObject>();
    for (OfferType offerType : Deployment.getOfferTypes().values())
      {
        JSONObject offerTypeJSON = offerType.getJSONRepresentation();
        offerTypes.add(offerTypeJSON);
      }

    /*****************************************
    *
    *  retrieve productTypes
    *
    *****************************************/

    List<JSONObject> productTypes = new ArrayList<JSONObject>();
    for (ProductType productType : Deployment.getProductTypes().values())
      {
        JSONObject productTypeJSON = productType.getJSONRepresentation();
        productTypes.add(productTypeJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("supportedLanguages", JSONUtilities.encodeArray(supportedLanguages));
    response.put("supportedCurrencies", JSONUtilities.encodeArray(supportedCurrencies));
    response.put("salesChannels", JSONUtilities.encodeArray(salesChannels));
    response.put("supportedDataTypes", JSONUtilities.encodeArray(supportedDataTypes));
    response.put("profileCriterionFields", JSONUtilities.encodeArray(profileCriterionFields));
    response.put("offerTypes", JSONUtilities.encodeArray(offerTypes));
    response.put("productTypes", JSONUtilities.encodeArray(productTypes));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getSupportedLanguages
  *
  *****************************************/

  private JSONObject processGetSupportedLanguages(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve supportedLanguages
    *
    *****************************************/

    List<JSONObject> supportedLanguages = new ArrayList<JSONObject>();
    for (SupportedLanguage supportedLanguage : Deployment.getSupportedLanguages().values())
      {
        JSONObject supportedLanguageJSON = supportedLanguage.getJSONRepresentation();
        supportedLanguages.add(supportedLanguageJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("supportedLanguages", JSONUtilities.encodeArray(supportedLanguages));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getSupportedCurrencies
  *
  *****************************************/

  private JSONObject processGetSupportedCurrencies(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve supportedCurrencies
    *
    *****************************************/

    List<JSONObject> supportedCurrencies = new ArrayList<JSONObject>();
    for (SupportedCurrency supportedCurrency : Deployment.getSupportedCurrencies().values())
      {
        JSONObject supportedCurrencyJSON = supportedCurrency.getJSONRepresentation();
        supportedCurrencies.add(supportedCurrencyJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("supportedCurrencies", JSONUtilities.encodeArray(supportedCurrencies));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getSupportedTimeUnits
  *
  *****************************************/

  private JSONObject processGetSupportedTimeUnits(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve supportedTimeUnits
    *
    *****************************************/

    List<JSONObject> supportedTimeUnits = new ArrayList<JSONObject>();
    for (SupportedTimeUnit supportedTimeUnit : Deployment.getSupportedTimeUnits().values())
      {
        JSONObject supportedTimeUnitJSON = supportedTimeUnit.getJSONRepresentation();
        supportedTimeUnits.add(supportedTimeUnitJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("supportedTimeUnits", JSONUtilities.encodeArray(supportedTimeUnits));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getSalesChannels
  *
  *****************************************/

  private JSONObject processGetSalesChannels(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve salesChannels
    *
    *****************************************/

    List<JSONObject> salesChannels = new ArrayList<JSONObject>();
    for (SalesChannel salesChannel : Deployment.getSalesChannels().values())
      {
        JSONObject salesChannelJSON = salesChannel.getJSONRepresentation();
        salesChannels.add(salesChannelJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("salesChannels", JSONUtilities.encodeArray(salesChannels));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getSupportedDataTypes
  *
  *****************************************/

  private JSONObject processGetSupportedDataTypes(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve supported data types
    *
    *****************************************/

    List<JSONObject> supportedDataTypes = new ArrayList<JSONObject>();
    for (SupportedDataType supportedDataType : Deployment.getSupportedDataTypes().values())
      {
        JSONObject supportedDataTypeJSON = supportedDataType.getJSONRepresentation();
        supportedDataTypes.add(supportedDataTypeJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("supportedDataTypes", JSONUtilities.encodeArray(supportedDataTypes));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getProfileCriterionFields
  *
  *****************************************/

  private JSONObject processGetProfileCriterionFields(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve profile criterion fields
    *
    *****************************************/

    List<JSONObject> profileCriterionFields = processCriterionFields(Deployment.getProfileCriterionFields());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("profileCriterionFields", JSONUtilities.encodeArray(profileCriterionFields));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getProfileCriterionFieldNames
  *
  *****************************************/

  private JSONObject processGetProfileCriterionFieldNames(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve profile criterion fields
    *
    *****************************************/

    List<JSONObject> profileCriterionFields = processCriterionFields(Deployment.getProfileCriterionFields());

    /*****************************************
    *
    *  strip out everything but name/display
    *
    *****************************************/

    List<JSONObject> profileCriterionFieldNames = new ArrayList<JSONObject>();
    for (JSONObject profileCriterionField : profileCriterionFields)
      {
        HashMap<String,Object> profileCriterionFieldName = new HashMap<String,Object>();
        profileCriterionFieldName.put("name", profileCriterionField.get("name"));
        profileCriterionFieldName.put("display", profileCriterionField.get("display"));
        profileCriterionFieldNames.add(JSONUtilities.encodeObject(profileCriterionFieldName));
      }
    
    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("profileCriterionFieldNames", JSONUtilities.encodeArray(profileCriterionFieldNames));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getProfileCriterionField
  *
  *****************************************/

  private JSONObject processGetProfileCriterionField(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve field name (setting it to null if blank)
    *
    *****************************************/

    String name = JSONUtilities.decodeString(jsonRoot, "name", true);
    name = (name != null && name.trim().length() == 0) ? null : name;

    /*****************************************
    *
    *  retrieve field with name
    *
    *****************************************/

    JSONObject requestedProfileCriterionField = null;
    if (name != null)
      {
        //
        //  retrieve profile criterion fields
        //

        List<JSONObject> profileCriterionFields = processCriterionFields(Deployment.getProfileCriterionFields());

        //
        //  find requested field
        //

        for (JSONObject profileCriterionField : profileCriterionFields)
          {
            if (Objects.equals(name, profileCriterionField.get("name")))
              {
                requestedProfileCriterionField = profileCriterionField;
                break;
              }
          }
      }
    
    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    if (requestedProfileCriterionField != null)
      {
        response.put("responseCode", "ok");
        response.put("profileCriterionField", requestedProfileCriterionField);
      }
    else if (name == null)
      {
        response.put("responseCode", "invalidRequest");
        response.put("responseMessage", "name argument not provided");
      }
    else
      {
        response.put("responseCode", "fieldNotFound");
        response.put("responseMessage", "could not find profile criterion field with name " + name);
      }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getOfferTypes
  *
  *****************************************/

  private JSONObject processGetOfferTypes(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve offerTypes
    *
    *****************************************/

    List<JSONObject> offerTypes = new ArrayList<JSONObject>();
    for (OfferType offerType : Deployment.getOfferTypes().values())
      {
        JSONObject offerTypeJSON = offerType.getJSONRepresentation();
        offerTypes.add(offerTypeJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("offerTypes", JSONUtilities.encodeArray(offerTypes));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getProductTypes
  *
  *****************************************/

  private JSONObject processGetProductTypes(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve productTypes
    *
    *****************************************/

    List<JSONObject> productTypes = new ArrayList<JSONObject>();
    for (ProductType productType : Deployment.getProductTypes().values())
      {
        JSONObject productTypeJSON = productType.getJSONRepresentation();
        productTypes.add(productTypeJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("productTypes", JSONUtilities.encodeArray(productTypes));
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processCriterionFields
  *
  *****************************************/

  private List<JSONObject> processCriterionFields(Map<String,CriterionField> criterionFields)
  {
    /****************************************
    *
    *  resolve field data types
    *
    ****************************************/

    Map<String, ResolvedFieldType> resolvedFieldTypes = new LinkedHashMap<String, ResolvedFieldType>();
    Map<String, List<JSONObject>> resolvedAvailableValues = new LinkedHashMap<String, List<JSONObject>>();
    for (CriterionField criterionField : criterionFields.values())
      {
        List<JSONObject> availableValues = evaluateAvailableValues(criterionField);
        resolvedFieldTypes.put(criterionField.getName(), new ResolvedFieldType(criterionField.getFieldDataType(), availableValues));
        resolvedAvailableValues.put(criterionField.getName(), availableValues);
      }

    /****************************************
    *
    *  default list of fields for each field data type
    *
    ****************************************/

    Map<ResolvedFieldType, List<CriterionField>> defaultFieldsForResolvedType = new LinkedHashMap<ResolvedFieldType, List<CriterionField>>();
    for (CriterionField criterionField : criterionFields.values())
      {
        ResolvedFieldType resolvedFieldType = resolvedFieldTypes.get(criterionField.getName());
        List<CriterionField> fields = defaultFieldsForResolvedType.get(resolvedFieldType);
        if (fields == null)
          {
            fields = new ArrayList<CriterionField>();
            defaultFieldsForResolvedType.put(resolvedFieldType, fields);
          }
        fields.add(criterionField);
      }

    /****************************************
    *
    *  process
    *
    ****************************************/

    List<JSONObject> result = new ArrayList<JSONObject>();
    for (CriterionField criterionField : criterionFields.values())
      {
        //
        //  remove server-side fields
        //
        
        JSONObject criterionFieldJSON = (JSONObject) criterionField.getJSONRepresentation().clone();
        criterionFieldJSON.remove("esField");
        criterionFieldJSON.remove("retriever");

        //
        //  evaluate operators
        //

        List<JSONObject> fieldAvailableValues = resolvedAvailableValues.get(criterionField.getName());
        List<JSONObject> operators = evaluateOperators(criterionFieldJSON, fieldAvailableValues);
        criterionFieldJSON.put("operators", operators);
        criterionFieldJSON.remove("includedOperators");
        criterionFieldJSON.remove("excludedOperators");

        //
        //  evaluate comparable fields
        //

        List<CriterionField> defaultComparableFields = defaultFieldsForResolvedType.get(resolvedFieldTypes.get(criterionField.getName()));
        criterionFieldJSON.put("singletonComparableFields", evaluateComparableFields(criterionField.getName(), criterionFieldJSON, defaultComparableFields, true));
        criterionFieldJSON.put("setValuedComparableFields", evaluateComparableFields(criterionField.getName(), criterionFieldJSON, defaultComparableFields, false));
        criterionFieldJSON.remove("includedComparableFields");
        criterionFieldJSON.remove("excludedComparableFields");

        //
        //  evaluate available values for reference data
        //

        criterionFieldJSON.put("availableValues", resolvedAvailableValues.get(criterionField.getName()));
        
        //
        //  add
        //
        
        result.add(criterionFieldJSON);
      }

    //
    //  return
    //
    
    return result;
  }

  /****************************************
  *
  *  evaluateOperators
  *
  ****************************************/

  private List<JSONObject> evaluateOperators(JSONObject criterionFieldJSON, List<JSONObject> fieldAvailableValues)
  {
    //
    //  all operators
    //
    
    Map<String,SupportedOperator> supportedOperatorsForType = Deployment.getSupportedDataTypes().get(criterionFieldJSON.get("dataType")).getOperators();

    //
    //  remove set operators for non-enumerated types
    //

    List<String> supportedOperators = new ArrayList<String>();
    for (String supportedOperatorName : supportedOperatorsForType.keySet())
      {
        SupportedOperator supportedOperator = supportedOperatorsForType.get(supportedOperatorName);
        if (! supportedOperator.getArgumentSet())
          supportedOperators.add(supportedOperatorName);
        else if (supportedOperator.getArgumentSet() && fieldAvailableValues != null)
          supportedOperators.add(supportedOperatorName);
      }

    //
    //  find list of explicitly included operators
    //

    List<String> requestedIncludedOperatorNames = null;
    if (criterionFieldJSON.get("includedOperators") != null)
      {
        requestedIncludedOperatorNames = new ArrayList<String>();
        for (String operator : supportedOperators)
          {
            for (String operatorRegex : (List<String>) criterionFieldJSON.get("includedOperators"))
              {
                Pattern pattern = Pattern.compile("^" + operatorRegex + "$");
                if (pattern.matcher(operator).matches())
                  {
                    requestedIncludedOperatorNames.add(operator);
                    break;
                  }
              }
          }
      }

    //
    //  find list of explicitly excluded operators
    //

    List<String> requestedExcludedOperatorNames = null;
    if (criterionFieldJSON.get("excludedOperators") != null)
      {
        requestedExcludedOperatorNames = new ArrayList<String>();
        for (String operator : supportedOperators)
          {
            for (String operatorRegex : (List<String>) criterionFieldJSON.get("excludedOperators"))
              {
                Pattern pattern = Pattern.compile("^" + operatorRegex + "$");
                if (pattern.matcher(operator).matches())
                  {
                    requestedExcludedOperatorNames.add(operator);
                    break;
                  }
              }
          }
      }

    //
    //  resolve included/excluded operators
    //

    List<String> includedOperatorNames = requestedIncludedOperatorNames != null ? requestedIncludedOperatorNames : supportedOperators;
    Set<String> excludedOperatorNames = requestedExcludedOperatorNames != null ? new LinkedHashSet<String>(requestedExcludedOperatorNames) : Collections.<String>emptySet();
    
    //
    //  evaluate
    //

    List<JSONObject> result = new ArrayList<JSONObject>();
    for (String operatorName : includedOperatorNames)
      {
        SupportedOperator operator = supportedOperatorsForType.get(operatorName);
        if (! excludedOperatorNames.contains(operatorName))
          {
            result.add(operator.getJSONRepresentation());
          }
      }

    //
    //  return
    //

    return result;
  }

  /****************************************
  *
  *  evaluateComparableFields
  *
  ****************************************/

  private List<JSONObject> evaluateComparableFields(String criterionFieldName, JSONObject criterionFieldJSON, List<CriterionField> allFields, boolean singleton)
  {
    //
    //  all fields
    //
    
    Map<String, CriterionField> comparableFields = new LinkedHashMap<String, CriterionField>();
    for (CriterionField criterionField : allFields)
      {
        comparableFields.put(criterionField.getName(), criterionField);
      }

    //
    //  find list of explicitly included fields
    //

    List<String> requestedIncludedComparableFieldNames = null;
    if (criterionFieldJSON.get("includedComparableFields") != null)
      {
        requestedIncludedComparableFieldNames = new ArrayList<String>();
        for (String comparableField : comparableFields.keySet())
          {
            for (String fieldRegex : (List<String>) criterionFieldJSON.get("includedComparableFields"))
              {
                Pattern pattern = Pattern.compile("^" + fieldRegex + "$");
                if (pattern.matcher(comparableField).matches())
                  {
                    requestedIncludedComparableFieldNames.add(comparableField);
                    break;
                  }
              }
          }
      }

    //
    //  find list of explicitly excluded fields
    //

    List<String> requestedExcludedComparableFieldNames = null;
    if (criterionFieldJSON.get("excludedComparableFields") != null)
      {
        requestedExcludedComparableFieldNames = new ArrayList<String>();
        for (String comparableField : comparableFields.keySet())
          {
            for (String fieldRegex : (List<String>) criterionFieldJSON.get("excludedComparableFields"))
              {
                Pattern pattern = Pattern.compile("^" + fieldRegex + "$");
                if (pattern.matcher(comparableField).matches())
                  {
                    requestedExcludedComparableFieldNames.add(comparableField);
                    break;
                  }
              }
          }
      }

    //
    //  resolve included/excluded fields
    //

    List<String> includedComparableFieldNames = requestedIncludedComparableFieldNames != null ? requestedIncludedComparableFieldNames : new ArrayList<String>(comparableFields.keySet());
    Set<String> excludedComparableFieldNames = requestedExcludedComparableFieldNames != null ? new LinkedHashSet<String>(requestedExcludedComparableFieldNames) : Collections.<String>emptySet();

    //
    //  evaluate
    //

    List<JSONObject> result = new ArrayList<JSONObject>();
    for (String comparableFieldName : includedComparableFieldNames)
      {
        CriterionField criterionField = comparableFields.get(comparableFieldName);
        if ((! excludedComparableFieldNames.contains(comparableFieldName)) && (singleton == criterionField.getFieldDataType().getSingletonType()) && (! comparableFieldName.equals(criterionFieldName)))
          {
            HashMap<String,Object> comparableFieldJSON = new HashMap<String,Object>();
            comparableFieldJSON.put("name", criterionField.getName());
            comparableFieldJSON.put("display", criterionField.getDisplay());
            result.add(JSONUtilities.encodeObject(comparableFieldJSON));
          }
      }

    //
    //  return
    //

    return result;
  }

  /****************************************
  *
  *  evaluateAvailableValues
  *
  ****************************************/

  private List<JSONObject> evaluateAvailableValues(CriterionField criterionField)
  {
    JSONObject criterionFieldJSON = (JSONObject) criterionField.getJSONRepresentation();
    JSONArray availableValues = JSONUtilities.decodeJSONArray(criterionFieldJSON, "availableValues", false);
    List<JSONObject> result = null;
    if (availableValues != null)
      {
        result = new ArrayList<JSONObject>();
        Pattern enumeratedValuesPattern = Pattern.compile("^#(.*?)#$");
        for (Object availableValueUnchecked : availableValues)
          {
            if (availableValueUnchecked instanceof String)
              {
                String availableValue = (String) availableValueUnchecked;
                Matcher matcher = enumeratedValuesPattern.matcher(availableValue);
                if (matcher.matches())
                  {
                    result.addAll(evaluateEnumeratedValues(matcher.group(1)));
                  }
                else
                  {
                    HashMap<String,Object> availableValueJSON = new HashMap<String,Object>();
                    availableValueJSON.put("name", availableValue);
                    availableValueJSON.put("display", availableValue);
                    result.add(JSONUtilities.encodeObject(availableValueJSON));
                  }
              }
            else if (availableValueUnchecked instanceof JSONObject)
              {
                JSONObject availableValue = (JSONObject) availableValueUnchecked;
                result.add(availableValue);
              }
            else
              {
                Object availableValue = (Object) availableValueUnchecked;
                HashMap<String,Object> availableValueJSON = new HashMap<String,Object>();
                availableValueJSON.put("name", availableValue);
                availableValueJSON.put("display", availableValue.toString());
                result.add(JSONUtilities.encodeObject(availableValueJSON));
              }
          }
      }
    return result;
  }

  /****************************************
  *
  *  evaluateEnumeratedValues
  *
  ****************************************/

  private List<JSONObject> evaluateEnumeratedValues(String reference)
  {
    List<JSONObject> result = new ArrayList<JSONObject>();
    switch (reference)
      {
        case "ratePlans":
          List<String> ratePlans = new ArrayList<String>();
          ratePlans.add("tariff001");
          ratePlans.add("tariff002");
          ratePlans.add("tariff003");
          for (String ratePlan : ratePlans)
            {
              HashMap<String,Object> availableValue = new HashMap<String,Object>();
              availableValue.put("name", ratePlan);
              availableValue.put("display", ratePlan + " (display)");
              result.add(JSONUtilities.encodeObject(availableValue));              
            }
          break;

        case "supportedLanguages":
          for (SupportedLanguage supportedLanguage : Deployment.getSupportedLanguages().values())
            {
              HashMap<String,Object> availableValue = new HashMap<String,Object>();
              availableValue.put("name", supportedLanguage.getName());
              availableValue.put("display", supportedLanguage.getDisplay());
              result.add(JSONUtilities.encodeObject(availableValue));
            }
          break;

        case "segments":
          Map<String,SubscriberGroupEpoch> subscriberGroupEpochs = subscriberGroupEpochReader.getAll();
          for (String groupName : subscriberGroupEpochs.keySet())
            {
              SubscriberGroupEpoch subscriberGroupEpoch = subscriberGroupEpochs.get(groupName);
              if (subscriberGroupEpoch.getActive() && ! groupName.equals(SubscriberProfile.ControlGroup) && ! groupName.equals(SubscriberProfile.UniversalControlGroup))
                {
                  HashMap<String,Object> availableValue = new HashMap<String,Object>();
                  availableValue.put("name", groupName);
                  availableValue.put("display", subscriberGroupEpoch.getDisplay());
                  result.add(JSONUtilities.encodeObject(availableValue));

                }
            }
          break;

        default:
          break;
      }
    return result;
  }
  
  /*****************************************
  *
  *  processGetJourneyList
  *
  *****************************************/

  private JSONObject processGetJourneyList(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve and convert journeys
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> journeys = new ArrayList<JSONObject>();
    for (GUIManagedObject journey : journeyService.getStoredJourneys())
      {
        JSONObject journeyJSON = journey.getJSONRepresentation();
        journeyJSON.put("accepted", journey.getAccepted());
        journeyJSON.put("processing", journeyService.isActiveJourney(journey, now));
        journeys.add(journeyJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("journeys", JSONUtilities.encodeArray(journeys));
    return JSONUtilities.encodeObject(response);
  }
                 
  /*****************************************
  *
  *  processPutJourney
  *
  *****************************************/

  private JSONObject processPutJourney(JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/
    
    HashMap<String,Object> response = new HashMap<String,Object>();
    
    /*****************************************
    *
    *  journeyID
    *
    *****************************************/
    
    String journeyID = JSONUtilities.decodeString(jsonRoot, "journeyID", false);
    if (journeyID == null)
      {
        journeyID = journeyService.generateJourneyID();
        jsonRoot.put("journeyID", journeyID);
      }
    
    /*****************************************
    *
    *  process journey
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    long epoch = epochServer.getKey();
    try
      {
        /*****************************************
        *
        *  existing journey
        *
        *****************************************/

        GUIManagedObject existingJourney = journeyService.getStoredJourney(journeyID);

        /****************************************
        *
        *  instantiate journey
        *
        ****************************************/

        Journey journey = new Journey(jsonRoot, epoch, existingJourney);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        journeyService.putJourney(journey);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("journeyID", journey.getJourneyID());
        response.put("accepted", journey.getAccepted());
        response.put("processing", journeyService.isActiveJourney(journey, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, "journeyID", epoch);

        //
        //  store
        //

        journeyService.putJourney(incompleteObject);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());
        
        //
        //  response
        //

        response.put("journeyID", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "journeyNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }
  
  /*****************************************
  *
  *  processRemoveJourney
  *
  *****************************************/

  private JSONObject processRemoveJourney(JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/
    
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  argument
    *
    ****************************************/
    
    String journeyID = JSONUtilities.decodeString(jsonRoot, "journeyID", true);
    
    /*****************************************
    *
    *  remove
    *
    *****************************************/

    journeyService.removeJourney(journeyID);

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", "ok");
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetOfferList
  *
  *****************************************/

  private JSONObject processGetOfferList(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve and convert offers
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> offers = new ArrayList<JSONObject>();
    for (GUIManagedObject offer : offerService.getStoredOffers())
      {
        JSONObject offerJSON = offer.getJSONRepresentation();
        offerJSON.put("accepted", offer.getAccepted());
        offerJSON.put("processing", offerService.isActiveOffer(offer, now));
        offers.add(offerJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("offers", JSONUtilities.encodeArray(offers));
    return JSONUtilities.encodeObject(response);
  }
                 
  /*****************************************
  *
  *  processPutOffer
  *
  *****************************************/

  private JSONObject processPutOffer(JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/
    
    HashMap<String,Object> response = new HashMap<String,Object>();
    
    /*****************************************
    *
    *  offerID
    *
    *****************************************/
    
    String offerID = JSONUtilities.decodeString(jsonRoot, "offerID", false);
    if (offerID == null)
      {
        offerID = offerService.generateOfferID();
        jsonRoot.put("offerID", offerID);
      }
    
    /*****************************************
    *
    *  process offer
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    long epoch = epochServer.getKey();
    try
      {
        /*****************************************
        *
        *  existing offer
        *
        *****************************************/

        GUIManagedObject existingOffer = offerService.getStoredOffer(offerID);

        /****************************************
        *
        *  instantiate offer
        *
        ****************************************/

        Offer offer = new Offer(jsonRoot, epoch, existingOffer);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        offerService.putOffer(offer);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("offerID", offer.getOfferID());
        response.put("accepted", offer.getAccepted());
        response.put("processing", offerService.isActiveOffer(offer, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, "offerID", epoch);

        //
        //  store
        //

        offerService.putOffer(incompleteObject);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());
        
        //
        //  response
        //

        response.put("offerID", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "offerNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }
  
  /*****************************************
  *
  *  processRemoveOffer
  *
  *****************************************/

  private JSONObject processRemoveOffer(JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/
    
    HashMap<String,Object> response = new HashMap<String,Object>();

    /****************************************
    *
    *  argument
    *
    ****************************************/
    
    String offerID = JSONUtilities.decodeString(jsonRoot, "offerID", true);
    
    /*****************************************
    *
    *  remove
    *
    *****************************************/

    offerService.removeOffer(offerID);

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", "ok");
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  class APIHandler
  *
  *****************************************/

  private class APIHandler implements HttpHandler
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private API api;

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    private APIHandler(API api)
    {
      this.api = api;
    }

    /*****************************************
    *
    *  handle -- HttpHandler
    *
    *****************************************/

    public void handle(HttpExchange exchange) throws IOException
    {
      handleAPI(api, exchange);
    }
  }

  /*****************************************
  *
  *  class ResolvedFieldType
  *
  *****************************************/

  private class ResolvedFieldType
  {
    //
    //  attributes
    //
    
    private CriterionDataType dataType;
    private Set<JSONObject> availableValues;

    //
    //  accessors
    //

    CriterionDataType getDataType() { return dataType; }
    Set<JSONObject> getAvailableValues() { return availableValues; }

    //
    //  constructor
    //

    ResolvedFieldType(CriterionDataType dataType, List<JSONObject> availableValues)
    {
      this.dataType = dataType.getBaseType();
      this.availableValues = availableValues != null ? new HashSet<JSONObject>(availableValues) : null;
    }

    /*****************************************
    *
    *  equals
    *
    *****************************************/

    public boolean equals(Object obj)
    {
      boolean result = false;
      if (obj instanceof ResolvedFieldType)
        {
          ResolvedFieldType resolvedFieldType = (ResolvedFieldType) obj;
          result = true;
          result = result && Objects.equals(dataType, resolvedFieldType.getDataType());
          result = result && Objects.equals(availableValues, resolvedFieldType.getAvailableValues());
        }
      return result;
    }

    /*****************************************
    *
    *  hashCode
    *
    *****************************************/

    public int hashCode()
    {
      return dataType.hashCode() + (availableValues != null ? availableValues.hashCode() : 0);
    }
  }

  /*****************************************
  *
  *  class GUIManagerException
  *
  *****************************************/

  public static class GUIManagerException extends Exception
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String responseParameter;

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public String getResponseParameter() { return responseParameter; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public GUIManagerException(String responseMessage, String responseParameter)
    {
      super(responseMessage);
      this.responseParameter = responseParameter;
    }

    /*****************************************
    *
    *  constructor - excpetion
    *
    *****************************************/

    public GUIManagerException(Throwable e)
    {
      super(e.getMessage(), e);
      this.responseParameter = null;
    }
  }
}
