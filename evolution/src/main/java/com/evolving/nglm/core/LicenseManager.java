/*****************************************************************************
*
*  LicenseManager.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import com.evolving.nglm.core.Alarm.AlarmLevel;
import com.evolving.nglm.core.Alarm.AlarmType;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rii.utilities.JSONUtilities.JSONUtilitiesException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Executors;
   
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

public class LicenseManager
{
  /*****************************************
  *
  *  standard formats
  *
  *****************************************/

  private static SimpleDateFormat licenseDateFormat;
  static
  {
    licenseDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
  }

  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  API
  //
  
  private enum API
  {
    checkLicense,
    getLicenseStatus,
    ping
  }

  //
  //  Mode
  //
  
  private enum Mode
  {
    Limited("limited"),
    Unlimited("unlimited");
    private String externalRepresentation;
    private String getExternalRepresentation() { return externalRepresentation; }
    private Mode(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public static Mode fromExternalRepresentation(String externalRepresentation) { for (Mode enumeratedValue : Mode.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return null; }
  }

  //
  // Outcome
  //
  
  public enum Outcome
  {
    LicenseValid,
    LimitReached,
    NoLicense,
    NoLicenseManager,
    NotAllowed,
    BadRequest;
    public boolean isValid() { return this == Outcome.LicenseValid; }
    public boolean wasDenied() { return !isValid(); } 
  }

  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(LicenseManager.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private static final int RESTAPIVersion = 1;
  private HttpServer restServer;
  private String bootstrapServers;
  private String alarmTopic = null;
  LicenseStatistics licenseStatistics = null;
  KafkaProducer<byte[], byte[]> kafkaProducer = null;

  /*****************************************
  *
  *  zookeeper
  *
  *****************************************/

  private ZooKeeper zookeeper = null;
  private boolean sessionKeyExists = false;
  private String licenseManagerKey;
  private String licenseManagerSessionKey;

  private String zookeeperConnect = null;
  private String licenseRoot = null;


  /*****************************************
  *
  *  license state
  *
  *****************************************/

  private License currentLicense;
  private byte[] rawLicenseData = null;
  private Object licenseLock = new Object();
  private String licenseTokenLocation;
  private List<Alarm> knownAlarms = new ArrayList<Alarm>();

  /*****************************************
  *
  *  license management alarms
  *
  *****************************************/

  TimeLimitAlarm timeLimitAlarm = null;
  NodeAndComponentLimitAlarm nodeLimitAlarm = null;
  NodeAndComponentLimitAlarm componentLimitAlarm = null;
  
  /*****************************************
  *
  *  uniqueKeyServer
  *
  *****************************************/
  
  private static UniqueKeyServer keyServer = new UniqueKeyServer();

  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args) throws Exception
  {
    NGLMRuntime.initialize(false);
    LicenseManager licenseManager = new LicenseManager();
    licenseManager.start(args);
  }

  /****************************************
  *
  *  start
  *
  *****************************************/

  private void start(String[] args) throws Exception
  {
    /*****************************************
    *
    *  configuration
    *
    *****************************************/

    this.bootstrapServers = args[0];
    int apiRestPort = parseInteger("apiRestPort", args[1]);
    this.licenseManagerKey = args[2];
    this.licenseManagerSessionKey = args[3];
    this.licenseTokenLocation = args[4];
    this.alarmTopic = args[5];

    //
    //  zookeeper
    // 

    zookeeperConnect = System.getProperty("zookeeper.connect");
    if (zookeeperConnect == null || zookeeperConnect.trim().length() == 0)  throw new RuntimeException ("must set property 'zookeeper.connect'");

    String zookeeperRoot = System.getProperty("nglm.zookeeper.root");
    if (zookeeperRoot == null || zookeeperRoot.trim().length() == 0)  throw new RuntimeException ("must set property 'nglm.zookeeper.root'");

    this.licenseRoot = zookeeperRoot + "/licensemanager";

    //
    //  log
    //

    log.info("main START: {} {} {} {} {} {}", bootstrapServers, apiRestPort, licenseManagerKey, licenseManagerSessionKey, licenseTokenLocation, alarmTopic);

    //
    //  read license
    //

    readLicense();

    //
    //  license
    //

    log.info("currentLicense {}", currentLicense);

    //
    //  deployment specific instructions for license management alarming
    //

    startLicenseManagement();
    
    /*****************************************
    *
    *  REST interface -- server and handlers
    *
    *****************************************/

    try
      {
        InetSocketAddress addr = new InetSocketAddress(apiRestPort);
        restServer = HttpServer.create(addr, 0);
        restServer.createContext("/nglm-license/checkLicense", new APIHandler(API.checkLicense));
        restServer.createContext("/nglm-license/getLicenseStatus", new APIHandler(API.getLicenseStatus));
        restServer.createContext("/nglm-license/ping", new APIHandler(API.ping));
        restServer.setExecutor(Executors.newFixedThreadPool(10));
        restServer.start();
      }
    catch (IOException e)
      {
        throw new ServerRuntimeException("could not initialize REST server", e);
      }

  }

  /*****************************************
  *
  *  readLicense
  *
  *****************************************/

  private void  readLicense()
  {
    //
    //  start reader
    //

    Runnable licenseReader = new Runnable() { @Override public void run() { readLicenseData(); } };
    Thread licenseReaderThread = new Thread(licenseReader);
    licenseReaderThread.start();

    //
    //  wait
    //

    while (currentLicense == null)
      {
        synchronized (licenseLock)
          {
            try
              {
                licenseLock.wait(500);
              }
            catch (InterruptedException e)
              {
                // nothing
              }
          }
      }
  }

  /*****************************************
  *
  *  readLicenseData
  *
  *****************************************/
  
  private void readLicenseData()
  {
    boolean continueReading = true;
    do
      {
        /*****************************************
        *
        *  ensure zookeeper 
        *
        *****************************************/

        boolean connected = false;
        do
          {
            ensureConnected();
            connected = (zookeeper != null) && (zookeeper.getState() == ZooKeeper.States.CONNECTED) && sessionKeyExists;
            if (!connected)
              {
                try { Thread.currentThread().sleep(5000); } catch (InterruptedException ie) { }
              }
          }
        while (!connected);

        //
        //  read data
        //
        
        byte[] rawData = null;
        try
          {
            rawData = zookeeper.getData(this.licenseRoot + "/license", null, null);
          }
        catch (KeeperException|InterruptedException e)
          {
            log.error("Exception getting license data: {}. Will retry.", e.getMessage());
            continue;
          }

        /*****************************************
        *
        *  handle license
        *
        *****************************************/

        License candidateLicense = null;
        if (rawData != null)
          {

            try
              {
                //
                //  decrypt 
                //

                DecryptLicense decrypter = new DecryptLicense(licenseTokenLocation, rawData);
                decrypter.decrypt();

                //
                //  parse
                //

                String licenseString = decrypter.getLicense();
                candidateLicense = new License(parseJSONRepresentation(licenseString));
              }
            catch (JSONUtilitiesException e)
              {
                log.error("Error reading license. Access will be removed.");
              }
            catch (Exception e)
              {
                log.error("Error decrypting license. Access will be removed.");
              }
          }

        //
        //  set and notify
        //
        
        synchronized(licenseLock)
          {
            rawLicenseData = rawData;
            currentLicense = candidateLicense;
            if (timeLimitAlarm != null)
              {
                timeLimitAlarm.updateAlarms(currentLicense.getExpireDate());
                if (licenseStatistics != null)
                  {
                    licenseStatistics.updateExpiryExpireDate(currentLicense.getExpireDate());
                  }
              }

            //
            //  notify
            //
            
            licenseLock.notifyAll();

            log.info("license refreshed {}", currentLicense);
          }

        /*****************************************
        *
        *  poll
        *
        *****************************************/

        try
          {
            Thread.currentThread().sleep(15*1000);
          }
        catch (InterruptedException ie)
          {
            // nothing
          }
      }
    while (continueReading);

  }

  /*****************************************
  *
  *  ensureConnected
  *
  *****************************************/

  private void ensureConnected()
  {
    /*****************************************
    *
    *  zookeeper client status
    *
    *****************************************/

    if (zookeeper != null && ! zookeeper.getState().isAlive())
      {
        log.info("closing zookeeper client due to status {}", zookeeper.getState());
        try { zookeeper.close(); } catch (InterruptedException e) { }
        zookeeper = null;
      }

    /*****************************************
    *
    *  (re-) create zookeeper client (if necessary)
    *
    *****************************************/

    while (zookeeper == null)
      {
        try
          {
            zookeeper = new ZooKeeper(System.getProperty("zookeeper.connect"), 15000, new Watcher() { @Override public void process(WatchedEvent event) { } }, false);
          }
        catch (IOException e)
          {
            log.info("could not create zookeeper client using {}", System.getProperty("zookeeper.connect"));
          }
      }

    /*****************************************
    *
    *  ensure connected
    *
    *****************************************/

    while (zookeeper.getState().isAlive() && ! zookeeper.getState().isConnected())
      {
        try { Thread.currentThread().sleep(200); } catch (InterruptedException ie) { }
      }

    /******************************************
    *
    *  create license manager key node
    *
    *****************************************/

    //
    //  read existing session node
    //

    sessionKeyExists = false;
    try
      {
        sessionKeyExists = (zookeeper.exists(licenseRoot + "/sessions/" + licenseManagerSessionKey, false) != null);
        if (!sessionKeyExists) log.info("session key node {} does not exist - path {}", licenseManagerSessionKey, licenseRoot + "/sessions/" + licenseManagerSessionKey);
      }
    catch (KeeperException e)
      {
        log.info("ensureConnected() - exists() - KeeperException code {}", e.code());
        return;
      }
    catch (InterruptedException e)
      {
        return;
      }

    //
    //  create session node (if necessary)
    //

    if (! sessionKeyExists)
      {
        try
          {
            zookeeper.create(licenseRoot + "/sessions/" + licenseManagerSessionKey, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            sessionKeyExists = true;
          }
        catch (KeeperException e)
          {
            log.info("ensureConnected() - create() - KeeperException code {}", e.code());
            return;
          }
        catch (InterruptedException e)
          {
            return;
          }
      }
  }

  /*****************************************
  *
  *  startLicenseManagement
  *
  *****************************************/
  
  private void startLicenseManagement()
  {
    if (currentLicense == null) throw new ServerRuntimeException("invariant violation: current license should not be null");

    //
    //  json
    //

    JSONObject licenseManagementJson = Deployment.getLicenseManagement();

    //
    //  time, node, andcomponent limit alarming
    //

    synchronized (licenseLock)
      {
        this.timeLimitAlarm = new TimeLimitAlarm(licenseManagementJson, currentLicense.getExpireDate());
        this.nodeLimitAlarm = new NodeAndComponentLimitAlarm(JSONUtilities.decodeJSONObject(licenseManagementJson, "nodeLimit", true));
        this.componentLimitAlarm = new NodeAndComponentLimitAlarm(JSONUtilities.decodeJSONObject(licenseManagementJson, "componentLimit", true));
      }

    //
    //  others TBD
    //

    //
    //  statistics
    //

    this.licenseStatistics = new LicenseStatistics("licensemanager-" + licenseManagerKey, currentLicense.getExpireDate());
                          
    //
    //  kafka producer for alarms
    //
    
    Properties producerProperties = new Properties();
    producerProperties.put("bootstrap.servers", bootstrapServers);
    producerProperties.put("acks", "all");
    producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    this.kafkaProducer = new KafkaProducer<byte[], byte[]>(producerProperties);

    //
    //  periodic alarm evaluation
    //

    Runnable licenseAlarmEvaluator = new Runnable() { @Override public void run() { evaluateForAlarms(); } };
    Thread licenseAlarmEvaluatorThread = new Thread(licenseAlarmEvaluator);
    licenseAlarmEvaluatorThread.start();

  }

  /*****************************************
  *
  *  evaluateForAlarms
  *
  *****************************************/

  private void evaluateForAlarms()
  {
    boolean continueEvaluating = true;
    do
      {
        /*****************************************
        *
        *  evaluate for new set of alarms
        *
        *****************************************/
        
        List<Alarm> updatedAlarms = new ArrayList<Alarm>();
        
        //
        //  time limit alarm
        //

        AlarmLevel timeLimitAlarmLevel = timeLimitAlarm.checkForAlarm(SystemTime.getCurrentTime());
        updatedAlarms.add(new Alarm("licensemanager", AlarmType.License_TimeLimit, timeLimitAlarmLevel, "system", SystemTime.getCurrentTime(), "Expires at " + currentLicense.getExpireDate().toString()));
        
        
        //
        //  other alarms
        //     node - n/a
        //     component - n/a
        //     capacity - TBD
        //

        /*****************************************
        *
        *  update
        *
        *****************************************/

        //
        //  known alarms
        //

        synchronized (licenseLock)
          {
            knownAlarms.clear();
            knownAlarms.addAll(updatedAlarms);
          }

        //
        //  publish most recent alarms
        //

        for (Alarm alarm : updatedAlarms)
          {
            if (alarm.getLevel().getExternalRepresentation() > 0)
              {
                publishAlarm(alarm);
              }
          }

        //
        //  statistics
        //

        licenseStatistics.updateExpiryExpireDate(currentLicense.getExpireDate());
        licenseStatistics.updateExpiryAlarmLevel(timeLimitAlarmLevel);

        //
        //  sleep
        //

        try
          {
            Thread.currentThread().sleep(60*1000);
          }
        catch (InterruptedException ie)
          {
            // nothing
          }
      }
    while (continueEvaluating);
  }

  /*****************************************
  *
  *  publishAlarm
  *
  *****************************************/
  
  private void publishAlarm(Alarm alarm)
  {
    LongKey key = new LongKey(keyServer.getKey());
    kafkaProducer.send(new ProducerRecord<byte[], byte[]>(alarmTopic, LongKey.serde().serializer().serialize(alarmTopic, key), Alarm.serde().serializer().serialize(alarmTopic, alarm)));
  }

  /*****************************************
  *
  *  handleAPI
  *
  *****************************************/

  private void handleAPI(API api, HttpExchange exchange) throws IOException
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
        JSONObject jsonRequest = (JSONObject) (new JSONParser()).parse(requestBodyStringBuilder.toString());

        /*****************************************
        *
        *  validate
        *
        *****************************************/

        int apiVersion = JSONUtilities.decodeInteger(jsonRequest, "apiVersion", true);
        if (apiVersion > RESTAPIVersion)
          {
            throw new ServerRuntimeException("unknown api version " + apiVersion);
          }
        jsonRequest.remove("apiVersion");

        /*****************************************
        *
        *  process
        *
        *****************************************/

        JSONObject jsonResponse = null;
        switch (api)
          {
            case checkLicense:
              jsonResponse = processCheckLicense(jsonRequest);
              break;
            
            case getLicenseStatus:
              jsonResponse = processGetLicenseStatus(jsonRequest);
              break;

            case ping:
              jsonResponse = processPing(jsonRequest);
              break;
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

        //
        //  log
        //

        logForAudit(exchange, jsonRequest, jsonResponse);

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

        HashMap<String,Object> response = new HashMap<String,Object>();;
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
  *  processCheckLicense
  *
  *****************************************/

  private JSONObject processCheckLicense(JSONObject jsonRoot)
  {
    /****************************************
    *
    *  argument
    *
    ****************************************/
    
    String componentID = JSONUtilities.decodeString(jsonRoot, "componentID", false);
    String nodeID = JSONUtilities.decodeString(jsonRoot, "nodeID", false);

    /*****************************************
    *
    *  validate
    *
    *****************************************/
    
    if (componentID == null)  return constructDeniedResponse(Outcome.BadRequest);
    if (nodeID == null)  return constructDeniedResponse(Outcome.BadRequest);
        
    /*****************************************
    *
    *  check limits
    *
    *****************************************/

    JSONObject result = null;

    //
    //  license configured
    //

    if (licenseNotAvailable())
      {
        result = constructDeniedResponse(Outcome.NoLicense);
      }
    else
      {
        //
        //  determine license status wrt all configured limits
        //

        List<Alarm> alarms = checkLicenseLimits(componentID, nodeID);

        //
        //  convert
        //
        
        List<JSONObject> alarmsList = new ArrayList<JSONObject>();
        for (Alarm alarm : alarms)
          {
            alarmsList.add(alarm.getJSONRepresentation());
          }

        //
        //  outcome
        //
        
        Outcome summaryOutcome = Outcome.LicenseValid;
        for (Alarm alarm : alarms)
          {
            if (alarm.getLevel() != AlarmLevel.None)
              {
                summaryOutcome = Outcome.LimitReached;
                break;
              }
          }

        //
        //  response
        //

        HashMap<String,Object> response = new HashMap<String,Object>();
        response.put("responseCode", "ok");
        response.put("response", "license limits checked");
        response.put("alarms", JSONUtilities.encodeArray(alarmsList));
        response.put("outcome", summaryOutcome.name());
        result = JSONUtilities.encodeObject(response);
      }

    //
    // return
    //
    
    return result;
  }

  /*****************************************
  *
  *  checkLicenseLimits
  *
  *****************************************/

  private List<Alarm> checkLicenseLimits(String componentID, String nodeID)
  {
    //
    //  result
    //

    List<Alarm> result = new ArrayList<Alarm>();
    
    //
    //  time limits
    //

    AlarmLevel timeLimitAlarmLevel = timeLimitAlarm.checkForAlarm(SystemTime.getCurrentTime());
    result.add(new Alarm("licensemanager", AlarmType.License_TimeLimit, timeLimitAlarmLevel, "system", SystemTime.getCurrentTime(), "Expires at " + currentLicense.getExpireDate().toString()));

    //
    //  component limits
    //

    AlarmLevel componentLimitAlarmLevel = componentNotAllowed(componentID) ? componentLimitAlarm.getConfiguredAlarmLevel(componentID) : AlarmLevel.None;
    result.add(new Alarm("licensemanager", AlarmType.License_ComponentLimit, componentLimitAlarmLevel, componentID, SystemTime.getCurrentTime(),  "For component " + componentID));

    //
    //  node limits
    //

    AlarmLevel nodeLimitAlarmLevel = nodeNotAllowed(nodeID) ? nodeLimitAlarm.getConfiguredAlarmLevel(nodeID) : AlarmLevel.None;
    result.add(new Alarm("licensemanager", AlarmType.License_NodeLimit, nodeLimitAlarmLevel, nodeID, SystemTime.getCurrentTime(),  "For node " + nodeID));

    //
    //  capacity limits TBD
    //

    //
    // return
    //

    return result;
  }

  /*****************************************
  *
  *  processGetLicenseStatus
  *
  *****************************************/

  private JSONObject processGetLicenseStatus(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  check limits
    *
    *****************************************/

    JSONObject result = null;

    //
    //  license configured
    //

    if (licenseNotAvailable())
      {
        result = constructDeniedResponse(Outcome.NoLicense);
      }
    else
      {
        //
        //  license status
        //
        
        JSONObject licenseStatus = constructLicenseStatus();
        
        //
        //  response
        //

        HashMap<String,Object> response = new HashMap<String,Object>();
        response.put("responseCode", "ok");
        response.put("response", "license status");
        response.put("licenseStatus", licenseStatus);
        response.put("outcome", Outcome.LicenseValid.name());
        result = JSONUtilities.encodeObject(response);
      }

    //
    // return
    //
    
    return result;
  }

  /*****************************************
  *
  *  constructLicenseStatus
  *
  *****************************************/

  private JSONObject constructLicenseStatus()
  {
    HashMap<String,Object> response = new HashMap<String,Object>();;

    synchronized (licenseLock)
      {
        //
        //  license info
        //

        response.put("expiration", licenseDateFormat.format(currentLicense.getExpireDate()));
        response.put("componentLimits", JSONUtilities.encodeArray(new ArrayList<String> (currentLicense.getAllowedComponents())));
        response.put("nodeLimits", JSONUtilities.encodeArray(new ArrayList<String> (currentLicense.getAllowedNodes())));

        //
        //  capacityLimites - TBD
        //

        response.put("capacityLimits", JSONUtilities.encodeArray(new ArrayList<String> ()));

        //
        //  alarms
        //

        List<JSONObject> alarmsList = new ArrayList<JSONObject>();
        for (Alarm alarm : knownAlarms)
          {
            alarmsList.add(alarm.getJSONRepresentation());
          }
        response.put("alarms", JSONUtilities.encodeArray(alarmsList));
      }
    
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPing
  *
  *****************************************/

  private JSONObject processPing(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("response", "pong");
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  logForAudit
  *
  *****************************************/
      
  private void logForAudit(HttpExchange exchange, JSONObject request, JSONObject response)
  {
    String caller = exchange.getRemoteAddress() != null ? exchange.getRemoteAddress().toString() : "unknown";
    log.info("logForAudit: remoteAddress={}, request={}, response={}", caller, request.toString(), response.toString());
  }

  /*****************************************
  *
  *  constructDeniedResponse
  *
  *****************************************/

  private JSONObject constructDeniedResponse (Outcome denial)
  {
    HashMap<String,Object> response = new HashMap<String,Object>();;

    response.put("responseCode", "nok");
    response.put("response", "denied");
    response.put("outcome", denial.name());

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
  *  class LicenseManagerAPIException
  *
  *****************************************/

  public static class LicenseManagerAPIException extends Exception
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

    public LicenseManagerAPIException(String responseMessage, String responseParameter)
    {
      super(responseMessage);
      this.responseParameter = responseParameter;
    }

    /*****************************************
    *
    *  constructor - exception
    *
    *****************************************/

    public LicenseManagerAPIException(Throwable e)
    {
      super(e.getMessage(), e);
      this.responseParameter = null;
    }
  }

  /*****************************************
  *
  *  licenseNotAvailable
  *
  *****************************************/

  private boolean licenseNotAvailable()
  {
    return currentLicense == null;
  }

  /*****************************************
  *
  *  licenseExpired
  *
  *****************************************/

  private boolean licenseExpired(Date date)
  {
    return currentLicense == null || date.after(currentLicense.getExpireDate());
  }

  /*****************************************
  *
  *  componentNotAllowed
  *
  *****************************************/

  private boolean componentNotAllowed(String componentID)
  {
    return currentLicense == null || (currentLicense.getComponentLimitMode() == Mode.Limited && !currentLicense.getAllowedComponents().contains(componentID));
  }

  /*****************************************
  *
  *  nodeNotAllowed
  *
  *****************************************/

  private boolean nodeNotAllowed(String nodeID)
  {
    return currentLicense == null || (currentLicense.getNodeLimitMode() == Mode.Limited &&!currentLicense.getAllowedNodes().contains(nodeID));
  }

  /*****************************************
  *
  *  maxSubscribersReached
  *
  *****************************************/

  private boolean maxSubscribersReached()
  {
    //
    //  TBD
    //

    return false;
  }

  /*****************************************
  *
  *  eventLimitReached
  *
  *****************************************/

  private boolean eventLimitReached()
  {
    //
    //  TBD
    //

    return false;
  }

  /***************************************************
  *
  *  class License
  *
  ***************************************************/

  private class License
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private int version;
    private Date expireDate;
    private String customerName;
    private String customerID;
    private String customerKey;
    private Mode componentLimitMode;
    private Set<String> allowedComponents;
    private Mode capacityLimitMode;
    private Integer maximumSubscribers;
    private Integer subscriberEventsPerDay;
    private Mode nodeLimitMode;
    private Set<String> allowedNodes;

    //
    //  accessors
    //
    
    int getVersion() { return version; }
    Date getExpireDate() { return expireDate; }
    String getCustomerName() { return customerName; }
    String getCustomerID() { return customerID; }
    String getCustomerKey() { return customerKey; }
    Mode getComponentLimitMode() { return componentLimitMode; }
    Set<String> getAllowedComponents() { return allowedComponents; }
    Mode getCapacityLimitMode() { return capacityLimitMode; }
    Integer getMaximumSubscribers() { return maximumSubscribers; }
    Integer getSubscriberEventsPerDay() { return subscriberEventsPerDay; }
    Mode getNodeLimitMode() { return nodeLimitMode; }
    Set<String> getAllowedNodes() { return allowedNodes; }

    /*****************************************
    *
    *  constructor (json)
    *
    *****************************************/

    License (JSONObject jsonRoot) throws JSONUtilitiesException
    {
      //
      //  the basics
      //

      this.version = JSONUtilities.decodeInteger(jsonRoot, "version", true);
      this.expireDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "expireDate", false));

      //
      //  customer info
      //

      JSONObject customerJson = JSONUtilities.decodeJSONObject(jsonRoot, "customerInformation", true);
      this.customerName = JSONUtilities.decodeString(customerJson, "name", true);
      this.customerID = JSONUtilities.decodeString(customerJson, "id", true);
      this.customerKey = JSONUtilities.decodeString(customerJson, "key", true);

      //
      // component limits
      //

      JSONObject componentJson = JSONUtilities.decodeJSONObject(jsonRoot, "componentLimits", true);
      this.componentLimitMode = Mode.fromExternalRepresentation(JSONUtilities.decodeString(componentJson, "mode", true));
      if (this.componentLimitMode == null) throw new RuntimeException("unknown mode in license");
      switch (this.componentLimitMode)
        {
          case Limited:
            this.allowedComponents = decodeStringList(componentJson, "allowedComponents");
            break;

          default:
            this.allowedComponents = new HashSet<String>();
        }

      //
      // capacity limits
      //

      JSONObject capacityJson = JSONUtilities.decodeJSONObject(jsonRoot, "capacityLimits", true);
      this.capacityLimitMode = Mode.fromExternalRepresentation(JSONUtilities.decodeString(capacityJson, "mode", true));
      if (this.capacityLimitMode == null) throw new RuntimeException("unknown mode in license");
      this.maximumSubscribers = JSONUtilities.decodeInteger(capacityJson, "maximumSubscribers", false);
      this.subscriberEventsPerDay = JSONUtilities.decodeInteger(capacityJson, "subscriberEventsPerDay", false);

      //
      // node limits
      //

      JSONObject nodeJson = JSONUtilities.decodeJSONObject(jsonRoot, "nodeLimits", true);
      this.nodeLimitMode = Mode.fromExternalRepresentation(JSONUtilities.decodeString(nodeJson, "mode", true));
      if (this.nodeLimitMode == null) throw new RuntimeException("unknown mode in license");
      switch (this.nodeLimitMode)
        {
          case Limited:
            this.allowedNodes = decodeStringList(nodeJson, "allowedNodes");
            break;

          default:
            this.allowedNodes = new HashSet<String>();
        }
    }

    /*****************************************
    *
    *  toString
    *
    *****************************************/

    public String toString()
    {
      StringBuilder buf = new StringBuilder();

      buf.append("version=" + this.version);
      buf.append(",expireDate=" + licenseDateFormat.format(this.expireDate));
      buf.append(",customer={" + this.customerName + "," + this.customerID + "," + this.customerKey + "}");
      buf.append(",componentLimits={" + this.componentLimitMode + "," + this.allowedComponents + "}");
      buf.append(",nodeLimits={" + this.nodeLimitMode + "," + this.allowedNodes + "}");
      buf.append(",capacityLimits={" + this.capacityLimitMode + "," + this.maximumSubscribers + "," + this.subscriberEventsPerDay + "}");

      return buf.toString();
    }

    /*****************************************
    *
    *  parseDateField
    *
    *****************************************/
    // TODO EVPRO-99 remove ?
    @Deprecated
    private Date parseDateField(String stringDate) throws JSONUtilitiesException
    {
      Date result = null;
      try
        {
          if (stringDate != null && stringDate.trim().length() > 0)
            {
              synchronized (licenseDateFormat)
                {
                  result = licenseDateFormat.parse(stringDate.trim());
                }
            }
        }
      catch (ParseException e)
        {
          throw new JSONUtilitiesException("parseDateField", e);
        }
      return result;
    }

    /*****************************************
    *
    *  decodeStringList
    *
    *****************************************/

    private Set<String> decodeStringList(JSONObject jsonRoot, String key)
    {
      Set<String> result = new HashSet<String>();
      JSONArray stringListArray = JSONUtilities.decodeJSONArray(jsonRoot, key, true);
      if (stringListArray != null)
        {
          for (int i=0; i<stringListArray.size(); i++)
            {
              result.add((String) stringListArray.get(i));
            }
        }
      return result;
    }

  }
  
  /***************************************************
  *
  *  class DecryptLicense
  *
  ***************************************************/

  private class DecryptLicense
  {
    private byte [] encryptedSecretKeyBytes;
    private byte [] privateKeyBytes;
    private byte [] encryptedLicenseBytes;
    private byte [] licenseBytes;

    String getLicense() { return new String (licenseBytes); }
    byte [] getLicenseBytes() { return licenseBytes; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    DecryptLicense(String privateKeyLocation, byte [] rawLicenseBytes) throws Exception
    {
      /*****************************************
      *
      *  deconstruct the license blob 
      *
      *****************************************/
      
      //
      //  decode
      //

      byte [] allBytes = Base64.getDecoder().decode(rawLicenseBytes);
      
      //
      //  version - first byte
      // 

      byte encryptVersion = allBytes[0];
      switch (encryptVersion)
        {
          case 1:
            break;

          default:
            throw new RuntimeException("Unsupported version: " + encryptVersion);
        }
      int pos = 1; 
      
      //
      //  encrypted symmetric key - bytes 1-256
      //

      int encryptedSecretKeySize = 256;
      byte [] encryptedSecretKeyPart = new byte[encryptedSecretKeySize];
      System.arraycopy(allBytes, pos, encryptedSecretKeyPart, 0, encryptedSecretKeySize);
      pos += encryptedSecretKeySize;

      //
      //  encrypted license bytes - bytes 257+
      //

      int encryptedLicenseSize = allBytes.length - (encryptedSecretKeySize+1);
      byte [] encryptedLicensePart = new byte[encryptedLicenseSize];
      System.arraycopy(allBytes, pos, encryptedLicensePart, 0, encryptedLicenseSize);

      //
      //  licenseToken (private key)
      //

      byte [] licenseTokenBytes = Files.readAllBytes(new File(privateKeyLocation).toPath());
      byte [] deobfuscatedPrivateKeyPart = deobfuscateLicenseToken(encryptVersion, licenseTokenBytes);

      /*****************************************
      *
      *  intialize
      *
      *****************************************/
      
      initializeParts(encryptedSecretKeyPart, deobfuscatedPrivateKeyPart, encryptedLicensePart);
    }

    /*****************************************
    *
    *  intializeParts
    *
    *****************************************/

    private void initializeParts(byte [] encryptedSecretKeyBytes, byte [] privateKeyBytes, byte [] encryptedLicenseBytes)
    {
      this.encryptedSecretKeyBytes = encryptedSecretKeyBytes;
      this.privateKeyBytes = privateKeyBytes;
      this.encryptedLicenseBytes = encryptedLicenseBytes;
    }

    /*****************************************
    *
    *  deobfuscateLicenseToken
    *
    *****************************************/
  
    private int saltSize = 8;
    private int [] swapPos = {37, 7, 41, 47, 61, 18, 35, 21, 88 };
    private byte [] deobfuscateLicenseToken(byte encryptVersion, byte [] bytesToDeobfuscate) 
    {
      //
      //  check version
      //

      if (encryptVersion != 1) throw new RuntimeException ("Unsupported version: " + encryptVersion);

      //
      //  decode
      //

      byte [] decodedBytes = Base64.getDecoder().decode(bytesToDeobfuscate);

      //
      //  swap back
      //

      for (int i=0; i<swapPos.length; i++)
        {
          byte tmp = decodedBytes[swapPos[i]];
          decodedBytes[swapPos[i]] = decodedBytes[i];
          decodedBytes[i] = tmp;
        }

      //
      //  remove salt
      //

      int originalLength = decodedBytes.length - saltSize;
      byte [] deobfuscatedBytes = new byte[originalLength];
      System.arraycopy(decodedBytes, saltSize, deobfuscatedBytes, 0, originalLength);

      //
      //  return
      // 

      return deobfuscatedBytes;
    }

    /*****************************************
    *
    *  decrypt
    *
    *****************************************/

    void decrypt() throws Exception
    {
      //
      //  decrypt the secret key
      //

      Cipher secretKeyCipher = Cipher.getInstance("RSA");
      secretKeyCipher.init(Cipher.DECRYPT_MODE, getPrivate(this.privateKeyBytes, "RSA"));
      byte [] secretKeyBytes = secretKeyCipher.doFinal(this.encryptedSecretKeyBytes);

      //
      //  decrypt licenseFile - use decrypted secret key against the encrypted license
      //

      Cipher licenseCipher = Cipher.getInstance("AES");
      licenseCipher.init(Cipher.DECRYPT_MODE, getSecretKey(secretKeyBytes, "AES"));
      this.licenseBytes = licenseCipher.doFinal(encryptedLicenseBytes);
    }

    /*****************************************
    *
    *  getPrivate
    *
    *****************************************/

    private PrivateKey getPrivate(byte [] keyBytes, String algorithm) throws Exception
    {
      PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
      KeyFactory kf = KeyFactory.getInstance(algorithm);
      return kf.generatePrivate(spec);
    }

    /*****************************************
    *
    *  getPublic
    *
    *****************************************/

    private PublicKey getPublic(byte [] keyBytes, String algorithm) throws Exception
    {
      X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
      KeyFactory kf = KeyFactory.getInstance(algorithm);
      return kf.generatePublic(spec);
    }

    /*****************************************
    *
    *  getSecretKey
    *
    *****************************************/

    private SecretKeySpec getSecretKey(byte [] keyBytes, String algorithm) throws IOException
    {
      return new SecretKeySpec(keyBytes, algorithm);
    }
  }

  /*****************************************
  *
  *  parseJSONRepresentation
  *
  *****************************************/

  private static JSONObject parseJSONRepresentation(String jsonString) throws JSONUtilitiesException
  {
    JSONObject result = null;
    try
      {
        result = (JSONObject) (new JSONParser()).parse(jsonString);
      }
    catch (org.json.simple.parser.ParseException e)
      {
        throw new JSONUtilitiesException("jsonRepresentation", e);
      }
    return result;
  }

  /***********************************************************************
  *
  *  class  TimeLimitAlarm
  *
  ***********************************************************************/
  
  public class TimeLimitAlarm
  {
    SortedMap<AlarmLevel, Pair<Integer,String>> alarmSpecification = null;
    SortedMap<AlarmLevel, Date> alarmTimes = null;

    public TimeLimitAlarm (JSONObject jsonRoot, Date licenseExpireTime)
    {
      //
      // create alarms
      //

      JSONArray alarms = JSONUtilities.decodeJSONArray(jsonRoot, "timeLimit", true);

      //
      //  construct specification
      //
      
      alarmSpecification = new TreeMap<AlarmLevel, Pair<Integer,String>>();
      for (int i=0; i<alarms.size(); i++)
        {
          //
          //  json
          //

          JSONObject alarmJson = (JSONObject) alarms.get(i);
          AlarmLevel alarmLevel = AlarmLevel.fromExternalRepresentation(JSONUtilities.decodeInteger(alarmJson, "alarmLevel", true));

          //
          //  parse
          //

          String [] raiseSpec = JSONUtilities.decodeString(alarmJson, "raise", true).split("\\s+", 2);
          Integer timeValue = new Integer(raiseSpec[0]);
          String timeUnit = raiseSpec[1].toLowerCase();
          switch (timeUnit)
            {
              case "minute":
              case "hour":
              case "day":
              case "week":
              case "month":
                break;

              default:
                throw new RuntimeException ("unsupported timeunit " + timeUnit);
            }

          //
          //  add to spec
          //
          
          alarmSpecification.put(alarmLevel, new Pair<Integer,String>(timeValue, timeUnit));
        }

      //
      //  setAlarmTimes
      //

      this.alarmTimes = setAlarmTimes(licenseExpireTime);

      //
      //  validate increasing order
      //

      Date compareTime = new Date(0L);
      for (Date alarmTime : this.alarmTimes.values())
        {
          if (alarmTime.before(compareTime)) throw new RuntimeException("Alarm times must be in order from Level 1 to Level 4");
          compareTime = alarmTime;
        }
    }

    /*****************************************
    *
    *  checkForAlarm
    *
    *****************************************/
    
    AlarmLevel checkForAlarm(Date currentTime)
    {
      AlarmLevel result = AlarmLevel.None;
      for (AlarmLevel alarmLevel : alarmTimes.keySet())
        {
          Date alarmTime = alarmTimes.get(alarmLevel);
          if (currentTime.before(alarmTime)) break;
          result = alarmLevel;
        }
      return result;
    }

    /*****************************************
    *
    *  nextAlarm
    *
    *****************************************/
    
    Pair <AlarmLevel, Date> nextAlarm(Date currentTime)
    {
      Pair<AlarmLevel, Date>  result = null;
      for (AlarmLevel alarmLevel : alarmTimes.keySet())
        {
          Date alarmTime = alarmTimes.get(alarmLevel);
          if (currentTime.before(alarmTime))
            {
              result = new Pair<AlarmLevel, Date> (alarmLevel,alarmTime);
              break;
            }
        }
      return result;
    }

    /*****************************************
    *
    *  updateAlarmTimes
    *
    *****************************************/
    
    public void updateAlarms(Date licenseExpireTime)
    {
      this.alarmTimes = setAlarmTimes(licenseExpireTime);
    }

    /*****************************************
    *
    *  setAlarmTimes
    *
    *****************************************/
    
    private SortedMap<AlarmLevel, Date> setAlarmTimes(Date referenceTime)
    {
      if (this.alarmSpecification == null) throw new RuntimeException("Alarm specification not set");

      SortedMap<AlarmLevel, Date> alarmTimes = new TreeMap<AlarmLevel, Date>();
      for (AlarmLevel alarmLevel : alarmSpecification.keySet())
        {
          Pair<Integer,String> spec = alarmSpecification.get(alarmLevel);
          Integer timeValue = (Integer) spec.getFirstElement();
          String timeUnit = (String) spec.getSecondElement();
          Date alarmTime;
          switch (timeUnit)
            {
              case "minute":
                alarmTime = RLMDateUtils.addMinutes(referenceTime, timeValue);
                break;

              case "hour":
                alarmTime = RLMDateUtils.addHours(referenceTime, timeValue);
                break;

              case "day":
                alarmTime = RLMDateUtils.addDays(referenceTime, timeValue, Deployment.getDefault().getTimeZone());
                break;

              case "week":
                alarmTime = RLMDateUtils.addWeeks(referenceTime, timeValue, Deployment.getDefault().getTimeZone());
                break;

              case "month":
                alarmTime = RLMDateUtils.addMonths(referenceTime, timeValue, Deployment.getDefault().getTimeZone());
                break;

              default:
                throw new RuntimeException ("unsupported timeunit " + timeUnit);
            }
          alarmTimes.put(alarmLevel, alarmTime);
        }

      return alarmTimes;
    }

    /*****************************************
    *
    *  toString
    *
    *****************************************/
    
    public String toString()
    {
      StringBuilder b = new StringBuilder();
      b.append("alarmSpecification=" + alarmSpecification);
      b.append("alarmTimes=" + alarmTimes);
      return b.toString();
    }
  }

  /***********************************************************************
  *
  *  class  NodeAndComponentLimitAlarm
  *
  ***********************************************************************/
  
  public class NodeAndComponentLimitAlarm
  {
    AlarmLevel defaultAlarmLevel = null;
    Map<String, AlarmLevel> configuredAlarmLevels = null;

    public NodeAndComponentLimitAlarm (JSONObject jsonRoot)
    {
      //
      // create alarms
      //

      this.defaultAlarmLevel = AlarmLevel.fromExternalRepresentation(JSONUtilities.decodeInteger(jsonRoot, "defaultAlarmLevel", true));
      JSONArray configuredAlarmLevelsJson = JSONUtilities.decodeJSONArray(jsonRoot, "configuredAlarmLevels", true);

      this.configuredAlarmLevels = new HashMap<String, AlarmLevel>();
      for (int i=0; i<configuredAlarmLevelsJson.size(); i++)
        {
          //
          //  json
          //

          JSONObject alarmJson = (JSONObject) configuredAlarmLevelsJson.get(i);
          String  entityName = JSONUtilities.decodeString(alarmJson, "name", true);
          AlarmLevel alarmLevel = AlarmLevel.fromExternalRepresentation(JSONUtilities.decodeInteger(alarmJson, "alarmLevel", true));

          //
          //  add to config
          //
          
          this.configuredAlarmLevels.put(entityName, alarmLevel);
        }
    }

    /*****************************************
    *
    *  getDefaultAlarmLevel
    *
    *****************************************/
    
    AlarmLevel getDefaultAlarmLevel()
    {
      return defaultAlarmLevel;
    }

    /*****************************************
    *
    *  getConfiguredAlarmLevel
    *
    *****************************************/
    
    AlarmLevel getConfiguredAlarmLevel(String name)
    {
      AlarmLevel result = configuredAlarmLevels.get(name);
      return (result != null) ? result : getDefaultAlarmLevel();
    }

    /*****************************************
    *
    *  toString
    *
    *****************************************/
    
    public String toString()
    {
      StringBuilder b = new StringBuilder();
      b.append("defaultAlarmLevel=" + defaultAlarmLevel);
      b.append(",alarmLevels=" + configuredAlarmLevels);
      return b.toString();
    }
  }

  /**********************************************************************
  *
  *  LicenseStatistics
  *
  **********************************************************************/

  public static class LicenseStatistics
  {
    //
    //  attributes
    //

    LicenseExpiryStatistics expiryStatistics;

    /*****************************************
    *
    *  constructor
    *
    *****************************************/
    
    public LicenseStatistics(String id, Date expireDate)
    {
      //
      //  expiry statistics
      //

      try
        {
          this.expiryStatistics = new LicenseExpiryStatistics(id, expireDate);
        }
      catch (ServerException e)
        {
          log.error("Could not create LicenseExpiryStatistics.");
        }
          
      //
      // component, node, capacity stats, TBD
      //
    }

    /*****************************************
    *
    *  updateExpiryAlarmLevel
    *
    *****************************************/
    
    void updateExpiryAlarmLevel (AlarmLevel level)
    {
      expiryStatistics.updateAlarmLevel(level);
    }

    /*****************************************
    *
    *  updateExpiryExpireDate
    *
    *****************************************/
    
    void updateExpiryExpireDate (Date expireDate)
    {
      expiryStatistics.updateExpireDate(expireDate);
    }
  }

  /***********************************************************************
  *
  *  class AlarmKey
  *
  ***********************************************************************/

  class AlarmKey
  {
    //
    //  attributes
    //
    
    String source;
    AlarmType type;
    String context;

    //
    //  accessors
    //
    
    public String getSource() { return source; }
    public AlarmType getType() { return type; }
    public String getContext() { return context; }
    /*****************************************
    *
    *  constructor - basic
    *
    *****************************************/

    public AlarmKey(String source, AlarmType type, String context)
    {
      this.source = source;
      this.type = type;
      this.context = context;
    }

    /*****************************************
    *
    *  equals/hashCode
    *
    *****************************************/

    //
    //  equals
    //

    public boolean equals(Object obj)
    {
      boolean result = false;
      if (obj instanceof AlarmKey)
        {
          AlarmKey other = (AlarmKey) obj;
          result = source.equals(other.getSource()) && context.equals(other.getContext()) && (type == other.getType());
        }
      return result;
    }

    //
    //  hashCode
    //

    public int hashCode()
    {
      return source.hashCode() + type.hashCode() + context.hashCode();
    }
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

}
