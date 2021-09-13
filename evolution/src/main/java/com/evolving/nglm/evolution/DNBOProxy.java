/*****************************************************************************
*
*  DNBOProxy.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.evolving.nglm.evolution.offeroptimizer.DNBOMatrixAlgorithmParameters;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.AssignSubscriberIDs;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.DeploymentCommon;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.RecordSubscriberID;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SubscriberIDService;
import com.evolving.nglm.core.SubscriberIDService.SubscriberIDServiceException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm.OfferOptimizationAlgorithmParameter;
import com.evolving.nglm.evolution.SubscriberProfileService.EngineSubscriberProfileService;
import com.evolving.nglm.evolution.SubscriberProfileService.SubscriberProfileServiceException;
import com.evolving.nglm.evolution.ThirdPartyManager.AuthenticatedResponse;
import com.evolving.nglm.evolution.ThirdPartyManager.ThirdPartyCredential;
import com.evolving.nglm.evolution.ThirdPartyManager.ThirdPartyManagerException;
import com.evolving.nglm.evolution.offeroptimizer.GetOfferException;
import com.evolving.nglm.evolution.offeroptimizer.OfferOptimizerAlgoManager;
import com.evolving.nglm.evolution.offeroptimizer.ProposedOfferDetails;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class DNBOProxy
{

  /*****************************************
  *
  *  ProductID
  *
  *****************************************/

  public static String ProductID = "Evolution-DNBOProxy";

  /*****************************************
  *
  *  enum
  *
  *****************************************/

  public enum API
  {
    getSubscriberOffers("getSubscriberOffers"),
    provisionSubscriber("provisionSubscriber"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private API(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static API fromExternalRepresentation(String externalRepresentation) { for (API enumeratedValue : API.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(DNBOProxy.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  //
  //  static
  //

  private static final int RESTAPIVersion = 1;
  
  private static Map<String, ThirdPartyMethodAccessLevel> methodPermissionsMapper = new LinkedHashMap<String,ThirdPartyMethodAccessLevel>();
  private static Integer authResponseCacheLifetimeInMinutes = null;

  //
  //  instance
  //

  private HttpServer restServer;
  private OfferService offerService;
  private SupplierService supplierService;
  private ProductService productService;
  private ProductTypeService productTypeService;
  private VoucherService voucherService;
  private VoucherTypeService voucherTypeService;
  private CatalogCharacteristicService catalogCharacteristicService;
  private ScoringStrategyService scoringStrategyService;
  private SalesChannelService salesChannelService;
  private SubscriberProfileService subscriberProfileService;
  private ReferenceDataReader<String, SubscriberGroupEpoch> subscriberGroupEpochReader = null;
  private SegmentationDimensionService segmentationDimensionService;
  private DNBOMatrixService dnboMatrixService;
  private DynamicCriterionFieldService dynamicCriterionFieldService;
  private SubscriberIDService subscriberIDService;
  private KafkaProducer<byte[], byte[]> kafkaProducer;
  private KafkaConsumer<byte[], byte[]> kafkaConsumer;
  private Map<AssignSubscriberIDs,LinkedBlockingQueue<String>> outstandingRequests = new HashMap<>();
  private Thread subscriberManagerTopicReaderThread = null;
  private volatile boolean stopRequested = false;
  
   
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

 private static final String ENV_CONF_THIRDPARTY_HTTP_CLIENT_TIMEOUT_MS = "DNBOPROXY_HTTP_CLIENT_TIMEOUT_MS";
 private static int httpTimeout = 10000;
 private String fwkServer = null; 
 static{
   String timeoutConf = System.getenv().get(ENV_CONF_THIRDPARTY_HTTP_CLIENT_TIMEOUT_MS);
   if(timeoutConf!=null && !timeoutConf.isEmpty()){
     try{
       httpTimeout=Integer.parseInt(timeoutConf);
       log.info("loading env conf "+ENV_CONF_THIRDPARTY_HTTP_CLIENT_TIMEOUT_MS+" "+httpTimeout);
     }catch (NumberFormatException e){
       log.warn("bad env conf "+ENV_CONF_THIRDPARTY_HTTP_CLIENT_TIMEOUT_MS, e);
     }
   }
 }
  RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(httpTimeout).setSocketTimeout(httpTimeout).setConnectionRequestTimeout(httpTimeout).build();
  
  //
  //  authCache
  //

  TimebasedCache<ThirdPartyCredential, AuthenticatedResponse> authCache;


  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args) throws Exception
  {
    NGLMRuntime.initialize(true);
    DNBOProxy dnboProxy = new DNBOProxy();
    dnboProxy.start(args);
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
    int apiRestPort = parseInteger("apiRestPort", args[1]);
    int dnboProxyThread = parseInteger("DNBOPROXY_THREADS", args[2]);
    this.fwkServer = args[3];

    //
    //  log
    //

    log.info("main START: {} {}", apiProcessKey, apiRestPort);
    
    methodPermissionsMapper = Deployment.getThirdPartyMethodPermissionsMap();
    authResponseCacheLifetimeInMinutes = Deployment.getAuthResponseCacheLifetimeInMinutes() == null ? new Integer(0) : Deployment.getAuthResponseCacheLifetimeInMinutes();
    authCache = TimebasedCache.getInstance(60000*authResponseCacheLifetimeInMinutes);


    /*****************************************
    *
    *  services - construct
    *
    *****************************************/
    
    dynamicCriterionFieldService = new DynamicCriterionFieldService(Deployment.getBrokerServers(), "dnboproxy-dynamiccriterionfieldservice-"+apiProcessKey, Deployment.getDynamicCriterionFieldTopic(), false);
    dynamicCriterionFieldService.start();
    CriterionContext.initialize(dynamicCriterionFieldService);  

    offerService = new OfferService(Deployment.getBrokerServers(), "dnboproxy-offerservice-" + apiProcessKey, Deployment.getOfferTopic(), false);
    supplierService = new SupplierService(Deployment.getBrokerServers(), "dnboproxy-supplierservice-" + apiProcessKey, Deployment.getSupplierTopic(), false);
    productService = new ProductService(Deployment.getBrokerServers(), "dnboproxy-productservice-" + apiProcessKey, Deployment.getProductTopic(), false);
    productTypeService = new ProductTypeService(Deployment.getBrokerServers(), "dnboproxy-producttypeservice-" + apiProcessKey, Deployment.getProductTypeTopic(), false);
    voucherService = new VoucherService(Deployment.getBrokerServers(), "dnboproxy-voucherservice-" + apiProcessKey, Deployment.getVoucherTopic());
    voucherTypeService = new VoucherTypeService(Deployment.getBrokerServers(), "dnboproxy-vouchertypeservice-" + apiProcessKey, Deployment.getVoucherTypeTopic(), false);
    catalogCharacteristicService = new CatalogCharacteristicService(Deployment.getBrokerServers(), "dnboproxy-catalogcharacteristicservice-" + apiProcessKey, Deployment.getCatalogCharacteristicTopic(), false);
    scoringStrategyService = new ScoringStrategyService(Deployment.getBrokerServers(), "dnboproxy-scoringstrategyservice-" + apiProcessKey, Deployment.getScoringStrategyTopic(), false);
    salesChannelService = new SalesChannelService(Deployment.getBrokerServers(), "dnboproxy-saleschannelservice-" + apiProcessKey, Deployment.getSalesChannelTopic(), false);
    subscriberProfileService = new EngineSubscriberProfileService(Deployment.getSubscriberProfileEndpoints(), dnboProxyThread);
    subscriberGroupEpochReader = ReferenceDataReader.<String, SubscriberGroupEpoch>startReader("dnboproxy-subscribergroupepoch", Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);
    segmentationDimensionService = new SegmentationDimensionService(Deployment.getBrokerServers(), "dnboproxy-segmentationdimensionservice-"+apiProcessKey, Deployment.getSegmentationDimensionTopic(), false);
    dnboMatrixService = new DNBOMatrixService(Deployment.getBrokerServers(),"dnboproxy-matrixservice"+apiProcessKey,Deployment.getDNBOMatrixTopic(),false);
    subscriberIDService = new SubscriberIDService(Deployment.getRedisSentinels(), "dnboproxy-" + apiProcessKey);
    
    /*****************************************
    *
    *  services - start
    *
    *****************************************/

    offerService.start();
    supplierService.start();
    productService.start();
    productTypeService.start();
    voucherService.start();
    voucherTypeService.start();
    catalogCharacteristicService.start();
    scoringStrategyService.start();
    salesChannelService.start();
    subscriberProfileService.start();
    segmentationDimensionService.start();
    dnboMatrixService.start();

    /*****************************************
    *
    *  subscriber provisioning
    *
    *****************************************/

    //
    //  kafka producer
    //
    
    Properties producerProperties = new Properties();
    producerProperties.put("bootstrap.servers", Deployment.getBrokerServers());
    producerProperties.put("acks", "all");
    producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProducer = new KafkaProducer<byte[], byte[]>(producerProperties);
    
    //
    // kafka consumer
    //

    Properties consumerProperties = new Properties();
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Deployment.getBrokerServers());
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "dnboproxy-subscriberprovisioning-" + apiProcessKey);
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    GUIService.setCommonConsumerProperties(consumerProperties);
    kafkaConsumer = new KafkaConsumer<>(consumerProperties);
    kafkaConsumer.subscribe(Arrays.asList(Deployment.getRecordSubscriberIDTopic()));

    //
    // response processor
    //

    Runnable subscriberManagerTopicReader = new Runnable() { @Override public void run() { readSubscriberManagerTopic(kafkaConsumer); } };
    subscriberManagerTopicReaderThread = new Thread(subscriberManagerTopicReader, "SubscriberManagerTopicReader");
    subscriberManagerTopicReaderThread.start();
    
    /*****************************************
    *
    *  REST interface -- server and handlers
    *
    *****************************************/

    try
      {
        InetSocketAddress addr = new InetSocketAddress(apiRestPort);
        restServer = HttpServer.create(addr, 0);
        restServer.createContext("/nglm-dnboproxy/getSubscriberOffers", new APIHandler(API.getSubscriberOffers));
        restServer.createContext("/nglm-dnboproxy/provisionSubscriber", new APIHandler(API.provisionSubscriber));
        restServer.setExecutor(Executors.newFixedThreadPool(dnboProxyThread));
        restServer.start();
      }
    catch (IOException e)
      {
        throw new ServerRuntimeException("could not initialize REST server", e);
      }

    /*****************************************
    *
    *  shutdown hook
    *
    *****************************************/

    NGLMRuntime.addShutdownHook(new ShutdownHook(this, restServer, offerService, supplierService, productService, productTypeService, voucherService, voucherTypeService, catalogCharacteristicService, scoringStrategyService, salesChannelService, subscriberProfileService, segmentationDimensionService, dnboMatrixService, subscriberIDService, kafkaProducer, kafkaConsumer, subscriberManagerTopicReaderThread));

    /*****************************************
    *
    *  log restServerStarted
    *
    *****************************************/

    log.info("main restServerStarted with " + dnboProxyThread + " threads");
  }

  /*****************************************
  *
  *  class ShutdownHook
  *
  *****************************************/

  private static class ShutdownHook implements NGLMRuntime.NGLMShutdownHook
  {
    //
    //  data
    //

    private DNBOProxy dnboProxy;
    private HttpServer restServer;
    private OfferService offerService;
    private SupplierService supplierService;
    private ProductService productService;   
    private ProductTypeService productTypeService;
    private VoucherService voucherService;
    private VoucherTypeService voucherTypeService;
    private CatalogCharacteristicService catalogCharacteristicService;
    private ScoringStrategyService scoringStrategyService;
    private SalesChannelService salesChannelService;
    private SubscriberProfileService subscriberProfileService;
    private SegmentationDimensionService segmentationDimensionService;
    private DNBOMatrixService dnboMatrixService;
    private SubscriberIDService subscriberIDService;
    private KafkaProducer<byte[], byte[]> kafkaProducer;
    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    Thread subscriberManagerTopicReaderThread;

    //
    //  constructor
    //

    private ShutdownHook(DNBOProxy dnboProxy, HttpServer restServer, OfferService offerService, SupplierService supplierService, ProductService productService, ProductTypeService productTypeService, VoucherService voucherService, VoucherTypeService voucherTypeService, CatalogCharacteristicService catalogCharacteristicService, ScoringStrategyService scoringStrategyService, SalesChannelService salesChannelService, SubscriberProfileService subscriberProfileService, SegmentationDimensionService segmentationDimensionService, DNBOMatrixService dnboMatrixService, SubscriberIDService subscriberIDService, KafkaProducer<byte[], byte[]> kafkaProducer, KafkaConsumer<byte[], byte[]> kafkaConsumer, Thread subscriberManagerTopicReaderThread)
    {
      this.dnboProxy = dnboProxy;
      this.restServer = restServer;
      this.offerService = offerService;
      this.supplierService = supplierService;
      this.productService = productService;
      this.productTypeService = productTypeService;
      this.voucherService = voucherService;
      this.voucherTypeService = voucherTypeService;
      this.catalogCharacteristicService = catalogCharacteristicService;
      this.scoringStrategyService = scoringStrategyService;
      this.salesChannelService = salesChannelService;
      this.subscriberProfileService = subscriberProfileService;
      this.segmentationDimensionService = segmentationDimensionService;
      this.dnboMatrixService = dnboMatrixService;
      this.subscriberIDService = subscriberIDService;
      this.kafkaProducer = kafkaProducer;
      this.kafkaConsumer = kafkaConsumer;
      this.subscriberManagerTopicReaderThread = subscriberManagerTopicReaderThread;
    }

    //
    //  shutdown
    //

    @Override public void shutdown(boolean normalShutdown)
    {
      //
      //  mark stopped
      //

      dnboProxy.stopRequested = true;
      
      //
      //  rest server
      //

      if (restServer != null) restServer.stop(1);
      
      //
      //  services
      //

      if (offerService != null) offerService.stop();
      if (supplierService != null) supplierService.stop();
      if (productService != null) productService.stop();
      if (productTypeService != null) productTypeService.stop();
      if (voucherService != null) voucherService.stop();
      if (voucherTypeService != null) voucherTypeService.stop();
      if (catalogCharacteristicService != null) catalogCharacteristicService.stop();
      if (scoringStrategyService != null) scoringStrategyService.stop();
      if (salesChannelService != null) salesChannelService.stop();
      if (subscriberProfileService != null) subscriberProfileService.stop();
      if (segmentationDimensionService != null) segmentationDimensionService.stop();
      if (dnboMatrixService != null) dnboMatrixService.stop();
      if (subscriberIDService != null) subscriberIDService.stop();

      //
      //  consumer thread
      //

      subscriberManagerTopicReaderThread.interrupt();
      try { subscriberManagerTopicReaderThread.join(); } catch (InterruptedException e) { }

      //
      //  kafka
      //

      if (kafkaConsumer != null) kafkaConsumer.close();
      if (kafkaProducer != null) kafkaProducer.close();
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
  /*****************************************
  *
  *  authenticateAndCheckAccess
  *
  *****************************************/

 private int authenticateAndCheckAccess(JSONObject jsonRoot, String api) throws ThirdPartyManagerException, ParseException, IOException
 {

   ThirdPartyCredential thirdPartyCredential = new ThirdPartyCredential(jsonRoot);

   //
   // confirm thirdPartyCredential format is ok
   //

   if (thirdPartyCredential.getLoginName() == null || thirdPartyCredential.getPassword() == null || thirdPartyCredential.getLoginName().isEmpty() || thirdPartyCredential.getPassword().isEmpty())
     {
       log.error("invalid request {}", "credential is missing");
       throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage() + "-{credential is missing}", RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseCode());
     }

   //
   // look up methodAccess configuration from deployment
   //

   ThirdPartyMethodAccessLevel methodAccessLevel = methodPermissionsMapper.get(api);

   //
   // access hack(dev purpose) return by default tenant 1
   //

   if (methodAccessLevel != null && methodAccessLevel.isByPassAuth()) return 1;

   //
   // lookup from authCache
   //

   AuthenticatedResponse authResponse = authCache.get(thirdPartyCredential);
   if(authResponse == null)
     {
       synchronized (authCache)
       {
         authResponse = authCache.get(thirdPartyCredential);
         
         //
         //  cache miss - reauthenticate
         //
   
         if (authResponse == null)
           {
             authResponse = authenticate(thirdPartyCredential);
             log.info("(Re)Authenticated: credential {} response {}", thirdPartyCredential, authResponse);
           }
       }
   }
   
   return authResponse.getTenantID();

 }

 /*****************************************
  *
  *  authenticate
  *
  *****************************************/

 private AuthenticatedResponse authenticate(ThirdPartyCredential thirdPartyCredential) throws IOException, ParseException, ThirdPartyManagerException
 {
   try (CloseableHttpClient httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build())
   {
     //
     // create request
     //

     StringEntity stringEntity = new StringEntity(thirdPartyCredential.getJSONString(), ContentType.create("application/json"));
     HttpPost httpPost = new HttpPost("http://" + fwkServer + "/api/account/login");
     httpPost.setEntity(stringEntity);

     //
     // submit request
     //

     HttpResponse httpResponse = httpClient.execute(httpPost);

     //
     // process response
     //

     if (httpResponse != null && httpResponse.getStatusLine() != null && httpResponse.getStatusLine().getStatusCode() == 200)
       {
         String jsonResponse = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
         log.info("FWK raw response : {}", jsonResponse);

         //
         // parse JSON response from FWK
         //

         JSONObject jsonRoot = (JSONObject) (new JSONParser()).parse(jsonResponse);

         //
         // prepare response
         //

         AuthenticatedResponse authResponse = new AuthenticatedResponse(jsonRoot);


         //
         // update cache
         //

         synchronized (authCache)
         {
           authCache.put(thirdPartyCredential, authResponse);
         }

         //
         // return
         //

         return authResponse;
       }
     else if (httpResponse != null && httpResponse.getStatusLine() != null && httpResponse.getStatusLine().getStatusCode() == 401)
       {
         log.error("FWK server HTTP reponse code {} message {} ", httpResponse.getStatusLine().getStatusCode(), EntityUtils.toString(httpResponse.getEntity(), "UTF-8"));
         throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.AUTHENTICATION_FAILURE);
       }
     else if (httpResponse != null && httpResponse.getStatusLine() != null)
       {
         log.error("FWK server HTTP reponse code is invalid {}", httpResponse.getStatusLine().getStatusCode());
         throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.AUTHENTICATION_FAILURE);
       }
     else
       {
         log.error("FWK server error httpResponse or httpResponse.getStatusLine() is null {} {} ", httpResponse, httpResponse.getStatusLine());
         throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.AUTHENTICATION_FAILURE);
       }
   }
   catch(ParseException pe) 
   {
     log.error("failed to Parse ParseException {} ", pe.getMessage());
     throw pe;
   }
   catch(IOException e) 
   {
     log.error("failed to authenticate in FWK server");
     log.error("IOException: {}", e.getMessage());
     throw e;
   }
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
        *  get the user
        *
        *****************************************/

        String userID = null;
        if (exchange.getRequestURI().getQuery() != null)
          {
            Pattern pattern = Pattern.compile("^(.*\\&user_id|user_id)=(.*?)(\\&.*$|$)");
            Matcher matcher = pattern.matcher(exchange.getRequestURI().getQuery());
            if (matcher.matches())
              {
                userID = matcher.group(2);
              }
          }

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
        JSONObject jsonRoot = (JSONObject) (new JSONParser()).parse((requestBodyStringBuilder.length()) > 0 ? requestBodyStringBuilder.toString() : "{ }");

        /*****************************************
         *
         *  authenticate and accessCheck
         *
         *****************************************/

        // EVPRO-1117
        int tenantID = DeploymentCommon.getDefaultTenant().getTenantID(); 
        if (! Deployment.getRegressionMode())
          {
            tenantID = authenticateAndCheckAccess(jsonRoot, api.name());
          }
        
        /*****************************************
        *
        *  backward compatibility (move arguments from the URI to the body)
        *
        *****************************************/

        //
        //  apiVersion
        //

        if (JSONUtilities.decodeInteger(jsonRoot, "apiVersion", false) == null) 
          {
            jsonRoot.put("apiVersion", RESTAPIVersion);
          }

        //
        //  scoringStrategy
        //
        
        if (jsonRoot.get("scoringStrategyID") == null && exchange.getRequestURI().getQuery() != null)
          {
            Pattern pattern = Pattern.compile("^(.*\\&strategy|strategy)=(.*?)(\\&.*$|$)");
            Matcher matcher = pattern.matcher(exchange.getRequestURI().getQuery());
            if (matcher.matches())
              {
                String scoringStrategyID = matcher.group(2);
                Object objectToAdd;
                if (scoringStrategyID != null && scoringStrategyID.indexOf(",") == -1)
                  {
                    //
                    // single value
                    //
                    objectToAdd = scoringStrategyID;
                  }
                else
                  {
                    //
                    // We got a list : 21,22,23 -> convert it to a JSON array
                    //
                    List<String> items = Arrays.asList(scoringStrategyID.split("\\s*,\\s*"));
                    objectToAdd = JSONUtilities.encodeArray(items);
                    
                  }
                jsonRoot.put("scoringStrategyID", objectToAdd);
              }
          }

        //
        //  subscriberID
        //

        if (JSONUtilities.decodeString(jsonRoot, "subscriberID", false) == null && exchange.getRequestURI().getQuery() != null)
          {
            Pattern pattern = Pattern.compile("^(.*\\&msisdn|msisdn|\\&subscriberID|subscriberID)=(.*?)(\\&.*$|$)");
            Matcher matcher = pattern.matcher(exchange.getRequestURI().getQuery());
            if (matcher.matches())
              {
                jsonRoot.put("subscriberID", matcher.group(2));
              }
          }

        //
        //  salesChannelID
        //

        if (JSONUtilities.decodeString(jsonRoot, "salesChannelID", false) == null && exchange.getRequestURI().getQuery() != null)
          {
            Pattern pattern = Pattern.compile("^(.*\\&salesChannel|salesChannel)=(.*?)(\\&.*$|$)");
            Matcher matcher = pattern.matcher(exchange.getRequestURI().getQuery());
            if (matcher.matches())
              {
                jsonRoot.put("salesChannelID", matcher.group(2));
              }
          }

        //
        //  salesChannelID
        //

        if (JSONUtilities.decodeDouble(jsonRoot, "rangeValue", false) == null && exchange.getRequestURI().getQuery() != null)
        {
          Pattern pattern = Pattern.compile("^(.*\\&rangeValue|rangeValue)=(.*?)(\\&.*$|$)");
          Matcher matcher = pattern.matcher(exchange.getRequestURI().getQuery());
          if (matcher.matches())
          {
            jsonRoot.put("rangeValue", Double.parseDouble(matcher.group(2)));
          }
        }
        
        //
        //  alternateIDs
        //

        if (jsonRoot.get("alternateIDs") == null && exchange.getRequestURI().getQuery() != null)
          {
            //
            //  alternateIDName
            //
            
            String alternateIDName = null;
            Pattern pattern = Pattern.compile("^(.*\\&alternateIDName|alternateIDName)=(.*?)(\\&.*$|$)");
            Matcher matcher = pattern.matcher(exchange.getRequestURI().getQuery());
            if (matcher.matches())
              {
                alternateIDName = matcher.group(2);
              }
            
            //
            //  alternateID
            //
            
            String alternateID = null;
            pattern = Pattern.compile("^(.*\\&alternateID|alternateID)=(.*?)(\\&.*$|$)");
            matcher = pattern.matcher(exchange.getRequestURI().getQuery());
            if (matcher.matches())
              {
                alternateID = matcher.group(2);
              }

            //
            //  construct alternateIDs JSON (array of array)
            //
            
            if (alternateIDName != null && alternateID != null)
              {
                List<String> elements = new ArrayList<String>();
                elements.add(alternateIDName);
                elements.add(alternateID);
                JSONArray singletonArray = JSONUtilities.encodeArray(elements);
                List<JSONArray> topLevelArray = new ArrayList<JSONArray>();
                topLevelArray.add(singletonArray);
                jsonRoot.put("alternateIDs", JSONUtilities.encodeArray(topLevelArray));
              }
          }
        
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
        *  process
        *
        *****************************************/

        JSONObject jsonResponse = null;
        switch (api)
          {
            case getSubscriberOffers:
              jsonResponse = processGetSubscriberOffers(userID, jsonRoot,
                  productService, productTypeService, voucherService, voucherTypeService,
                  catalogCharacteristicService, subscriberGroupEpochReader,
                  segmentationDimensionService, offerService, supplierService, tenantID);
              break;
              
            case provisionSubscriber:
              jsonResponse = processProvisionSubscriber(userID, jsonRoot, tenantID);
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
    catch (org.json.simple.parser.ParseException | DNBOProxyException | SubscriberProfileServiceException | IOException | ServerException | RuntimeException | ThirdPartyManagerException e )
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
        response.put("statusCode", 1);
        response.put("status", "systemError");
        response.put("message", e.getMessage());
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
  *  processGetSubscriberOffers
  *
  *****************************************/

  private JSONObject processGetSubscriberOffers(String userID, JSONObject jsonRoot,
      ProductService productService, ProductTypeService productTypeService, VoucherService voucherService, VoucherTypeService voucherTypeService,
      CatalogCharacteristicService catalogCharacteristicService,
      ReferenceDataReader<String, SubscriberGroupEpoch> subscriberGroupEpochReader,
      SegmentationDimensionService segmentationDimensionService, OfferService offerService, SupplierService supplierService, int tenantID) throws DNBOProxyException, SubscriberProfileServiceException
  {
    /*****************************************
    *
    *  arguments
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    String subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
    String salesChannelID = JSONUtilities.decodeString(jsonRoot, "salesChannelID", true);

    //range value extract and validation
    Double rangeValue = JSONUtilities.decodeDouble(jsonRoot,"rangeValue",false);
    if(rangeValue == null)
    {
      rangeValue = new Double(0);
    }else if (rangeValue.isNaN())
    {
      throw new DNBOProxyException("Range value = "+rangeValue.toString()+" present but is not a number","");
    }

    List<String> scoringStrategyIDList;
    boolean multipleStrategies = true;
    Object scoringStrategyObject = jsonRoot.get("scoringStrategyID");
    if (scoringStrategyObject == null)
      {
        throw new DNBOProxyException("Missing required 'scoringStrategyID' parameter", "");
      }
    if (scoringStrategyObject instanceof String) // Single parameter. In this case, we construct a list with one element
      {
        String scoringStrategyID = (String) scoringStrategyObject;
        scoringStrategyIDList = new ArrayList<>();
        scoringStrategyIDList.add(scoringStrategyID);
        multipleStrategies = false;
      }
    else if (scoringStrategyObject instanceof JSONArray)
      {
        scoringStrategyIDList = (JSONArray) scoringStrategyObject;
      }
    else
      {
        throw new DNBOProxyException("Internal error, wrong type for 'scoringStrategyID' : "+scoringStrategyObject.getClass().getCanonicalName(), "");
      }
    
    /*****************************************
    *
    *  evaluate arguments
    *
    *****************************************/
    
    SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID);
    SalesChannel salesChannel = salesChannelService.getActiveSalesChannel(salesChannelID, now); 
    
    /*****************************************
    *
    *  validate arguments
    *
    *****************************************/

    if (subscriberProfile == null) throw new DNBOProxyException("unknown subscriber", subscriberID);
    if (salesChannel == null) throw new DNBOProxyException("unknown sales channel", salesChannelID);

    //
    // Process list of ScoringStrategies
    //
    
    List<JSONObject> resArray = new ArrayList<>();
    boolean allScoringStrategiesBad = true;
    boolean atLeastOneError = false;
    DNBOProxyException lastException = null;
    for (String scoringStrategyID : scoringStrategyIDList ) // process all scoringStrategies
      {
        if (log.isDebugEnabled())
          {
            log.debug("DNBOproxy.processGetSubscriberOffers Processing "+scoringStrategyID+" ...");
          }
        ScoringStrategy scoringStrategy = scoringStrategyService.getActiveScoringStrategy(scoringStrategyID, now);
        if (scoringStrategy == null)
          {
            log.info("DNBOproxy.processGetSubscriberOffers Unknown scoring strategy in list : " + scoringStrategyID);
            atLeastOneError = true;
            continue;
          }
        try
        {
          JSONObject valueRes = getOffers(now, subscriberID, scoringStrategyID, salesChannelID, subscriberProfile, scoringStrategy,
              productService, productTypeService, voucherService, voucherTypeService,
              catalogCharacteristicService, subscriberGroupEpochReader,
              segmentationDimensionService, offerService, supplierService, rangeValue.doubleValue(), tenantID);
          resArray.add(valueRes);
          allScoringStrategiesBad = false;
        }
        catch (DNBOProxyException e)
        {
          log.warn("DNBOProxy.processGetSubscriberOffers Exception " + e.getClass().getName() + " while processing " + scoringStrategyID, e);
          lastException = e;
          atLeastOneError = true;
        }
     
      }
    //
    //  response
    //

    if (allScoringStrategiesBad)
      {
        StringBuilder res = new StringBuilder();
        for (String scoringStrategyID : scoringStrategyIDList)
          {
            res.append(scoringStrategyID+" ");
          }
        log.info("DNBOproxy.processGetSubscriberOffers All scoring strategies are bad : " + res.toString());
        if (lastException == null)
          {
            throw new DNBOProxyException("All scoring strategies are bad : "+ res.toString(), "");
          }
        else
          {
            throw lastException;
          }
      }

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("statusCode", 0);
    response.put("status", "SUCCESS");
    String msg = "getSubscriberOffers ";
    if (atLeastOneError)
      {
        msg += "partially executed";
      }
    else
      {
        msg += "well executed";
      }
    response.put("message", msg);
    //searching if one of the strategies returned in list match the requested strategy, and remove others
    //in case list does not contain correct strategy will become empty and the second if will throw exception
    if(!multipleStrategies && resArray.size() > 1)
      {
        log.warn("DNBOproxy.processGetSubscriberOffers Internal error, list should contain exactly 1 element, not " + resArray.size() + ", Searching if correct strategy is present");
        resArray.removeIf(p->!JSONUtilities.decodeString(p,"scoringStrategyID",true).equals((String) scoringStrategyObject));
      }
    if (resArray.size() == 0)
    {
      log.error("DNBOproxy.processGetSubscriberOffers Internal error, list is empty");
      throw new DNBOProxyException("Unexpected error : list is empty", "");
    }
    response.put("value", JSONUtilities.encodeArray(resArray));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getOffers
  *
  *****************************************/

  private JSONObject getOffers(Date now, String subscriberID, String scoringStrategyID, String salesChannelID,
      SubscriberProfile subscriberProfile, ScoringStrategy scoringStrategy,
      ProductService productService, ProductTypeService productTypeService,
      VoucherService voucherService, VoucherTypeService voucherTypeService,
      CatalogCharacteristicService catalogCharacteristicService,
      ReferenceDataReader<String, SubscriberGroupEpoch> subscriberGroupEpochReader,
      SegmentationDimensionService segmentationDimensionService, OfferService offerService, SupplierService supplierService, double rangeValue, int tenantID)
      throws DNBOProxyException
  {

    /*****************************************
    *
    *  score
    *
    *****************************************/
    
    StringBuffer returnedLog = new StringBuffer();
    String logFragment;

    // ###################################### SCORING GROUP
    // ####################################################################

    String msisdn = subscriberID;

    JSONObject valueRes;
    
    try {

      DNBOMatrixAlgorithmParameters dnboMatrixAlgorithmParameters = new DNBOMatrixAlgorithmParameters(dnboMatrixService,rangeValue);
      Collection<ProposedOfferDetails> offerAvailabilityFromPropensityAlgo = TokenUtils.getOffersWithScoringStrategy(
          now, salesChannelID, subscriberProfile, scoringStrategy,
          productService, productTypeService, voucherService, voucherTypeService,
          catalogCharacteristicService, subscriberGroupEpochReader, segmentationDimensionService, dnboMatrixAlgorithmParameters, offerService,
          supplierService, returnedLog, msisdn, null, tenantID);
      if (offerAvailabilityFromPropensityAlgo == null)
        {
          log.warn("DNBOProxy.getOffers Exception "+returnedLog);
          throw new DNBOProxyException("Exception "+returnedLog, scoringStrategyID);          
        }

      /*****************************************
       *
       *  response
       *
       *****************************************/

      //
      //  offers
      //

      List<JSONObject> scoredOffersJSON = new ArrayList<JSONObject>();
      for (ProposedOfferDetails proposedOffer : offerAvailabilityFromPropensityAlgo)
      {
        scoredOffersJSON.add(proposedOffer.getJSONRepresentation(offerService));
      }

      //
      //  value
      //

      HashMap<String,Object> value = new HashMap<String,Object>();
      value.put("subscriberID", subscriberID);
      value.put("msisdn", subscriberID); // subscriberID expected by caller here (for compatibility reasons)
      value.put("scoringStrategyID", scoringStrategyID);
      value.put("strategy", scoringStrategyID);
      value.put("offers", scoredOffersJSON);
      value.put("log", returnedLog.toString());
      valueRes = JSONUtilities.encodeObject(value);
    }
    catch (Exception e)
    {
      log.warn("DNBOProxy.getOffers Exception " + e.getClass().getName() + " " + e.getMessage() + " while retrieving offers " + msisdn + " " + scoringStrategyID, e);
      throw new DNBOProxyException("Exception while retrieving offers ", scoringStrategyID);
    }
    return valueRes;
  }

 
  /*****************************************
  *
  *  processProvisionSubscriber
  *
  *****************************************/

  private JSONObject processProvisionSubscriber(String userID, JSONObject jsonRoot, int tenantID) throws DNBOProxyException, SubscriberProfileServiceException
  {
    /*****************************************
    *
    *  arguments
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    String subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", false);
    JSONArray alternateIDsArray = JSONUtilities.decodeJSONArray(jsonRoot, "alternateIDs", true);
    
    /*****************************************
    *
    *  evaluate/validate alternate ids
    *
    *****************************************/

    //
    //  alternateIDsArray
    //
    
    Map<String,String> assignAlternateIDs = new HashMap<String,String>();
    for (int i = 0; i < alternateIDsArray.size(); i++)
      {
        //
        //  get alternateID
        //
        
        JSONArray alternateIDArray = (JSONArray) alternateIDsArray.get(i);
        String alternateIDName = (String) alternateIDArray.get(0);
        String alternateID = (String) alternateIDArray.get(1);

        //
        //  ensure alternateID
        //

        if (Deployment.getAlternateIDs().get(alternateIDName) == null)
          {
            throw new DNBOProxyException("unknown alternate id", alternateIDName);
          }

        //
        //  update assignAlternateIDs
        //
        
        assignAlternateIDs.put(alternateIDName, alternateID);
      }

    /*****************************************
    *
    *  resolve effectiveSubscriberID
    *
    *****************************************/

    String effectiveSubscriberID;
    boolean autoProvision;
    if (subscriberID != null)
      {
        effectiveSubscriberID = subscriberID;
        autoProvision = false;
      }
    else
      {
        effectiveSubscriberID = assignAlternateIDs.get(Deployment.getDeployment(tenantID).getExternalSubscriberID());
        autoProvision = true;
      }

    //
    //  verify
    //
    
    if (effectiveSubscriberID == null)
      {
        throw new DNBOProxyException("subscriberID not provided", Deployment.getDeployment(tenantID).getExternalSubscriberID());
      }

    //
    //  validate
    //

    try
      {
        Long.parseLong(effectiveSubscriberID);
      }
    catch (NumberFormatException e)
      {
        throw new DNBOProxyException("subscriberID invalid", effectiveSubscriberID);
      }

    /*****************************************
    *
    *  submit to SubscriberManager
    *
    *****************************************/

    String result = null;
    LinkedBlockingQueue<String> responseQueue = new LinkedBlockingQueue<String>();
    AssignSubscriberIDs assignSubscriberIDs = new AssignSubscriberIDs(effectiveSubscriberID, SystemTime.getCurrentTime(), assignAlternateIDs, tenantID);
    String topic = autoProvision ? Deployment.getAssignExternalSubscriberIDsTopic() : Deployment.getAssignSubscriberIDsTopic();
    try
      {
        //
        //  submit to Kafka
        //

        synchronized (outstandingRequests){
          outstandingRequests.put(assignSubscriberIDs, responseQueue);
        }
        kafkaProducer.send(new ProducerRecord<byte[], byte[]>(topic, StringKey.serde().serializer().serialize(topic, new StringKey(effectiveSubscriberID)), AssignSubscriberIDs.serde().serializer().serialize(topic, assignSubscriberIDs)));

        //
        //  retrieve provisioned subscriberID once processing is complete - timing out if necessary
        //

        try
          {
            result = responseQueue.poll(10L, TimeUnit.SECONDS);
          }
        catch (InterruptedException e)
          {
            // ignore
          }
      }
    finally
      {
        synchronized (outstandingRequests){
          outstandingRequests.remove(assignSubscriberIDs);
        }
      }
    
    /*****************************************
    *
    *  if timed out, check subscriber in redis cache
    *
    *****************************************/

    String timeoutSubscriberID = null;
    if (result == null)
      {
        try
          {
            timeoutSubscriberID = subscriberIDService.getSubscriberID(Deployment.getDeployment(tenantID).getExternalSubscriberID(), assignAlternateIDs.get(Deployment.getDeployment(tenantID).getExternalSubscriberID()));
          }
        catch (SubscriberIDServiceException e)
          {
            log.error("SubscriberIDServiceException can not resolve subscriberID for {}: {}", assignAlternateIDs.get(Deployment.getDeployment(tenantID).getExternalSubscriberID()), e.getMessage());
          }
      }
    
    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", (result != null) ? "ok" : ((timeoutSubscriberID != null) ? "alreadyProvisioned" : "timeout"));
    if (result != null) response.put("subscriberID", result);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  readSubscriberManagerTopic
  *
  *****************************************/

  private void readSubscriberManagerTopic(KafkaConsumer<byte[], byte[]> kafkaConsumer)
  {
    String externalSubscriberIDName = Deployment.getExternalSubscriberID();
    Map<String,LinkedBlockingQueue<String>> outstandingSubscriberIDs = new HashMap<String,LinkedBlockingQueue<String>>();
    Map<String,LinkedBlockingQueue<String>> outstandingExternalSubscriberIDs = new HashMap<String,LinkedBlockingQueue<String>>();
    while (! stopRequested)
      {
        //
        // poll
        //

        ConsumerRecords<byte[], byte[]> recordSubscriberIDRecords;
        try
          {
            recordSubscriberIDRecords = kafkaConsumer.poll(5000);
          }
        catch (WakeupException e)
          {
            recordSubscriberIDRecords = ConsumerRecords.<byte[], byte[]>empty();
          }

        //
        //  processing?
        //

        if (stopRequested) continue;

        //
        //  identify the outstanding records to search for
        //

        outstandingSubscriberIDs.clear();
        outstandingExternalSubscriberIDs.clear();
        synchronized (outstandingRequests)
        {
          for (AssignSubscriberIDs assignSubscriberIDs : outstandingRequests.keySet())
            {
              String passedSubscriberID = assignSubscriberIDs.getSubscriberID();
              String externalSubscriberID = assignSubscriberIDs.getAlternateIDs().get(externalSubscriberIDName);
              if (! Objects.equals(passedSubscriberID, externalSubscriberID))
                outstandingSubscriberIDs.put(passedSubscriberID, outstandingRequests.get(assignSubscriberIDs));
              else
                outstandingExternalSubscriberIDs.put(passedSubscriberID, outstandingRequests.get(assignSubscriberIDs));
            }
        }

        //
        //  process - only searching through the records if there are outstanding records to consider
        //

        if (outstandingRequests.size() > 0)
          {
            for (ConsumerRecord<byte[], byte[]> recordSubscriberIDRecord : recordSubscriberIDRecords)
              {
                //
                //  parse
                //

                RecordSubscriberID recordSubscriberID = null;
                try
                  {
                    recordSubscriberID = RecordSubscriberID.serde().deserializer().deserialize(Deployment.getRecordSubscriberIDTopic(), recordSubscriberIDRecord.value());
                  }
                catch (SerializationException e)
                  {
                    log.info("error reading recordSubscriberID: {}", e.getMessage());
                  }

                //
                //  check for match, returning if we find a match
                //
                
                if (recordSubscriberID != null)
                  {
                    //
                    //  outstandingSubscriberIDs
                    //
                    
                    LinkedBlockingQueue<String> outstandingSubscriberIDsResponseQueue = outstandingSubscriberIDs.get(recordSubscriberID.getSubscriberID());
                    if (outstandingSubscriberIDsResponseQueue != null) outstandingSubscriberIDsResponseQueue.offer(recordSubscriberID.getSubscriberID());

                    //
                    //  outstandingExternalSubscriberIDs
                    //

                    LinkedBlockingQueue<String> outstandingExternalSubscriberIDsResponseQueue = Objects.equals(recordSubscriberID.getIDField(), externalSubscriberIDName) ? outstandingExternalSubscriberIDs.get(recordSubscriberID.getAlternateID()) : null;
                    if (outstandingExternalSubscriberIDsResponseQueue != null) outstandingExternalSubscriberIDsResponseQueue.offer(recordSubscriberID.getSubscriberID());
                  }
              }
          }

        //
        //  commit offsets
        //

        kafkaConsumer.commitSync();
      }
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
  *  class DNBOProxyException
  *
  *****************************************/

  public static class DNBOProxyException extends Exception
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

    public DNBOProxyException(String responseMessage, String responseParameter)
    {
      super(responseMessage);
      this.responseParameter = responseParameter;
    }

    /*****************************************
    *
    *  constructor - exception
    *
    *****************************************/

    public DNBOProxyException(Throwable e)
    {
      super(e.getMessage(), e);
      this.responseParameter = null;
    }

    /*****************************************
    *
    *  toString
    *
    *****************************************/

    public String toString()
    {
      return super.toString() + "(" + responseParameter + ")";
    }
  }
  

//  private ScoringSplit getScoringSplit(String msisdn, ScoringGroup selectedScoringGroup) throws GetOfferException
//  {
//    // now let evaluate the split testing...
//    // let compute the split bashed on hash...
//    int nbSamples = selectedScoringGroup.getScoringSplits().size();
//    // let get the same sample for the subscriber for the whole strategy
//    String hashed = msisdn + nbSamples;
//    int hash = hashed.hashCode();
//    if (hash < 0)
//      {
//        hash = hash * -1; // can happen because a hashcode is a SIGNED
//        // integer
//      }
//    int sampleId = hash % nbSamples; // modulo
//
//    // now retrieve the good split strategy for this user:
//    ScoringSplit selectedScoringSplit = selectedScoringGroup.getScoringSplits().get(sampleId);
//
//    if (selectedScoringSplit == null)
//      {
//        // should never happen since modulo plays on the number of samples
//        log.warn("DNBOProxy.getScoringSplit Split Testing modulo problem for " + msisdn + " and subStrategy " + selectedScoringGroup);
//        throw new GetOfferException("Split Testing modulo problem for " + msisdn + " and subStrategy " + selectedScoringGroup);
//      }
//    return selectedScoringSplit;
//  }

//  private ScoringGroup getScoringGroup(ScoringStrategy strategy, SubscriberProfile subscriberProfile) throws GetOfferException
//  {
//    // let retrieve the first sub strategy that maps this user:
//    Date now = SystemTime.getCurrentTime();
//    SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, now);
//
//    ScoringGroup selectedScoringGroup = strategy.evaluateScoringGroups(evaluationRequest);
//    if (log.isDebugEnabled())
//      {
//        log.debug("DNBOProxy.getScoringGroup Retrieved matching scoringGroup " + (selectedScoringGroup != null ? display(selectedScoringGroup.getProfileCriteria()) : null));
//      }
//
//    if (selectedScoringGroup == null)
//      {
//        throw new GetOfferException("Can't retrieve ScoringGroup for strategy " + strategy.getScoringStrategyID() + " and msisdn " + subscriberProfile.getSubscriberID());
//      }
//    return selectedScoringGroup;
//  }

 

  public static String display(List<EvaluationCriterion> profileCriteria) {
    StringBuffer res = new StringBuffer();
    for (EvaluationCriterion ec : profileCriteria) {
      res.append("["+ec.getCriterionContext()+","+ec.getCriterionField()+","+ec.getCriterionOperator()+","+ec.getArgumentExpression()+","+ec.getStoryReference()+"]");
    }
    return res.toString();
  }
}