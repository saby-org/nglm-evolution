/****************************************************************************
*
*  EvolutionEngine.java 
*
****************************************************************************/

package com.evolving.nglm.evolution;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.evolving.nglm.evolution.ExternalAggregates.SubscriberStatus;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.KStreamsUniqueKeyServer;
import com.evolving.nglm.core.NGLMKafkaClientSupplier;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.RecordAlternateID;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.core.SubscriberTrace;
import com.evolving.nglm.core.SubscriberTraceControl;

import com.rii.utilities.InternCache;
import com.rii.utilities.JSONUtilities;
import com.rii.utilities.SystemTime;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class EvolutionEngine
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(EvolutionEngine.class);

  //
  //  hardwired arguments
  //

  public static String baseTimeZone = Deployment.getBaseTimeZone();

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private static ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
  private static JourneyService journeyService;
  private static EvolutionEngineStatistics evolutionEngineStatistics;
  private static KStreamsUniqueKeyServer uniqueKeyServer = new KStreamsUniqueKeyServer();

  /****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args) throws Exception
  {
    /*****************************************
    *
    *  runtime
    *
    *****************************************/

    NGLMRuntime.initialize();
    
    /*****************************************
    *
    *  configuration
    *
    *****************************************/

    //
    //  kafka configuration
    //

    String applicationID = "streams-evolutionengine";
    String stateDirectory = args[0];
    String bootstrapServers = args[1];
    String evolutionEngineKey = args[2];
    Integer kafkaReplicationFactor = Integer.parseInt(args[3]);
    Integer kafkaStreamsStandbyReplicas = Integer.parseInt(args[4]);
    Integer numberOfStreamThreads = Integer.parseInt(args[5]);

    //
    //  source topics 
    //

    String recordAlternateIDTopic = Deployment.getRecordAlternateIDTopic();
    String externalAggregatesTopic = Deployment.getExternalAggregatesTopic();
    String subscriberGroupTopic = Deployment.getSubscriberGroupTopic();
    String subscriberTraceControlTopic = Deployment.getSubscriberTraceControlTopic();

    //
    //  sink topics
    //

    String subscriberUpdateTopic = Deployment.getSubscriberUpdateTopic();
    String journeyStatisticTopic = Deployment.getJourneyStatisticTopic();
    String subscriberTraceTopic = Deployment.getSubscriberTraceTopic();

    //
    //  changelogs
    //

    String subscriberStateChangeLog = Deployment.getSubscriberStateChangeLog();
    String subscriberStateChangeLogTopic = Deployment.getSubscriberStateChangeLogTopic();

    //
    //  log
    //

    log.info("main START: {} {} {} {} {}", stateDirectory, bootstrapServers, kafkaStreamsStandbyReplicas, numberOfStreamThreads, kafkaReplicationFactor);

    //
    //  journeyService
    //

    journeyService = new JourneyService(bootstrapServers, "evolutionengine-journeyservice-" + evolutionEngineKey, Deployment.getJourneyTopic(), false);
    journeyService.start();

    //
    //  subscriberGroupEpochReader
    //

    subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("evolutionEngine-subscriberGroupEpoch", evolutionEngineKey, Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);
    
    //
    //  create monitoring object
    //

    evolutionEngineStatistics = new EvolutionEngineStatistics(applicationID);

    /*****************************************
    *
    *  stream properties
    *
    *****************************************/
    
    Properties streamsProperties = new Properties();
    streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationID);
    streamsProperties.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory);
    streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    streamsProperties.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, EvolutionEventTimestampExtractor.class.getName());
    streamsProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Integer.toString(numberOfStreamThreads));
    streamsProperties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, Integer.toString(kafkaReplicationFactor));
    streamsProperties.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, Integer.toString(kafkaStreamsStandbyReplicas));
    streamsProperties.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.toString());
    streamsProperties.put("producer.batch.size", Integer.toString(100000));
    StreamsConfig streamsConfig = new StreamsConfig(streamsProperties);

    /*****************************************
    *
    *  stream builder
    *
    *****************************************/

    KStreamBuilder builder = new KStreamBuilder();
    
    /*****************************************
    *
    *  fulfillment managers topics/serdes
    *
    *****************************************/

    Map<DeliveryManagerDeclaration,String> fulfillmentManagerRequestTopics = new HashMap<DeliveryManagerDeclaration,String>();
    Map<DeliveryManagerDeclaration,String> fulfillmentManagerResponseTopics = new HashMap<DeliveryManagerDeclaration,String>();
    Map<DeliveryManagerDeclaration,ConnectSerde<? extends DeliveryRequest>> fulfillmentManagerResponseSerdes = new HashMap<DeliveryManagerDeclaration,ConnectSerde<? extends DeliveryRequest>>();
    for (DeliveryManagerDeclaration fulfillmentManager : Deployment.getFulfillmentManagers().values())
      {
        fulfillmentManagerRequestTopics.put(fulfillmentManager, fulfillmentManager.getRequestTopic());
        fulfillmentManagerResponseTopics.put(fulfillmentManager, fulfillmentManager.getResponseTopic());
        fulfillmentManagerResponseSerdes.put(fulfillmentManager, fulfillmentManager.getRequestSerde());
      }

    /*****************************************
    *
    *  serdes
    *
    *****************************************/

    final ConnectSerde<StringKey> stringKeySerde = StringKey.serde();
    final ConnectSerde<RecordAlternateID> recordAlternateIDSerde = RecordAlternateID.serde();
    final ConnectSerde<ExternalAggregates> externalAggregatesSerde = ExternalAggregates.serde();
    final ConnectSerde<SubscriberGroup> subscriberGroupSerde = SubscriberGroup.serde();
    final ConnectSerde<SubscriberTraceControl> subscriberTraceControlSerde = SubscriberTraceControl.serde();
    final ConnectSerde<SubscriberState> subscriberStateSerde = SubscriberState.serde();
    final ConnectSerde<SubscriberProfile> subscriberProfileSerde = SubscriberProfile.serde();
    final Serde<JourneyStatistic> journeyStatisticSerde = JourneyStatistic.serde();
    final Serde<SubscriberTrace> subscriberTraceSerde = SubscriberTrace.serde();

    /*****************************************
    *
    *  deployment objects
    *  - serdes
    *  - source nodes (topics)
    *  - trigger event streams
    *
    *****************************************/

    ArrayList<ConnectSerde<? extends SubscriberStreamEvent>> evolutionEventSerdes = new ArrayList<ConnectSerde<? extends SubscriberStreamEvent>>();
    evolutionEventSerdes.add(recordAlternateIDSerde);
    evolutionEventSerdes.add(externalAggregatesSerde);
    evolutionEventSerdes.add(subscriberGroupSerde);
    evolutionEventSerdes.add(subscriberTraceControlSerde);
    evolutionEventSerdes.addAll(fulfillmentManagerResponseSerdes.values());
    final ConnectSerde<SubscriberStreamEvent> evolutionEventSerde = new ConnectSerde<SubscriberStreamEvent>("evolution_event", false, evolutionEventSerdes.toArray(new ConnectSerde[0]));

    /****************************************
    *
    *  ensure copartitioned
    *
    ****************************************/
    
    List<String> sourceNodes = new ArrayList<String>();
    sourceNodes.add(recordAlternateIDTopic);
    sourceNodes.add(externalAggregatesTopic);
    sourceNodes.add(subscriberGroupTopic);
    sourceNodes.add(subscriberTraceControlTopic);
    sourceNodes.add(subscriberStateChangeLogTopic);
    sourceNodes.addAll(fulfillmentManagerResponseTopics.values());
    builder.copartitionSources(sourceNodes);
    
    /*****************************************
    *
    *  source streams
    *
    *****************************************/

    //
    //  core streams
    //

    KStream<StringKey, RecordAlternateID> recordAlternateIDSourceStream = builder.stream(stringKeySerde, recordAlternateIDSerde, recordAlternateIDTopic);
    KStream<StringKey, ExternalAggregates> externalAggregatesSourceStream = builder.stream(stringKeySerde, externalAggregatesSerde, externalAggregatesTopic);
    KStream<StringKey, SubscriberGroup> subscriberGroupSourceStream = builder.stream(stringKeySerde, subscriberGroupSerde, subscriberGroupTopic);
    KStream<StringKey, SubscriberTraceControl> subscriberTraceControlSourceStream = builder.stream(stringKeySerde, subscriberTraceControlSerde, subscriberTraceControlTopic);
    
    //
    //  filter (if necessary)
    //

    KStream<StringKey, RecordAlternateID> filteredRecordAlternateIDSourceStream = recordAlternateIDSourceStream.filter((key,value) -> (! value.getKeyByAlternateID()));

    //
    //  fulfillment manager response source streams
    //

    Set<KStream<StringKey, ? extends SubscriberStreamEvent>> fulfillmentManagerResponseStreams = new HashSet<KStream<StringKey, ? extends SubscriberStreamEvent>>();
    for (DeliveryManagerDeclaration fulfillmentManagerDeclaration : Deployment.getFulfillmentManagers().values())
      {
        fulfillmentManagerResponseStreams.add(builder.stream(stringKeySerde, fulfillmentManagerResponseSerdes.get(fulfillmentManagerDeclaration), fulfillmentManagerResponseTopics.get(fulfillmentManagerDeclaration)));
      }

    //
    //  merge source streams
    //

    ArrayList<KStream<StringKey, ? extends SubscriberStreamEvent>> evolutionEventStreams = new ArrayList<KStream<StringKey, ? extends SubscriberStreamEvent>>();
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) filteredRecordAlternateIDSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) externalAggregatesSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) subscriberGroupSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) subscriberTraceControlSourceStream);
    evolutionEventStreams.addAll(fulfillmentManagerResponseStreams);
    KStream<StringKey, SubscriberStreamEvent> evolutionEventStream = builder.merge(evolutionEventStreams.toArray(new KStream[0]));
    
    /*****************************************
    *
    *  subscriberState -- update
    *
    *****************************************/

    StateStoreSupplier subscriberStateStore = Stores.create(subscriberStateChangeLog).withKeys(stringKeySerde).withValues(subscriberStateSerde.optionalSerde()).persistent().build();
    KTable<StringKey, SubscriberState> subscriberState = evolutionEventStream.groupByKey(stringKeySerde, evolutionEventSerde).aggregate(EvolutionEngine::nullSubscriberState, EvolutionEngine::updateSubscriberState, subscriberStateStore);

    /*****************************************
    *
    *  convert to stream
    *
    *****************************************/
    
    KStream<StringKey, SubscriberState> subscriberStateStream = evolutionEventStream.leftJoin(subscriberState, EvolutionEngine::getSubscriberState);

    /*****************************************
    *
    *  get outputs
    *
    *****************************************/

    KStream<StringKey, SubscriberStreamOutput> evolutionEngineOutputs = subscriberStateStream.flatMapValues(EvolutionEngine::getEvolutionEngineOutputs);
    
    /*****************************************
    *
    *  branch output streams
    *
    *****************************************/

    KStream<StringKey, ? extends SubscriberStreamOutput>[] branchedEvolutionEngineOutputs = evolutionEngineOutputs.branch((key,value) -> (value instanceof SubscriberProfile), (key,value) -> (value instanceof FulfillmentRequest), (key,value) -> (value instanceof JourneyStatistic), (key,value) -> (value instanceof SubscriberTrace));
    KStream<StringKey, SubscriberProfile> subscriberUpdateStream = (KStream<StringKey, SubscriberProfile>) branchedEvolutionEngineOutputs[0];
    KStream<StringKey, FulfillmentRequest> fulfillmentRequestStream = (KStream<StringKey, FulfillmentRequest>) branchedEvolutionEngineOutputs[1];
    KStream<StringKey, JourneyStatistic> journeyStatisticStream = (KStream<StringKey, JourneyStatistic>) branchedEvolutionEngineOutputs[2];
    KStream<StringKey, SubscriberTrace> subscriberTraceStream = (KStream<StringKey, SubscriberTrace>) branchedEvolutionEngineOutputs[3];

    /*****************************************
    *
    *  branch fulfillment requests
    *
    *****************************************/
    
    //
    //  build predicates for fulfillment requests
    //

    String[] fulfillmentManagerFulfillmentTypes = new String[Deployment.getFulfillmentManagers().size()];
    FulfillmentManagerPredicate[] fulfillmentManagerPredicates = new FulfillmentManagerPredicate[Deployment.getFulfillmentManagers().size()];
    int i = 0;
    for (DeliveryManagerDeclaration fulfillmentManager : Deployment.getFulfillmentManagers().values())
      {
        fulfillmentManagerFulfillmentTypes[i] = fulfillmentManager.getRequestType();
        fulfillmentManagerPredicates[i] = new FulfillmentManagerPredicate(fulfillmentManager.getRequestType());
        i += 1;
      }

    //
    //  branch
    //

    KStream<StringKey, FulfillmentRequest>[] branchedFulfillmentRequestStreams = (Deployment.getFulfillmentManagers().size() > 0) ? fulfillmentRequestStream.branch(fulfillmentManagerPredicates) : new KStream[0];

    //
    //  fulfillment request streams
    //

    Map<String, KStream<StringKey, FulfillmentRequest>> fulfillmentRequestStreams = new HashMap<String, KStream<StringKey, FulfillmentRequest>>();
    for (int j=0; j<branchedFulfillmentRequestStreams.length; j++)
      {
        fulfillmentRequestStreams.put(fulfillmentManagerFulfillmentTypes[j], branchedFulfillmentRequestStreams[j]);
      }

    /*****************************************
    *
    *  sink
    *
    *****************************************/

    //
    //  sink - core streams
    //

    subscriberUpdateStream.to(stringKeySerde, subscriberProfileSerde, subscriberUpdateTopic);
    journeyStatisticStream.to(stringKeySerde, journeyStatisticSerde, journeyStatisticTopic);
    subscriberTraceStream.to(stringKeySerde, subscriberTraceSerde, subscriberTraceTopic);
    
    //
    //  sink - fulfillment request streams
    //

    for (String fulfillmentType : fulfillmentRequestStreams.keySet())
      {
        DeliveryManagerDeclaration fulfillmentManager = Deployment.getFulfillmentManagers().get(fulfillmentType);
        String requestTopic = fulfillmentManager.getRequestTopic();
        ConnectSerde<FulfillmentRequest> requestSerde = (ConnectSerde<FulfillmentRequest>) fulfillmentManager.getRequestSerde();
        KStream<StringKey, FulfillmentRequest> requestStream = fulfillmentRequestStreams.get(fulfillmentType);
        requestStream.to(stringKeySerde, requestSerde, requestTopic);
      }

    /*****************************************
    *
    *  runtime
    *
    *****************************************/

    KafkaStreams streams = new KafkaStreams(builder, streamsConfig, new NGLMKafkaClientSupplier());

    /*****************************************
    *
    *  shutdown hook
    *
    *****************************************/
    
    NGLMRuntime.addShutdownHook(new ShutdownHook(streams));

    /*****************************************
    *
    *  start
    *
    *****************************************/

    streams.start();
  }

  /****************************************
  *
  *  class FulfillmentManagerPredicate
  *
  ****************************************/

  private static class FulfillmentManagerPredicate implements Predicate<StringKey, FulfillmentRequest>
  {
    //
    //  data
    //
    
    private String fulfillmentType;

    //
    //  constructor
    //

    private FulfillmentManagerPredicate(String fulfillmentType)
    {
      this.fulfillmentType = fulfillmentType;
    }

    //
    //  test (Predicate interface)
    //

    @Override public boolean test(StringKey stringKey, FulfillmentRequest fulfillmentRequest)
    {
      return fulfillmentType.equals(fulfillmentRequest.getFulfillmentType());
    }
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

    private KafkaStreams kafkaStreams;

    //
    //  constructor
    //

    private ShutdownHook(KafkaStreams kafkaStreams)
    {
      this.kafkaStreams = kafkaStreams;
    }

    //
    //  shutdown
    //

    @Override public void shutdown(boolean normalShutdown)
    {
      //
      //  stop stats collection
      //

      if (evolutionEngineStatistics != null) evolutionEngineStatistics.unregister();

      //
      //  stop streams
      //
      
      boolean streamsCloseSuccessful = kafkaStreams.close(60, TimeUnit.SECONDS);
      log.info("Stopped EvolutionEngine" + (streamsCloseSuccessful ? "" : " (timed out)"));
    }
  }

  /*****************************************
  *
  *  castToSubscriberStreamEvent
  *
  *****************************************/

  public static SubscriberStreamEvent castToSubscriberStreamEvent(SubscriberStreamEvent subscriberStreamEvent) { return subscriberStreamEvent; }
  
  /*****************************************
  *
  *  nullSubscriberState
  *
  ****************************************/

  public static SubscriberState nullSubscriberState() { return (SubscriberState) null; }

  /*****************************************
  *
  *  updateSubscriberState
  *
  *****************************************/

  public static SubscriberState updateSubscriberState(StringKey aggKey, SubscriberStreamEvent evolutionEvent, SubscriberState currentSubscriberState)
  {
    /****************************************
    *
    *  get (or create) entry
    *
    ****************************************/

    SubscriberState subscriberState = (currentSubscriberState != null) ? new SubscriberState(currentSubscriberState) : new SubscriberState(evolutionEvent.getSubscriberID());
    SubscriberProfile subscriberProfile = subscriberState.getSubscriberProfile();
    EvolutionEventContext context = new EvolutionEventContext(subscriberState, subscriberGroupEpochReader, uniqueKeyServer);
    boolean subscriberStateUpdated = (currentSubscriberState != null) ? false : true;

    /*****************************************
    *
    *  clear state
    *
    *****************************************/

    //
    //  statusUpdated
    //

    if (subscriberState.getSubscriberStatusUpdated())
      {
        subscriberState.setSubscriberStatusUpdated(false);
        subscriberStateUpdated = true;
      }

    //
    //  fulfillmentRequests
    //

    if (subscriberState.getFulfillmentRequests().size() > 0)
      {
        subscriberState.getFulfillmentRequests().clear();
        subscriberStateUpdated = true;
      }

    //
    //  journeyStatistics
    //

    if (subscriberState.getJourneyStatistics().size() > 0)
      {
        subscriberState.getJourneyStatistics().clear();
        subscriberStateUpdated = true;
      }

    //
    //  subscriberTrace
    //

    if (subscriberState.getSubscriberTrace() != null)
      {
        subscriberState.setSubscriberTrace(null);
        subscriberStateUpdated = true;
      }

    /*****************************************
    *
    *  update SubscriberProfile
    *
    *****************************************/

    subscriberStateUpdated = updateSubscriberProfile(context, evolutionEvent) || subscriberStateUpdated;

    /*****************************************
    *
    *  update journeyStates
    *
    *****************************************/
    
    subscriberStateUpdated = updateJourneys(context, evolutionEvent) || subscriberStateUpdated;

    /*****************************************
    *
    *  subscriberStatusUpdated
    *
    *****************************************/

    if (currentSubscriberState == null || subscriberProfile.getSubscriberStatus() != currentSubscriberState.getSubscriberProfile().getSubscriberStatus())
      {
        subscriberState.setSubscriberStatusUpdated(true);
        subscriberStateUpdated = true;
      }

    /*****************************************
    *
    *  subscriberTrace
    *
    *****************************************/

    if (subscriberProfile.getSubscriberTraceEnabled())
      {
        subscriberState.setSubscriberTrace(new SubscriberTrace(generateSubscriberTraceMessage(evolutionEvent, currentSubscriberState, subscriberState, context.getSubscriberTraceDetails())));
        subscriberStateUpdated = true;
      }

    /****************************************
    *
    *  return
    *
    ****************************************/

    return subscriberStateUpdated ? subscriberState : currentSubscriberState;
  }

  /*****************************************
  *
  *  updateSubscriberProfile
  *
  *****************************************/

  private static boolean updateSubscriberProfile(EvolutionEventContext context, SubscriberStreamEvent evolutionEvent)
  {
    /*****************************************
    *
    *  result
    *
    *****************************************/

    SubscriberProfile subscriberProfile = context.getSubscriberState().getSubscriberProfile();
    boolean subscriberProfileUpdated = false;

    /*****************************************
    *
    *  re-evaluate subscriberGroups for epoch changes
    *
    *****************************************/

    Iterator<String> groupNames = subscriberProfile.getSubscriberGroups().keySet().iterator();
    while (groupNames.hasNext())
      {
        String groupName = groupNames.next();
        int subscriberGroupEpoch = subscriberProfile.getSubscriberGroups().get(groupName);
        int groupEpoch = (subscriberGroupEpochReader.get(groupName) != null) ? subscriberGroupEpochReader.get(groupName).getEpoch() : 0;
        if (subscriberGroupEpoch < groupEpoch)
          {
            groupNames.remove();
            subscriberProfileUpdated = true;
          }
      }

    /*****************************************
    *
    *  process subscriberTraceControl
    *
    *****************************************/

    if (evolutionEvent instanceof SubscriberTraceControl)
      {
        SubscriberTraceControl subscriberTraceControl = (SubscriberTraceControl) evolutionEvent;
        subscriberProfile.setSubscriberTraceEnabled(subscriberTraceControl.getSubscriberTraceEnabled());
        subscriberProfileUpdated = true;
      }
    
    /*****************************************
    *
    *  process recordAlternateID
    *
    *****************************************/

    if (evolutionEvent instanceof RecordAlternateID)
      {
        //
        //  apply
        //
        
        RecordAlternateID recordAlternateID = (RecordAlternateID) evolutionEvent;
        if (recordAlternateID.getIDField().equals("msisdn")) subscriberProfile.setMSISDN(recordAlternateID.getAlternateID());
        if (recordAlternateID.getIDField().equals("contractID")) subscriberProfile.setContractID(recordAlternateID.getAlternateID());
        subscriberProfileUpdated = true;

        //
        //  statistics
        //

        updateEvolutionEngineStatistics(evolutionEvent);
      }

    /*****************************************
    *
    *  process externalAggregates
    *
    *****************************************/

    if (evolutionEvent instanceof ExternalAggregates)
      {
        //
        //  apply
        //
        
        ExternalAggregates externalAggregates = (ExternalAggregates) evolutionEvent;
        subscriberProfile.setAccountTypeID(externalAggregates.getAccountType());
        subscriberProfile.setRatePlan(externalAggregates.getPrimaryTariffPlan());
        subscriberProfile.setActivationDate(externalAggregates.getActivationDate());
        subscriberProfile.setSubscriberStatus(externalAggregates.getSubscriberStatus());
        subscriberProfile.setStatusChangeDate(RLMDateUtils.addDays(externalAggregates.getEventDate(), -1 * externalAggregates.getDaysInCurrentStatus(), baseTimeZone));
        subscriberProfile.setPreviousSubscriberStatus(externalAggregates.getPreviousSubscriberStatus());
        subscriberProfile.setLastRechargeDate(externalAggregates.getLastRechargeDate());
        subscriberProfile.setRatePlanChangeDate(externalAggregates.getTariffPlanChangeDate());
        subscriberProfile.setMainBalanceValue(externalAggregates.getMainBalanceValue());
        subscriberProfile.getTotalChargeHistory().update(externalAggregates.getEventDate(), externalAggregates.getTotalCharge());
        subscriberProfile.getDataChargeHistory().update(externalAggregates.getEventDate(), externalAggregates.getDataCharge());
        subscriberProfile.getCallsChargeHistory().update(externalAggregates.getEventDate(), externalAggregates.getCallsCharge());
        subscriberProfile.getRechargeChargeHistory().update(externalAggregates.getEventDate(), externalAggregates.getRechargeCharge());
        subscriberProfile.getRechargeCountHistory().update(externalAggregates.getEventDate(), externalAggregates.getRechargeCount());
        subscriberProfile.getMOCallsChargeHistory().update(externalAggregates.getEventDate(), externalAggregates.getMOCallsCharge());
        subscriberProfile.getMOCallsCountHistory().update(externalAggregates.getEventDate(), externalAggregates.getMOCallsCount());
        subscriberProfile.getMOCallsDurationHistory().update(externalAggregates.getEventDate(), externalAggregates.getMOCallsDuration());
        subscriberProfile.getMTCallsCountHistory().update(externalAggregates.getEventDate(), externalAggregates.getMTCallsCount());
        subscriberProfile.getMTCallsDurationHistory().update(externalAggregates.getEventDate(), externalAggregates.getMTCallsDuration());
        subscriberProfile.getMTCallsIntCountHistory().update(externalAggregates.getEventDate(), externalAggregates.getMTCallsIntCount());
        subscriberProfile.getMTCallsIntDurationHistory().update(externalAggregates.getEventDate(), externalAggregates.getMTCallsIntDuration());
        subscriberProfile.getMOCallsIntChargeHistory().update(externalAggregates.getEventDate(), externalAggregates.getMOCallsIntCharge());
        subscriberProfile.getMOCallsIntCountHistory().update(externalAggregates.getEventDate(), externalAggregates.getMOCallsIntCount());
        subscriberProfile.getMOCallsIntDurationHistory().update(externalAggregates.getEventDate(), externalAggregates.getMOCallsIntDuration());
        subscriberProfile.getMOSMSChargeHistory().update(externalAggregates.getEventDate(), externalAggregates.getMOSMSCharge());
        subscriberProfile.getMOSMSCountHistory().update(externalAggregates.getEventDate(), externalAggregates.getMOSMSCount());
        subscriberProfile.getDataVolumeHistory().update(externalAggregates.getEventDate(), externalAggregates.getDataVolume());
        subscriberProfile.getDataBundleChargeHistory().update(externalAggregates.getEventDate(), externalAggregates.getDataBundleCharge());
        subscriberProfile.setRegion(externalAggregates.getSubscriberRegion());
        subscriberProfileUpdated = true;

        //
        //  statistics
        //

        updateEvolutionEngineStatistics(evolutionEvent);
      }

    /*****************************************
    *
    *  process subscriberGroup
    *
    *****************************************/

    if (evolutionEvent instanceof SubscriberGroup)
      {
        //
        //  apply
        //
        
        SubscriberGroup subscriberGroup = (SubscriberGroup) evolutionEvent;
        subscriberProfile.setSubscriberGroup(subscriberGroup.getGroupName(), subscriberGroup.getEpoch(), subscriberGroup.getAddSubscriber());
        subscriberProfileUpdated = true;

        //
        //  statistics
        //

        updateEvolutionEngineStatistics(evolutionEvent);
      }

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return subscriberProfileUpdated;
  }

  /*****************************************
  *
  *  updateJourneys
  *
  *****************************************/

  private static boolean updateJourneys(EvolutionEventContext context, SubscriberStreamEvent evolutionEvent)
  {
    /*****************************************
    *
    *  result
    *
    *****************************************/

    SubscriberState subscriberState = context.getSubscriberState();
    boolean subscriberStateUpdated = false;

    /*****************************************
    *
    *  update JourneyState(s) to enter new autoTargetedJourneys
    *
    *****************************************/

    for (Journey journey : journeyService.getActiveJourneys(evolutionEvent.getEventDate()))
      {
        if (journey.getAutoTargeted())
          {
            /*****************************************
            *
            *  enterJourney
            *
            *****************************************/

            boolean enterJourney = true;

            /*****************************************
            *
            *  already in journey?
            *
            *****************************************/

            if (enterJourney)
              {
                for (JourneyState journeyState : subscriberState.getJourneyStates())
                  {
                    if (Objects.equals(journeyState.getJourneyID(), journey.getJourneyID()))
                      {
                        enterJourney = false;
                      }
                  }
              }

            /*****************************************
            *
            *  pass auto-targeting criteria
            *
            *****************************************/

            if (enterJourney)
              {
                SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, evolutionEvent.getEventDate());
                if (! EvaluationCriterion.evaluateCriteria(evaluationRequest, journey.getAutoTargetingCriteria()))
                  {
                    enterJourney = false;
                  }
                context.getSubscriberTraceDetails().addAll(evaluationRequest.getTraceDetails());
              }

            /*****************************************
            *
            *  enterJourney
            *
            *****************************************/

            if (enterJourney)
              {
                JourneyState journeyState = new JourneyState(context, journey.getJourneyID(), journey.getStartNodeID(), evolutionEvent.getEventDate());
                subscriberState.getJourneyStates().add(journeyState);
                subscriberState.getJourneyStatistics().add(new JourneyStatistic(journeyState.getJourneyInstanceID(), journey.getJourneyID(), subscriberState.getSubscriberID(), evolutionEvent.getEventDate(), null, null, journey.getStartNodeID(), false));
                subscriberStateUpdated = true;
              }
          }
      }
        
    /*****************************************
    *
    *  update JourneyState(s) for all current journeys
    *
    *****************************************/

    for (JourneyState journeyState : subscriberState.getJourneyStates())
      {
        //
        //  TBD
        //
      }

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return subscriberStateUpdated;
  }

  /*****************************************
  *
  *  updateEvolutionEngineStatistics
  *
  *****************************************/

  private static void updateEvolutionEngineStatistics(SubscriberStreamEvent event)
  {
    evolutionEngineStatistics.updateEventProcessedCount(1);
    evolutionEngineStatistics.updateEventCount(event, 1);
  }

  /*****************************************
  *
  *  getSubscriberState
  *
  *****************************************/

  private static SubscriberState getSubscriberState(SubscriberStreamEvent evolutionEvent, SubscriberState subscriberState)
  {
    return subscriberState;
  }

  /****************************************
  *
  *  getEvolutionEngineOutputs
  *
  ****************************************/

  private static List<SubscriberStreamOutput> getEvolutionEngineOutputs(SubscriberState subscriberState)
  {
    List<SubscriberStreamOutput> result = new ArrayList<SubscriberStreamOutput>();
    result.addAll(subscriberState.getSubscriberStatusUpdated() ? Collections.<SubscriberProfile>singletonList(subscriberState.getSubscriberProfile()) : Collections.<SubscriberProfile>emptyList());
    result.addAll(subscriberState.getFulfillmentRequests());
    result.addAll(subscriberState.getJourneyStatistics());
    result.addAll((subscriberState.getSubscriberTrace() != null) ? Collections.<SubscriberTrace>singletonList(subscriberState.getSubscriberTrace()) : Collections.<SubscriberTrace>emptyList());
    return result;
  }
  
  /*****************************************
  *
  *  generateSubscriberTraceMessage
  *
  *****************************************/

  private static String generateSubscriberTraceMessage(SubscriberStreamEvent evolutionEvent, SubscriberState currentSubscriberState, SubscriberState subscriberState, List<String> subscriberTraceDetails)
  {
    /*****************************************
    *
    *  convert to JSON
    *
    *****************************************/

    //
    //  initialize converter
    //

    JsonConverter converter = new JsonConverter();
    Map<String, Object> converterConfigs = new HashMap<String, Object>();
    converterConfigs.put("schemas.enable","false");
    converter.configure(converterConfigs, false);
    JsonDeserializer deserializer = new JsonDeserializer();
    deserializer.configure(Collections.<String, Object>emptyMap(), false);

    //
    //  JsonNodeFactory
    //

    JsonNodeFactory jsonNodeFactory = new JsonNodeFactory(false);

    //
    //  field -- evolutionEvent
    //

    JsonNode evolutionEventNode = deserializer.deserialize(null, converter.fromConnectData(null, evolutionEvent.subscriberStreamEventSchema(),  evolutionEvent.subscriberStreamEventPack(evolutionEvent)));

    //
    //  field -- currentSubscriberState
    //

    JsonNode currentSubscriberStateNode = (currentSubscriberState != null) ? deserializer.deserialize(null, converter.fromConnectData(null, SubscriberState.schema(),  SubscriberState.pack(currentSubscriberState))) : null;

    //
    //  field -- subscriberState
    //

    JsonNode subscriberStateNode = deserializer.deserialize(null, converter.fromConnectData(null, SubscriberState.schema(),  SubscriberState.pack(subscriberState)));

    /*****************************************
    *
    *  hack/massage triggerStateNodes to remove unwanted/misleading/spurious fields from currentTriggerState
    *
    *****************************************/

    //
    //  outgoing messages (currentSubscriberStateNode)
    //

    if (currentSubscriberStateNode != null) ((ObjectNode) currentSubscriberStateNode).remove("subscriberTraceMessage");

    //
    //  other (subscriberStateNode)
    //

    ((ObjectNode) subscriberStateNode).remove("subscriberTraceMessage");

    /*****************************************
    *
    *  subscriberTraceDetails
    *
    *****************************************/

    ArrayNode subscriberTraceDetailsNode = jsonNodeFactory.arrayNode();
    for (String detail : subscriberTraceDetails)
      {
        subscriberTraceDetailsNode.add(detail);
      }

    /*****************************************
    *
    *  subscriberTraceMessage
    *
    *****************************************/

    //
    //  create parent JsonNode (to package fields)
    //

    ObjectNode subscriberTraceMessageNode = jsonNodeFactory.objectNode();
    subscriberTraceMessageNode.put("source", "EvolutionEngine");
    subscriberTraceMessageNode.put("subscriberID", evolutionEvent.getSubscriberID());
    subscriberTraceMessageNode.put("processingTime", SystemTime.getCurrentTime().getTime());
    subscriberTraceMessageNode.set(evolutionEvent.getClass().getSimpleName(), evolutionEventNode);
    subscriberTraceMessageNode.set("currentSubscriberState", currentSubscriberStateNode);
    subscriberTraceMessageNode.set("subscriberState", subscriberStateNode);
    subscriberTraceMessageNode.set("details", subscriberTraceDetailsNode);

    //
    //  convert to string
    //

    return subscriberTraceMessageNode.toString();
  }

  /*****************************************
  *
  *  class EvolutionEventContext
  *
  *****************************************/

  public static class EvolutionEventContext
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private SubscriberState subscriberState;
    private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
    private KStreamsUniqueKeyServer uniqueKeyServer;
    private List<String> subscriberTraceDetails;
    
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public EvolutionEventContext(SubscriberState subscriberState, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, KStreamsUniqueKeyServer uniqueKeyServer)
    {
      this.subscriberState = subscriberState;
      this.subscriberGroupEpochReader = subscriberGroupEpochReader;
      this.uniqueKeyServer = uniqueKeyServer;
      this.subscriberTraceDetails = new ArrayList<String>();
    }

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public SubscriberState getSubscriberState() { return subscriberState; }
    public ReferenceDataReader<String,SubscriberGroupEpoch> getSubscriberGroupEpochReader() { return subscriberGroupEpochReader; }
    public KStreamsUniqueKeyServer getUniqueKeyServer() { return uniqueKeyServer; }
    public List<String> getSubscriberTraceDetails() { return subscriberTraceDetails; }
    public boolean getSubscriberTraceEnabled() { return subscriberState.getSubscriberProfile().getSubscriberTraceEnabled(); }

    /*****************************************
    *
    *  getUniqueKey
    *
    *****************************************/

    public String getUniqueKey()
    {
      return String.format("%020d", uniqueKeyServer.getKey());
    }

    /*****************************************
    *
    *  subscriberTrace
    *
    *****************************************/

    public void subscriberTrace(String messageFormatString, Object... args)
    {
      if (getSubscriberTraceEnabled())
        {
          subscriberTraceDetails.add(MessageFormat.format(messageFormatString, args));
        }
    }
  }

  /*****************************************
  *
  *  class EvolutionEventTimestampExtractor
  *
  *****************************************/

  public static class EvolutionEventTimestampExtractor implements TimestampExtractor
  {
    /*****************************************
    *
    *  extract
    *
    *****************************************/

    @Override public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp)
    {
      long result = record.timestamp();

      //
      //  SubscriberStreamEvent
      //

      result = (record.value() instanceof SubscriberStreamEvent) ? ((SubscriberStreamEvent) record.value()).getEventDate().getTime() : result;

      //
      //  return (protect against time before the epoch)
      //

      return (result > 0L) ? result : record.timestamp();
    }
  }
}
