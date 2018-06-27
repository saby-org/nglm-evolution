/****************************************************************************
*
*  ProfileEngine.java 
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
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ProfileEngine
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(ProfileEngine.class);

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
  private static ProfileEngineStatistics profileEngineStatistics;
  
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

    String applicationID = "streams-profileengine";
    String stateDirectory = args[0];
    String bootstrapServers = args[1];
    String profileEngineKey = args[2];
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
    String subscriberTraceTopic = Deployment.getSubscriberTraceTopic();

    //
    //  changelogs
    //

    String subscriberProfileChangeLog = Deployment.getSubscriberProfileChangeLog();
    String subscriberProfileChangeLogTopic = Deployment.getSubscriberProfileChangeLogTopic();

    //
    //  log
    //

    log.info("main START: {} {} {} {} {}", stateDirectory, bootstrapServers, kafkaStreamsStandbyReplicas, numberOfStreamThreads, kafkaReplicationFactor);

    //
    //  subscriberGroupEpochReader
    //

    subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("profileEngine-subscriberGroupEpoch", profileEngineKey, Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);
    
    //
    //  create monitoring object
    //

    profileEngineStatistics = new ProfileEngineStatistics(applicationID);

    /*****************************************
    *
    *  stream properties
    *
    *****************************************/
    
    Properties streamsProperties = new Properties();
    streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationID);
    streamsProperties.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory);
    streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    streamsProperties.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, ProfileEventTimestampExtractor.class.getName());
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
    *  serdes
    *
    *****************************************/

    final ConnectSerde<StringKey> stringKeySerde = StringKey.serde();
    final ConnectSerde<RecordAlternateID> recordAlternateIDSerde = RecordAlternateID.serde();
    final ConnectSerde<ExternalAggregates> externalAggregatesSerde = ExternalAggregates.serde();
    final ConnectSerde<SubscriberGroup> subscriberGroupSerde = SubscriberGroup.serde();
    final ConnectSerde<SubscriberTraceControl> subscriberTraceControlSerde = SubscriberTraceControl.serde();
    final ConnectSerde<SubscriberProfile> subscriberProfileSerde = SubscriberProfile.serde();
    final Serde<SubscriberTrace> subscriberTraceSerde = SubscriberTrace.serde();

    /*****************************************
    *
    *  deployment objects
    *  - serdes
    *  - source nodes (topics)
    *  - trigger event streams
    *
    *****************************************/

    final ConnectSerde<SubscriberStreamEvent> profileEventSerde = new ConnectSerde<SubscriberStreamEvent>("profileevent", false, recordAlternateIDSerde, externalAggregatesSerde, subscriberGroupSerde, subscriberTraceControlSerde);

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
    sourceNodes.add(subscriberProfileChangeLogTopic);
    builder.copartitionSources(sourceNodes);
    
    /*****************************************
    *
    *  source streams
    *
    *****************************************/

    KStream<StringKey, RecordAlternateID> recordAlternateIDSourceStream = builder.stream(stringKeySerde, recordAlternateIDSerde, recordAlternateIDTopic);
    KStream<StringKey, ExternalAggregates> externalAggregatesSourceStream = builder.stream(stringKeySerde, externalAggregatesSerde, externalAggregatesTopic);
    KStream<StringKey, SubscriberGroup> subscriberGroupSourceStream = builder.stream(stringKeySerde, subscriberGroupSerde, subscriberGroupTopic);
    KStream<StringKey, SubscriberTraceControl> subscriberTraceControlSourceStream = builder.stream(stringKeySerde, subscriberTraceControlSerde, subscriberTraceControlTopic);
    
    //
    //  filter (if necessary)
    //

    KStream<StringKey, RecordAlternateID> filteredRecordAlternateIDSourceStream = recordAlternateIDSourceStream.filter((key,value) -> (! value.getKeyByAlternateID()));

    //
    //  merge source streams
    //

    KStream<StringKey, SubscriberStreamEvent> profileEventStream = builder.merge(filteredRecordAlternateIDSourceStream.mapValues(ProfileEngine::castToSubscriberStreamEvent), externalAggregatesSourceStream.mapValues(ProfileEngine::castToSubscriberStreamEvent), subscriberGroupSourceStream.mapValues(ProfileEngine::castToSubscriberStreamEvent), subscriberTraceControlSourceStream.mapValues(ProfileEngine::castToSubscriberStreamEvent));
    
    /*****************************************
    *
    *  subscriberProfile -- update
    *
    *****************************************/

    StateStoreSupplier subscriberProfileStore = Stores.create(subscriberProfileChangeLog).withKeys(stringKeySerde).withValues(subscriberProfileSerde.optionalSerde()).persistent().build();
    KTable<StringKey, SubscriberProfile> subscriberProfile = profileEventStream.groupByKey(stringKeySerde, profileEventSerde).aggregate(ProfileEngine::nullSubscriberProfile, ProfileEngine::updateSubscriberProfile, subscriberProfileStore);

    /*****************************************
    *
    *  convert to stream
    *
    *****************************************/
    
    KStream<StringKey, SubscriberProfile> subscriberProfileStream = profileEventStream.leftJoin(subscriberProfile, ProfileEngine::getSubscriberProfile);

    /*****************************************
    *
    *  get outputs
    *
    *****************************************/

    KStream<StringKey, SubscriberStreamOutput> profileEngineOutputs = subscriberProfileStream.flatMapValues(ProfileEngine::getProfileEngineOutputs);
    
    /*****************************************
    *
    *  branch output streams
    *
    *****************************************/

    KStream<StringKey, ? extends SubscriberStreamOutput>[] branchedProfileEngineOutputs = profileEngineOutputs.branch((key,value) -> (value instanceof SubscriberProfile), (key,value) -> (value instanceof SubscriberTrace));
    KStream<StringKey, SubscriberProfile> subscriberUpdateStream = (KStream<StringKey, SubscriberProfile>) branchedProfileEngineOutputs[0];
    KStream<StringKey, SubscriberTrace> subscriberTraceStream = (KStream<StringKey, SubscriberTrace>) branchedProfileEngineOutputs[1];

    /*****************************************
    *
    *  sink
    *
    *****************************************/

    subscriberUpdateStream.to(stringKeySerde, subscriberProfileSerde, subscriberUpdateTopic);
    subscriberTraceStream.to(stringKeySerde, subscriberTraceSerde, subscriberTraceTopic);
    
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

      if (profileEngineStatistics != null) profileEngineStatistics.unregister();

      //
      //  stop streams
      //
      
      boolean streamsCloseSuccessful = kafkaStreams.close(60, TimeUnit.SECONDS);
      log.info("Stopped ProfileEngine" + (streamsCloseSuccessful ? "" : " (timed out)"));
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
  *  nullSubscriberProfile
  *
  ****************************************/

  public static SubscriberProfile nullSubscriberProfile() { return (SubscriberProfile) null; }

  /*****************************************
  *
  *  updateSubscriberProfile
  *
  *****************************************/

  public static SubscriberProfile updateSubscriberProfile(StringKey aggKey, SubscriberStreamEvent profileEvent, SubscriberProfile currentSubscriberProfile)
  {
    /****************************************
    *
    *  get (or create) entry
    *
    ****************************************/

    SubscriberProfile subscriberProfile = (currentSubscriberProfile != null) ? new SubscriberProfile(currentSubscriberProfile) : new SubscriberProfile(profileEvent.getSubscriberID());
    List<String> subscriberTraceDetails = new ArrayList<String>();
    boolean subscriberProfileUpdated = (currentSubscriberProfile != null) ? false : true;

    /*****************************************
    *
    *  clear state
    *
    *****************************************/

    //
    //  subscriberTrace
    //

    if (subscriberProfile.getSubscriberTrace() != null)
      {
        subscriberProfile.setSubscriberTrace(null);
        subscriberProfileUpdated = true;
      }

    //
    //  subscriber groups
    //

    Iterator<String> groupNames = subscriberProfile.getSubscriberGroups().keySet().iterator();
    while (groupNames.hasNext())
      {
        String groupName = groupNames.next();
        int subscriberGroupEpoch = subscriberProfile.getSubscriberGroups().get(groupName);
        int groupEpoch = (subscriberGroupEpochReader.get(groupName) != null) ? subscriberGroupEpochReader.get(groupName).getEpoch() : 0;
        if (subscriberGroupEpoch < groupEpoch)
          {
            groupNames.remove();
          }
      }
    
    //
    //  statusUpdated
    //

    subscriberProfile.setSubscriberStatusUpdated(false);

    /*****************************************
    *
    *  process subscriberTraceControl
    *
    *****************************************/

    if (profileEvent instanceof SubscriberTraceControl)
      {
        SubscriberTraceControl subscriberTraceControl = (SubscriberTraceControl) profileEvent;
        subscriberProfile.setSubscriberTraceEnabled(subscriberTraceControl.getSubscriberTraceEnabled());
        subscriberProfileUpdated = true;
      }
    
    /*****************************************
    *
    *  process recordAlternateID
    *
    *****************************************/

    if (profileEvent instanceof RecordAlternateID)
      {
        //
        //  apply
        //
        
        RecordAlternateID recordAlternateID = (RecordAlternateID) profileEvent;
        if (recordAlternateID.getIDField().equals("msisdn")) subscriberProfile.setMSISDN(recordAlternateID.getAlternateID());
        if (recordAlternateID.getIDField().equals("contractID")) subscriberProfile.setContractID(recordAlternateID.getAlternateID());
        subscriberProfileUpdated = true;

        //
        //  statistics
        //

        updateProfileEngineStatistics(profileEvent);
      }

    /*****************************************
    *
    *  process externalAggregates
    *
    *****************************************/

    if (profileEvent instanceof ExternalAggregates)
      {
        //
        //  apply
        //
        
        ExternalAggregates externalAggregates = (ExternalAggregates) profileEvent;
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

        updateProfileEngineStatistics(profileEvent);
      }

    /*****************************************
    *
    *  process subscriberGroup
    *
    *****************************************/

    if (profileEvent instanceof SubscriberGroup)
      {
        //
        //  apply
        //
        
        SubscriberGroup subscriberGroup = (SubscriberGroup) profileEvent;
        subscriberProfile.setSubscriberGroup(subscriberGroup.getGroupName(), subscriberGroup.getEpoch(), subscriberGroup.getAddSubscriber());
        subscriberProfileUpdated = true;

        //
        //  statistics
        //

        updateProfileEngineStatistics(profileEvent);
      }

    /*****************************************
    *
    *  subscriberStatusUpdated
    *
    *****************************************/

    if (currentSubscriberProfile == null || subscriberProfile.getSubscriberStatus() != currentSubscriberProfile.getSubscriberStatus())
      {
        subscriberProfile.setSubscriberStatusUpdated(true);
        subscriberProfileUpdated = true;
      }

    /*****************************************
    *
    *  subscriberTrace
    *
    *****************************************/

    if (subscriberProfile.getSubscriberTraceEnabled())
      {
        subscriberProfile.setSubscriberTrace(new SubscriberTrace(generateSubscriberTraceMessage(profileEvent, currentSubscriberProfile, subscriberProfile)));
        subscriberProfileUpdated = true;
      }

    /****************************************
    *
    *  return
    *
    ****************************************/

    return subscriberProfileUpdated ? subscriberProfile : currentSubscriberProfile;
  }

  /*****************************************
  *
  *  updateProfileEngineStatistics
  *
  *****************************************/

  private static void updateProfileEngineStatistics(SubscriberStreamEvent event)
  {
    profileEngineStatistics.updateEventProcessedCount(1);
    profileEngineStatistics.updateEventCount(event, 1);
  }

  /*****************************************
  *
  *  getSubscriberProfile
  *
  *****************************************/

  private static SubscriberProfile getSubscriberProfile(SubscriberStreamEvent profileEvent, SubscriberProfile subscriberProfile)
  {
    return subscriberProfile;
  }

  /****************************************
  *
  *  getProfileEngineOutputs
  *
  ****************************************/

  private static List<SubscriberStreamOutput> getProfileEngineOutputs(SubscriberProfile subscriberProfile)
  {
    List<SubscriberStreamOutput> result = new ArrayList<SubscriberStreamOutput>();
    result.addAll(subscriberProfile.getSubscriberStatusUpdated() ? Collections.<SubscriberProfile>singletonList(subscriberProfile) : Collections.<SubscriberProfile>emptyList());
    result.addAll((subscriberProfile.getSubscriberTrace() != null) ? Collections.<SubscriberTrace>singletonList(subscriberProfile.getSubscriberTrace()) : Collections.<SubscriberTrace>emptyList());
    return result;
  }
  
  /*****************************************
  *
  *  generateSubscriberTraceMessage
  *
  *****************************************/

  private static String generateSubscriberTraceMessage(SubscriberStreamEvent profileEvent, SubscriberProfile currentSubscriberProfile, SubscriberProfile subscriberProfile)
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
    //  field -- profileEvent
    //

    JsonNode profileEventNode = deserializer.deserialize(null, converter.fromConnectData(null, profileEvent.subscriberStreamEventSchema(),  profileEvent.subscriberStreamEventPack(profileEvent)));

    //
    //  field -- currentSubscriberProfile
    //

    JsonNode currentSubscriberProfileNode = (currentSubscriberProfile != null) ? deserializer.deserialize(null, converter.fromConnectData(null, SubscriberProfile.schema(),  SubscriberProfile.pack(currentSubscriberProfile))) : null;

    //
    //  field -- subscriberProfile
    //

    JsonNode subscriberProfileNode = deserializer.deserialize(null, converter.fromConnectData(null, SubscriberProfile.schema(),  SubscriberProfile.pack(subscriberProfile)));

    /*****************************************
    *
    *  hack/massage triggerStateNodes to remove unwanted/misleading/spurious fields from currentTriggerState
    *
    *****************************************/

    //
    //  outgoing messages (currentSubscriberStateNode)
    //

    if (currentSubscriberProfileNode != null) ((ObjectNode) currentSubscriberProfileNode).remove("subscriberTraceMessage");

    //
    //  other (subscriberStateNode)
    //

    ((ObjectNode) subscriberProfileNode).remove("subscriberTraceMessage");

    /*****************************************
    *
    *  subscriberTraceMessage
    *
    *****************************************/

    //
    //  create parent JsonNode (to package fields)
    //

    ObjectNode subscriberTraceMessageNode = jsonNodeFactory.objectNode();
    subscriberTraceMessageNode.put("source", "ProfileEngine");
    subscriberTraceMessageNode.put("subscriberID", profileEvent.getSubscriberID());
    subscriberTraceMessageNode.put("processingTime", SystemTime.getCurrentTime().getTime());
    subscriberTraceMessageNode.set(profileEvent.getClass().getSimpleName(), profileEventNode);
    subscriberTraceMessageNode.set("currentSubscriberProfile", currentSubscriberProfileNode);
    subscriberTraceMessageNode.set("subscriberProfile", subscriberProfileNode);

    //
    //  convert to string
    //

    return subscriberTraceMessageNode.toString();
  }

  /*****************************************
  *
  *  class ProfileEventTimestampExtractor
  *
  *****************************************/

  public static class ProfileEventTimestampExtractor implements TimestampExtractor
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
