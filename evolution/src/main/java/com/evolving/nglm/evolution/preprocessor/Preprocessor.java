package com.evolving.nglm.evolution.preprocessor;

import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SubscriberIDService;
import com.evolving.nglm.evolution.*;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;
import com.evolving.nglm.evolution.statistics.CounterStat;
import com.evolving.nglm.evolution.statistics.StatBuilder;
import com.evolving.nglm.evolution.statistics.StatsBuilders;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.elasticsearch.ElasticsearchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Preprocessor {

	private static final Logger log = LoggerFactory.getLogger(Preprocessor.class);

	private static ElasticsearchClientAPI elasticsearchClientAPI;
	static {
		try {
			elasticsearchClientAPI = new ElasticsearchClientAPI("PreprocessorEngine");
		} catch (ElasticsearchException e) {
			throw new ServerRuntimeException("could not initialize elasticsearch client", e);
		}
	}
	private static StatBuilder<CounterStat> statsPreprocessorInputCounter;
	private static StatBuilder<CounterStat> statsPreprocessorOutputCounter;
	private static SubscriberIDService subscriberIDService;
	static {
		subscriberIDService = new SubscriberIDService(Deployment.getRedisSentinels());
	}
	private static SupplierService supplierService;
	private static VoucherService voucherService;
	private static VoucherTypeService voucherTypeService;

	public static void startPreprocessorStreams(String evolutionEngineKey, SupplierService supplierService, VoucherService voucherService, VoucherTypeService voucherTypeService){

		if(!Deployment.isPreprocessorNeeded()) return;// no preprocessor needed

		log.info("need to start preprocessors");

		// conf
		Properties preprocessorStreamsProperties = ConfigUtils.envPropertiesWithPrefix("KAFKA_STREAMS"); // start taking all evolution engine stream conf (yet not all evolution engine streams conf comes from this ENV properties reader)
		preprocessorStreamsProperties.putAll(ConfigUtils.envPropertiesWithPrefix("PREPROCESSOR_STREAMS")); // override special for preprocessor
		preprocessorStreamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-preprocessor");
		preprocessorStreamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Deployment.getBrokerServers());
		// take evolution engine stream thread value if not given
		if(!preprocessorStreamsProperties.containsValue(StreamsConfig.NUM_STREAM_THREADS_CONFIG)) preprocessorStreamsProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Deployment.getEvolutionEngineStreamThreads());

		StreamsBuilder preprocessorStreamsBuilder = new StreamsBuilder();

		// the simple topology
		for(EvolutionEngineEventDeclaration declaration:Deployment.getEvolutionEngineEvents().values()){
			if(declaration.getPreprocessTopic()!=null){
				log.info("Preprocessor.startPreprocessorStreams : will run preprocessor from "+declaration.getPreprocessTopic().getName()+" to "+declaration.getEventTopic());
				preprocessorStreamsBuilder
					.stream(declaration.getPreprocessTopic().getName(), Consumed.with(StringKey.serde(),declaration.getPreprocessorSerde()))//from topic
					.flatMap(preprocessorKeyValueMapper())//transform into no, one, or multiple records
					.to(declaration.getEventTopic(), Produced.with(StringKey.serde(),declaration.getEventSerde()));//to topic
			}
		}

		//static services context init if needed
		synchronized (Preprocessor.class){
			if(evolutionEngineKey!=null && Preprocessor.statsPreprocessorInputCounter==null) Preprocessor.statsPreprocessorInputCounter = StatsBuilders.getEvolutionCounterStatisticsBuilder("preprocessor_input","evolutionengine-"+evolutionEngineKey);
			if(evolutionEngineKey!=null && Preprocessor.statsPreprocessorOutputCounter==null) Preprocessor.statsPreprocessorOutputCounter = StatsBuilders.getEvolutionCounterStatisticsBuilder("preprocessor_output","evolutionengine-"+evolutionEngineKey);
			if(supplierService!=null && Preprocessor.supplierService==null) Preprocessor.supplierService=supplierService;
			if(voucherService!=null && Preprocessor.voucherService==null) Preprocessor.voucherService=voucherService;
			if(voucherTypeService!=null && Preprocessor.voucherTypeService==null) Preprocessor.voucherTypeService=voucherTypeService;
		}

		// starting the kstreams app
		KafkaStreams preprocessorStreams = new KafkaStreams(preprocessorStreamsBuilder.build(),preprocessorStreamsProperties);
		preprocessorStreams.start();

		// shutdown hook, stopping streams app cleanly
		NGLMRuntime.addShutdownHook(notUsed->preprocessorStreams.close());

		log.info("Preprocessor.startPreprocessorStreams : preprocessor started with properties "+preprocessorStreamsProperties);

	}

	// the transformer
	private static KeyValueMapper<StringKey, PreprocessorEvent, Iterable<KeyValue<StringKey,EvolutionEngineEvent>>> preprocessorKeyValueMapper(){
		return ((key, value) -> {

			Preprocessor.statsPreprocessorInputCounter.withLabel(StatsBuilders.LABEL.name.name(),value.getClass().getSimpleName()).getStats().increment();

			PreprocessorContext context = new PreprocessorContext(elasticsearchClientAPI,subscriberIDService,supplierService,voucherService,voucherTypeService);
			Collection<EvolutionEngineEvent> preprocessedEvents;
			try{
				value.preprocessEvent(context);// trigger preprocessing
				preprocessedEvents = value.getPreprocessedEvents();// get result
			}catch (Exception e){
				// custo code calls, not crashing on exception
				log.error("Preprocessor preprocessEvent exception",e);
				return Collections.emptyList();
			}

			if(preprocessedEvents==null||preprocessedEvents.isEmpty()) return Collections.emptyList();//nothing returned

			// create new EvolutionEngineEvent records derived, all keyed by subscriberId returned
			List<KeyValue<StringKey,EvolutionEngineEvent>> toRet = new ArrayList<>();
			preprocessedEvents.forEach(event -> {
				Preprocessor.statsPreprocessorOutputCounter.withLabel(StatsBuilders.LABEL.name.name(),event.getEventName()).getStats().increment();
				toRet.add(new KeyValue<>(new StringKey(event.getSubscriberID()),event));
			});
			return toRet;

		});
	}


}
