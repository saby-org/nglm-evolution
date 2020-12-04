package com.evolving.nglm.evolution.kafka;

import com.evolving.nglm.core.StringKey;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

// extending the default, though it just always fail, and does not use any of the conf, but if that change for good reason, I'm "keeping it" in, so we know
public class EvolutionProductionExceptionHandler extends DefaultProductionExceptionHandler {

	private static final Logger log = LoggerFactory.getLogger(EvolutionProductionExceptionHandler.class);

	@Override
	public ProductionExceptionHandlerResponse handle(final ProducerRecord<byte[], byte[]> record, final Exception exception) {

		//extract usefull info for logging
		String topic = record!=null ? record.topic() : "(unknown topic)";
		Integer partition = record!=null ? record.partition() : null;

		String key = null;
		if(topic!=null){
			// usually it is a StringKey
			try{key=StringKey.serde().deserializer().deserialize(topic,record.key()).getKey();}catch (Exception e){}
			// might be as well simple String
			if(key==null) try{key=Serdes.String().deserializer().deserialize(topic,record.key());}catch (Exception e){}
			// or I don't know
			if(key==null) key=new String(record.value());
		}

		// get first what would "super" do
		ProductionExceptionHandlerResponse superResponse = super.handle(record,exception);

		// if super fails, but we don't want :
		if(superResponse==ProductionExceptionHandlerResponse.FAIL){

			// the "org.apache.kafka.common.errors.ApiException" got stack trace removed, adding it back to print a big bad error log
			exception.setStackTrace(Thread.currentThread().getStackTrace());

			// overriding cause we want to skip those
			if(exception instanceof RecordTooLargeException){
				log.error("EvolutionProductionExceptionHandler SKIPPING WRITTING RECORD TO "+topic+":"+partition+" FOR KEY "+key,exception);
				return ProductionExceptionHandlerResponse.CONTINUE;
			}

		}else{
			log.info("EvolutionProductionExceptionHandler exception already skipped by super handler, so we skip",exception);
		}
		return superResponse;
	}

	@Override
	public void configure(final Map<String, ?> configs) {
		super.configure(configs);
	}
}
