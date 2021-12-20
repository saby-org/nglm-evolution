package com.evolving.nglm.evolution.event;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.evolution.ParameterMap;
import kafka.Kafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class ProductExternalEvent extends ExternalEvent{

	private static final Schema schema;
	static {
		SchemaBuilder schemaBuilder = ExternalEvent.struct();
		schemaBuilder.name("productexternalevent");
		schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
		schemaBuilder.field("profileUpdates", ParameterMap.schema()).optional();
		schema = schemaBuilder.build();
	}
	public static Schema schema() { return schema; }
	public static Object pack(Object value){
		ProductExternalEvent productExternalEvent = (ProductExternalEvent) value;
		Struct struct = new Struct(schema);
		ExternalEvent.packExternalEvent(struct,productExternalEvent);
		if(productExternalEvent.getProfileUpdates()!=null) struct.put("profileUpdates",ParameterMap.pack(productExternalEvent.getProfileUpdates()));
		return struct;
	}
	public static ProductExternalEvent unpack(SchemaAndValue schemaAndValue){return new ProductExternalEvent(schemaAndValue);}
	public ProductExternalEvent(SchemaAndValue schemaAndValue) {
		super(schemaAndValue);
		Schema schema = schemaAndValue.schema();
		Struct valueStruct = (Struct)schemaAndValue.value();
		if(schema.field("profileUpdate")!=null){
			this.profileUpdates = ParameterMap.unpack(new SchemaAndValue(schema.field("profileUpdates").schema(), valueStruct.get("profileUpdates")));
		}
	}
	private static ConnectSerde<ProductExternalEvent> serde = new ConnectSerde<>(schema, false, ProductExternalEvent.class, ProductExternalEvent::pack, ProductExternalEvent::unpack);
	public static ConnectSerde<ProductExternalEvent> serde() { return serde; }

	// so far there is no event declaration for this one, so overriding those (coming from event declaration in super class)
	@Override public Schema subscriberStreamEventSchema() {return schema();}
	@Override public Object subscriberStreamEventPack(Object value) {return pack(this);}
	@Override public String getEventName() {return this.getClass().getSimpleName();}

	private ParameterMap profileUpdates;
	public ParameterMap getProfileUpdates() { return profileUpdates; }
	public void setProfileUpdates(ParameterMap profileUpdates){this.profileUpdates=profileUpdates;}

	public ProductExternalEvent(String subscriberID){
		super(subscriberID);
	}

	public ProductExternalEvent(AlternateID alternateID, String alternateIDValue){
		super(alternateID,alternateIDValue);
	}

	public void sendToEngine(KafkaProducer<byte[],byte[]> kafkaProducer){
		String topic = Deployment.getProductExternalEventRequestTopic();
		kafkaProducer.send(new ProducerRecord<>(
				topic,
				StringKey.serde().serializer().serialize(topic,new StringKey(this.getSubscriberID())),
				ProductExternalEvent.serde().serializer().serialize(topic,this)
		));
	}

}
