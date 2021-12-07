package com.evolving.nglm.evolution.event;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class ProductExternalEvent extends ExternalEvent{

	private static final Schema schema;
	static {
		SchemaBuilder schemaBuilder = ExternalEvent.struct();
		schemaBuilder.name("productexternalevent");
		schemaBuilder.version(SchemaUtilities.packSchemaVersion(0));
		schema = schemaBuilder.build();
	}
	public static Schema schema() { return schema; }
	public static Object pack(Object value){
		ProductExternalEvent productExternalEvent = (ProductExternalEvent) value;
		Struct struct = new Struct(schema);
		ExternalEvent.packExternalEvent(struct,productExternalEvent);
		return struct;
	}
	public static ProductExternalEvent unpack(SchemaAndValue schemaAndValue){return new ProductExternalEvent(schemaAndValue);}
	public ProductExternalEvent(SchemaAndValue schemaAndValue) {
		super(schemaAndValue);
	}
	private static ConnectSerde<ProductExternalEvent> serde = new ConnectSerde<>(schema, false, ProductExternalEvent.class, ProductExternalEvent::pack, ProductExternalEvent::unpack);
	public static ConnectSerde<ProductExternalEvent> serde() { return serde; }

	// so far there is no event declaration for this one, so overriding those (coming from event declaration in super class)
	@Override public Schema subscriberStreamEventSchema() {return schema();}
	@Override public Object subscriberStreamEventPack(Object value) {return pack(this);}
	@Override public String getEventName() {return this.getClass().getSimpleName();}

	public ProductExternalEvent(String subscriberID){
		super(subscriberID);
	}

}
