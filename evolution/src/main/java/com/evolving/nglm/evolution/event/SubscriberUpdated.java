package com.evolving.nglm.evolution.event;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.HashMap;
import java.util.Map;

public class SubscriberUpdated extends SubscriberStreamOutput implements SubscriberStreamEvent {

	private static final Schema schema;
	static {
		SchemaBuilder schemaBuilder = SchemaBuilder.struct();
		schemaBuilder.name("subscriber_updated");
		schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(),1));
		for(Field field:subscriberStreamOutputSchema().fields()) schemaBuilder.field(field.name(),field.schema());
		schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
		schemaBuilder.field("tenantID", Schema.INT16_SCHEMA);
		schemaBuilder.field("subscriberDeleted", Schema.OPTIONAL_BOOLEAN_SCHEMA);
		schemaBuilder.field("alternateIDsToRemove", SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.OPTIONAL_STRING_SCHEMA).optional().schema());
		schemaBuilder.field("alternateIDsToAdd", SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.OPTIONAL_STRING_SCHEMA).optional().schema());
		schema = schemaBuilder.build();
	}
	public static Schema schema() { return schema; }
	@Override public Schema subscriberStreamEventSchema() {return schema();}

	public static Object pack(Object value){
		SubscriberUpdated subscriberUpdated = (SubscriberUpdated) value;
		Struct struct = new Struct(schema);
		packSubscriberStreamOutput(struct,subscriberUpdated);
		struct.put("subscriberID", subscriberUpdated.getSubscriberID());
		struct.put("tenantID", subscriberUpdated.getTenantID().shortValue());
		struct.put("subscriberDeleted", subscriberUpdated.getSubscriberDeleted());
		if(!subscriberUpdated.getAlternateIDsToRemove().isEmpty()) struct.put("alternateIDsToRemove",SubscriberStreamOutput.packAlternateIDs(subscriberUpdated.getAlternateIDsToRemove()));
		if(!subscriberUpdated.getAlternateIDsToAdd().isEmpty()) struct.put("alternateIDsToAdd",SubscriberStreamOutput.packAlternateIDs(subscriberUpdated.getAlternateIDsToAdd()));
		return struct;
	}
	@Override public Object subscriberStreamEventPack(Object value) {return pack(value);}

	public static SubscriberUpdated unpack(SchemaAndValue schemaAndValue){return new SubscriberUpdated(schemaAndValue);}
	private SubscriberUpdated(SchemaAndValue schemaAndValue){
		super(schemaAndValue);
		Schema schema = schemaAndValue.schema();
		Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;
		Struct valueStruct = (Struct) schemaAndValue.value();
		this.subscriberID = valueStruct.getString("subscriberID");
		this.tenantID = schema.field("tenantID") != null ? valueStruct.getInt16("tenantID").intValue() : 1;
		this.subscriberDeleted = schema.field("subscriberDeleted")!=null ? valueStruct.getBoolean("subscriberDeleted") : false;
		Map<String,String> packedAlternateIDsToRemove = schema.field("alternateIDsToRemove")!=null ? valueStruct.getMap("alternateIDsToRemove") : null;
		if(packedAlternateIDsToRemove!=null) this.alternateIDsToRemove=SubscriberStreamOutput.unpackAlternateIDs(packedAlternateIDsToRemove);
		Map<String,String> packedAlternateIDsToAdd = schema.field("alternateIDsToAdd")!=null ? valueStruct.getMap("alternateIDsToAdd") : null;
		if(packedAlternateIDsToAdd!=null) this.alternateIDsToAdd=SubscriberStreamOutput.unpackAlternateIDs(packedAlternateIDsToAdd);
	}

	private static ConnectSerde<SubscriberUpdated> serde = new ConnectSerde<>(schema, false, SubscriberUpdated.class, SubscriberUpdated::pack, SubscriberUpdated::unpack);
	public static ConnectSerde<SubscriberUpdated> serde() { return serde; }

	private String subscriberID;
	private Integer tenantID;
	private boolean subscriberDeleted = false;
	private Map<AlternateID,String> alternateIDsToRemove = new HashMap<>();
	private Map<AlternateID,String> alternateIDsToAdd = new HashMap<>();

	public SubscriberUpdated(String subscriberID, Integer tenantID){this.subscriberID = subscriberID; this.tenantID=tenantID;}

	@Override public String getSubscriberID(){return this.subscriberID;}
	public Integer getTenantID(){return this.tenantID;}
	public boolean getSubscriberDeleted(){return this.subscriberDeleted;}
	public Map<AlternateID,String> getAlternateIDsToRemove(){return this.alternateIDsToRemove;}
	public Map<AlternateID,String> getAlternateIDsToAdd(){return this.alternateIDsToAdd;}

	public void subscriberDeleted(){this.subscriberDeleted=true;}
	public void addAlternateIDToRemove(AlternateID alternateID, String value){this.alternateIDsToRemove.put(alternateID,value);}
	public void addAlternateIDToAdd(AlternateID alternateID, String value){this.alternateIDsToAdd.put(alternateID,value);}

}
