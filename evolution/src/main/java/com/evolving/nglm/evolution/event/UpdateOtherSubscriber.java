package com.evolving.nglm.evolution.event;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.DeploymentCommon;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.evolution.SupportedRelationship;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class UpdateOtherSubscriber extends SubscriberStreamOutput implements SubscriberStreamEvent {

	private static final Schema schema;
	static {
		SchemaBuilder schemaBuilder = SchemaBuilder.struct();
		schemaBuilder.name("update_other_subscriber");
		schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(),0));
		for(Field field:subscriberStreamOutputSchema().fields()) schemaBuilder.field(field.name(),field.schema());
		schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
		schemaBuilder.field("childAddedParent", Schema.OPTIONAL_INT64_SCHEMA);
		schemaBuilder.field("addedParentRelationship", Schema.OPTIONAL_STRING_SCHEMA);
		schemaBuilder.field("childRemovedParent", Schema.OPTIONAL_INT64_SCHEMA);
		schemaBuilder.field("removedParentRelationship", Schema.OPTIONAL_STRING_SCHEMA);
		schema = schemaBuilder.build();
	}
	public static Schema schema() { return schema; }
	@Override public Schema subscriberStreamEventSchema() {return schema();}

	public static Object pack(Object value){
		UpdateOtherSubscriber updateOtherSubscriber = (UpdateOtherSubscriber) value;
		Struct struct = new Struct(schema);
		packSubscriberStreamOutput(struct,updateOtherSubscriber);
		struct.put("subscriberID", updateOtherSubscriber.getSubscriberID());
		if(updateOtherSubscriber.getParentAdded()!=null){
			struct.put("childAddedParent",updateOtherSubscriber.getParentAdded().getFirstElement());
			struct.put("addedParentRelationship",updateOtherSubscriber.getParentAdded().getSecondElement().getID());
		}
		if(updateOtherSubscriber.getParentRemoved()!=null){
			struct.put("childRemovedParent",updateOtherSubscriber.getParentRemoved().getFirstElement());
			struct.put("removedParentRelationship",updateOtherSubscriber.getParentRemoved().getSecondElement().getID());
		}
		return struct;
	}
	@Override public Object subscriberStreamEventPack(Object value) {return pack(value);}

	public static UpdateOtherSubscriber unpack(SchemaAndValue schemaAndValue){return new UpdateOtherSubscriber(schemaAndValue);}
	private UpdateOtherSubscriber(SchemaAndValue schemaAndValue){
		super(schemaAndValue);
		Schema schema = schemaAndValue.schema();
		Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;
		Struct valueStruct = (Struct) schemaAndValue.value();
		this.subscriberID = valueStruct.getString("subscriberID");
		Long packedChildAddedParent = schema.field("childAddedParent")!=null ? valueStruct.getInt64("childAddedParent") : null;
		if(packedChildAddedParent!=null){
			SupportedRelationship supportedRelationship = DeploymentCommon.getSupportedRelationships().get(valueStruct.getString("addedParentRelationship"));
			if(supportedRelationship!=null) this.parentAdded = new Pair<>(packedChildAddedParent,supportedRelationship);
		}
		Long packedChildRemovedParent = schema.field("childRemovedParent")!=null ? valueStruct.getInt64("childRemovedParent") : null;
		if(packedChildRemovedParent!=null){
			SupportedRelationship supportedRelationship = DeploymentCommon.getSupportedRelationships().get(valueStruct.getString("removedParentRelationship"));
			if(supportedRelationship!=null) this.parentRemoved = new Pair<>(packedChildRemovedParent,supportedRelationship);
		}

	}

	private static ConnectSerde<UpdateOtherSubscriber> serde = new ConnectSerde<>(schema, false, UpdateOtherSubscriber.class, UpdateOtherSubscriber::pack, UpdateOtherSubscriber::unpack);
	public static ConnectSerde<UpdateOtherSubscriber> serde() { return serde; }

	private String subscriberID;
	private Pair<Long,SupportedRelationship> parentAdded;
	private Pair<Long,SupportedRelationship> parentRemoved;

	public UpdateOtherSubscriber(String otherSubscriberID){
		this.subscriberID = otherSubscriberID;
	}

	@Override public String getSubscriberID() {return subscriberID;}
	public Pair<Long,SupportedRelationship> getParentAdded(){return parentAdded;}
	public Pair<Long,SupportedRelationship> getParentRemoved(){return parentRemoved;}

	public void parentAdded(Long child, SupportedRelationship relationship){this.parentAdded = new Pair<>(child,relationship);}
	public void parentRemoved(Long child, SupportedRelationship relationship){this.parentRemoved = new Pair<>(child,relationship);}

}
