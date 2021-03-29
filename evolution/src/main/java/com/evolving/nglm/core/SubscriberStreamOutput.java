package com.evolving.nglm.core;

import com.evolving.nglm.evolution.*;
import com.evolving.nglm.evolution.DeliveryRequest.DeliveryPriority;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public abstract class SubscriberStreamOutput implements SubscriberStreamPriority{

	private static final Logger log = LoggerFactory.getLogger(SubscriberStreamOutput.class);

	// the common schema
	private static Schema subscriberStreamOutputSchema;
	private static int currentSchemaVersion = 9;
	static {
		SchemaBuilder schemaBuilder = SchemaBuilder.struct();
		schemaBuilder.name("subscriber_stream_output");
		schemaBuilder.version(SchemaUtilities.packSchemaVersion(currentSchemaVersion));
		schemaBuilder.field("alternateIDs", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).optional().schema());
		schemaBuilder.field("deliveryPriority", Schema.STRING_SCHEMA);
		subscriberStreamOutputSchema = schemaBuilder.schema();
	}
	public static Schema subscriberStreamOutputSchema() { return subscriberStreamOutputSchema; }

	// the common pack
	public static void  packSubscriberStreamOutput(Struct struct, SubscriberStreamOutput subscriberStreamOutput){
		struct.put("alternateIDs",subscriberStreamOutput.getAlternateIDs());
		struct.put("deliveryPriority", subscriberStreamOutput.getDeliveryPriority().getExternalRepresentation());
	}

	// the common attributes
	private Map<String,String> alternateIDs;
	private DeliveryPriority deliveryPriority;
	// the common getters
	public Map<String, String> getAlternateIDs(){return alternateIDs;}
	@Override public DeliveryPriority getDeliveryPriority(){return forcedDeliveryPriority!=null ? forcedDeliveryPriority : deliveryPriority; }

	// the not packed data
	// if set, the deliveryPriority will be this one (to override the default behavior of priority is set by the incoming event)
	private DeliveryPriority forcedDeliveryPriority = null;
	public void forceDeliveryPriority(DeliveryPriority forcedDeliveryPriority){this.forcedDeliveryPriority=forcedDeliveryPriority;}


	// for now, based how we implements inheritance, that is the cleanest it seems we could do

	// the empty super constructor cause too lazy to force all the subclass to always construct with value (this break the enforcement of having it!)
	public SubscriberStreamOutput(){
		this.alternateIDs=new LinkedHashMap<>();
		this.deliveryPriority = DeliveryPriority.Standard;
	}
	// the super constructor for the "unpack" subclass object creation
	public SubscriberStreamOutput(SchemaAndValue schemaAndValue){
		Schema schema = schemaAndValue.schema();
		Object value = schemaAndValue.value();
		Struct valueStruct = (Struct) value;
		this.alternateIDs = (schema.field("alternateIDs")!=null && valueStruct.get("alternateIDs") != null) ? (Map<String,String>) valueStruct.get("alternateIDs") : new LinkedHashMap<>();
		this.deliveryPriority = schema.field("deliveryPriority")!=null ? DeliveryPriority.fromExternalRepresentation(valueStruct.getString("deliveryPriority")) : DeliveryPriority.Standard;
	}
	// the copy constructor
	public SubscriberStreamOutput(SubscriberStreamOutput subscriberStreamOutput){
		this.alternateIDs = new LinkedHashMap<>();
		if (subscriberStreamOutput.getAlternateIDs()!=null) getAlternateIDs().putAll(subscriberStreamOutput.getAlternateIDs());
		this.deliveryPriority = subscriberStreamOutput.getDeliveryPriority();
	}
	// with subscriberprofile context constructor
	public SubscriberStreamOutput(SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, DeliveryPriority deliveryPriority, int tenantID){
		this.alternateIDs = buildAlternateIDs(subscriberProfile,subscriberGroupEpochReader, tenantID);
		this.deliveryPriority = deliveryPriority;
	}
	// enrich directly
	public void enrichSubscriberStreamOutput(SubscriberStreamEvent originatingEvent, SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, int tenantID){
		this.alternateIDs = buildAlternateIDs(subscriberProfile,subscriberGroupEpochReader, tenantID);
		this.deliveryPriority = originatingEvent.getDeliveryPriority();
	}

	// build alternateIDs populated
	private Map<String,String> buildAlternateIDs(SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, int tenantID) {
		Map<String,String> alternateIDs = new LinkedHashMap<>();
		SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, SystemTime.getCurrentTime(), tenantID);

		for (Map.Entry<String,AlternateID> entry:Deployment.getAlternateIDs().entrySet()) {
			AlternateID alternateID = entry.getValue();
			if (alternateID.getProfileCriterionField() == null ) {
				log.warn("ProfileCriterionField is not given for alternate ID {} - skiping", entry.getKey());
				continue;
			}
			CriterionField criterionField = Deployment.getProfileCriterionFields().get(alternateID.getProfileCriterionField());
			String criterionFieldValue = (String)criterionField.retrieveNormalized(evaluationRequest);
			if(log.isTraceEnabled()) log.trace("adding {} {} for subscriber {}",entry.getKey(),criterionFieldValue,subscriberProfile.getSubscriberID());
			alternateIDs.put(entry.getKey(), criterionFieldValue);
		}
		return alternateIDs;
	}

}
