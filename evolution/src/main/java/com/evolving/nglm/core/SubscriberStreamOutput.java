package com.evolving.nglm.core;

import com.evolving.nglm.evolution.*;
import com.evolving.nglm.evolution.Deployment;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public abstract class SubscriberStreamOutput {

	private static final Logger log = LoggerFactory.getLogger(SubscriberStreamOutput.class);

	// the common schema
	private static Schema subscriberStreamOutputSchema;
	private static int currentSchemaVersion = 2;
	static {
		SchemaBuilder schemaBuilder = SchemaBuilder.struct();
		schemaBuilder.name("subscriber_stream_output");
		schemaBuilder.version(SchemaUtilities.packSchemaVersion(currentSchemaVersion));
		schemaBuilder.field("alternateIDs", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).optional().schema());
		subscriberStreamOutputSchema = schemaBuilder.schema();
	}
	public static Schema subscriberStreamOutputSchema() { return subscriberStreamOutputSchema; }

	// the common pack
	public static void  packSubscriberStreamOutput(Struct struct, SubscriberStreamOutput subscriberStreamOutput){
		struct.put("alternateIDs",subscriberStreamOutput.getAlternateIDs());
	}

	// the common attributes
	private Map<String,String> alternateIDs;
	// the common getters
	public Map<String, String> getAlternateIDs(){return alternateIDs;}


	// for now, based how we implements inheritance, that is the cleanest it seems we could do

	// the empty super constructor cause too lazy to force all the subclass to always construct with value (this break the enforcement of having it!)
	public SubscriberStreamOutput(){
		this.alternateIDs=new LinkedHashMap<>();
	}
	// the super constructor for the "unpack" subclass object creation
	public SubscriberStreamOutput(SchemaAndValue schemaAndValue){
		Schema schema = schemaAndValue.schema();
		Object value = schemaAndValue.value();
		Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;
		Struct valueStruct = (Struct) value;
		this.alternateIDs = (schemaVersion >= 1 && schema.field("alternateIDs")!=null && valueStruct.get("alternateIDs") != null) ? (Map<String,String>) valueStruct.get("alternateIDs") : new LinkedHashMap<>();
	}
	// the copy constructor
	public SubscriberStreamOutput(SubscriberStreamOutput subscriberStreamOutput){
		this.alternateIDs = new LinkedHashMap<>();
		if (subscriberStreamOutput.getAlternateIDs()!=null) getAlternateIDs().putAll(subscriberStreamOutput.getAlternateIDs());
	}
	// with subscriberprofile context constructor
	public SubscriberStreamOutput(SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader){
		this.alternateIDs = buildAlternateIDs(subscriberProfile,subscriberGroupEpochReader);
	}
	// enrich directly
	public void enrichSubscriberStreamOutput(SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader){
		this.alternateIDs = buildAlternateIDs(subscriberProfile,subscriberGroupEpochReader);
	}

	// build alternateIDs populated
	private Map<String,String> buildAlternateIDs(SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader) {
		Map<String,String> alternateIDs = new LinkedHashMap<>();
		SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, SystemTime.getCurrentTime());

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
