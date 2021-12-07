package com.evolving.nglm.core;

import com.evolving.nglm.evolution.*;
import com.evolving.nglm.evolution.DeliveryRequest.DeliveryPriority;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class SubscriberStreamOutput implements SubscriberStreamPriority, SubscriberStreamTimeStamped {

	private static final Logger log = LoggerFactory.getLogger(SubscriberStreamOutput.class);

	// the common schema
	private static Schema subscriberStreamOutputSchema;
	private static int currentSchemaVersion = 10;
	static {
		SchemaBuilder schemaBuilder = SchemaBuilder.struct();
		schemaBuilder.name("subscriber_stream_output");
		schemaBuilder.version(SchemaUtilities.packSchemaVersion(currentSchemaVersion));
		schemaBuilder.field("alternateIDs", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).optional().schema());
		schemaBuilder.field("deliveryPriority", Schema.STRING_SCHEMA);
		schemaBuilder.field("eventDate", Schema.OPTIONAL_INT64_SCHEMA);
		schemaBuilder.field("eventID", Schema.OPTIONAL_STRING_SCHEMA);
		subscriberStreamOutputSchema = schemaBuilder.schema();
	}
	public static Schema subscriberStreamOutputSchema() { return subscriberStreamOutputSchema; }

	// the common pack
	public static void  packSubscriberStreamOutput(Struct struct, SubscriberStreamOutput subscriberStreamOutput){
		struct.put("alternateIDs",subscriberStreamOutput.getAlternateIDs());
		struct.put("deliveryPriority", subscriberStreamOutput.getDeliveryPriority().getExternalRepresentation());
		struct.put("eventDate", subscriberStreamOutput.getEventDate().getTime());
		struct.put("eventID", subscriberStreamOutput.getEventID());
	}

	// the common attributes
	private Map<String,String> alternateIDs;
	private DeliveryPriority deliveryPriority;
	private Date eventDate;
	private String eventID;
	// the common getters
	public Map<String, String> getAlternateIDs(){return alternateIDs;}
	@Override public DeliveryPriority getDeliveryPriority(){return forcedDeliveryPriority!=null ? forcedDeliveryPriority : deliveryPriority; }
	@Override public Date getEventDate(){return eventDate;}
	@Override public String getEventID(){return eventID;}
	// the common setters
	public void setEventID(String eventID){this.eventID=eventID;}
	public void setEventDate(Date eventDate){this.eventDate=eventDate;}

	// the not packed data
	// if set, the deliveryPriority will be this one (to override the default behavior of priority is set by the incoming event)
	private DeliveryPriority forcedDeliveryPriority = null;
	public void forceDeliveryPriority(DeliveryPriority forcedDeliveryPriority){this.forcedDeliveryPriority=forcedDeliveryPriority;}


	// for now, based how we implements inheritance, that is the cleanest it seems we could do

	// the empty super constructor cause too lazy to force all the subclass to always construct with value (this break the enforcement of having it!)
	public SubscriberStreamOutput(){
		this.alternateIDs=new LinkedHashMap<>();
		this.deliveryPriority = DeliveryPriority.Standard;
		this.eventDate = SystemTime.getCurrentTime();
	}
	// the super constructor for the "unpack" subclass object creation
	public SubscriberStreamOutput(SchemaAndValue schemaAndValue){
		Schema schema = schemaAndValue.schema();
		Object value = schemaAndValue.value();
		Struct valueStruct = (Struct) value;
		this.alternateIDs = (schema.field("alternateIDs")!=null && valueStruct.get("alternateIDs") != null) ? (Map<String,String>) valueStruct.get("alternateIDs") : new LinkedHashMap<>();
		this.deliveryPriority = schema.field("deliveryPriority")!=null ? DeliveryPriority.fromExternalRepresentation(valueStruct.getString("deliveryPriority")) : DeliveryPriority.Standard;
		this.eventDate = (schema.field("eventDate")!=null && valueStruct.get("eventDate") != null) ? new Date(valueStruct.getInt64("eventDate")) : SystemTime.getCurrentTime();
		this.eventID = schema.field("eventID")!=null ? valueStruct.getString("eventID") : null;
	}
	// the copy constructor
	public SubscriberStreamOutput(SubscriberStreamOutput subscriberStreamOutput){
		this.alternateIDs = new LinkedHashMap<>();
		if (subscriberStreamOutput.getAlternateIDs()!=null) getAlternateIDs().putAll(subscriberStreamOutput.getAlternateIDs());
		this.deliveryPriority = subscriberStreamOutput.getDeliveryPriority();
		this.eventDate = subscriberStreamOutput.getEventDate();
		this.eventID = subscriberStreamOutput.getEventID();
	}
	// with an incoming event
	public SubscriberStreamOutput(DeliveryPriority deliveryPriority, Date eventDate, String eventID){
		this.alternateIDs = new LinkedHashMap<>();
		this.deliveryPriority = deliveryPriority;
		this.eventDate = eventDate;
		this.eventID = eventID;
	}
	// with subscriberprofile context constructor
	public SubscriberStreamOutput(SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, DeliveryPriority deliveryPriority){
		this.alternateIDs = buildAlternateIDs(subscriberProfile,subscriberGroupEpochReader);
		this.deliveryPriority = deliveryPriority;
		this.eventDate = SystemTime.getCurrentTime();
	}
	// from ES
	public SubscriberStreamOutput(Map<String, Object> esFields){
		this();
		this.eventID = (String) esFields.get("eventID");
	}
	// enrich directly
	public void enrichSubscriberStreamOutput(SubscriberStateOutputWrapper subscriberStateOutputWrapper){
		this.alternateIDs = buildAlternateIDs(subscriberStateOutputWrapper.getEvolutionEventContext().getSubscriberState().getSubscriberProfile(),subscriberStateOutputWrapper.getEvolutionEventContext().getSubscriberGroupEpochReader());
		this.deliveryPriority = subscriberStateOutputWrapper.getOriginalEvent().getDeliveryPriority();
		this.eventDate = subscriberStateOutputWrapper.getEvolutionEventContext().eventDate();
		this.eventID = subscriberStateOutputWrapper.getEvolutionEventContext().getEventID();
	}

	// build alternateIDs populated
	public static Map<String,String> buildAlternateIDs(SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader) {
		Map<String,String> alternateIDs = new LinkedHashMap<>();
		SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, SystemTime.getCurrentTime(), subscriberProfile.getTenantID());

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

	public static Map<String,String> packAlternateIDs(Map<AlternateID,String> alternateIDs){
		Map<String,String> toRet = new HashMap<>();
		for(Map.Entry<AlternateID,String> entry:alternateIDs.entrySet()) toRet.put(entry.getKey().getID(),entry.getValue());
		return toRet;
	}
	public static Map<AlternateID,String> unpackAlternateIDs(Map<String,String> packedAlternateIDs){
		Map<AlternateID,String> toRet = new HashMap<>();
		for(Map.Entry<String,String> entry:packedAlternateIDs.entrySet()){
			AlternateID alternateID = DeploymentCommon.getAlternateIDOrNull(entry.getKey());
			if(alternateID==null) continue;
			toRet.put(alternateID,entry.getValue());
		}
		return toRet;
	}

}
