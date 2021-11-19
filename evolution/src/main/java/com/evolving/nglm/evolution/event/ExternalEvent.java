package com.evolving.nglm.evolution.event;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.DeploymentCommon;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.evolution.DeliveryRequest;
import com.evolving.nglm.evolution.EvolutionEngineEvent;
import com.evolving.nglm.evolution.EvolutionEngineEventDeclaration;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public abstract class ExternalEvent implements EvolutionEngineEvent {

	private static final Logger log = LoggerFactory.getLogger(ExternalEvent.class);

	// the data real sub-schema
	private final static Schema productFieldSchema;
	static{
		SchemaBuilder schemaBuilder = SchemaBuilder.struct();
		schemaBuilder.name("evolution_event_fields");
		schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
		schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
		schemaBuilder.field("tenantID", Schema.OPTIONAL_INT16_SCHEMA);
		schemaBuilder.field("action", Schema.OPTIONAL_STRING_SCHEMA);
		schemaBuilder.field("eventDate", Schema.OPTIONAL_INT64_SCHEMA);
		schemaBuilder.field("eventID", Schema.OPTIONAL_STRING_SCHEMA);
		schemaBuilder.field("priority", Schema.OPTIONAL_STRING_SCHEMA);
		schemaBuilder.field("alternateIDs", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).optional().schema());
		productFieldSchema = schemaBuilder.build();
	}
	public static Object packProductFields(ExternalEvent externalEvent) {
		Struct struct = new Struct(productFieldSchema);
		struct.put("subscriberID", externalEvent.getSubscriberID());
		if(externalEvent.getTenantID()!=null) struct.put("tenantID", externalEvent.getTenantID().shortValue());
		if(externalEvent.getSubscriberAction()!=null) struct.put("action", externalEvent.getSubscriberAction().getExternalRepresentation());
		if(externalEvent.getEventDate()!=null) struct.put("eventDate", externalEvent.getEventDate().getTime());
		if(externalEvent.getEventID()!=null) struct.put("eventID", externalEvent.getEventID());
		if(externalEvent.getDeliveryPriority()!=null) struct.put("priority", externalEvent.getDeliveryPriority().getExternalRepresentation());
		Map<String,String> packedAlternateIDs = externalEvent.getForPackAlternateIDsCreated();
		if(packedAlternateIDs!=null && !packedAlternateIDs.isEmpty()) struct.put("alternateIDs", packedAlternateIDs);
		return struct;
	}
	// constructor forcing sub-class while unpack
	public ExternalEvent(SchemaAndValue schemaAndValue){
		if(schemaAndValue.schema().field("evolution_event_fields")!=null){
			Struct productFieldsStruct = ((Struct)schemaAndValue.value()).getStruct("evolution_event_fields");
			Schema productFieldSchema = productFieldsStruct.schema();
			Integer productFieldSchemaVersion = SchemaUtilities.unpackSchemaVersion0(productFieldSchema.version());

			Struct valueStruct = productFieldsStruct;
			this.subscriberID = Long.valueOf(valueStruct.getString("subscriberID"));
			Short packedTenantID = productFieldSchema.field("tenantID") != null ? valueStruct.getInt16("tenantID") : null;
			this.tenantID = packedTenantID != null ? packedTenantID.intValue() : null ;
			String packedSubscriberAction = productFieldSchema.field("action") != null ? valueStruct.getString("action") : null;
			this.subscriberAction = packedSubscriberAction != null ? SubscriberStreamEvent.SubscriberAction.fromExternalRepresentation(packedSubscriberAction) : SubscriberStreamEvent.SubscriberAction.Standard;
			Long packedEventDate = productFieldSchema.field("eventDate") != null ? valueStruct.getInt64("eventDate") : null;
			this.eventDate = packedEventDate != null ? new Date(packedEventDate) : new Date();
			this.eventID = productFieldSchema.field("eventID") != null ? valueStruct.getString("eventID") : null;
			String packedPriority = productFieldSchema.field("priority") != null ? valueStruct.getString("priority") : null;
			this.priority = packedPriority != null ? DeliveryRequest.DeliveryPriority.fromExternalRepresentation(packedPriority) : DeliveryRequest.DeliveryPriority.Standard;
			Map<String,String> packedAlternateIDs = (productFieldSchema.field("alternateIDs")!=null && valueStruct.get("alternateIDs") != null) ? (Map<String,String>) valueStruct.get("alternateIDs") : new HashMap<>();
			this.alternateIDsCreated = new HashMap<>();
			for(Map.Entry<String,String> entry:packedAlternateIDs.entrySet()) this.alternateIDsCreated.put(DeploymentCommon.getAlternateID(entry.getKey()),entry.getValue());
		}
	}

	// the common schema builder to be used by sub-classes, we put all in a sub schema field, to avoid any overlap
	private static final SchemaBuilder externalEventSchemaBuilder;
	static {
		externalEventSchemaBuilder = SchemaBuilder.struct();
		externalEventSchemaBuilder.field("evolution_event_fields", productFieldSchema).optional();
	}
	public static SchemaBuilder struct(){return externalEventSchemaBuilder;}
	// the common pack to be used by sub-classes
	public static void packExternalEvent(Struct struct, ExternalEvent externalEvent) {
		if(externalEvent.getSubscriberID()!=null) struct.put("evolution_event_fields", packProductFields(externalEvent));
	}

	// packed
	private Long subscriberID;// TODO can we not pack it, this is a duplication of message key
	private Integer tenantID;
	private SubscriberStreamEvent.SubscriberAction subscriberAction;
	private Date eventDate;
	private String eventID;
	private DeliveryRequest.DeliveryPriority priority;
	private Map<AlternateID,String> alternateIDsCreated;
	// not packed
	private Pair<AlternateID, String> alternateID;
	private Long subscriberIDToAssign;

	// only 1 available constructor, from an alternateID value
	public ExternalEvent(AlternateID alternateID, String alternateIDValue){
		this.alternateID = new Pair<>(alternateID,alternateIDValue);
	}

	public Long getSubscriberIDLong(){return this.subscriberID;}
	@Override public String getSubscriberID() {return this.subscriberID+"";}
	@Override public Schema subscriberStreamEventSchema() {return DeploymentCommon.getEvolutionEngineEventDeclaration(this).getEventSerde().schema();}
	@Override public Object subscriberStreamEventPack(Object value) {return DeploymentCommon.getEvolutionEngineEventDeclaration(this).getEventSerde().pack(this);}
	@Override public String getEventName() {return DeploymentCommon.getEvolutionEngineEventDeclaration(this).getName();}
	@Override public SubscriberAction getSubscriberAction() {return this.subscriberAction;}
	@Override public Date getEventDate() {return this.eventDate;}
	@Override public String getEventID() {return this.eventID;}
	@Override public DeliveryRequest.DeliveryPriority getDeliveryPriority() {return this.priority;}
	public Integer getTenantID() {return this.tenantID;}
	public Map<AlternateID,String> getAlternateIDsCreated(){return this.alternateIDsCreated;}
	// need string/string for the pack method
	private Map<String,String> getForPackAlternateIDsCreated() {
		Map<String,String> result = new HashMap<>();
		if(getAlternateIDsCreated()!=null && !getAlternateIDsCreated().isEmpty()){
			for(Map.Entry<AlternateID,String> entry:getAlternateIDsCreated().entrySet()){
				result.put(entry.getKey().getID(),entry.getValue());
			}
		}
		// to be safe we always add the main one as well for the subriberProfile update if it failed on creation
		if(getAlternateID()!=null){
			result.put(getAlternateID().getFirstElement().getID(),getAlternateID().getSecondElement());
		}
		return result;
	}

	// internal use
	protected Pair<AlternateID, String> getAlternateID() {return this.alternateID;}
	protected Long getSubscriberIDToAssign() {return this.subscriberIDToAssign;}

	// called when we resolve from redis SubscriberID
	protected void resolve(Pair<Long,Integer> subscriberIDTenantID){
		this.subscriberID = subscriberIDTenantID.getFirstElement();
		this.tenantID = subscriberIDTenantID.getSecondElement();
	}
	// called when we choose internally a subscriberID
	protected void setSubscriberIDToAssign(Long subscriberIDToAssign){this.subscriberIDToAssign=subscriberIDToAssign;}
	// called when it is created to redis
	protected void createdOnRedis(AlternateID alternateID, String alternateIDValue){
		if(alternateIDsCreated==null) alternateIDsCreated = new HashMap<>();
		alternateIDsCreated.put(alternateID,alternateIDValue);
		this.subscriberID=this.subscriberIDToAssign;
	}

	// optional settings

	public ExternalEvent createIfDoesNotExist(int tenantID){
		this.tenantID = tenantID;
		this.subscriberAction = SubscriberAction.Create;
		return this;
	}
	public ExternalEvent createIfDoesNotExistWithForcedSubscriberID(int tenantID, Long subscriberID){
		this.subscriberIDToAssign = subscriberID;
		return createIfDoesNotExist(tenantID);
	}
	public ExternalEvent delete(){
		if(this.subscriberAction!=null) log.info("delete called for "+getSubscriberForLogging()+" while "+this.subscriberAction.getExternalRepresentation()+" asked before");
		this.subscriberAction = SubscriberAction.Cleanup;
		return this;
	}
	public ExternalEvent deleteImmediate(){
		if(this.subscriberAction!=null) log.info("deleteImmediate called for "+getSubscriberForLogging()+" while "+this.subscriberAction.getExternalRepresentation()+" asked before");
		this.subscriberAction = SubscriberAction.CleanupImmediate;
		return this;
	}
	public ExternalEvent priority(DeliveryRequest.DeliveryPriority priority){
		this.priority = priority;
		return this;
	}
	public ExternalEvent eventDate(Date eventDate){
		this.eventDate = eventDate;
		return this;
	}
	public ExternalEvent eventID(String eventID){
		this.eventID = eventID;
		return this;
	}

	// transform to SourceRecord for connect
	public SourceRecord toSourceRecord(){
		EvolutionEngineEventDeclaration engineEventDeclaration = DeploymentCommon.getEvolutionEngineEventDeclaration(this);
		if(log.isTraceEnabled()) log.trace("packing "+this.getClass().getSimpleName()+ " with schema fields "+engineEventDeclaration.getEventSerde().schema().fields());
		return new SourceRecord(null,null,
				engineEventDeclaration.getEventTopic(),
				Schema.STRING_SCHEMA, getSubscriberID(),
				engineEventDeclaration.getEventSerde().schema(), engineEventDeclaration.getEventSerde().pack(this)
		);
	}

	@Override public String toString(){
		return "["+getEventName()+", "+getSubscriberForLogging()+"]";
	}
	protected String getSubscriberForLogging(){
		if(this.alternateID!=null) return this.alternateID.getFirstElement().getID()+" "+this.alternateID.getSecondElement();
		return "subscriberID "+this.subscriberID;
	}

}
