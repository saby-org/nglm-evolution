package com.evolving.nglm.evolution.event;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.DeploymentCommon;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.evolution.DeliveryRequest;
import com.evolving.nglm.evolution.EvolutionEngineEvent;
import com.evolving.nglm.evolution.EvolutionEngineEventDeclaration;
import com.evolving.nglm.evolution.SupportedRelationship;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class ExternalEvent implements EvolutionEngineEvent {

	private static final Logger log = LoggerFactory.getLogger(ExternalEvent.class);

	// the data real sub-schema
	private final static Schema productFieldSchema;
	static{
		SchemaBuilder schemaBuilder = SchemaBuilder.struct();
		schemaBuilder.name("evolution_event_fields");
		schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
		schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
		schemaBuilder.field("tenantID", Schema.OPTIONAL_INT16_SCHEMA);
		schemaBuilder.field("action", Schema.OPTIONAL_STRING_SCHEMA);
		schemaBuilder.field("eventDate", Schema.OPTIONAL_INT64_SCHEMA);
		schemaBuilder.field("eventID", Schema.OPTIONAL_STRING_SCHEMA);
		schemaBuilder.field("priority", Schema.OPTIONAL_STRING_SCHEMA);
		schemaBuilder.field("alternateIDs", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).optional().schema());
		schemaBuilder.field("addParents", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).optional().schema());
		schemaBuilder.field("removeParents", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).optional().schema());
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
		if(!externalEvent.getParentToAddSubscriberIDs().isEmpty()) struct.put("addParents", externalEvent.getParentToAddSubscriberIDs());
		if(!externalEvent.getParentToRemoveSubscriberIDs().isEmpty()) struct.put("removeParents", externalEvent.getParentToRemoveSubscriberIDs());
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
			this.parentToAddSubscriberIDs = (productFieldSchema.field("addParents")!=null && valueStruct.get("addParents") != null) ? (Map<String,Long>) valueStruct.get("addParents") : new HashMap<>();
			this.parentToRemoveSubscriberIDs = (productFieldSchema.field("removeParents")!=null && valueStruct.get("removeParents") != null) ? (Map<String,Long>) valueStruct.get("removeParents") : new HashMap<>();
		}
	}

	// the common schema builder to be used by sub-classes, we put all in a sub schema field, to avoid any overlap
	// can not be static singleton as will be called by several sub-classes for different final schema
	public static SchemaBuilder struct(){
		SchemaBuilder externalEventSchemaBuilder = SchemaBuilder.struct();
		externalEventSchemaBuilder.field("evolution_event_fields", productFieldSchema).optional();
		return externalEventSchemaBuilder;
	}
	// the common pack to be used by sub-classes
	public static void packExternalEvent(Struct struct, ExternalEvent externalEvent) {
		if(externalEvent.getSubscriberID()!=null) struct.put("evolution_event_fields", packProductFields(externalEvent));
	}

	// packed
	private Long subscriberID;// TODO can we not pack it, this is a duplication of message key
	private Integer tenantID;
	private SubscriberStreamEvent.SubscriberAction subscriberAction = SubscriberAction.Standard;
	private Date eventDate;
	private String eventID;
	private DeliveryRequest.DeliveryPriority priority = DeliveryRequest.DeliveryPriority.Standard;
	private Map<AlternateID,String> alternateIDsCreated = new HashMap<>();
	private Map<String/*relationshipID*/,Long> parentToAddSubscriberIDs = new HashMap<>();
	private Map<String/*relationshipID*/,Long> parentToRemoveSubscriberIDs = new HashMap<>();
	// not packed
	private Pair<AlternateID, String> alternateID;
	private List<Long> resolvedSubscriberIDs = new ArrayList<>();
	private Long subscriberIDToAssign;
	private HashMap<String/*relationshipID*/,Pair<AlternateID,String>> parentsToAdd = new HashMap<>();
	private HashMap<String/*relationshipID*/,Pair<AlternateID,String>> parentsToRemove = new HashMap<>();
	private String oldAlternateIDValueToSwapWith;
	private Map<AlternateID, String> otherAlternateIDsToAdd = new HashMap<>();
	private Map<AlternateID, String> otherAlternateIDsToRemove = new HashMap<>();

	// only 1 available constructor, from an alternateID value
	public ExternalEvent(AlternateID alternateID, String alternateIDValue){
		this.alternateID = new Pair<>(alternateID,alternateIDValue);
	}
	// and a protected one for internal use
	protected ExternalEvent(String subscriberID){
		this.subscriberID = Long.valueOf(subscriberID);
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

	public Map<String,Long> getParentToAddSubscriberIDs(){return parentToAddSubscriberIDs;}
	public Map<String,Long> getParentToRemoveSubscriberIDs(){return parentToRemoveSubscriberIDs;}
	// same derived
	public Map<SupportedRelationship,Long> getParentRelationshipToAddSubscriberIDs(){ return getTranslatedRelationships(getParentToAddSubscriberIDs());}
	public Map<SupportedRelationship,Long> getParentRelationshipToRemoveSubscriberIDs(){return getTranslatedRelationships(getParentToRemoveSubscriberIDs());}
	protected  <T> Map<SupportedRelationship,T> getTranslatedRelationships(Map<String,T> toTranslate){
		Map<SupportedRelationship,T> toRet = new HashMap<>();
		for(Map.Entry<String,T> entry:toTranslate.entrySet()){
			SupportedRelationship supportedRelationship = DeploymentCommon.getSupportedRelationships().get(entry.getKey());
			if(supportedRelationship==null){
				log.info(getEventName()+" got unsupported relationship id "+entry.getKey());
				continue;
			}
			toRet.put(supportedRelationship,entry.getValue());
		}
		return toRet;
	}


	// internal use
	protected Pair<AlternateID, String> getAlternateID() {return this.alternateID;}
	protected Long getSubscriberIDToAssign() {return this.subscriberIDToAssign;}
	protected String getOldAlternateIDValueToSwapWith() {return this.oldAlternateIDValueToSwapWith;}
	protected List<Pair<AlternateID,String>> getParentToResolved(){
		List<Pair<AlternateID,String>> toRet = new ArrayList<>(parentsToAdd.values());
		toRet.addAll(parentsToRemove.values());
		return toRet;
	}
	protected Map<AlternateID, String> getOtherAlternateIDsToAdd(){return this.otherAlternateIDsToAdd;}
	protected Map<AlternateID, String> getOtherAlternateIDsToRemove(){return this.otherAlternateIDsToRemove;}
	// called when parent is resolved
	protected void parentResolved(AlternateID alternateID, String alternateIDValue, Long parentSubscriberID){
		parentResolved(parentsToAdd.entrySet().iterator(),parentToAddSubscriberIDs,alternateID,alternateIDValue,parentSubscriberID);
		parentResolved(parentsToRemove.entrySet().iterator(),parentToRemoveSubscriberIDs,alternateID,alternateIDValue,parentSubscriberID);
	}
	private void parentResolved(Iterator<Map.Entry<String,Pair<AlternateID,String>>> iterator, Map<String,Long> resolved, AlternateID alternateID, String alternateIDValue, Long subscriberID){
		while(iterator.hasNext()){
			Map.Entry<String,Pair<AlternateID,String>> next = iterator.next();
			if(next.getValue().getFirstElement()==alternateID && next.getValue().getSecondElement().equals(alternateIDValue)){
				if(subscriberID!=null) resolved.put(next.getKey(),subscriberID);
				iterator.remove();
			}
		}
	}
	// called for subscriber swap
	protected void setMappingCreationSwapWith(Pair<Integer,List<Long>> oldEntry){
		if(oldEntry.getSecondElement().size()>1) throw new RuntimeException(getSubscriberForLogging()+" trying to swap with shared alternateID "+oldEntry.getSecondElement());
		if(this.tenantID!=null && this.tenantID!=oldEntry.getFirstElement()) log.warn(getEventName()+" swapping "+this.oldAlternateIDValueToSwapWith+" was register on tenant "+oldEntry.getFirstElement()+" while new was asked to be created if not exist on tenant "+this.tenantID+", we keep using old tenant value");
		this.subscriberIDToAssign = oldEntry.getSecondElement().get(0);
		this.tenantID = oldEntry.getFirstElement();
	}

	// called when we resolve from redis SubscriberID
	protected void resolve(Pair<Integer,List<Long>> tenantIDSubscriberIDs){
		this.resolvedSubscriberIDs = tenantIDSubscriberIDs.getSecondElement();
		if(resolvedSubscriberIDs!=null && !this.resolvedSubscriberIDs.isEmpty()) this.subscriberID = this.resolvedSubscriberIDs.get(0);//probably hacky, assigned one to be sure considered as resolved
		this.tenantID = tenantIDSubscriberIDs.getFirstElement();
	}
	// called when we choose internally a subscriberID
	protected void setSubscriberIDToAssign(Long subscriberIDToAssign){this.subscriberIDToAssign=subscriberIDToAssign;}
	// called when it is created to redis
	protected void createdOnRedis(AlternateID alternateID, String alternateIDValue){
		if(alternateIDsCreated==null) alternateIDsCreated = new HashMap<>();
		alternateIDsCreated.put(alternateID,alternateIDValue);
		this.subscriberID=this.subscriberIDToAssign;
	}
	protected void updatedOnRedisSecondary(AlternateID alternateID){
		if(this.alternateIDsCreated==null) this.alternateIDsCreated = new HashMap<>();
		if(this.alternateIDsCreated.get(alternateID)==null && this.otherAlternateIDsToRemove.get(alternateID)!=null) this.alternateIDsCreated.put(alternateID,null);
		if(this.otherAlternateIDsToAdd.get(alternateID)!=null) this.alternateIDsCreated.put(alternateID,this.otherAlternateIDsToAdd.get(alternateID));
	}

	// optional settings

	public ExternalEvent createIfDoesNotExist(int tenantID){
		if(this.alternateID!=null && this.alternateID.getFirstElement().getSharedID()){
			log.info(getEventName()+" "+getSubscriberForLogging()+" can not create on sharedID, will ignore if not existing");
			return this;
		}
		this.tenantID = tenantID;
		this.subscriberAction = SubscriberAction.Create;
		return this;
	}
	public ExternalEvent createIfDoesNotExistWithForcedSubscriberID(int tenantID, Long subscriberID){
		if(this.alternateID.getFirstElement().getSharedID()){
			log.info(getEventName()+" "+getSubscriberForLogging()+" can not create on sharedID, will ignore if not existing");
			return this;
		}
		this.subscriberIDToAssign = subscriberID;
		return createIfDoesNotExist(tenantID);
	}
	public ExternalEvent delete(){
		if(this.alternateID!=null && this.alternateID.getFirstElement().getSharedID()){
			log.info(getEventName()+" "+getSubscriberForLogging()+" can not delete on sharedID, will ignore if not existing");
			return this;
		}
		if(this.subscriberAction!=null) log.info(getEventName()+" delete called for "+getSubscriberForLogging()+" while "+this.subscriberAction.getExternalRepresentation()+" asked before");
		this.subscriberAction = SubscriberAction.Cleanup;
		return this;
	}
	public ExternalEvent deleteImmediate(){
		if(this.alternateID!=null && this.alternateID.getFirstElement().getSharedID()){
			log.info(getEventName()+" "+getSubscriberForLogging()+" can not delete on sharedID, will ignore if not existing");
			return this;
		}
		if(this.subscriberAction!=null) log.info(getEventName()+" deleteImmediate called for "+getSubscriberForLogging()+" while "+this.subscriberAction.getExternalRepresentation()+" asked before");
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
	public ExternalEvent addParent(SupportedRelationship relationship, AlternateID parentAlternateID, String parentAlternateIDValue){
		if(this.alternateID!=null && this.alternateID.getFirstElement().getSharedID() || parentAlternateID.getSharedID()){
			log.info(getEventName()+" "+getSubscriberForLogging()+" can not addParent on sharedID, will ignore if not existing");
			return this;
		}
		if(this.parentsToRemove.get(relationship.getID())!=null){
			log.info(getEventName()+" addParent "+relationship.getDisplay()+" called for "+getSubscriberForLogging()+" while removeParent asked before, removeParent will be ignored");
			this.parentsToRemove.remove(relationship.getID());
		}
		if(this.parentsToAdd.get(relationship.getID())!=null) log.info(getEventName()+" addParent "+relationship.getDisplay()+" called for "+getSubscriberForLogging()+" while addParent asked before, previous will be ignored");
		this.parentsToAdd.put(relationship.getID(),new Pair<>(parentAlternateID,parentAlternateIDValue));
		return this;
	}
	public ExternalEvent removeParent(SupportedRelationship relationship, AlternateID parentAlternateID, String parentAlternateIDValue){
		if(this.alternateID!=null && this.alternateID.getFirstElement().getSharedID() || parentAlternateID.getSharedID()){
			log.info(getEventName()+" "+getSubscriberForLogging()+" can not removeParent on sharedID, will ignore if not existing");
			return this;
		}
		if(this.parentsToAdd.get(relationship.getID())!=null){
			log.info(getEventName()+" removeParent "+relationship.getDisplay()+" called for "+getSubscriberForLogging()+" while addParent asked before, this is ignored");
			return this;
		}
		if(this.parentsToAdd.get(relationship.getID())!=null) log.info(getEventName()+" removeParent "+relationship.getDisplay()+" called for "+getSubscriberForLogging()+" while removeParent asked before, previous will be ignored");
		this.parentsToRemove.put(relationship.getID(),new Pair<>(parentAlternateID,parentAlternateIDValue));
		return this;
	}
	public ExternalEvent swapWithExistingAlternateID(String existingAlternateIDValue){
		if(this.alternateID.getFirstElement().getSharedID()){
			log.info(getEventName()+" "+getSubscriberForLogging()+" can not swap alternateID on sharedID, will ignore if not existing");
			return this;
		}
		if(this.alternateID.getSecondElement().equals(existingAlternateIDValue)){
			log.info(getEventName()+" swapWithExistingAlternateID called for with old/new value equals, "+existingAlternateIDValue+"/"+this.alternateID.getSecondElement()+", ignoring");
			return this;
		}
		if(Delete_actions.contains(this.subscriberAction)){
			log.info(getEventName()+" swapWithExistingAlternateID called while deletion asked for "+getSubscriberForLogging()+", ignoring");
			return this;
		}
		this.oldAlternateIDValueToSwapWith=existingAlternateIDValue;
		return this;
	}
	public ExternalEvent addOtherAlternateID(AlternateID alternateID, String alternateIDValue){
		if(this.alternateID!=null && this.alternateID.getFirstElement().getSharedID()){
			log.info(getEventName()+" "+getSubscriberForLogging()+" can not addOtherAlternateID on sharedID, will ignore if not existing");
			return this;
		}
		if(this.alternateID!=null && alternateID.equals(this.alternateID.getFirstElement())){
			log.info(getEventName()+" addOtherAlternateID called for same AlternateID than main "+getSubscriberForLogging()+", ignoring");
			return this;
		}
		if(alternateIDValue==null){
			log.info(getEventName()+" "+getSubscriberForLogging()+" addOtherAlternateID called with value "+alternateIDValue+", ignoring");
			return this;
		}
		String alreadyCalledFor = this.otherAlternateIDsToAdd.get(alternateID);
		if(alreadyCalledFor!=null) log.info(getEventName()+" addOtherAlternateID called for "+alternateID.getDisplay()+", new "+alternateIDValue+" will override previous call "+alreadyCalledFor);
		this.otherAlternateIDsToAdd.put(alternateID,alternateIDValue);
		return this;
	}
	public ExternalEvent removeOtherAlternateID(AlternateID alternateID, String alternateIDValue){
		if(this.alternateID!=null && this.alternateID.getFirstElement().getSharedID()){
			log.info(getEventName()+" "+getSubscriberForLogging()+" can not removeOtherAlternateID on sharedID, will ignore if not existing");
			return this;
		}
		if(this.alternateID!=null && alternateID.equals(this.alternateID.getFirstElement())){
			log.info(getEventName()+" removeOtherAlternateID called for same AlternateID than main "+getSubscriberForLogging()+", ignoring");
			return this;
		}
		if(alternateIDValue==null){
			log.info(getEventName()+" "+getSubscriberForLogging()+" removeOtherAlternateID called with value "+alternateIDValue+", ignoring");
			return this;
		}
		String alreadyCalledFor = this.otherAlternateIDsToRemove.get(alternateID);
		if(alreadyCalledFor!=null) log.info(getEventName()+" removeOtherAlternateID called for "+alternateID.getDisplay()+", new "+alternateIDValue+" will override previous call "+alreadyCalledFor);
		this.otherAlternateIDsToRemove.put(alternateID,alternateIDValue);
		return this;
	}



	// transform to SourceRecord for connect
	public List<SourceRecord> toSourceRecords(){
		if(this.resolvedSubscriberIDs.isEmpty()) return Collections.singletonList(toSourceRecord());
		// if several subscriberIDs resolved we send 1 event for each
		List<SourceRecord> toRet = new ArrayList<>(this.resolvedSubscriberIDs.size());
		for(Long subscriberID:this.resolvedSubscriberIDs){
			this.subscriberID=subscriberID;
			toRet.add(this.toSourceRecord());
		}
		return toRet;
	}
	private SourceRecord toSourceRecord(){
		EvolutionEngineEventDeclaration engineEventDeclaration = DeploymentCommon.getEvolutionEngineEventDeclaration(this);
		if(log.isTraceEnabled()) log.trace(getEventName()+" packing "+this.getClass().getSimpleName()+ " with schema fields "+engineEventDeclaration.getEventSerde().schema().fields());
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
		if(this.alternateID!=null) return this.alternateID.getFirstElement().getDisplay()+" "+this.alternateID.getSecondElement();
		return "subscriberID "+this.subscriberID;
	}

}
