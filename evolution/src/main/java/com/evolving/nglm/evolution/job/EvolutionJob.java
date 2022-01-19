package com.evolving.nglm.evolution.job;


import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class EvolutionJob {

	private static final Logger log = LoggerFactory.getLogger(EvolutionJob.class);

	public enum Name{
		pushAlternateIDsToRedis("pushAlternateIDsToRedis"),
		triggerMicroscope("triggerMicroscope"),
		unknown("unknown");
		private String externalRepresentation;
		Name(String externalRepresentation){this.externalRepresentation=externalRepresentation;}
		public String getExternalRepresentation(){return this.externalRepresentation;}
		public static Name fromExternalRepresentation(String externalRepresentation){for(Name name:Name.values()){if(name.getExternalRepresentation().equals(externalRepresentation))return name;}return unknown;}
	}


	public enum Status{
		created("created"),
		pending("pending"),
		inProgress("inProgress"),
		rebalanced("rebalanced"),
		processed("processed"),
		failed("failed");
		private String externalRepresentation;
		Status(String externalRepresentation){this.externalRepresentation=externalRepresentation;}
		public String getExternalRepresentation(){return this.externalRepresentation;}
		public static Status fromExternalRepresentation(String externalRepresentation){for(Status status:Status.values()){if(status.getExternalRepresentation().equals(externalRepresentation))return status;}return failed;}

		public static EnumSet<Status> terminatedStatuses = EnumSet.of(processed,failed);
	}

	private static final Schema schema;
	static {
		SchemaBuilder schemaBuilder = SchemaBuilder.struct();
		schemaBuilder.name("evolution_job");
		schemaBuilder.version(SchemaUtilities.packSchemaVersion(0));
		schemaBuilder.field("name", Schema.STRING_SCHEMA);
		schemaBuilder.field("status", Schema.STRING_SCHEMA);
		schemaBuilder.field("processID", Schema.STRING_SCHEMA);
		schemaBuilder.field("creationTime", Schema.INT64_SCHEMA);
		schemaBuilder.field("updateTime", Schema.INT64_SCHEMA);
		schemaBuilder.field("tenantID", Schema.INT16_SCHEMA);
		schemaBuilder.field("totalParts", Schema.OPTIONAL_INT32_SCHEMA);
		schemaBuilder.field("workerParts",SchemaBuilder.map(Schema.INT32_SCHEMA,Schema.STRING_SCHEMA).optional());
		schema = schemaBuilder.build();
	}
	private static Object pack(Object value){
		EvolutionJob job = (EvolutionJob) value;
		Struct struct = new Struct(schema);
		struct.put("name", job.getName().getExternalRepresentation());
		struct.put("status", job.getStatus().getExternalRepresentation());
		struct.put("processID", job.getProcessID());
		struct.put("creationTime", job.getCreationTime().getTime());
		struct.put("updateTime", job.getUpdateTime().getTime());
		struct.put("tenantID",job.getTenantID().shortValue());
		if(job.getTotalParts()!=null) struct.put("totalParts", job.getTotalParts());
		if(!job.getWorkerParts().isEmpty()) struct.put("workerParts", job.packedWorkerParts());
		return struct;
	}
	private static EvolutionJob unpack(SchemaAndValue schemaAndValue){return new EvolutionJob(schemaAndValue);}
	private EvolutionJob(SchemaAndValue schemaAndValue){
		Schema schema = schemaAndValue.schema();
		Struct value = (Struct) schemaAndValue.value();
		Integer version = SchemaUtilities.unpackSchemaVersion0(schema.version());
		this.name = Name.fromExternalRepresentation(value.getString("name"));
		this.status = Status.fromExternalRepresentation(value.getString("status"));
		this.processID = value.getString("processID");
		this.creationTime = new Date(value.getInt64("creationTime"));
		this.updateTime = new Date(value.getInt64("updateTime"));
		this.tenantID = value.getInt16("tenantID").intValue();
		if(schema.field("totalParts")!=null && value.get("totalParts")!=null) this.totalParts = value.getInt32("totalParts");
		if(schema.field("workerParts")!=null && value.get("workerParts")!=null) unpackWorkerParts(value.getMap("workerParts"));
	}
	private static final ConnectSerde<EvolutionJob> serde = new ConnectSerde<>(schema,false,EvolutionJob.class,EvolutionJob::pack,EvolutionJob::unpack);
	protected static ConnectSerde<EvolutionJob> serde(){return serde;}

	private Name name;
	private Status status;
	private String processID;
	private Date creationTime;
	private Date updateTime;
	private Integer tenantID;
	private Integer totalParts;
	private Map<Integer,Status> workerParts = new HashMap<>();
	private String jobID;

	private EvolutionJob(Name name, String processID, Integer tenantID){
		this.name = name;
		this.status = Status.created;
		this.processID = processID;
		this.creationTime = new Date();
		this.updateTime = creationTime;
		this.tenantID = tenantID;
	}

	protected EvolutionJob(EvolutionJob job){
		this.name = job.getName();
		this.status = job.getStatus();
		this.processID = job.getProcessID();
		this.creationTime = job.getCreationTime();
		this.updateTime = job.getUpdateTime();
		this.tenantID = job.getTenantID();
		this.workerParts.putAll(job.getWorkerParts());
		this.totalParts = job.getTotalParts();
		this.jobID = job.getJobID();
	}

	protected EvolutionJob(String processID, Integer tenantID, JSONObject json) throws GUIManager.GUIManagerException{
		this(null,processID,tenantID);
		try{
			String nameString = JSONUtilities.decodeString(json,"name",true);
			this.name = Name.fromExternalRepresentation(nameString);
			if(this.name==Name.unknown) throw new GUIManager.GUIManagerException("unknown job",nameString);
		}catch (JSONUtilities.JSONUtilitiesException ex){
			throw new GUIManager.GUIManagerException(ex);
		}
		this.jobID = processID+"-"+RandomHolder.getRandomID()+"-"+(new Date().getTime());
	}
	private static class RandomHolder{
		private static Random random = new Random();
		private static Integer getRandomID(){return random.nextInt(999999);}
	}

	protected Name getName(){return name;}
	protected synchronized String getProcessID(){return processID;}
	protected synchronized Status getStatus(){return status;}
	protected Date getCreationTime(){return creationTime;}
	protected Date getUpdateTime(){return updateTime;}
	protected Integer getTenantID(){return tenantID;}
	protected synchronized Integer getTotalParts(){return totalParts;}
	protected synchronized Map<Integer,Status> getWorkerParts(){return workerParts;}
	protected String getJobID(){return jobID;}

	protected synchronized void setProcessID(String processID){this.processID=processID;}
	protected synchronized void setStatus(Status status){this.status=status;}
	protected void setJobID(String jobID){this.jobID=jobID;}
	protected void setUpdateTime(Date date){this.updateTime=date;}
	protected synchronized void setTotalParts(int totalParts){this.totalParts=totalParts;}
	protected synchronized void setWorkerParts(Map<Integer,Status> workerParts){this.workerParts = workerParts;}

	private Map<Integer,String> packedWorkerParts(){
		Map<Integer,String> packedMap = new HashMap<>();
		getWorkerParts().forEach((key,value)->packedMap.put(key,value.getExternalRepresentation()));
		return packedMap;
	}
	private synchronized void unpackWorkerParts(Map<Integer,String> packedMap){
		packedMap.forEach((key,value)->this.workerParts.put(key,Status.fromExternalRepresentation(value)));
	}

	protected synchronized void setPartStatus(Integer part,Status status){
		workerParts.put(part,status);
		computeStatus();
	}

	protected void computeStatus(){
		if(getWorkerParts().isEmpty()) return;//not job with part, nothing to do
		boolean allOK = true;
		boolean pending=false;
		for(Status partStatus:getWorkerParts().values()){
			if (partStatus==Status.inProgress){
				setStatus(Status.inProgress);
				return;
			}
			if(partStatus==Status.pending) pending=true;
			if(partStatus==Status.failed) allOK=false;
		}
		if(pending){
			setStatus(Status.pending);
			return;
		}
		if(getTotalParts()!=getWorkerParts().size()) return;//not yet finished
		if(!allOK){
			setStatus(Status.failed);
			return;
		}
		setStatus(Status.processed);
	}

	protected JSONObject toJSON(){
		JSONObject json = new JSONObject();
		json.put("id",this.getJobID());
		json.put("name",this.getName().getExternalRepresentation());
		json.put("status",this.getStatus().externalRepresentation);
		return json;
	}
	protected JSONArray getWorkerPartsJSON(){
		JSONArray workerParts = new JSONArray();
		for(Map.Entry<Integer,Status> entry:getWorkerParts().entrySet()){
			JSONObject part = new JSONObject();
			part.put("part",entry.getKey());
			part.put("status",entry.getValue().getExternalRepresentation());
			workerParts.add(part);
		}
		return workerParts;
	}

	@Override
	public String toString() {
		return "EvolutionJob{" +
					   "name=" + this.getName() +
					   ", status=" + this.getStatus() +
					   ", processID=" + this.getProcessID() +
					   ", creationTime=" + this.getCreationTime() +
					   ", updateTime=" + this.getUpdateTime() +
					   ", tenantID=" + this.getTenantID() +
					   ", totalParts=" + this.getTotalParts() +
					   ", workerParts=" + this.getWorkerParts() +
					   ", jobID=" + this.getJobID() +
					   '}';
	}
}
