package com.evolving.nglm.evolution.redis;

import com.evolving.nglm.core.AlternateID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateAlternateIDSubscriberIDs {

	private final static Logger log = LoggerFactory.getLogger(UpdateAlternateIDSubscriberIDs.class);

	private AlternateID alternateIDConf;
	private String alternateIDValue;
	private Integer tenantID;
	private Long subscriberIDToAdd;
	private Long subscriberIDToRemove;

	public AlternateID getAlternateIDConf() {return alternateIDConf;}
	public String getAlternateIDValue() {return alternateIDValue;}
	public Integer getTenantID(){return tenantID;}
	public Long getSubscriberIDToAdd() {return subscriberIDToAdd;}
	public Long getSubscriberIDToRemove() {return subscriberIDToRemove;}

	public UpdateAlternateIDSubscriberIDs(AlternateID alternateIDConf, String alternateIDValue, Integer tenantID){
		this.alternateIDConf = alternateIDConf;
		this.alternateIDValue = alternateIDValue;
		this.tenantID=tenantID;
	}

	public UpdateAlternateIDSubscriberIDs setSubscriberIDToAdd(Long subscriberID){
		this.subscriberIDToAdd=subscriberID;
		return this;
	}
	public UpdateAlternateIDSubscriberIDs setSubscriberIDToRemove(Long subscriberID){
		this.subscriberIDToRemove=subscriberID;
		return this;
	}
}
