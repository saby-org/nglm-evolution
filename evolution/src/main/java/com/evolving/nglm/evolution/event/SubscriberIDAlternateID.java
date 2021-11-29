package com.evolving.nglm.evolution.event;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.DeploymentCommon;

public class SubscriberIDAlternateID {

	private Long subscriberID;
	private AlternateID alternateIDConf;
	private String alternateIDValue;

	public Long getSubscriberID() {return subscriberID;}
	public AlternateID getAlternateIDConf() {return alternateIDConf;}
	public String getAlternateIDValue() {return alternateIDValue;}

	public SubscriberIDAlternateID(Long subscriberID, AlternateID alternateIDConf, String alternateIDValue){
		this.subscriberID = subscriberID;
		this.alternateIDConf = alternateIDConf;
		this.alternateIDValue = alternateIDValue;
	}

	public SubscriberIDAlternateID(Long subscriberID, String alternateIDConfID, String alternateIDValue){
		this.subscriberID = subscriberID;
		this.alternateIDConf = DeploymentCommon.getAlternateID(alternateIDConfID);
		this.alternateIDValue = alternateIDValue;
	}
}
