package com.evolving.nglm.evolution.commoditydelivery;

import com.evolving.nglm.evolution.RESTAPIGenericReturnCodes;

public class CommodityDeliveryException extends Exception {

	RESTAPIGenericReturnCodes error;
	String details;
	public RESTAPIGenericReturnCodes getError() { return error; }
	public String getDetails() { return details; }

	CommodityDeliveryException(RESTAPIGenericReturnCodes error, String details){
		super(error.getGenericResponseMessage()+" : "+details);
		this.error = error;
		this.details = details;
	}

}
