package com.evolving.nglm.evolution.preprocessor;

import com.evolving.nglm.core.SubscriberIDService;
import com.evolving.nglm.evolution.SupplierService;
import com.evolving.nglm.evolution.VoucherService;
import com.evolving.nglm.evolution.VoucherTypeService;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

public class PreprocessorContext {

	ElasticsearchClientAPI elasticsearchClientAPI;
	SubscriberIDService subscriberIDService;
	SupplierService supplierService;
	VoucherService voucherService;
	VoucherTypeService voucherTypeService;

	public PreprocessorContext(ElasticsearchClientAPI elasticsearchClientAPI, SubscriberIDService subscriberIDService, SupplierService supplierService, VoucherService voucherService, VoucherTypeService voucherTypeService){
		this.elasticsearchClientAPI=elasticsearchClientAPI;
		this.subscriberIDService=subscriberIDService;
		this.supplierService=supplierService;
		this.voucherService=voucherService;
		this.voucherTypeService=voucherTypeService;
	}

	public ElasticsearchClientAPI getElasticsearchClientAPI() { return elasticsearchClientAPI; }
	public SubscriberIDService getSubscriberIDService() { return subscriberIDService; }
	public SupplierService getSupplierService() { return supplierService; }
	public VoucherService getVoucherService() { return voucherService; }
	public VoucherTypeService getVoucherTypeService() { return voucherTypeService; }
}
