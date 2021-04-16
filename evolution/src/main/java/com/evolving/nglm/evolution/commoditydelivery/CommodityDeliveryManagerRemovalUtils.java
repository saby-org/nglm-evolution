package com.evolving.nglm.evolution.commoditydelivery;

import com.evolving.nglm.core.*;
import com.evolving.nglm.evolution.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/*
* This class is an attempt to replace the CommodityDeliveryManager process by just few utils functions
* this is mainly copy/past of the initial code, code structure itself make still not much sense, but at least one stupid process is removed
*/
public class CommodityDeliveryManagerRemovalUtils {

	private static Logger log = LoggerFactory.getLogger(CommodityDeliveryManagerRemovalUtils.class);

	// a kafka producer for static util send
	private static KafkaProducer deliveryRequestProducer = null;
	static{
		Properties kafkaProducerProperties = new Properties();
		kafkaProducerProperties.put("bootstrap.servers", Deployment.getBrokerServers());
		kafkaProducerProperties.put("acks", "all");
		kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		deliveryRequestProducer = new KafkaProducer<byte[], byte[]>(kafkaProducerProperties);
	}

	public static DeliveryRequest createDeliveryRequest(/*String applicationID, */CommodityDeliveryManager.CommodityDeliveryRequest commodityDeliveryRequest, PaymentMeanService paymentMeanService, DeliverableService deliverableService) throws CommodityDeliveryException {

		if(log.isDebugEnabled()) log.debug("CommodityDeliveryManagerRemovalUtils.createCommodityDeliveryRequest : processing "+commodityDeliveryRequest);

		String subscriberID = commodityDeliveryRequest.getSubscriberID();
		String commodityID = commodityDeliveryRequest.getCommodityID();
		CommodityDeliveryManager.CommodityDeliveryOperation operation = commodityDeliveryRequest.getOperation();
		int amount = commodityDeliveryRequest.getAmount();

		if(amount < 0){
			log.error("CommodityDeliveryManagerRemovalUtils.createCommodityDeliveryRequest : (commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : bad field value for amount");
			throw new CommodityDeliveryException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE, "amount < 0");
		}

		if(subscriberID == null){
			log.error("CommodityDeliveryManagerRemovalUtils.createCommodityDeliveryRequest (commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : bad field value for subscriberID");
			throw new CommodityDeliveryException(RESTAPIGenericReturnCodes.MISSING_PARAMETERS, "subscriberID");
		}

		String externalAccountID = null;
		DeliveryManagerDeclaration provider = null;
		CommodityDeliveryManager.CommodityType commodityType = null;
		String deliveryType = null;
		PaymentMean paymentMean = null;
		Deliverable deliverable = null;

		// debit case, provider is in paymentMean
		if(operation.equals(CommodityDeliveryManager.CommodityDeliveryOperation.Debit)){
			paymentMean = paymentMeanService.getActivePaymentMean(commodityID, SystemTime.getCurrentTime());
			if(paymentMean == null){
				log.info("CommodityDeliveryManagerRemovalUtils.createCommodityDeliveryRequest (commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : paymentMean not found ");
				throw new CommodityDeliveryException(RESTAPIGenericReturnCodes.BONUS_NOT_FOUND, "unknown payment mean");
			}
			externalAccountID = paymentMean.getExternalAccountID();
			provider = Deployment.getFulfillmentProviders().get(paymentMean.getFulfillmentProviderID());
		// all other are in deliverable
		}else{
			deliverable = deliverableService.getActiveDeliverable(commodityID, SystemTime.getCurrentTime());
			if(deliverable == null){
				log.info("CommodityDeliveryManagerRemovalUtils.createCommodityDeliveryRequest (commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : commodity not found ");
				throw new CommodityDeliveryException(RESTAPIGenericReturnCodes.BONUS_NOT_FOUND, "unknown commodity");
			}
			externalAccountID = deliverable.getExternalAccountID();
			provider = Deployment.getFulfillmentProviders().get(deliverable.getFulfillmentProviderID());
		}

		if(provider == null){
			log.error("CommodityDeliveryManagerRemovalUtils.createCommodityDeliveryRequest (commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : provider not found ");
			throw new CommodityDeliveryException(RESTAPIGenericReturnCodes.BONUS_NOT_FOUND, "unknown provider");
		}

		commodityType = provider.getProviderType();
		deliveryType = provider.getDeliveryType();

		return constructSubDeliveryRequest(/*applicationID, */commodityDeliveryRequest, commodityType, deliveryType, externalAccountID, deliverable);

	}

	private static DeliveryRequest constructSubDeliveryRequest(/*String applicationID, */CommodityDeliveryManager.CommodityDeliveryRequest commodityDeliveryRequest, CommodityDeliveryManager.CommodityType commodityType, String deliveryType, String externalAccountID, Deliverable deliverable){

		if(log.isDebugEnabled()) log.debug("CommodityDeliveryManagerRemovalUtils.constructSubDeliveryRequest("+commodityType+", "+deliveryType+") : method called ...");

		Map<String, String> diplomaticBriefcase = commodityDeliveryRequest.getDiplomaticBriefcase();
		if(diplomaticBriefcase == null){
			diplomaticBriefcase = new HashMap<String, String>();
		}
		//diplomaticBriefcase.put(CommodityDeliveryManager.APPLICATION_ID, applicationID == null ? CommodityDeliveryManager.COMMODITY_DELIVERY_ID : applicationID);
		diplomaticBriefcase.put(CommodityDeliveryManager.COMMODITY_DELIVERY_BRIEFCASE, commodityDeliveryRequest.getJSONRepresentation(commodityDeliveryRequest.getTenantID()).toJSONString());

		//
		// execute commodity request
		//

		String validityPeriodType = commodityDeliveryRequest.getValidityPeriodType() != null ? commodityDeliveryRequest.getValidityPeriodType().getExternalRepresentation() : null;
		Integer validityPeriodQuantity = commodityDeliveryRequest.getValidityPeriodQuantity();
		//String newDeliveryRequestID = zookeeperUniqueKeyServer.getStringKey();
		String newDeliveryRequestID = commodityDeliveryRequest.getDeliveryRequestID();//not new, really needed now ?
		switch (commodityType) {

			case IN:

				HashMap<String,Object> inRequestData = new HashMap<String,Object>();

				inRequestData.put("deliveryRequestID", newDeliveryRequestID);
				inRequestData.put("originatingRequest", commodityDeliveryRequest.getOriginatingRequest());
				inRequestData.put("deliveryType", deliveryType);

				inRequestData.put("eventID", commodityDeliveryRequest.getEventID());
				inRequestData.put("moduleID", commodityDeliveryRequest.getModuleID());
				inRequestData.put("featureID", commodityDeliveryRequest.getFeatureID());

				inRequestData.put("subscriberID", commodityDeliveryRequest.getSubscriberID());
				inRequestData.put("providerID", commodityDeliveryRequest.getProviderID());
				inRequestData.put("commodityID", commodityDeliveryRequest.getCommodityID());
				inRequestData.put("externalAccountID", externalAccountID);

				inRequestData.put("operation", CommodityDeliveryManager.CommodityDeliveryOperation.fromExternalRepresentation(commodityDeliveryRequest.getOperation().getExternalRepresentation()).getExternalRepresentation());
				inRequestData.put("amount", commodityDeliveryRequest.getAmount());
				inRequestData.put("validityPeriodType", validityPeriodType);
				inRequestData.put("validityPeriodQuantity", validityPeriodQuantity);
				if(commodityDeliveryRequest.getDiplomaticBriefcase()!=null) inRequestData.put("diplomaticBriefcase", commodityDeliveryRequest.getDiplomaticBriefcase());
				inRequestData.put("dateFormat", "yyyy-MM-dd'T'HH:mm:ss:XX");

				INFulfillmentManager.INFulfillmentRequest inRequest = new INFulfillmentManager.INFulfillmentRequest(commodityDeliveryRequest, JSONUtilities.encodeObject(inRequestData), Deployment.getDeliveryManagers().get(deliveryType), commodityDeliveryRequest.getTenantID());
				return inRequest;

			case POINT:

				HashMap<String,Object> pointRequestData = new HashMap<String,Object>();

				pointRequestData.put("deliveryRequestID", newDeliveryRequestID);
				pointRequestData.put("originatingRequest", commodityDeliveryRequest.getOriginatingRequest());
				pointRequestData.put("deliveryType", deliveryType);

				pointRequestData.put("eventID", commodityDeliveryRequest.getEventID());
				pointRequestData.put("moduleID", commodityDeliveryRequest.getModuleID());
				pointRequestData.put("featureID", commodityDeliveryRequest.getFeatureID());

				pointRequestData.put("subscriberID", commodityDeliveryRequest.getSubscriberID());
				pointRequestData.put("pointID", externalAccountID);

				pointRequestData.put("operation", commodityDeliveryRequest.getOperation().getExternalRepresentation());
				pointRequestData.put("amount", commodityDeliveryRequest.getAmount());
				pointRequestData.put("validityPeriodType", validityPeriodType);
				pointRequestData.put("validityPeriodQuantity", validityPeriodQuantity);
				if(commodityDeliveryRequest.getDiplomaticBriefcase()!=null) pointRequestData.put("diplomaticBriefcase", commodityDeliveryRequest.getDiplomaticBriefcase());

				PointFulfillmentRequest pointRequest = new PointFulfillmentRequest(commodityDeliveryRequest, JSONUtilities.encodeObject(pointRequestData), Deployment.getDeliveryManagers().get(deliveryType), commodityDeliveryRequest.getTenantID());
				return pointRequest;

			case REWARD:

				//
				//  request (JSON)
				//

				HashMap<String,Object> rewardRequestData = new HashMap<String,Object>();
				rewardRequestData.put("deliveryRequestID", newDeliveryRequestID);
				rewardRequestData.put("originatingRequest", commodityDeliveryRequest.getOriginatingRequest());
				rewardRequestData.put("subscriberID", commodityDeliveryRequest.getSubscriberID());
				rewardRequestData.put("eventID", commodityDeliveryRequest.getEventID());
				rewardRequestData.put("moduleID", commodityDeliveryRequest.getModuleID());
				rewardRequestData.put("featureID", commodityDeliveryRequest.getFeatureID());
				rewardRequestData.put("deliveryType", deliveryType);
				if(commodityDeliveryRequest.getDiplomaticBriefcase()!=null) rewardRequestData.put("diplomaticBriefcase", commodityDeliveryRequest.getDiplomaticBriefcase());
				rewardRequestData.put("msisdn", commodityDeliveryRequest.getSubscriberID());
				rewardRequestData.put("providerID", commodityDeliveryRequest.getProviderID());
				rewardRequestData.put("deliverableID", deliverable.getDeliverableID());
				rewardRequestData.put("deliverableName", deliverable.getDeliverableName());
				rewardRequestData.put("operation", commodityDeliveryRequest.getOperation().getExternalRepresentation());
				rewardRequestData.put("amount", commodityDeliveryRequest.getAmount());
				rewardRequestData.put("periodQuantity", (validityPeriodQuantity == null ? 1 : validityPeriodQuantity)); //mandatory in RewardManagerRequest => set default value if nul
				rewardRequestData.put("periodType", (validityPeriodType == null ? EvolutionUtilities.TimeUnit.Day.getExternalRepresentation() : validityPeriodType)); //mandatory in RewardManagerRequest => set default value if nul

				//
				//  send
				//

				if(log.isDebugEnabled()) log.debug("CommodityDeliveryManager.proceedCommodityDeliveryRequest(...) : generating "+ CommodityDeliveryManager.CommodityType.REWARD+" request DONE");

				RewardManagerRequest rewardRequest = new RewardManagerRequest(commodityDeliveryRequest,JSONUtilities.encodeObject(rewardRequestData), Deployment.getDeliveryManagers().get(deliveryType), commodityDeliveryRequest.getTenantID());
				return  rewardRequest;

			case JOURNEY:

				HashMap<String,Object> journeyRequestData = new HashMap<String,Object>();

				journeyRequestData.put("deliveryRequestID", newDeliveryRequestID);
				journeyRequestData.put("originatingRequest", commodityDeliveryRequest.getOriginatingRequest());
				journeyRequestData.put("deliveryType", deliveryType);

				journeyRequestData.put("eventID", commodityDeliveryRequest.getEventID());
				journeyRequestData.put("moduleID", commodityDeliveryRequest.getModuleID());
				journeyRequestData.put("featureID", commodityDeliveryRequest.getFeatureID());
				journeyRequestData.put("journeyRequestID", commodityDeliveryRequest.getDeliveryRequestID());

				journeyRequestData.put("subscriberID", commodityDeliveryRequest.getSubscriberID());
				journeyRequestData.put("eventDate", SystemTime.getCurrentTime());
				journeyRequestData.put("journeyID", externalAccountID);

				if(commodityDeliveryRequest.getDiplomaticBriefcase()!=null) journeyRequestData.put("diplomaticBriefcase", commodityDeliveryRequest.getDiplomaticBriefcase());

				if(log.isDebugEnabled()) log.debug("CommodityDeliveryManager.proceedCommodityDeliveryRequest(...) : generating "+ CommodityDeliveryManager.CommodityType.JOURNEY+" request DONE");

				JourneyRequest journeyRequest = new JourneyRequest(commodityDeliveryRequest, JSONUtilities.encodeObject(journeyRequestData), Deployment.getDeliveryManagers().get(deliveryType), commodityDeliveryRequest.getTenantID());
				return journeyRequest;

			default:
				commodityDeliveryRequest.setCommodityDeliveryStatus(CommodityDeliveryManager.CommodityDeliveryStatus.SUCCESS);
				return commodityDeliveryRequest;

		}

	}

	// this one is used by PurchaseFulfillmentManager
	public static void sendCommodityDeliveryRequest(PaymentMeanService paymentMeanService, DeliverableService deliverableService, DeliveryRequest originatingDeliveryRequest, JSONObject briefcase, String applicationID, String deliveryRequestID, String originatingDeliveryRequestID, boolean originatingRequest, String eventID, String moduleID, String featureID, String subscriberID, String providerID, String commodityID, CommodityDeliveryManager.CommodityDeliveryOperation operation, long amount, EvolutionUtilities.TimeUnit validityPeriodType, Integer validityPeriodQuantity, String origin) throws CommodityDeliveryException {
		HashMap<String,Object> requestData = createCommodityDeliveryRequest(briefcase,applicationID,deliveryRequestID,originatingDeliveryRequestID,originatingRequest,eventID,moduleID,featureID,subscriberID,providerID,commodityID,operation,amount,validityPeriodType,validityPeriodQuantity,origin);
		CommodityDeliveryManager.CommodityDeliveryRequest commodityDeliveryRequest = new CommodityDeliveryManager.CommodityDeliveryRequest(originatingDeliveryRequest,JSONUtilities.encodeObject(requestData), Deployment.getDeliveryManagers().get(CommodityDeliveryManager.COMMODITY_DELIVERY_TYPE), originatingDeliveryRequest.getTenantID());
		DeliveryRequest deliveryRequest = createDeliveryRequest(/*applicationID, */commodityDeliveryRequest,paymentMeanService,deliverableService);
		send(deliveryRequest);
	}

	public static void sendCommodityDeliveryRequest(PaymentMeanService paymentMeanService, DeliverableService deliverableService, SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, JSONObject briefcase, String applicationID, String deliveryRequestID, String originatingDeliveryRequestID, boolean originatingRequest, String eventID, String moduleID, String featureID, String subscriberID, String providerID, String commodityID, CommodityDeliveryManager.CommodityDeliveryOperation operation, long amount, EvolutionUtilities.TimeUnit validityPeriodType, Integer validityPeriodQuantity, DeliveryRequest.DeliveryPriority priority, String origin, int tenantID) throws CommodityDeliveryException {
		HashMap<String,Object> requestData = createCommodityDeliveryRequest(briefcase,applicationID,deliveryRequestID,originatingDeliveryRequestID,originatingRequest,eventID,moduleID,featureID,subscriberID,providerID,commodityID,operation,amount,validityPeriodType,validityPeriodQuantity,origin);
		CommodityDeliveryManager.CommodityDeliveryRequest commodityDeliveryRequest = new CommodityDeliveryManager.CommodityDeliveryRequest(subscriberProfile, subscriberGroupEpochReader, JSONUtilities.encodeObject(requestData), Deployment.getDeliveryManagers().get(CommodityDeliveryManager.COMMODITY_DELIVERY_TYPE), tenantID);
		commodityDeliveryRequest.forceDeliveryPriority(priority);
		DeliveryRequest deliveryRequest = createDeliveryRequest(/*applicationID, */commodityDeliveryRequest,paymentMeanService,deliverableService);
		send(deliveryRequest);
	}

	private static HashMap<String,Object> createCommodityDeliveryRequest(JSONObject briefcase, String applicationID, String deliveryRequestID, String originatingDeliveryRequestID, boolean originatingRequest, String eventID, String moduleID, String featureID, String subscriberID, String providerID, String commodityID, CommodityDeliveryManager.CommodityDeliveryOperation operation, long amount, EvolutionUtilities.TimeUnit validityPeriodType, Integer validityPeriodQuantity, String origin){
		HashMap<String,Object> requestData = new HashMap<String,Object>();
		requestData.put("deliveryRequestID", deliveryRequestID);
		requestData.put("originatingRequest", originatingRequest);
		requestData.put("originatingDeliveryRequestID", originatingDeliveryRequestID);
		requestData.put("deliveryType", CommodityDeliveryManager.COMMODITY_DELIVERY_TYPE);
		requestData.put("eventID", eventID);
		requestData.put("moduleID", moduleID);
		requestData.put("featureID", featureID);
		requestData.put("subscriberID", subscriberID);
		//TODO:fix that hack
		requestData.put("externalSubscriberID", subscriberID);
		requestData.put("providerID", providerID);
		requestData.put("commodityID", commodityID);
		requestData.put("operation", operation.getExternalRepresentation());
		requestData.put("amount", amount);
		requestData.put("validityPeriodType", (validityPeriodType != null ? validityPeriodType.getExternalRepresentation() : null));
		requestData.put("validityPeriodQuantity", validityPeriodQuantity);
		requestData.put("origin", origin);
		requestData.put("commodityDeliveryStatusCode", CommodityDeliveryManager.CommodityDeliveryStatus.PENDING.getReturnCode());
		Map<String,String> diplomaticBriefcase = new HashMap<String,String>();
		if(applicationID != null){diplomaticBriefcase.put(CommodityDeliveryManager.APPLICATION_ID, applicationID);}
		if(briefcase != null){diplomaticBriefcase.put(CommodityDeliveryManager.APPLICATION_BRIEFCASE, briefcase.toJSONString());}
		requestData.put("diplomaticBriefcase", diplomaticBriefcase);
		return requestData;
	}

	private static void send(DeliveryRequest deliveryRequest){
		DeliveryManagerDeclaration deliveryManagerDeclaration = Deployment.getDeliveryManagers().get(deliveryRequest.getDeliveryType());
		String topic = deliveryManagerDeclaration.getRequestTopic(deliveryRequest.getDeliveryPriority());
		//I feel we should always key by subscriberId, we are loosing a possible important ordering in bonus delivery not doing so, and I think we just increase complexity for nothing
		String key = deliveryManagerDeclaration.isProcessedByEvolutionEngine() ? deliveryRequest.getSubscriberID() : deliveryRequest.getDeliveryRequestID();
		deliveryRequestProducer.send(new ProducerRecord<>(topic, StringKey.serde().serializer().serialize(topic, new StringKey(key)), ((ConnectSerde<DeliveryRequest>)deliveryManagerDeclaration.getRequestSerde()).serializer().serialize(topic, deliveryRequest)));
	}

}
