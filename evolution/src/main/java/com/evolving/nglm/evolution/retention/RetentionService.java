package com.evolving.nglm.evolution.retention;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.*;
import com.evolving.nglm.evolution.retention.Cleanable.RetentionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

public class RetentionService {

	private static final Logger log = LoggerFactory.getLogger(RetentionService.class);

	private JourneyService journeyService;
	public JourneyService getJourneyService() { return journeyService; }

	public RetentionService(JourneyService journeyService){
		if(journeyService==null) throw new RuntimeException("journeyService dependency missing");
		this.journeyService = journeyService;
	}

	// the kafka subscriberState clean up
	public boolean cleanSubscriberState(SubscriberState subscriberState){

		if(log.isTraceEnabled()) log.trace("cleanSubscriberState called for "+subscriberState.getSubscriberID());

		SubscriberProfile subscriberProfile = subscriberState.getSubscriberProfile();

		//journeys
		boolean subscriberUpdated = !filterOutRetentionOver(subscriberState.getRecentJourneyStates().iterator(), RetentionType.KAFKA_DELETION).isEmpty();

		// those for journeys are kept forever till campaign is deleted
		Iterator<String> iterator = subscriberProfile.getSubscriberJourneys().keySet().iterator();
		while(iterator.hasNext()){
			String journeyId = iterator.next();
			if(journeyService.getStoredJourney(journeyId)==null){
				iterator.remove();
				subscriberProfile.getSubscriberJourneysEnded().remove(journeyId);// they should all be in as well
			}
		}
		iterator = subscriberProfile.getSubscriberJourneysEnded().keySet().iterator();
		while(iterator.hasNext()){
			String journeyId = iterator.next();
			if(journeyService.getStoredJourney(journeyId)==null){
				iterator.remove();
				subscriberProfile.getSubscriberJourneysEnded().remove(journeyId);
			}
		}

		//TODO tokens (yet there are cleaned as soon as "burned", and logic rely on that, so more complicated to apply EVPRO-325 retention)

		// loyalties
		subscriberUpdated = !filterOutRetentionOver(subscriberProfile.getLoyaltyPrograms().values().iterator(), Cleanable.RetentionType.KAFKA_DELETION).isEmpty() || subscriberUpdated;

		// vouchers
		subscriberUpdated = !filterOutRetentionOver(subscriberProfile.getVouchers().iterator(), Cleanable.RetentionType.KAFKA_DELETION).isEmpty() || subscriberUpdated;

		return subscriberUpdated;

	}

	// the kafka subscriberHistory clean up
	public boolean cleanSubscriberHistory(SubscriberHistory subscriberHistory, int tenantID){

		if(log.isTraceEnabled()) log.trace("cleanSubscriberHistory called for "+subscriberHistory.getSubscriberID());

		boolean subscriberUpdated = false;

		//TODO: before EVPRO-325 too much deliveryRequest were stored, not enough filtering, hence a forced cleaning that can be removed once all customers are in the right version
		// ------ START CLEANING COULD BE REMOVED
		Iterator<DeliveryRequest> iterator = subscriberHistory.getDeliveryRequests().iterator();
		while (iterator.hasNext()){
			DeliveryRequest toCheck = iterator.next();
			if(!toCheck.isToStoreInHistoryStateStore()){
				if(log.isDebugEnabled()) log.debug("cleaning history of "+toCheck.getClass().getSimpleName());
				iterator.remove();
				subscriberUpdated = true;
			}
		}
		// ------ END CLEANING COULD BE REMOVED

		// deliveryRequest
		subscriberUpdated = !filterOutRetentionOver(subscriberHistory.getDeliveryRequests().iterator(), Cleanable.RetentionType.KAFKA_DELETION).isEmpty() || subscriberUpdated;

		// journeyStatistics
		subscriberUpdated = !filterOutRetentionOver(subscriberHistory.getJourneyHistory().iterator(), Cleanable.RetentionType.KAFKA_DELETION).isEmpty() || subscriberUpdated;

		return subscriberUpdated;

	}

	private <T extends Cleanable> Collection<T> filterOutRetentionOver(Iterator<T> cleanableIterator, RetentionType retentionType){
		Collection<T> toRet=new ArrayList<>();
		if(cleanableIterator==null || retentionType==null) return toRet;
		while(cleanableIterator.hasNext()){
			T toCheck = cleanableIterator.next();
			if(isRetentionPeriodOver(toCheck,retentionType)){
				if(log.isTraceEnabled()) log.trace("RetentionService.filterOutRetentionOver : filtering out "+toCheck.getClass().getSimpleName()+" for "+retentionType+", expirationDate : "+toCheck.getExpirationDate(this)+", now : "+SystemTime.getCurrentTime());
				cleanableIterator.remove();
				toRet.add(toCheck);
			}
		}
		return toRet;
	}

	private boolean isRetentionPeriodOver(Cleanable cleanable, RetentionType retentionType){
		if(cleanable==null||cleanable.getExpirationDate(this)==null||retentionType==null||cleanable.getRetention(retentionType,this)==null) return false;
		Date cleanBeforeThisDate = EvolutionUtilities.removeTime(SystemTime.getCurrentTime(),cleanable.getRetention(retentionType,this));
		return cleanable.getExpirationDate(this).before(cleanBeforeThisDate);
	}

	public boolean isExpired(Expirable expirable){
		if(expirable==null||expirable.getExpirationDate(this)==null) return false;
		return SystemTime.getCurrentTime().after(expirable.getExpirationDate(this));
	}

	// this is shared code hence grouped
	public Duration getJourneyRetention(RetentionType type, String journeyID) {
		GUIManagedObject journey = journeyService.getStoredJourney(journeyID);
		if(journey==null) return java.time.Duration.ofDays(Integer.MIN_VALUE);//deleted journey clean now
		switch (type){
			case KAFKA_DELETION:
				switch (journey.getGUIManagedObjectType()){
					case BulkCampaign:
						return java.time.Duration.ofDays(Deployment.getKafkaRetentionDaysBulkCampaigns());
					case Journey:
						return java.time.Duration.ofDays(Deployment.getKafkaRetentionDaysJourneys());
				}
				return java.time.Duration.ofDays(Deployment.getKafkaRetentionDaysCampaigns());
		}
		log.info("no retention applying for {} of type {}",journey.getGUIManagedObjectType(),type);
		return null;
	}
}
