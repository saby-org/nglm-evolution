package com.evolving.nglm.evolution.retention;

import com.evolving.nglm.core.Deployment;
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
	private TargetService targetService;
	public JourneyService getJourneyService() { return journeyService; }
	public TargetService getTargetService() { return targetService; }

	public RetentionService(JourneyService journeyService, TargetService targetService){
		if(journeyService==null) throw new RuntimeException("journeyService dependency missing");
		this.journeyService = journeyService;
		if(targetService==null) throw new RuntimeException("targetService dependency missing");
		this.targetService = targetService;
	}

	// the kafka subscriberState clean up
	public boolean cleanSubscriberState(SubscriberState subscriberState){

		if(log.isTraceEnabled()) log.trace("cleanSubscriberState called for "+subscriberState.getSubscriberID());

		SubscriberProfile subscriberProfile = subscriberState.getSubscriberProfile();

		boolean subscriberUpdated = false;

		// those for journeys are kept forever till campaign is deleted
		Iterator<String> iterator = subscriberProfile.getSubscriberJourneys().keySet().iterator();
		while(iterator.hasNext()){
			String journeyId = iterator.next();
			if(journeyService.getStoredJourney(journeyId)==null){
				subscriberUpdated=true;
				iterator.remove();
				subscriberProfile.getSubscriberJourneysEnded().remove(journeyId);// they should all be in as well
			}
		}
		iterator = subscriberProfile.getSubscriberJourneysEnded().keySet().iterator();
		while(iterator.hasNext()){
			String journeyId = iterator.next();
			if(journeyService.getStoredJourney(journeyId)==null){
				subscriberUpdated=true;
				iterator.remove();
				subscriberProfile.getSubscriberJourneysEnded().remove(journeyId);
			}
		}
		
		//
		//  clean targets
		//
		
		iterator = subscriberProfile.getTargets().keySet().iterator();
        while (iterator.hasNext())
          {
            String targetId = iterator.next();
            if (targetId != null)
              {
                GUIManagedObject targetUnchecked = targetService.getStoredTarget(targetId);
                if (targetUnchecked == null || isRetentionPeriodOverForTarget(targetUnchecked, RetentionType.KAFKA_DELETION))
                  {
                    log.info("retention period is over for target {} and removing for subscriber {}", targetId, subscriberProfile.getSubscriberID());
                    subscriberUpdated = true;
                    iterator.remove();
                    subscriberProfile.getTargets().remove(targetId);
                  }
              }
          }

		//TODO tokens (yet there are cleaned as soon as "burned", and logic rely on that, so more complicated to apply EVPRO-325 retention)

		// loyalties
		subscriberUpdated = !filterOutRetentionOver(subscriberProfile.getLoyaltyPrograms().values().iterator(), Cleanable.RetentionType.KAFKA_DELETION).isEmpty() || subscriberUpdated;

		// vouchers
		subscriberUpdated = !filterOutRetentionOver(subscriberProfile.getVouchers().iterator(), Cleanable.RetentionType.KAFKA_DELETION).isEmpty() || subscriberUpdated;

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
	
    private boolean isRetentionPeriodOverForTarget(GUIManagedObject target, RetentionType retentionType)
    {
      if (target == null || target.getEffectiveEndDate() == null || retentionType == null || getTargetRetention(retentionType, target.getGUIManagedObjectID()) == null)
        {
          return false;
        }
      Date cleanBeforeThisDate = EvolutionUtilities.removeTime(SystemTime.getCurrentTime(), getTargetRetention(retentionType, target.getGUIManagedObjectID()));
      return target.getEffectiveEndDate().before(cleanBeforeThisDate);
    }

	public boolean isExpired(Expirable expirable){
		if(expirable==null||expirable.getExpirationDate(this)==null) return false;
		return SystemTime.getCurrentTime().after(expirable.getExpirationDate(this));
	}
	
	public Duration getTargetRetention(RetentionType type, String targetID) {
		Duration duration;
		GUIManagedObject target = targetService.getStoredTarget(targetID);
		if(target==null) duration= java.time.Duration.ofDays(Integer.MIN_VALUE);//deleted journey clean now
		else
		duration= java.time.Duration.ofDays(Deployment.getKafkaRetentionDaysTargets());
		return duration;
	}
}
