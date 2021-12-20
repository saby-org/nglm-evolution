package com.evolving.nglm.evolution.event;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.core.SubscriberIDService;
import com.evolving.nglm.evolution.redis.UpdateAlternateIDSubscriberIDs;
import com.evolving.nglm.evolution.uniquekey.ZookeeperUniqueKeyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class MapperUtils {

	private static final Logger log = LoggerFactory.getLogger(MapperUtils.class);

	public static <T extends ExternalEvent> List<T> resolve(List<T> externalEvents, SubscriberIDService subscriberIDService, AtomicBoolean stopRequested) throws SubscriberIDService.SubscriberIDServiceException, InterruptedException {
		return resolve(1,externalEvents,subscriberIDService,stopRequested);
	}
	// this resolve the subscriberID for the existing entry, or create if not existing and needed
	private static <T extends ExternalEvent> List<T> resolve(int recursiveOccurrence, List<T> externalEvents, SubscriberIDService subscriberIDService, AtomicBoolean stopRequested) throws SubscriberIDService.SubscriberIDServiceException, InterruptedException {

		if(log.isTraceEnabled()) log.trace("resolve called for "+externalEvents.size()+" events");

		List<T> toResolveEvents = new ArrayList<>(externalEvents);
		List<T> resolvedEvents = new ArrayList<>(externalEvents.size());

		// prepare redis resolution per batch of different alternateID returned
		HashMap<AlternateID, Set<String>> alternateIDValues = new HashMap<>();
		Iterator<T> iterator = toResolveEvents.iterator();
		while(iterator.hasNext()){
			T externalEvent = iterator.next();
			if(externalEvent.getSubscriberIDLong()!=null && externalEvent.getParentToResolved().isEmpty()){
				// check if nothing to do
				if(log.isTraceEnabled()) log.trace(externalEvent.getEventName()+" resolve subscriberID "+externalEvent.getSubscriberIDLong()+" known, doing nothing");
				resolvedEvents.add(externalEvent);
				iterator.remove();
			}else{
				// prepare for resolution query
				// possible subscriber itself
				if(externalEvent.getSubscriberIDLong()==null){
					alternateIDValues.computeIfAbsent(externalEvent.getAlternateID().getFirstElement(), k ->new HashSet<>()).add(externalEvent.getAlternateID().getSecondElement());
				}
				// possible parents relationship
				if(!externalEvent.getParentToResolved().isEmpty()){
					for(Pair<AlternateID,String> parent:externalEvent.getParentToResolved()) alternateIDValues.computeIfAbsent(parent.getFirstElement(), k ->new HashSet<>()).add(parent.getSecondElement());
				}
				// possible swap existing alternateID to new value
				if(externalEvent.getOldAlternateIDValueToSwapWith()!=null){
					alternateIDValues.computeIfAbsent(externalEvent.getAlternateID().getFirstElement(), k ->new HashSet<>()).add(externalEvent.getOldAlternateIDValueToSwapWith());
				}
			}
		}

		if(log.isTraceEnabled()){
			int nbResolution = 0;
			for(Set<String> resolutionBatch:alternateIDValues.values()) nbResolution+=resolutionBatch.size();
			log.trace("resolve needed for "+toResolveEvents.size()+" events, into "+alternateIDValues.keySet().size()+" batch and total of "+nbResolution);
		}

		// resolve each batch
		for(Map.Entry<AlternateID,Set<String>> alternateIDEntry:alternateIDValues.entrySet()){
			if(log.isTraceEnabled()) log.trace("resolve checking resolution for "+alternateIDEntry.getKey().getName()+" with "+alternateIDEntry.getValue().size()+" elements");
			Map<String, Pair<Integer,List<Long>>> alternateIDs = subscriberIDService.getSubscriberIDsBlocking(alternateIDEntry.getKey().getName(),alternateIDEntry.getValue(),stopRequested);
			iterator = toResolveEvents.iterator();
			while(iterator.hasNext()){
				T externalEvent = iterator.next();

				// into subscriber resolution batch
				if(externalEvent.getAlternateID()!=null && externalEvent.getAlternateID().getFirstElement()==alternateIDEntry.getKey()){
					Pair<Integer,List<Long>> resolvedSubscriberID = alternateIDs.get(externalEvent.getAlternateID().getSecondElement());
					Pair<Integer,List<Long>> resolvedSubscriberIDForSwapping = null;
					if(externalEvent.getOldAlternateIDValueToSwapWith()!=null) resolvedSubscriberIDForSwapping = alternateIDs.get(externalEvent.getOldAlternateIDValueToSwapWith());
					// not existing
					if(resolvedSubscriberID==null && resolvedSubscriberIDForSwapping==null){
						//not for creation doing nothing
						if(externalEvent.getTenantID()==null){
							if(log.isTraceEnabled()) log.trace(externalEvent.getEventName()+" resolve "+externalEvent.getSubscriberForLogging()+" does not exist, not marked for creation, doing nothing");
							iterator.remove();
							continue;
						}
					// normal case subscriber id resolution
					}else if(resolvedSubscriberIDForSwapping==null){
						if(log.isTraceEnabled()) log.trace(externalEvent.getEventName()+" resolve "+externalEvent.getSubscriberForLogging()+" to "+resolvedSubscriberID.getFirstElement()+":"+resolvedSubscriberID.getSecondElement());
						externalEvent.resolve(resolvedSubscriberID);
						// logging case of swap asked but current already exist, and old not
						if(externalEvent.getOldAlternateIDValueToSwapWith()!=null) log.info(externalEvent.getEventName()+" swap asked "+externalEvent.getSubscriberForLogging()+" new entry "+externalEvent.getAlternateID().getSecondElement()+" already there, and old entry "+externalEvent.getOldAlternateIDValueToSwapWith()+" not, not changing any mapping");
					// alternateID swap with an already existing one cases
					}else if(resolvedSubscriberIDForSwapping!=null){
						if(resolvedSubscriberIDForSwapping.getSecondElement().size()>1 || (resolvedSubscriberID!=null&&resolvedSubscriberID.getSecondElement().size()>1)) throw new RuntimeException(externalEvent.getSubscriberForLogging()+" trying to swap with shared alternateID "+resolvedSubscriberIDForSwapping.getSecondElement());
						// the new one already exist
						if(resolvedSubscriberID!=null){
							// they are just already matching, we delete the old entry
							if(resolvedSubscriberID.equals(resolvedSubscriberIDForSwapping)){
								if(log.isTraceEnabled()) log.trace(externalEvent.getEventName()+" swap asked "+externalEvent.getSubscriberForLogging()+" new entry already there, just deleting old entry "+externalEvent.getAlternateID().getSecondElement());
								// instant delete request
								subscriberIDService.deleteRedisStraightAlternateID(alternateIDEntry.getKey().getID(),externalEvent.getOldAlternateIDValueToSwapWith());
							}
							// they mismatch, it is an issue, we warn it, but doing nothing, this need troubleshoot
							else{
								log.warn(externalEvent.getEventName()+" resolve "+externalEvent.getSubscriberForLogging()+" swaping to "+externalEvent.getOldAlternateIDValueToSwapWith()+" "+resolvedSubscriberIDForSwapping+" asked but current already exist mismatching "+externalEvent.getAlternateID().getSecondElement()+" "+resolvedSubscriberID+", not changing any mapping");
							}
							// in both cases resolved the same
							externalEvent.resolve(resolvedSubscriberID);
						}
						// the normal case, new one does not exist, old one yes
						else{
							// instant delete old
							subscriberIDService.deleteRedisStraightAlternateID(alternateIDEntry.getKey().getID(),externalEvent.getOldAlternateIDValueToSwapWith());
							// prepare for creation of new
							externalEvent.setMappingCreationSwapWith(resolvedSubscriberIDForSwapping);
						}
					}

				}

				// any parent resolution
				for(Pair<AlternateID,String> parentToResolve:externalEvent.getParentToResolved()){
					// into parent resolution batch
					if(parentToResolve.getFirstElement()==alternateIDEntry.getKey()){
						Pair<Integer,List<Long>> resolvedSubscriberID = alternateIDs.get(parentToResolve.getSecondElement());
						// not existing, we don't create parent
						if(resolvedSubscriberID==null) log.info(externalEvent.getEventName()+" resolved for non existing parent "+parentToResolve.getSecondElement()+" "+parentToResolve.getFirstElement().getDisplay()+" for "+externalEvent.getSubscriberForLogging()+", this is ignored");
						if(resolvedSubscriberID!=null && resolvedSubscriberID.getSecondElement().size()>1) throw new RuntimeException(externalEvent.getSubscriberForLogging()+" trying to change parent with shared alternateID "+resolvedSubscriberID.getSecondElement());
						externalEvent.parentResolved(parentToResolve.getFirstElement(),parentToResolve.getSecondElement(),resolvedSubscriberID!=null?resolvedSubscriberID.getSecondElement().get(0):null);
					}
				}

				// if no more job to do
				if(externalEvent.getSubscriberIDLong()!=null && externalEvent.getParentToResolved().isEmpty()){
					resolvedEvents.add(externalEvent);
					iterator.remove();
					continue;
				}
			}
		}

		if(log.isTraceEnabled()) log.trace("resolve creation needed for "+toResolveEvents.size()+" events");

		// prepare main alternateID redis creation per batch of different alternateID returned
		HashMap<AlternateID, Map<String/*alternateIDValue*/,Pair<Integer/*tenantID*/,Set<Long/*subscriberIDValue*/>>>> alternateIDsToCreate = new HashMap<>();
		for(ExternalEvent externalEvent:toResolveEvents){
			if(externalEvent.getSubscriberIDLong()!=null) continue;// this one is resolved, not for main alternateID creation
			// prepare for resolution query
			AlternateID alternateID = externalEvent.getAlternateID().getFirstElement();
			if(externalEvent.getSubscriberIDToAssign()==null) externalEvent.setSubscriberIDToAssign(generateNewSubscriberID());
			alternateIDsToCreate.computeIfAbsent(alternateID, k ->new HashMap<>()).put(externalEvent.getAlternateID().getSecondElement(),new Pair<>(externalEvent.getTenantID(), Collections.singleton(externalEvent.getSubscriberIDToAssign())));
		}
		// create redis mapping if not exist
		for(Map.Entry<AlternateID, Map<String/*alternateIDValue*/,Pair<Integer/*tenantID*/,Set<Long>/*subscriberIDs*/>>> perAlternateID:alternateIDsToCreate.entrySet()){
			if(subscriberIDService.putAlternateIDs(perAlternateID.getKey(), perAlternateID.getValue(), true, stopRequested)){
				// created with our values
				iterator = toResolveEvents.iterator();
				while(iterator.hasNext()){
					T externalEvent = iterator.next();
					if(externalEvent.getAlternateID().getFirstElement()!=perAlternateID.getKey()) continue;// not the same alternateID batch resolution
					// mark as resolved
					externalEvent.createdOnRedis(perAlternateID.getKey(),externalEvent.getAlternateID().getSecondElement());
					resolvedEvents.add(externalEvent);
					iterator.remove();
				}
			}
		}
		if(log.isTraceEnabled()) log.trace("resolve still "+toResolveEvents.size()+" to create after occurrence "+recursiveOccurrence);
		// we can have remaining if concurrent update of the same alternateID, so we just resolve remaining
		if(recursiveOccurrence>1000){//but this clearly is a bug
			log.warn("reached occurence "+recursiveOccurrence+" THIS IS A BUG, aborting");
			throw new RuntimeException("too many recursivity occurrence "+recursiveOccurrence);
		}
		if(toResolveEvents.size()>0) resolvedEvents.addAll(resolve(recursiveOccurrence+1,toResolveEvents,subscriberIDService,stopRequested));

		//TODO: we should extra check tenantID consistency among subscriber/parents

		// prepare for secondary alternateID update if needed, once resolved over
		Map<AlternateID,Map<UpdateAlternateIDSubscriberIDs,ExternalEvent>> sharedAlternateIDsToCreate = new HashMap<>();
		for(ExternalEvent externalEvent:resolvedEvents){
			if(externalEvent.getOtherAlternateIDsToAdd().isEmpty() && externalEvent.getOtherAlternateIDsToRemove().isEmpty()) continue;//no secondary alternateID to update
			for(Map.Entry<AlternateID,String> entry:externalEvent.getOtherAlternateIDsToAdd().entrySet()){
				sharedAlternateIDsToCreate.computeIfAbsent(entry.getKey(), k ->new HashMap<>()).put(new UpdateAlternateIDSubscriberIDs(entry.getKey(),entry.getValue(),externalEvent.getTenantID()).setSubscriberIDToAdd(externalEvent.getSubscriberIDLong()),externalEvent);
			}
			for(Map.Entry<AlternateID,String> entry:externalEvent.getOtherAlternateIDsToRemove().entrySet()){
				sharedAlternateIDsToCreate.computeIfAbsent(entry.getKey(), k ->new HashMap<>()).put(new UpdateAlternateIDSubscriberIDs(entry.getKey(),entry.getValue(),externalEvent.getTenantID()).setSubscriberIDToRemove(externalEvent.getSubscriberIDLong()),externalEvent);
			}
		}
		// execute it and enrich event if executed
		for(Map.Entry<AlternateID,Map<UpdateAlternateIDSubscriberIDs,ExternalEvent>> entry:sharedAlternateIDsToCreate.entrySet()){
			for(UpdateAlternateIDSubscriberIDs updated:subscriberIDService.updateAlternateIDsAddOrRemoveSubscriberID(entry.getKey(),new ArrayList<>(entry.getValue().keySet()),stopRequested)){
				entry.getValue().get(updated).updatedOnRedisSecondary(entry.getKey());
			}
		}

		if(log.isTraceEnabled()) log.trace("resolve complete with "+toResolveEvents.size()+" returned events");
		if(!toResolveEvents.isEmpty()) log.error("resolve skipped "+toResolveEvents.size()+" events, this is a bug. "+toResolveEvents);
		return resolvedEvents;
	}

	public static void updateAlternateIDs(List<UpdateAlternateIDSubscriberIDs> updates, SubscriberIDService subscriberIDService, AtomicBoolean stopRequested) {
		if(log.isTraceEnabled()) log.trace("updateAlternateIDs for "+updates.size()+" entries");
		// prepare per alternateID
		Map<AlternateID,List<UpdateAlternateIDSubscriberIDs>> perAlternateIDRequests = new HashMap<>();
		for(UpdateAlternateIDSubscriberIDs update:updates){
			if(update.getAlternateIDValue()!=null && (update.getSubscriberIDToRemove()!=null||update.getSubscriberIDToAdd()!=null)){
				if(log.isTraceEnabled()) log.trace("will update "+ update.getAlternateIDConf().getID()+" "+ update.getAlternateIDValue()+" remove "+update.getSubscriberIDToRemove()+" add "+update.getSubscriberIDToAdd());
				perAlternateIDRequests.computeIfAbsent(update.getAlternateIDConf(),k->new ArrayList<>()).add(update);
			}
		}
		// execute
		for(Map.Entry<AlternateID,List<UpdateAlternateIDSubscriberIDs>> entry:perAlternateIDRequests.entrySet()){
			subscriberIDService.updateAlternateIDsAddOrRemoveSubscriberID(entry.getKey(),entry.getValue(),stopRequested);
		}
	}

	public static final String SUBSCRIBERID_GROUP = "subscriberID";
	public static Long generateNewSubscriberID(){return ZookeeperUniqueKeyServer.get(SUBSCRIBERID_GROUP).getKey();}

}
