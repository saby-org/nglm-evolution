package com.evolving.nglm.evolution.event;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.core.SubscriberIDService;
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

	public static List<ExternalEvent> resolve(List<ExternalEvent> externalEvents, SubscriberIDService subscriberIDService, AtomicBoolean stopRequested) throws SubscriberIDService.SubscriberIDServiceException, InterruptedException {
		return resolve(1,externalEvents,subscriberIDService,stopRequested);
	}
	// this resolve the subscriberID for the existing entry, or create if not existing and needed
	private static List<ExternalEvent> resolve(int recursiveOccurrence, List<ExternalEvent> externalEvents, SubscriberIDService subscriberIDService, AtomicBoolean stopRequested) throws SubscriberIDService.SubscriberIDServiceException, InterruptedException {

		if(log.isTraceEnabled()) log.trace("resolve called for "+externalEvents.size()+" events");

		List<ExternalEvent> toResolveEvents = new ArrayList<>(externalEvents);
		List<ExternalEvent> resolvedEvents = new ArrayList<>(externalEvents.size());

		// prepare redis resolution per batch of different alternateID returned
		HashMap<AlternateID, Set<String>> alternateIDValues = new HashMap<>();
		Iterator<ExternalEvent> iterator = toResolveEvents.iterator();
		while(iterator.hasNext()){
			ExternalEvent externalEvent = iterator.next();
			if(externalEvent.getSubscriberIDLong()!=null){
				// check if nothing to do
				if(log.isTraceEnabled()) log.trace("resolve subscriberID "+externalEvent.getSubscriberIDLong()+" known, doing nothing");
				resolvedEvents.add(externalEvent);
				iterator.remove();
			}else{
				// prepare for resolution query
				AlternateID alternateID = externalEvent.getAlternateID().getFirstElement();
				alternateIDValues.computeIfAbsent(alternateID, k ->new HashSet<>()).add(externalEvent.getAlternateID().getSecondElement());
			}
		}

		if(log.isTraceEnabled()) log.trace("resolve needed for "+toResolveEvents.size()+" events");

		// resolve each batch
		for(Map.Entry<AlternateID,Set<String>> alternateIDEntry:alternateIDValues.entrySet()){
			if(log.isTraceEnabled()) log.trace("resolve checking resolution for "+alternateIDEntry.getKey().getName()+" with "+alternateIDEntry.getValue().size()+" elements");
			Map<String, Pair<Long,Integer>> alternateIDs = subscriberIDService.getSubscriberIDsBlocking(alternateIDEntry.getKey().getName(),alternateIDEntry.getValue(),stopRequested);
			iterator = toResolveEvents.iterator();
			while(iterator.hasNext()){
				ExternalEvent externalEvent = iterator.next();
				if(externalEvent.getAlternateID().getFirstElement()!=alternateIDEntry.getKey()) continue;// not the same alternateID batch resolution
				Pair<Long,Integer> resolvedSubscriberID = alternateIDs.get(externalEvent.getAlternateID().getSecondElement());
				// not existing
				if(resolvedSubscriberID==null){
					if(externalEvent.getTenantID()==null){
						//not for creation doing nothing
						if(log.isTraceEnabled()) log.trace("resolve "+externalEvent.getSubscriberForLogging()+" does not exist, not marked for creation, doing nothing");
						iterator.remove();
						continue;
					}
				}else{
					// subscriberID resolved
					if(log.isTraceEnabled()) log.trace("resolve "+externalEvent.getSubscriberForLogging()+" to "+resolvedSubscriberID.getFirstElement()+":"+resolvedSubscriberID.getSecondElement());
					externalEvent.resolve(resolvedSubscriberID);
					resolvedEvents.add(externalEvent);
					iterator.remove();
				}
			}
		}

		if(log.isTraceEnabled()) log.trace("resolve creation needed for "+toResolveEvents.size()+" events");

		// prepare redis creation per batch of different alternateID returned
		HashMap<AlternateID, HashMap<String/*alternateIDValue*/,Pair<Integer/*tenantID*/,Set<Long/*subscriberIDValue*/>>>> alternateIDsToCreate = new HashMap<>();
		for(ExternalEvent externalEvent:toResolveEvents){
			// prepare for resolution query
			AlternateID alternateID = externalEvent.getAlternateID().getFirstElement();
			if(externalEvent.getSubscriberIDToAssign()==null) externalEvent.setSubscriberIDToAssign(generateNewSubscriberID());
			alternateIDsToCreate.computeIfAbsent(alternateID, k ->new HashMap<>()).put(externalEvent.getAlternateID().getSecondElement(),new Pair<>(externalEvent.getTenantID(), Collections.singleton(externalEvent.getSubscriberIDToAssign())));
		}
		// create redis mapping if not exist
		for(Map.Entry<AlternateID, HashMap<String/*alternateIDValue*/,Pair<Integer/*tenantID*/,Set<Long>/*subscriberIDs*/>>> perAlternateID:alternateIDsToCreate.entrySet()){
			if(subscriberIDService.putAlternateIDs(perAlternateID.getKey(), perAlternateID.getValue(), true, stopRequested)){
				// created with our values
				iterator = toResolveEvents.iterator();
				while(iterator.hasNext()){
					ExternalEvent externalEvent = iterator.next();
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

		if(log.isTraceEnabled()) log.trace("resolve complete with "+toResolveEvents.size()+" returned events");
		if(!toResolveEvents.isEmpty()) log.error("resolve skipped "+toResolveEvents.size()+" events, this is a bug. "+toResolveEvents);
		return resolvedEvents;
	}

	//TODO: need to do it right, this is a very quick way
	// this delete the alternateID/susbcriberID entry
	public static void delete(List<SubscriberIDAlternateID> toDelete, SubscriberIDService subscriberIDService, AtomicBoolean stopRequested) throws SubscriberIDService.SubscriberIDServiceException, InterruptedException {
		if(log.isTraceEnabled()) log.trace("resolve delete for "+toDelete.size()+" entries");
		for(SubscriberIDAlternateID subscriberIDAlternateID:toDelete){
			if(subscriberIDAlternateID.getAlternateIDValue()!=null){
				if(log.isTraceEnabled()) log.trace("deleting "+subscriberIDAlternateID.getAlternateIDConf().getID()+" "+subscriberIDAlternateID.getAlternateIDValue());
				subscriberIDService.deleteRedisStraightAlternateID(subscriberIDAlternateID.getAlternateIDConf().getID(),subscriberIDAlternateID.getAlternateIDValue());
			}
		}
	}

	public static final String SUBSCRIBERID_GROUP = "subscriberID";
	public static Long generateNewSubscriberID(){return ZookeeperUniqueKeyServer.get(SUBSCRIBERID_GROUP).getKey();}

}
