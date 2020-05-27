package com.evolving.nglm.evolution.propensity;

import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.PropensityRule;
import com.evolving.nglm.evolution.SubscriberGroupEpoch;
import com.evolving.nglm.evolution.SubscriberProfile;
import com.evolving.nglm.evolution.zookeeper.Configuration;
import com.evolving.nglm.evolution.zookeeper.Node;
import com.evolving.nglm.evolution.zookeeper.ZookeeperEvolutionClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PropensityService {

	private static final Logger log = LoggerFactory.getLogger(PropensityService.class);

	private static final long READER_PERIOD_MS = 5000L;
	private static final long WRITER_PERIOD_MS = 5000L;

	// key is offerID, only one map for the jvm should be better
	private static volatile Map<String,PropensityOffer> propensities = new ConcurrentHashMap<>();
	// to avoid to write to zookeeper if there is nothing to
	private static volatile boolean changesToFlushToZookeeper = false;
	// only one Timer background thread to synchronized data with zookeeper as well
	private final static Timer zookeeperDataSynchronizer = new Timer("propensity-zookeeper-sync",true);
	// only used by previous timer, one instance for the jvm as well
	private volatile static ZookeeperEvolutionClient zookeeperEvolutionClient;
	// checking the code this sounds like thread safe as well, putting it static
	private static ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;


	public PropensityService(ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader){

		log.info("PropensityService : creating new propensity service");

		// lazy init
		synchronized (zookeeperDataSynchronizer){
			if(zookeeperEvolutionClient==null){

				log.info("PropensityService : initializing shared resources");

				this.zookeeperEvolutionClient = new ZookeeperEvolutionClient();
				this.subscriberGroupEpochReader = subscriberGroupEpochReader;

				// if dont exist
				zookeeperEvolutionClient.createPersistentNodeIfNotExists(Configuration.NODE.PROPENSITY.node());

				// a first one blocking call to init from zookeeper
				log.info("PropensityService : loading from zookeeper");
				updateDataFromZookeeper();

				// then starting the background task that will handle synchro with zookeeper data

				// the reader
				log.info("PropensityService : scheduling reading from zookeeper every "+READER_PERIOD_MS+" ms");
				zookeeperDataSynchronizer.schedule(new TimerTask() {
					@Override
					public void run() {
						updateDataFromZookeeper();
					}
				},READER_PERIOD_MS,READER_PERIOD_MS);

				// the writer
				log.info("PropensityService : scheduling writing to zookeeper every "+WRITER_PERIOD_MS+" ms");
				zookeeperDataSynchronizer.schedule(new TimerTask() {
					@Override
					public void run() {
						flushLocalDataToZookeeper();
					}
				},WRITER_PERIOD_MS,WRITER_PERIOD_MS);

				// add a shutdown hook to force a flush of data on stop
				NGLMRuntime.addShutdownHook(notUsed-> {
						flushLocalDataToZookeeper();
						zookeeperEvolutionClient.close();
					}
				);
			}
		}

	}

	public void incrementPropensity(String offerID, SubscriberProfile subscriberProfile, boolean presented, boolean accepted){

		if(log.isDebugEnabled()) log.debug("PropensityService.incrementPropensity : for offer "+offerID);

		List<String> stratum = getSubscriberStratum(subscriberProfile);

		PropensityOffer propensityOffer = propensities.get(offerID);
		if(propensityOffer==null){
			synchronized (propensities){
				propensityOffer = propensities.get(offerID);
				if(propensityOffer==null){
					propensityOffer=new PropensityOffer(stratum);
					propensities.put(offerID,propensityOffer);
				}
			}
		}
		PropensityState propensityState = propensityOffer.getPropensityStateForStratum(stratum);
		if(presented){
			if(log.isDebugEnabled()) log.debug("PropensityService.incrementPropensity : will increment presented");
			propensityState.incrementLocalPresentationCount();
		}
		if(accepted){
			if(log.isDebugEnabled()) log.debug("PropensityService.incrementPropensity : will increment accepted");
			propensityState.incrementLocalAcceptanceCount();
		}

		if(!changesToFlushToZookeeper) changesToFlushToZookeeper=true;//notify that changed happened that will need to be write to zookeeper

	}

	public double getPropensity(String offerID, SubscriberProfile subscriberProfile, double initialPropensity, Date effectiveStartDate, int presentationThreshold, int daysThreshold){

		if(log.isDebugEnabled()) log.debug("PropensityService.getPropensity : for offerID "+offerID+" and subscriberID "+subscriberProfile.getSubscriberID());

		List<String> stratum = getSubscriberStratum(subscriberProfile);
		if(log.isDebugEnabled()) log.debug("PropensityService.getPropensity : subscriberID stratum is "+stratum);

		PropensityOffer propensityOffer = propensities.get(offerID);
		if(propensityOffer==null){
			synchronized (propensities){
				propensityOffer = propensities.get(offerID);
				if(propensityOffer==null){
					propensityOffer=new PropensityOffer(stratum);
					propensities.put(offerID,propensityOffer);
				}
			}
		}
		PropensityState propensityState = propensityOffer.getPropensityStateForStratum(stratum);
		if(log.isDebugEnabled()) log.debug("PropensityService.getPropensity : will compute with propensityState "+propensityState);

		double toRet = propensityState.getPropensity(initialPropensity,effectiveStartDate,presentationThreshold,daysThreshold);
		if(log.isDebugEnabled()) log.debug("PropensityService.getPropensity : will return "+toRet);
		return toRet;
	}

	private List<String> getSubscriberStratum(SubscriberProfile subscriberProfile){

		// Construct the propensity stratum from the subscriber profile
		// Segments will be arranged in the same order than the one specified in Propensity Rule

		List<String> toRet = new LinkedList<>();
		PropensityRule propensityRule = Deployment.getPropensityRule();
		List<String> dimensions = propensityRule.getSelectedDimensions();
		Map<String, String> subscriberGroups = subscriberProfile.getSegmentsMap(subscriberGroupEpochReader);

		for(String dimensionID : dimensions) {
			String segmentID = subscriberGroups.get(dimensionID);
			if(segmentID != null) {
				toRet.add(segmentID);
			}
			else {
				// This should not happen
				throw new IllegalStateException("Required dimension (dimensionID: " + dimensionID + ") for propensity could not be found in the subscriber profile (subscriberID: " + subscriberProfile.getSubscriberID() + ").");
			}
		}

		return toRet;

	}

	// the job writing local data to zookeeper
	private void flushLocalDataToZookeeper(){

		long startTime = SystemTime.currentTimeMillis();
		if(log.isDebugEnabled()) log.debug("PropensityService.flushLocalDataToZookeeper : starting");

		if(!changesToFlushToZookeeper){
			if(log.isDebugEnabled()) log.debug("PropensityService.flushLocalDataToZookeeper : no changes happened, nothing to flush");
			return;
		}

		changesToFlushToZookeeper=false;

		synchronized (zookeeperDataSynchronizer){
			for(Map.Entry<String,PropensityOffer> entry:propensities.entrySet()){

				if(log.isDebugEnabled()) log.debug("PropensityService.flushLocalDataToZookeeper : will flush propensity data for offer "+entry.getKey());

				// create node for this offerId if does not exists
				zookeeperEvolutionClient.createPersistentNodeWithDataIfNotExists(getOfferPropensityNode(entry.getKey()),entry.getValue());
				zookeeperEvolutionClient.atomicUpdate(getOfferPropensityNode(entry.getKey()),entry.getValue());
			}
		}

		long duration_ms = SystemTime.currentTimeMillis() - startTime;
		if(log.isDebugEnabled()) log.debug("PropensityService.flushLocalDataToZookeeper : ends in "+duration_ms+" ms");
	}

	// the job updating propensity with global data from  zookeeper
	private void updateDataFromZookeeper(){

		long startTime = SystemTime.currentTimeMillis();
		if(log.isDebugEnabled()) log.debug("PropensityService.updateDataFromZookeeper : starting");
		PropensityOffer initialObject = new PropensityOffer();//need one object kind to read from zookeeper

		synchronized (zookeeperDataSynchronizer){
			for(Node node:zookeeperEvolutionClient.getChildren(Configuration.NODE.PROPENSITY.node())){
				String offerID = getOfferID(node);
				if(log.isDebugEnabled()) log.debug("PropensityService.updateDataFromZookeeper : will get data from zookeeper for offer "+offerID);
				PropensityOffer storedPropensityOffer = zookeeperEvolutionClient.read(node,initialObject);

				PropensityOffer cachedPropensityOffer = propensities.get(offerID);
				if(cachedPropensityOffer==null){
					synchronized (propensities){
						cachedPropensityOffer = propensities.get(offerID);
						if(cachedPropensityOffer==null){
							propensities.put(offerID,storedPropensityOffer);
							continue;
						}
					}
				}
				// here we can not just replace object, we would be erasing the local not yet committed to zookeeper counters
				cachedPropensityOffer.updatePropensityFromZookeeper(storedPropensityOffer);
			}
		}

		long duration_ms = SystemTime.currentTimeMillis() - startTime;
		if(log.isDebugEnabled()) log.debug("PropensityService.updateDataFromZookeeper : ends in "+duration_ms+" ms");

	}

	// utils to transform to/from zookeeper node name to/from offerID
	private final static String OFFER_NODE_PREFIX = "offer-";
	private Node getOfferPropensityNode(String offerID){
		return Configuration.NODE.PROPENSITY.node().newChild(OFFER_NODE_PREFIX+offerID);
	}
	private String getOfferID(Node node){
		return node.nodeName().replaceFirst(OFFER_NODE_PREFIX,"");
	}

}
