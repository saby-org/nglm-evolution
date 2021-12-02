package com.evolving.nglm.evolution.uniquekey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/*****************************************************************************
 *  max long is  : 922|3372036854|775807
 *  max key  is  : 921|9999999999|999999
 *  - 3 first digits [0-921] : prefix uniqueID, max is 921
 *  - 10 following digits [0-9999999999] : unix epoch in seconds
 *  - 6 following digits [0-999999] : incrementing counter, so AROUND every 1,000,000 keys unix epoch is set again
 *
 *  we can not exceed 1,000,000 keys per sec with this implementation, we do sleep and warn if this happens
 *  if server time is going backward, this can lead to issue
 *
 *  this implementation is Thread Safe
 ******************************************************************************/
public class UniqueKeyServer {

	private static final long EPOCH_PADDING = 1_000_000L;
	private static final long NODEID_PADDING = EPOCH_PADDING * 10_000_000_000L;

	private static final Logger log = LoggerFactory.getLogger(UniqueKeyServer.class);

	private int nodeUniqueID;

	private AtomicLong counter=new AtomicLong();
	private long epoch=0;//hold the epoch in seconds
	private AtomicLong maxValue;//hold the max value counter can reach before rolling epoch

	public UniqueKeyServer(int nodeUniqueID) {
		if(nodeUniqueID>921) throw new UnsupportedOperationException("UniqueKeyServer  nodeUniqueID is greater than 921, not allowed with current implementation");
		this.nodeUniqueID=nodeUniqueID;
		rollEpoch();
	}

	private synchronized void rollEpoch(){
		if(epoch!=0 && getCounterEpoch()==epoch) return;//one other thread already did it
		long lastEpoch = this.epoch;
		this.epoch = (new Date().getTime())/1000;
		while(this.epoch<=lastEpoch){
			log.warn("UniqueKeyServer seems more than 1,000,000 keys generated in less than 1 second, current implementation can not handle that, we are sleeping 1 second");
			try {Thread.sleep(1000L);} catch (InterruptedException e) {}
			this.epoch = (new Date().getTime())/1000;
		}
		long newCounterValue = this.nodeUniqueID*NODEID_PADDING + this.epoch*EPOCH_PADDING;
		this.maxValue = new AtomicLong(newCounterValue + EPOCH_PADDING - 1 );
		this.counter.set(newCounterValue);
		log.info("UniqueKeyServer "+Thread.currentThread().getName()+" rollEpoch done, counter at "+counter.get());
	}

	private long getCounterEpoch(){
		return (counter.get() % NODEID_PADDING)/EPOCH_PADDING;
	}

	public long getKey() {
		long toRet = counter.getAndIncrement();
		if(toRet>maxValue.get()){
			rollEpoch();
			return getKey();
		}
		return toRet;
	}

}
