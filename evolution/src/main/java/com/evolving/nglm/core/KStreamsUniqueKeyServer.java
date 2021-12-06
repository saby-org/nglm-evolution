package com.evolving.nglm.core;

import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KStreamsUniqueKeyServer {

  private static final Logger log = LoggerFactory.getLogger(KStreamsUniqueKeyServer.class);

  private static final Map<Thread,UniqueKeyServer> uniqueKeyServers = new ConcurrentHashMap<>();

  // SHOULD BE CALLED ON KSTREAMS REBALANCING (I did not find a way to force this with dependency injection, cause kafkastream only hold one state change listener)
  public static void streamRebalanced(){
    log.info("KStreamsUniqueKeyServer streamRebalanced called");
    uniqueKeyServers.clear();
  }

  public String getKey() {

    Thread currentThread = Thread.currentThread();
    UniqueKeyServer uniqueKeyServer = uniqueKeyServers.get(currentThread);

    if(uniqueKeyServer==null){

      if(!(currentThread instanceof StreamThread)) throw new UnsupportedOperationException("KStreamsUniqueKeyServer requires StreamThread");

      StreamThread streamThread = (StreamThread) currentThread;

      //taking the lowest partition from the lowest topicGroupId
      int smallestTopicGroupId=Integer.MAX_VALUE;
      int smallestPartition=Integer.MAX_VALUE;
      for(TaskId taskId:streamThread.tasks().keySet()){
        if(log.isDebugEnabled()) log.debug("KStreamsUniqueKeyServer "+Thread.currentThread().getName()+" checking "+taskId+" for init key");
        if(taskId.topicGroupId<smallestTopicGroupId){
          smallestTopicGroupId=taskId.topicGroupId;
        }
        if(taskId.topicGroupId==smallestTopicGroupId){
          if(taskId.partition<smallestPartition){
            smallestPartition=taskId.partition;
          }
        }
      }

      if(smallestPartition==Integer.MAX_VALUE) throw new UnsupportedOperationException("KStreamsUniqueKeyServer getKey called while stream not yet started");

      uniqueKeyServer = new UniqueKeyServer(smallestPartition);
      uniqueKeyServers.put(currentThread,uniqueKeyServer);

    }

    return Long.toString(uniqueKeyServer.getKey()); // this is a long value as String, so can be cast to long (as needed for subscriberId, because of how stored in redis)

  }

  private class UniqueKeyServer {

    /*****************************************************************************
    *
    *  long value split as follow:
    *  max long : 922|3372036854|775807
    *  max key  : 921|9999999999|999999
    *  - 3 first digits [0-921] : thread unique id (so max nb of partitions/threads is 921 with this implementation)
    *  - 10 following digits [0-9999999999] : unix epoch in seconds
    *  - 6 following digits [0-999999] : the incrementing counter (so every max 1 0000 000, unix epoch digits will be updated)
    *
    ******************************************************************************/

    private int threadIdPrefix;
    private int incrementingCounterPart;

    // hold threadIdPrefixPart+epochPrefixPart
    private long threadIdAndEpochPrefixPart;

    private UniqueKeyServer(int uniqueKeyNode) {
      setPrefix(uniqueKeyNode);
    }

    private void setPrefix(int uniqueKeyNode){
      if(uniqueKeyNode>921) throw new UnsupportedOperationException("KStreamsUniqueKeyServer$UniqueKeyServer number of partitions/streamthreads is greater than 921, not allowed with current implementation");
      this.threadIdPrefix=uniqueKeyNode;
      threadIdAndEpochPrefixPart = (this.threadIdPrefix * 10000000000000000L) + ((System.currentTimeMillis()/1000) * 1000000L);
      incrementingCounterPart=-1;
      log.info("KStreamsUniqueKeyServer "+Thread.currentThread().getName()+" init with "+threadIdPrefix+", "+threadIdAndEpochPrefixPart+", "+incrementingCounterPart);
    }

    private long getKey() {
      incrementingCounterPart++;
      if(incrementingCounterPart>999999) setPrefix(threadIdPrefix);
      return threadIdAndEpochPrefixPart + incrementingCounterPart;
    }
  }
}
