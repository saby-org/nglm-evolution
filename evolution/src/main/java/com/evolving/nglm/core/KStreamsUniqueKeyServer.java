package com.evolving.nglm.core;

import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StreamThread;

import java.util.Map;
import java.util.HashMap;

public class KStreamsUniqueKeyServer {

  private static final Map<Thread,UniqueKeyServer> uniqueKeyServers = new HashMap<>();

  // SHOULD BE CALLED ON KSTREAMS REBALANCING (I did not find a way to force this with dependency injection, cause kafkastream only hold one state change listener)
  public static void streamRebalanced(){
    synchronized (uniqueKeyServers){
      uniqueKeyServers.clear();
    }
  }

  public String getKey() {

    Thread currentThread = Thread.currentThread();
    UniqueKeyServer uniqueKeyServer = uniqueKeyServers.get(currentThread);

    if(uniqueKeyServer==null){

      synchronized (uniqueKeyServers){

        if(!(currentThread instanceof StreamThread)) throw new UnsupportedOperationException("KStreamsUniqueKeyServer requires StreamThread");

        StreamThread streamThread = (StreamThread) currentThread;

        //taking the lowest partition from the lowest topicGroupId
        int smallestTopicGroupId=Integer.MAX_VALUE;
        int smallestPartition=Integer.MAX_VALUE;
        for(TaskId taskId:streamThread.allTasks().keySet()){
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

    }

    return Long.toString(uniqueKeyServer.getKey()); // this is a long value as String, so can be cast to long (as needed for subscriberId, because of how stored in redis)

  }

  private class UniqueKeyServer {

    /*****************************************************************************
    *
    *  long value split as follow:
    *  max long : 922|3372036854|775807
    *  - 3 first digits [0-922] : thread unique id (so max nb of partitions/threads is 922 with this implementation)
    *  - 10 following digits [0-3372036854] : unix epoch in seconds (so max is in 2076)
    *  - 6 following digits [0-775807] : the incrementing counter (so every max 775807, unix epoch digits will be updated)
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
      if(uniqueKeyNode>922) throw new UnsupportedOperationException("KStreamsUniqueKeyServer$UniqueKeyServer number of partitions/streamthreads is greater than 922, not allowed with current implementation");
      this.threadIdPrefix=uniqueKeyNode;
      threadIdAndEpochPrefixPart = (this.threadIdPrefix * 10000000000000000L) + ((System.currentTimeMillis()/1000) * 1000000L);
      incrementingCounterPart=-1;
    }

    private long getKey() {
      incrementingCounterPart++;
      if(incrementingCounterPart>775807) setPrefix(threadIdPrefix);
      return threadIdAndEpochPrefixPart + incrementingCounterPart;
    }
  }
}
