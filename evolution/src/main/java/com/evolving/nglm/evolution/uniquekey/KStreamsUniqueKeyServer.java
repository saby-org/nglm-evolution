package com.evolving.nglm.evolution.uniquekey;

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
      for(TaskId taskId:streamThread.allTasks().keySet()){
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

}
