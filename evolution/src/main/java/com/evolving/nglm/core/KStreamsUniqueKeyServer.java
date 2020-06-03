/*****************************************************************************
*
*  KStreamsUniqueKeyServer.java
*
*  counter bits are as follows:
*  - 1 sign bit
*  - 10 bits partition identifier
*  - 43 bits date
*  - 10 bits extra for counter
*
*  S |--Partn-| |-----------------Date--------------------| |--extra-|
*  + PPPPPPPPPP DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD CCCCCCCCCC
*
*    long uniqueKey = uniqueKeyServer.getKey();
*    log.info("UUID debug: {}:{}:{}", Long.toString(uniqueKey >> 53), Long.toString((uniqueKey >> 10) & 0x7FFFFFFFFFFL), Long.toString(uniqueKey & 0x3FFL));
*
*****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StreamThread;

import java.util.Map;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;

public class KStreamsUniqueKeyServer
{
  /****************************************
  *
  *  attributes
  *
  ****************************************/

  private Map<StreamThread,UniqueKeyServer> context;

  /****************************************
  *
  *  constructor
  *
  ****************************************/

  public KStreamsUniqueKeyServer()
  {
    this.context = new HashMap<StreamThread,UniqueKeyServer>();
  }

  /****************************************
  *
  *  getKey
  *
  ****************************************/

  public synchronized long getKey()
  {
    /*****************************************
    *
    *  uniqueKeyNode
    *
    *****************************************/

    //
    //  validate on StreamThread
    //

    if (! (Thread.currentThread() instanceof StreamThread)) throw new UnsupportedOperationException("KStreamsUniqueKeyServer requires StreamThread");

    //
    //  partitionMap
    //

    StreamThread streamThread = (StreamThread) Thread.currentThread();
    SortedMap<Integer,Integer> partitionMap = new TreeMap<Integer,Integer>();
    for (TaskId taskID : streamThread.tasks().keySet())
      {
        if (partitionMap.get(taskID.topicGroupId) != null && taskID.partition < partitionMap.get(taskID.topicGroupId))
          partitionMap.put(taskID.topicGroupId, taskID.partition);
        else if (partitionMap.get(taskID.topicGroupId) == null)
          partitionMap.put(taskID.topicGroupId, taskID.partition);
      }

    //
    //  validate -- all topicGroupIDs co-partitioned?
    //

    Integer uniqueKeyNode = null;
    if (partitionMap.size() > 0)
      {
        int topicGroupID = partitionMap.firstKey().intValue();
        int partition = partitionMap.get(topicGroupID).intValue();
        uniqueKeyNode = 60 * topicGroupID + partition;
      }
    if (uniqueKeyNode == null) throw new UnsupportedOperationException("KStreamsUniqueKeyServer.getKey() unable to identify uniqueKeyCode: " + partitionMap);

    /*****************************************
    *
    *  uniqueKeyServer
    *
    *****************************************/

    UniqueKeyServer uniqueKeyServer = context.get(streamThread);
    if (uniqueKeyServer == null || uniqueKeyServer.getUniqueKeyNode() != uniqueKeyNode.intValue())
      {
        uniqueKeyServer = new UniqueKeyServer(uniqueKeyNode.intValue());
        context.put(streamThread, uniqueKeyServer);
      }

    //
    //  return
    //

    return uniqueKeyServer.getKey();
  }
  
  /****************************************
  *
  *  class UniqueKeyServer
  *
  ****************************************/
  
  private class UniqueKeyServer
  {
    //
    //  data
    //
    
    private int uniqueKeyNode;
    private long epoch;
    private long counter;

    //
    //  constructor
    //

    private UniqueKeyServer(int uniqueKeyNode)
    {
      this.uniqueKeyNode = uniqueKeyNode;
      this.epoch = (((long) uniqueKeyNode) << 53) | (System.currentTimeMillis() << 10);
      this.counter = epoch;
    }

    //
    //  accessors
    //

    private int getUniqueKeyNode() { return uniqueKeyNode; }
    private long getEpoch() { return epoch; }

    //
    //  getKey  
    //
    
    private long getKey()
    {
      counter += 1;
      return counter;
    }
  }
}
