package com.evolving.nglm.evolution.uniquekey;

import com.evolving.nglm.evolution.zookeeper.ZookeeperEvolution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ZookeeperUniqueKeyServer {

  private static final Logger log = LoggerFactory.getLogger(ZookeeperUniqueKeyServer.class);

  private String groupID;
  private int globalNodeID;
  private UniqueKeyServer uniqueKeyServer;

  // using singleton per group because we use 1 zookeeper client only
  private final static Map<String,ZookeeperUniqueKeyServer> servers = new HashMap<>();
  public static ZookeeperUniqueKeyServer get(String groupID){
    ZookeeperUniqueKeyServer zookeeperUniqueKeyServer = servers.get(groupID);
    if(zookeeperUniqueKeyServer==null){
      synchronized (ZookeeperUniqueKeyServer.class){
        zookeeperUniqueKeyServer=servers.get(groupID);
        if(zookeeperUniqueKeyServer==null){
          zookeeperUniqueKeyServer=new ZookeeperUniqueKeyServer(groupID);
          servers.put(groupID,zookeeperUniqueKeyServer);
        }
      }
    }
    return zookeeperUniqueKeyServer;
  }

  private ZookeeperUniqueKeyServer(String groupID) {
    this.groupID=groupID;
    this.globalNodeID = ZookeeperEvolution.getZookeeperEvolutionInstance().getGlobalUniqueID(groupID);
    this.uniqueKeyServer = new UniqueKeyServer(this.globalNodeID);
    log.info("ZookeeperUniqueKeyServer init for groupID "+groupID+" with globalNodeID "+this.globalNodeID);
  }

  public long getKey() {
    long startTime = System.nanoTime();
    int uniqueNodeID = ZookeeperEvolution.getZookeeperEvolutionInstance().getGlobalUniqueID(this.groupID);// need to be called on each call to make sure we are still connected to zookeeper
    if(uniqueNodeID!=this.globalNodeID){
      synchronized (ZookeeperUniqueKeyServer.class){
        if(uniqueNodeID!=this.globalNodeID){
          log.info("ZookeeperUniqueKeyServer re-init for groupID "+this.groupID+" with globalNodeID "+uniqueNodeID+ "(previous was "+this.globalNodeID+")");
          this.uniqueKeyServer=new UniqueKeyServer(uniqueNodeID);
          this.globalNodeID = uniqueNodeID;
        }
      }
    }
    long key = this.uniqueKeyServer.getKey();
    if(log.isDebugEnabled()) log.debug("ZookeeperUniqueKeyServer getKey "+key+" generated in "+(System.nanoTime()-startTime)+" ns");
    return key;
  }
  public String getStringKey(){return ""+getKey();}

}