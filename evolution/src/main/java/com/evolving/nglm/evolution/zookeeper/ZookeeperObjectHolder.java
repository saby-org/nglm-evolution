package com.evolving.nglm.evolution.zookeeper;

import org.apache.zookeeper.data.Stat;

public class ZookeeperObjectHolder<T extends ZookeeperStoredObject<T>>{

	// the reason we have a ZookeeperObjectHolder, we keep the zookeeper stat version with it
	private Stat stat;
	// the object itself
	private T object;

	protected T get(){return object;}
	protected Stat getStat(){return stat;}

	// construct with data from zookeeper
	protected ZookeeperObjectHolder(Stat stat, byte[] zookeeperData, T initialObject){
		this.stat=stat;
		this.object=zookeeperData==null?null:initialObject.getFromZookeeperData(zookeeperData);
	}

}
