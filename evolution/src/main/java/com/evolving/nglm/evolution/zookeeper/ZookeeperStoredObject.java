package com.evolving.nglm.evolution.zookeeper;

public abstract class ZookeeperStoredObject<T> {

	protected abstract T getFromZookeeperData(byte[] zookeeperData);
	protected abstract byte[] getFromObject();
	protected abstract T updateAtomic(T storedInZookeeperObject);
	protected abstract void onUpdateAtomicSucces();
}
