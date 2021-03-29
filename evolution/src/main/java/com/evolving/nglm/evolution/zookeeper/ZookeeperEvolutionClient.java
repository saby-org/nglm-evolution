package com.evolving.nglm.evolution.zookeeper;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.Pair;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static com.evolving.nglm.evolution.zookeeper.Configuration.NODE;

public class ZookeeperEvolutionClient {

	private static final Logger log = LoggerFactory.getLogger(ZookeeperEvolutionClient.class);

	private static final long THREAD_SLEEP_BEFORE_RETRY_MS = 100;

	// the zookeeper connection, we keep it internal avoiding low level code around zookeeper outside here
	private ZooKeeper zookeeper;
	// hold the on hold locks if any (key lock name, value real node name in zookeeper)
	private Map<String, String> onHoldLocks;
	// keep list of persistent node already created (or already checked "is exists") to avoid always calling zookeeper to check
	private List<String> persistentNodesAlreadyCreated;

	public ZookeeperEvolutionClient(){
		onHoldLocks = new ConcurrentHashMap<>();
		persistentNodesAlreadyCreated = new CopyOnWriteArrayList<>();
		// just reset the "already exist" cache every minute (hope thats enough, it seems not possible to put a watcher only on delete event)
		new Timer("zookeeper-nodes-cache-cleaner",true).schedule(new TimerTask() {
			@Override
			public void run() {
				if(log.isDebugEnabled()) log.debug("zookeeper-nodes-cache-cleaner : clearing cache");
				persistentNodesAlreadyCreated.clear();
			}
		},60000,60000);
		// object hold a connected zookeeper client
		ensureConnected();
		createPersistentNodeIfNotExists(NODE.LOCKS.node());
	}

	// this is a closable resource as is the underlying zookeeper client
	public void close(){
		if(zookeeper!=null)
		try {
			zookeeper.close();
		} catch (InterruptedException e) {}
	}

	// create a node if not exist
	public void createPersistentNodeIfNotExists(Node node){
		createPersistentNodeWithDataIfNotExists(node,new byte[0]);
	}

	// create a node with data if not exist
	public <T extends ZookeeperStoredObject<T>> void createPersistentNodeWithDataIfNotExists(Node node, T object){
		createPersistentNodeWithDataIfNotExists(node,object.getFromObject());
	}

	private void createPersistentNodeWithDataIfNotExists(Node node, byte[] data){

		if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.createPersistentNodeWithDataIfNotExists : will create node if not exists "+node);

		if(nodeExists(node)){
			if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.createPersistentNodeWithDataIfNotExists : already exists "+node);
			return;
		}

		while(true){
			try {
				ensureConnected();
				String nodeCreated = zookeeper.create(node.fullPath(),data, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
				if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.createPersistentNodeWithDataIfNotExists : created "+nodeCreated);
				return;
			} catch (KeeperException.NodeExistsException e) {
				log.info("ZookeeperEvolutionClient.createPersistentNodeWithDataIfNotExists : node already exist "+node);
				return;
			} catch (KeeperException e) {
				log.error("ZookeeperEvolutionClient.createPersistentNodeWithDataIfNotExists : exception doing "+node,e);
				return;
			} catch (InterruptedException e) {
				log.warn("ZookeeperEvolutionClient.createPersistentNodeWithDataIfNotExists : exception doing "+node,e);
			}

			try {
				// error happened will give a CPU release
				Thread.sleep(THREAD_SLEEP_BEFORE_RETRY_MS);
			} catch (InterruptedException e) {}
		}

	}

	// delete a node if not exist
	public void deletePersistentNodeIfNotExists(Node node){
		if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.deletePersistentNodeIfNotExists : will delete if exists "+node);
		if(!nodeExists(node)){
			if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.deletePersistentNodeIfNotExists : node does not exists "+node);
			return;
		}
		while(true){
			ensureConnected();
			try {
				zookeeper.delete(node.fullPath(),-1);
				if(persistentNodesAlreadyCreated.contains(node.fullPath())) persistentNodesAlreadyCreated.remove(node.fullPath());
				return;
			} catch (KeeperException e) {
				log.error("ZookeeperEvolutionClient.deletePersistentNodeIfNotExists : exception doing "+node,e);
				return;
			} catch (InterruptedException e) {
				log.warn("ZookeeperEvolutionClient.deletePersistentNodeIfNotExists : exception doing "+node,e);
			}

			try {
				// error happened will give a CPU release
				Thread.sleep(THREAD_SLEEP_BEFORE_RETRY_MS);
			} catch (InterruptedException e) {}
		}
	}

	// get an env global lock
	// released on call releaseLock(lockName) or if our zookeeper connection die
	// please note we can successfully get a lock but loosing it if zookeeper connection is lost and we reconnect it (so hasLock should be called to be sure)
	// please note LOCK IS NOT ENFORCED BY ZOOKEEPER ITSELF, IT IS UP TO CLIENTS TO USE THIS PARADIGM
	public void getLock(String lock){

		if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.getLock : "+lock);

		// if we already got it return directly
		if(onHoldLocks.get(lock)!=null){
			log.info("ZookeeperEvolutionClient.getLock : "+lock+" already hold");
			return;
		}

		final Node lockRoot = NODE.LOCKS.node().newChild(lock);// the root path lock will be created in
		createPersistentNodeIfNotExists(lockRoot);

		// will block till we got the lock
		while(true){

			ensureConnected();
			// a lock to sync event in function
			Object syncLock = new Object();

			synchronized(syncLock){

				try{
					// create a node named "lock" with sequential numeric suffix
					String myZookeeperLock = zookeeper.create(lockRoot.fullPath()+"/lock", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
					while(true){
						List<String> allWaitingLocks = zookeeper.getChildren(lockRoot.fullPath(), event->{
							if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.getLock : event received "+event);
							synchronized(syncLock){
								syncLock.notifyAll();//unlock waiting process on event arriving
							}
						});
						// check if we got the lock (we say our node being the first of list sorted)
						Collections.sort(allWaitingLocks);
						if(myZookeeperLock.endsWith(allWaitingLocks.get(0))){
							// my lock is now the first in the list I got it
							// save it for the releaseLock call
							if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.getLock : match, lock granted, "+allWaitingLocks.get(0)+" vs "+myZookeeperLock);
							onHoldLocks.put(lock,myZookeeperLock);
							return;
						}else{
							// else we wait for a change in the
							try{
								if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.getLock : does not match, still need to wait, "+allWaitingLocks+", waiting "+myZookeeperLock);
								syncLock.wait();
							}catch(InterruptedException e){/*normal notify*/}
						}
					}
				} catch (KeeperException e) {
					log.error("ZookeeperEvolutionClient.getLock : exception while creating lock "+lock,e);
					return;
				} catch (InterruptedException e) {
					log.warn("ZookeeperEvolutionClient.getLock : exception while creating lock "+lock,e);
				}

				try{
					// error happened will give a CPU release
					syncLock.wait(THREAD_SLEEP_BEFORE_RETRY_MS);
				}catch (InterruptedException e){/*normal notify*/}
			}
			log.info("ZookeeperEvolutionClient.getLock : issue while trying to get lock "+lock+" will retry");
		}
	}

	// to release a lock we got
	public void releaseLock(String lock){

		if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.releaseLock : "+lock);

		if(zookeeper==null || !zookeeper.getState().isAlive()){
			log.info("ZookeeperEvolutionClient.releaseLock : "+lock+" not currently hold");
			onHoldLocks.clear();// we don't have any locks if connection lost
			return;
		}

		// get the zookeeper lock node name
		String zookeeperLockNode=onHoldLocks.get(lock);
		if(zookeeperLockNode==null){
			log.info("ZookeeperEvolutionClient.releaseLock : "+lock+" not currently hold");
			return;
		}

		try{
			onHoldLocks.remove(lock);
			zookeeper.delete(zookeeperLockNode,-1);
			if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.releaseLock : lock released "+lock);
		}catch(KeeperException|InterruptedException e){
			log.info("ZookeeperEvolutionClient.releaseLock : issue while deleting lock "+lock,e);
		}

	}

	// check if client has the lock
	public boolean hasLock(String lock){
		if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.hasLock : "+lock);
		if(onHoldLocks.get(lock)!=null){
			if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.hasLock : has "+lock);
			return true;
		}else{
			if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.hasLock : does not have "+lock);
			return false;
		}
	}

	// connect new one or ensure still connected
	// blocking till not connected
	private void ensureConnected(){

		// if zookeeper was already connected, but has been closed, will close it and recreate one
		if(zookeeper!=null && !zookeeper.getState().isAlive()){
			log.info("ZookeeperEvolutionClient.ensureConnected : closing zookeeper client due to status "+zookeeper.getState());
			try { zookeeper.close(); } catch (InterruptedException e) {}
			zookeeper = null;
			onHoldLocks.clear();// we don't have any locks if we lost connection
		}

		// will block till we do not have a connected zookeeper
		while(zookeeper==null || zookeeper.getState()!=ZooKeeper.States.CONNECTED){

			// zookeeper was created, but state is not connect, should not happens
			if(zookeeper!=null){
				log.info("ZookeeperEvolutionClient.ensureConnected : looping in ensureConnected while zookeeper not null "+zookeeper.getState().toString());
				try{
					zookeeper.close();
					onHoldLocks.clear();// we don't have any locks if connection loose connection
				}catch (InterruptedException e){
					log.info("ZookeeperEvolutionClient.ensureConnected : exception while closing ",e);
				}
			}

			try{
				// this will hold the latch till we receive the event back that zookeeper is well connected
				CountDownLatch connectionWait = new CountDownLatch(1);
				zookeeper = new ZooKeeper(Deployment.getZookeeperConnect(), Configuration.SESSION_TIMEOUT_MS,
						event->{
							if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.ensureConnected : zookeeper connection event received "+event.getState());
							if(event.getState()==Watcher.Event.KeeperState.SyncConnected){
								connectionWait.countDown();
							}
						},
						false);
				// wait for the latch to be released by event the watcher
				connectionWait.await();
			}catch(InterruptedException e){
				// normal notify on connectionWait
			}catch (IOException e){
				log.warn("ZookeeperEvolutionClient.ensureConnected : could not create zookeeper client using "+Deployment.getZookeeperConnect());
			}

		}
		if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.ensureConnected : connected");
	}

	// just  check if a node exists
	public boolean nodeExists(Node node){

		if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.nodeExists : will check if node exists "+node);

		if(persistentNodesAlreadyCreated.contains(node.fullPath())){
			if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.nodeExists : from cache node exists "+node);
			return true;
		}

		while(true){
			try {
				ensureConnected();
				boolean exists = zookeeper.exists(node.fullPath(),false)!=null;
				if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.nodeExists : node "+node+" exists : "+exists);
				if(exists && !persistentNodesAlreadyCreated.contains(node.fullPath())) persistentNodesAlreadyCreated.add(node.fullPath());
				return exists;
			} catch (KeeperException e) {
				log.error("ZookeeperEvolutionClient.nodeExists : exception while checking node exists "+node,e);
				return false;
			}catch (InterruptedException e) {
				log.warn("ZookeeperEvolutionClient.nodeExists : exception while checking node exists "+node,e);
			}

			try {
				// error happened will give a CPU release
				Thread.sleep(THREAD_SLEEP_BEFORE_RETRY_MS);
			} catch (InterruptedException e) {}
		}
	}

	// just  return a list of children
	public List<Node> getChildren(Node node){

		if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.getChildren : will get children for "+node);

		while(true){
			try {
				ensureConnected();
				List<Node> toRet = new ArrayList<>();
				for(String child:zookeeper.getChildren(node.fullPath(),false)){
					if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.getChildren : adding "+child+" to the list of children of "+node);
					toRet.add(node.newChild(child));
				}
				return toRet;
			} catch (KeeperException e) {
				log.error("ZookeeperEvolutionClient.getChildren : exception while checking node exists "+node,e);
				return new ArrayList<>();
			} catch (InterruptedException e) {
				log.warn("ZookeeperEvolutionClient.getChildren : exception while checking node exists "+node,e);
			}

			try {
				// error happened will give a CPU release
				Thread.sleep(THREAD_SLEEP_BEFORE_RETRY_MS);
			} catch (InterruptedException e) {}
		}
	}

	// this will lock, read (to get current version), then write, unlock, return the final result
	public <T extends ZookeeperStoredObject<T>> T write(Node node, T object){
		if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.atomicUpdate : atomic update "+node+" called");
		return atomicWrite(node,object,false);
	}

	// this will lock, read, apply the "atomic update" method of the object, then write, unlock, return the final result
	public <T extends ZookeeperStoredObject<T>> T atomicUpdate(Node node, T object){
		if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.atomicUpdate : atomic update "+node+" called");
		return atomicWrite(node,object,true);
	}

	// this will lock, read, apply the "atomic update" method of the object if called, then write, unlock, return the final result
	private <T extends ZookeeperStoredObject<T>> T atomicWrite(Node node, T object, boolean isUpdate){
		if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.atomicWrite : atomic write "+node+" called");
		String lock = getLockNameAtomicReadWrite(node);
		while(true){
			// get a lock
			getLock(lock);
			// get data
			ZookeeperObjectHolder<T> fromZookeeper = readWithMetaData(node,object);
			// if no object there yet there, this is an error, createPersistentNodeWithDataIfNotExists should be called very first time to store data
			if(fromZookeeper.get()==null){
				releaseLock(lock);
				log.error("ZookeeperEvolutionClient.atomicWrite : called but not data yet stored !");
				throw new IllegalStateException("ZookeeperEvolutionClient.atomicWrite without yet data in zookeeper");
			}
			// check before write call we do still have the lock (LOCK IS NOT ENFORCED BY ZOOKEEPER)
			if(!hasLock(lock)){
				log.warn("ZookeeperEvolutionClient.atomicWrite : we lost lock before writing, we are looping again");
				try{
					// error happened will give a CPU release
					Thread.sleep(THREAD_SLEEP_BEFORE_RETRY_MS);
				}catch (InterruptedException e){/*normal notify*/}
				continue;// start loop again
			}
			T tooStore = isUpdate?object.updateAtomic(fromZookeeper.get()):object;
			Stat write = maybeWriteRaw(node,tooStore.getFromObject(),fromZookeeper.getStat());
			// release the lock
			releaseLock(lock);
			// check if write was OK
			if(write==null){
				log.warn("ZookeeperEvolutionClient.atomicWrite : error while writing, we are looping again");
				try{
					// error happened will give a CPU release
					Thread.sleep(THREAD_SLEEP_BEFORE_RETRY_MS);
				}catch (InterruptedException e){/*normal notify*/}
				continue;// start loop again
			}
			// if OK, call the confirm for an update
			if(isUpdate) object.onUpdateAtomicSucces();
			return tooStore;
		}
	}

	// just read a node and return the object
	public <T extends ZookeeperStoredObject<T>> T read(Node node, T initialObject){
		if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.read : read "+node+" called");
		ZookeeperObjectHolder<T> zookeeperObjectHolder = readWithMetaData(node,initialObject);
		return zookeeperObjectHolder.get();
	}

	// still private, don't expose zookeeper metadata outside here, should not be needed
	private <T extends ZookeeperStoredObject<T>> ZookeeperObjectHolder<T> readWithMetaData(Node node, T initialObject){
		if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.readWithMetaData : read "+node+" called");
		Pair<Stat,byte[]> fromZookeeper = readRaw(node);
		return new ZookeeperObjectHolder<>(fromZookeeper.getFirstElement(),fromZookeeper.getSecondElement(),initialObject);
	}

	// blocking call keep raw one private, expose only once returning object
	private Pair<Stat, byte[]> readRaw(Node node){

		if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.readRaw : read from "+node+" called");

		ensureConnected();
		if(!nodeExists(node)) return new Pair<>(new Stat(),null);

		// blocking call
		while(true){
			try {
				Stat stat=new Stat();
				byte[] rawData=zookeeper.getData(node.fullPath(),false,stat);
				return new Pair<>(stat,rawData);
			} catch (KeeperException|InterruptedException e) {
				log.warn("ZookeeperEvolutionClient.readRaw : error when trying to read file from "+node,e);
			}
			try{
				// error happened will give a CPU release
				Thread.sleep(THREAD_SLEEP_BEFORE_RETRY_MS);
			}catch (InterruptedException e){/*normal notify*/}
		}
	}

	// this one can failed because of concurrency update, we wont allow to use it (otherwise the one with lock could still failed as well)
	private Stat maybeWriteRaw(Node node, byte[] data, Stat stat){

		if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.maybeWriteRaw : write to "+node+" called");

		ensureConnected();
		if(!nodeExists(node)) return null;

		try {
			// if stat givent we use it (so failed can happen if concurrency update happened)
			if(stat==null){
				// otherwise we get the current zookeeper stat (write will still failed if update happens between this and this update)
				stat=new Stat();
				zookeeper.getData(node.fullPath(),false,stat);
			}
			// we do the write
			return zookeeper.setData(node.fullPath(),data,stat.getVersion());
		} catch (KeeperException|InterruptedException e) {
			log.debug("ZookeeperEvolutionClient.maybeWriteRaw : error when trying to write file to "+node,e);
		}
		return null;
	}


	private String getLockNameAtomicReadWrite(Node node){
		return node.toString().replace("/","_");
	}


	// utils that might be needed only for "migration" code (the "fromNode" will be deleted, the "toNode" will be created if not exists
	protected void moveNode(Node fromNode, Node toNode){
		if(log.isDebugEnabled()) log.debug("ZookeeperEvolutionClient.moveNode : will move "+fromNode+" to "+toNode);
		ensureConnected();
		if(nodeExists(fromNode)){
			try {
				Pair<Stat,byte[]> original = readRaw(fromNode);
				byte[] originalData = original.getSecondElement();
				int originalVersion = original.getFirstElement().getVersion();
				createPersistentNodeIfNotExists(toNode);
				Stat stat = new Stat();// new stat for the new file
				maybeWriteRaw(toNode,originalData,stat);
				// and move children as well
				List<String> children=zookeeper.getChildren(fromNode.fullPath(),false);
				for(String child:children){
					Node childFrom=fromNode.newChild(child);
					Node childTo=toNode.newChild(child);
					moveNode(childFrom,childTo);
				}
				// then delete
				log.debug("ZookeeperEvolutionClient.moveNode : deleting "+fromNode);
				zookeeper.delete(fromNode.fullPath(),originalVersion);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}else{
			log.info("ZookeeperEvolutionClient.moveNode : node from "+fromNode+" does not exist, nothing to do");
		}
	}

}
