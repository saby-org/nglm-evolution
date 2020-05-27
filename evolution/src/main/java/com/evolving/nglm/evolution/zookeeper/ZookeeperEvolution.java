package com.evolving.nglm.evolution.zookeeper;

import com.evolving.nglm.core.NGLMRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperEvolution {

	private static final Logger log = LoggerFactory.getLogger(ZookeeperEvolution.class);

	// lazy instantiate (not all process would need it), using inner class holder
	private static class ZookeeperEvolutionSingletonHolder{
		// real instance (in inner class, so class loader will do the lazy init)
		private final static ZookeeperEvolutionClient INSTANCE = new ZookeeperEvolutionClient();
		static {
			NGLMRuntime.addShutdownHook(notused->INSTANCE.close());
		}
	}
	// get the one singleton instance
	public static ZookeeperEvolutionClient getZookeeperEvolutionInstance(){
		return ZookeeperEvolutionSingletonHolder.INSTANCE;
	}

	// check the zookeeper server env is well init/installed
	private static volatile boolean envInit = false;
	// DO NOT USE THAT FOR NOW
	synchronized public static void initServerEnv(){
		if(!envInit){

			log.info("ZookeeperEvolution.initServerEnv : will init zookeeper server env");

			// create all env nodes :
			for(Configuration.NODE node: Configuration.NODE.values()){
				if(log.isDebugEnabled()) log.debug("ZookeeperEvolution.initServerEnv : will check if need to create "+node.node());
				getZookeeperEvolutionInstance().createPersistentNodeIfNotExists(node.node());
			}

			// migration code
			migrate();

			envInit=true;
		}
	}
	// the migration code only if needed ONLY CALLED IN initServerEnv()
	private static void migrate(){
		// voucher change path : can be removed once all customer are in 2.4.X:TODO
		getZookeeperEvolutionInstance().moveNode(Configuration.NODE.NGLM_ROOT.node().newChild("voucherPersonalImport"), Configuration.NODE.VOUCHER_PERSONNAL_IMPORT.node());
	}



}
