package com.evolving.nglm.evolution;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.SubscriberIDService;

public class SingletonServices {

	// singleton lazy init holder
	private static class SubscriberIDServiceHolder {
		private static final SubscriberIDService INSTANCE = new SubscriberIDService(Deployment.getRedisSentinels(), "singleton", true);
	}
	public static SubscriberIDService getSubscriberIDService() {return SubscriberIDServiceHolder.INSTANCE;}

}
