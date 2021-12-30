package com.evolving.nglm.evolution.job;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {

	private String name;
	private boolean daemon;
	private AtomicInteger nb = new AtomicInteger(0);

	public NamedThreadFactory(String name, boolean daemon){
		this.name = name;
		this.daemon = daemon;
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread thread = new Thread(r,this.name+"-"+this.nb.get());
		thread.setDaemon(daemon);
		nb.incrementAndGet();
		return thread;
	}
}
