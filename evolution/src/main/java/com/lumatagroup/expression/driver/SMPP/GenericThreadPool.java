package com.lumatagroup.expression.driver.SMPP;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A bounded general purpose thread pool implementation
 * <p>Inspired from org.pnuts.multithread.ThreadPool
 * @author fduclos
 *
 */
public class GenericThreadPool implements Runnable
{
	private static Logger logger = LoggerFactory.getLogger(GenericThreadPool.class);
	
	public static final int DEFAULT_THREAD_NUMBER = 5;
	public static final int DEFAULT_DELAY_BEFORE_RETRY = 20;
	
   /**
     * The priority of the threads created by pool.
     */
    protected int priority = Thread.NORM_PRIORITY;
	
    /**
     * A flag indicating whether the pool should create daemon threads.
     */
    protected boolean isDaemon = false;

    protected String name = null;
	
	/** The set of threads in the pool. */
    protected Set<GenericWorkerThread> threads = null;
	
	/** 
	* The remaining Runnable, which are waiting for the thread pool. 
	* <p> Runnable are pushed at the bottom of the queue and used from the top.
	*/
    protected Queue requests = null;
	
    protected int numberOfWorkingThread = 0;
	
    protected long delayBeforeRetry = DEFAULT_DELAY_BEFORE_RETRY; // milliseconds
	
    protected Object lock = new Object();

	/**
	* The background thread.
	*/
	protected Thread monitorThread = null;
	
	protected HashMap<String, Integer> lastUsedThreads = new HashMap<String, Integer>();
	
	/**
	* Period of monitor flush (milliseconds)
	*/
	protected long monitorFlushTime = 60000L; // 1 minute
	
	protected volatile boolean frozen = false;

	/**
	* The constructor.
	* 
	*/
	public GenericThreadPool(String name)
	{
		if (name != null)
		{
			this.name = name;
			this.priority = Thread.currentThread().getPriority();
			this.isDaemon = Thread.currentThread().isDaemon();
			
			this.monitorThread = new Thread(this, name+"-ThreadPoolMonitor");
			this.monitorThread.setPriority(Thread.MIN_PRIORITY);
			// The Java Virtual Machine exits when the only threads running are all 
		    // daemon threads.
			this.monitorThread.setDaemon(true);
			this.monitorThread.start();
		}
	}

	/**
	 * Initialization
	 * 
	 */
	public void init(GenericWorkerThread[] threads)
	{
		if (this.threads == null)
		{
			if (logger.isInfoEnabled()) logger.info("GenericThreadPool.init ["+this.name+"]");
			if (threads != null && threads.length > 0) {
				// Initialize the queue of requests
				this.requests = new Queue(this.name);
				
		    	// Initialize the array of threads
				this.threads = new HashSet<GenericWorkerThread>(threads.length/* initial capacity */);
				this.lastUsedThreads.clear();
				this.numberOfWorkingThread = 0;
				// Put threads in the pool and and start them
				for (GenericWorkerThread t: threads)
				{
					this.threads.add(t);
					t.setPriority(this.priority);
					t.setDaemon(this.isDaemon);
					if (logger.isDebugEnabled()) logger.debug("GenericThreadPool.init ["+this.name+"] start Thread "+t.getName());
					t.start();
				}
				
			} else {
				logger.warn("GenericThreadPool.init ["+this.name+"] cannot initialize");
			}
		}
		else
		{
			logger.warn("GenericThreadPool.init ["+this.name+"] already initialized");
		}
	} /* end of init */

	
	/**
     * Destroy a worker thread by scheduling it for shutdown.
     *
     * @param worker the worker thread
     */
    protected void destroyWorker(final GenericWorkerThread worker)
    {
        worker.dispose();
    }
	
	/**
	* Called when JVM is exiting, and responsible for interrupting and destroying all working threads in pool.
 	*/
    @Override
	protected void finalize()  
	{
		if (logger.isInfoEnabled()) logger.info("GenericThreadPool.finalize ["+this.name+"]");
		final Iterator<GenericWorkerThread> iterator = this.threads.iterator();
		while( iterator.hasNext() )
		{
			GenericWorkerThread t = iterator.next();
			destroyWorker(t);
		}
		this.threads = null;
		this.lastUsedThreads.clear();
		this.numberOfWorkingThread = 0;
		this.requests = null;
		this.frozen = false;
	}

	/**
     * Set flag indicating whether daemon threads should be created by pool.
     *
     * @param isDaemon flag indicating whether daemon threads should be created by pool.
     */
    protected void setDaemon(boolean isDaemon)
    {
    	this.isDaemon = isDaemon;
    }

    /**
     * Return flag indicating whether daemon threads should be created by pool.
     *
     * @return flag indicating whether daemon threads should be created by pool.
     */
    protected boolean isDaemon()
    {
        return this.isDaemon;
    }

    /**
     * Set the priority of threads created for pool.
     *
     * @param priority the priority of threads created for pool.
     */
    protected void setPriority(int priority)
    {
    	this.priority = priority;
    	final Iterator<GenericWorkerThread> iterator = this.threads.iterator();
		while( iterator.hasNext() )
		{
			GenericWorkerThread t = iterator.next();
			t.setPriority(this.priority);
		}
    }

    /**
     * Return the priority of threads created for pool.
     *
     * @return the priority of threads created for pool.
     */
    protected int getPriority()
    {
        return this.priority;
    }
    
    protected boolean checkOnRequest()
    {
    	boolean frozenLog = false;
    	boolean OK = true;
    	if (this.requests == null)
    	{
    		logger.error("GenericThreadPool.checkOnRequest ["+this.name+"] not initialized");
    		OK = false;
    	}
		while(OK && this.frozen){
			if (!frozenLog && this.frozen){
				if (logger.isDebugEnabled()) logger.debug("GenericThreadPool.checkOnRequest ["+this.name+"] frozen");
				frozenLog = true;
			}
			try {
				Thread.sleep(this.delayBeforeRetry);
			} catch (InterruptedException e) {
				logger.warn("GenericThreadPool.checkOnRequest ["+this.name+"] got interrupted",e);
				e.printStackTrace();
			}
		}
		return OK;
    }
    
   /**
    * Adds a request to the pool. The request will be run in the first
    * available thread.
	* 
	* @param request A generic object
	*/
	public void addRequest(Object request)
	{
		if (checkOnRequest()) {
			this.requests.enqueue(request);
		}
	}
	
	/**
	 * Same as addRequest()
	 * 
	 * @param requests An array of generic objects
	 */
	public void addRequests(Object[] requests)
	{
		if (checkOnRequest()) {
			this.requests.enqueue(requests);
		}
	}

	/**
	* Get a new request in order to process it.
	* 
	* @return a request
	*/
	public Object getRequest()
	{
		try 
		{
			// The calling threads will wait here until a request is added
			return this.requests.dequeue(/* no timeout */);
		}
		catch (InterruptedException e) 
		{
			if (logger.isWarnEnabled())
			{
				logger.warn("GenericThreadPool.getAssignment ["+this.name+"]: a problem occured when getting a request ",e);
			}	
			e.printStackTrace();
		}
		return null;
	} /* end of getRequest */

	public int getNumberOfWorkingThread() {
		synchronized(lock){
			int tmp = this.numberOfWorkingThread;
			if (logger.isDebugEnabled()) logger.debug("GenericThreadPool.getNumberOfWorkingThread ["+this.name+"] return "+tmp);
			return tmp;
		}
	}

	public int getNumberOfWorkingThreadOverLastPeriod() {
		synchronized(lock){
			int tmp = this.lastUsedThreads.size();
			if (logger.isDebugEnabled()) logger.debug("GenericThreadPool.getNumberOfWorkingThreadOverLastPeriod ["+this.name+"] return "+tmp);
			return tmp;
		}
	}
	
	public int getWaitingTasks() {
		int tmp = (this.requests != null?this.requests.size():0);
		if (logger.isDebugEnabled()) logger.debug("GenericThreadPool.getWaitingTasks ["+this.name+"] return "+tmp);
		return tmp;
	}
	
	
	
	/**
	 * Done by the monitoring Thread
	 */
	@Override
	public void run()
	{
		while(true)
		{
			try
			{
				try
				{
					if (logger.isDebugEnabled()) logger.debug("GenericThreadPool.run ["+this.name+"] Entering sleeping state for "+(this.monitorFlushTime/1000L)+" seconds");
					Thread.sleep(this.monitorFlushTime);
					synchronized(lock){
						if (logger.isDebugEnabled())
						{
							int totalRun = 0;
							Iterator<String> enumThread = this.lastUsedThreads.keySet().iterator();
							while(enumThread.hasNext())
							{
								String id = enumThread.next();
								int run = this.lastUsedThreads.get(id);
								if (logger.isTraceEnabled()) logger.trace("GenericThreadPool.run ["+this.name+"] Thread "+id+" worked "+run+" times over last period");
								totalRun += run;
							}
							logger.debug("GenericThreadPool.run ["+this.name+"] "+lastUsedThreads.size()+" Threads worked "+totalRun+" times over last period");
						}
						this.lastUsedThreads.clear();
					}
				}
				catch (InterruptedException e)
				{
		             if (logger.isInfoEnabled()) logger.info("GenericThreadPool.run ["+this.name+"] InterruptedException. "+e);
		             break; // to exit the loop
				}
			}
			catch (Throwable e)
			{
				logger.warn("GenericThreadedPool.run ["+this.name+"] Throwable "+e);
                // Print exception stack trace
				e.printStackTrace();
        	}
		} // infinite loop
	}
	
	/* ***************
	* INNER CLASSES *
	*****************/

	/**
	 * Worker thread that really do the job.
	 */
	public abstract class GenericWorkerThread extends Thread 
	{
		protected boolean terminated = false;
		protected GenericThreadPool pool;
		protected String id;

		public GenericWorkerThread(String name)
		{
			super(name);
		}
		
		public GenericWorkerThread(GenericThreadPool pool, String name)
		{
			this(name);
			this.pool = pool;
			this.id = ""+this.hashCode();
		}

		public void dispose()
		{
			terminated = true;
			interrupt();
		}

		@Override
		public void run()
		{
			Object request = null;
			init();
			// While the execution is not interrupted, get the workers one by one from the GenericThreadPool
			do 
			{
				// Get a request
				request = pool.getRequest();
				if (request!=null)
				{
					boolean incr = false;
					try{
						// declare this thread as working
						synchronized(lock){
							numberOfWorkingThread ++;
							incr = true;
							if (lastUsedThreads.get(this.id) == null) {
								lastUsedThreads.put(this.id, 1);
							} else {
								lastUsedThreads.put(this.id, lastUsedThreads.get(this.id)+1);
							}
						}
						process(request);
					}
					catch(Exception e){
						logger.warn("GenericThreadPool.GenericWorkerThread.run ["+name+"]: Exception " + e.getClass()  + " " + e.getMessage());
						e.printStackTrace();
					}
					finally {
						if (incr) {
							// declare this thread as non working
							synchronized(lock){
								numberOfWorkingThread --;
							}
						}
					}
				}
			} 
			while (request!=null && !terminated);
			logger.error("GenericThreadPool.GenericWorkerThread.run ["+name+"] stop ("+terminated+")");
		} /* end of run */
		
		public void init() {
			// do nothing here
		}
		public abstract void process(Object request);
	} /* end of inner class GenericWorkerThread */
	
} /* end of class GenericThreadPool */