package com.lumatagroup.expression.driver.SMPP;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple queue implementation (synchronized)
 * <p>Inspired from org.pnuts.multithread.Queue
 * @author fduclos
 */
public class Queue {

	private static Logger logger = LoggerFactory.getLogger(Queue.class);

	private String name;
	private Cell head;
	private Cell tail;
	private int size = 0;
	private Object block = new Object();

	public Queue(String name)
	{
		this.name = name;
	}

	static class Cell {
		Object value;
		Cell next;

		Cell(Object value){
			this.value = value;
			this.next = null;
		}
	}

	public void block_enqueue(int min_size) throws InterruptedException {
		synchronized(block) {
			while (size/*not synchronized*/ >= min_size) {
				if (logger.isTraceEnabled()) {
					logger.trace("Queue.block_enqueue ["+this.name+"]: items in the queue : " + size);
				}
				block.wait(); // wait for this Queue to get dequeued.
			}
		}
		// return immediately
	}
	
	public void enqueue(Object value){
		Cell c = new Cell(value);
		synchronized (this){
			size++;
			if (isEmpty()){
				head = c;
				tail = c;
				// Notify a waiting thread that an object has been queued
				notify();
			} else {
				tail.next = c;
				tail = c;
				// bug fix => notify also in this case
				// case was: if 2 Threads are waiting for a request added very rapidly, Thread-1 will get the 1st request,
				// Thread-2 will not be woken up (cos there's no notify if request is added to non-empty queue) and Thread-1 will get the
				// 2nd request (cos it does not "wait()" since the queue is not empty) when it finished 1st request.
				notify();
			}
			if (logger.isDebugEnabled()) {
				logger.debug("Queue.enqueue ["+this.name+"]: items in the queue : " + size);
			}
		}
	}
	
	/**
	 * Quicker than enqueuing every single Object. 
	 */
	public void enqueue(Object[] values){
		synchronized (this){
			for (int i=0; i<values.length; i++)
			{
				Cell c = new Cell(values[i]);
				size++;
				if (isEmpty()){
					head = c;
					tail = c;
					
				} else {
					tail.next = c;
					tail = c;
				}
			}
			if (logger.isDebugEnabled()) {
				logger.debug("Queue.enqueue ["+this.name+"]: items in the queue : " + size);
			}
			// Notify all waiting thread that several objects have been queued
			notifyAll();
		}
	}
		
	public synchronized Object dequeue() throws InterruptedException {
		return dequeue(-1);
	}

	public synchronized Object dequeue(long timeout) throws InterruptedException {
		Object ret = null;
		if (timeout > 0){
			if (isEmpty()){
				wait(timeout); // wait for a notify until timeout
			}
		} else if (timeout < 0){
			while (isEmpty()){
				wait(); // wait for a notify
			}
		}
		if (!isEmpty()){
			ret = head.value;
			head = head.next;
			size--;
			if (logger.isDebugEnabled()) {
				logger.debug("Queue.dequeue ["+this.name+"]: items in the queue : " + size);
			}
			synchronized(block) {
				block.notify(); // notify any waiter Thread on block_enqueue call
			}
		}
		return ret;
	}

	public boolean isEmpty(){
		return head == null;
	}

	/**
	 * Returns the number of elements in this queue.
	 *
	 * @return the number of elements in this queue
	 */
	 public synchronized int size() {
		 return size;
	 }
}
