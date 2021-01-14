package com.evolving.nglm.evolution;

import com.evolving.nglm.core.SubscriberStreamEvent;
import org.apache.kafka.connect.data.Schema;

import java.util.Date;

public class SubscriberStateOutputWrapper implements SubscriberStreamEvent {

	SubscriberState subscriberState;
	SubscriberStreamEvent event;
	SubscriberStateOutputWrapper(SubscriberStreamEvent event){
		this.event=event;
	}

	public SubscriberStreamEvent getOriginalEvent() {return event;}
	public void enrichWithSubscriberState(SubscriberState subscriberState){this.subscriberState=subscriberState;}
	public SubscriberState getSubscriberState() {return subscriberState;}

	@Override
	public String getSubscriberID() {
		return event.getSubscriberID();
	}
	
	

	@Override
	public Date getEventDate() {
		return event.getEventDate();
	}

	@Override
	public Schema subscriberStreamEventSchema() {
		return event.subscriberStreamEventSchema();
	}

	@Override
	public Object subscriberStreamEventPack(Object value) {
		return event.subscriberStreamEventPack(value);
	}

	@Override
	public DeliveryRequest.DeliveryPriority getDeliveryPriority(){return event.getDeliveryPriority(); }
}
