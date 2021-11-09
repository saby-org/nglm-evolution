package com.evolving.nglm.evolution;

import com.evolving.nglm.core.SubscriberStreamEvent;
import org.apache.kafka.connect.data.Schema;

import java.util.Date;

public class SubscriberStateOutputWrapper implements SubscriberStreamEvent {

	SubscriberStreamEvent event;
	EvolutionEngine.EvolutionEventContext evolutionEventContext;
	SubscriberStateOutputWrapper(SubscriberStreamEvent event){
		this.event=event;
	}

	public SubscriberStreamEvent getOriginalEvent() {return event;}
	public void enrichWithContext(EvolutionEngine.EvolutionEventContext evolutionEventContext){this.evolutionEventContext=evolutionEventContext;}
    public EvolutionEngine.EvolutionEventContext getEvolutionEventContext() {return evolutionEventContext;}
    
	@Override
	public String getSubscriberID() {
		return event.getSubscriberID();
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
