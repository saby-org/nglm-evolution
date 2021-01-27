package com.lumatagroup.expression.driver.SMPP.receiver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lumatagroup.expression.driver.SMPP.GenericThreadPool;
import com.lumatagroup.expression.driver.SMPP.GenericThreadPool.GenericWorkerThread;

import ie.omk.smpp.message.DeliverSM;
import ie.omk.smpp.message.SMPPPacket;
import ie.omk.smpp.message.SubmitSM;
import ie.omk.smpp.message.SubmitSMResp;

/**
 * Thread that taks in account any incoming information from the SMSC (submit sm response and deliver sm). 
 * @author fduclos
 *
 */
public class ReceiverWorkerThread extends GenericWorkerThread {

    private static final Logger logger = LoggerFactory.getLogger(ReceiverWorkerThread.class);
	
	public ReceiverWorkerThread(GenericThreadPool genericThreadPool, String name) {
		genericThreadPool.super(genericThreadPool, name);	
		
	}

	@Override
	public void process(Object request) {
		long start = System.currentTimeMillis();
		String commandId = null;
		try{
			if(logger.isDebugEnabled()) logger.debug("ReceiverWorkerThread.process() : execution started...");
			// Ensure the request object is a SMPPReceiverRequest
			if(!(request instanceof SMPPReceiverRequest)){
					logger.warn("ReceiverWorkerThread.process: bad request type " + request.getClass().getName());
			}
			else {
				SMPPReceiverRequest smppRequest = (SMPPReceiverRequest)request;
				SMPPPacket packet = smppRequest.getPacket();
				logger.debug("Packet[commandId] "+packet.getCommandId());
				commandId = ""+packet.getCommandId();
				switch(packet.getCommandId()) {
					case SMPPPacket.SUBMIT_SM_RESP:
						smppRequest.getListener().onSubmitSmResp((SubmitSMResp)packet);
					break;
					case SMPPPacket.DELIVER_SM:
						smppRequest.getListener().onDeliverSm((DeliverSM)packet);
					break;
					case SMPPPacket.SUBMIT_SM:
						smppRequest.getListener().onSubmitSM((SubmitSM)packet);
					break;
					
				default:
					// nothing here, handled by SMPPConnection
					break;
				}
			}
		}
		catch(Exception e){
			logger.warn("ReceiverWorkerThread.process Exception " + e.getClass().getName() +" " + e.getMessage() + " while handling message " + request, e);

		}

		if(logger.isDebugEnabled()) logger.debug("ReceiverWorkerThread.process Duration of request " + request + " with command id=" + commandId + " last " + (System.currentTimeMillis() - start) + " ms" );
	}

}
