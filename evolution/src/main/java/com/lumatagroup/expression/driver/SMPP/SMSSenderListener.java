package com.lumatagroup.expression.driver.SMPP;


import com.lumatagroup.expression.driver.SMPP.receiver.SMPPReceiverRequest;

import ie.omk.smpp.Connection;
import ie.omk.smpp.message.DeliverSM;
import ie.omk.smpp.message.SMPPPacket;
import ie.omk.smpp.message.SubmitSM;
import ie.omk.smpp.message.SubmitSMResp;

public abstract class SMSSenderListener {
	protected final SMPPConnection conn;
	
	public SMSSenderListener(SMPPConnection conn) {
		this.conn = conn;
	}

	public abstract void onSubmitSmResp(SubmitSMResp packet);
	public abstract void onDeliverSm(DeliverSM packet);
	public abstract void onSubmitSM(SubmitSM packet);

	public void packetReceived(Connection source, SMPPPacket packet) {
	  SMPPReceiverRequest request = new SMPPReceiverRequest(source, packet, this);
	  conn.getReceiverThreadPool().addRequest(request);
	}

	public SMPPConnection getSMPPConnection(){
	  return conn;
	}
}
