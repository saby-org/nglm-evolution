package com.lumatagroup.expression.driver.SMPP.receiver;

import com.lumatagroup.expression.driver.SMPP.SMSSenderListener;

import ie.omk.smpp.Connection;
import ie.omk.smpp.message.SMPPPacket;

public class SMPPReceiverRequest {
	private Connection source;
	private SMPPPacket packet;
	private SMSSenderListener listener;
	
	public SMPPReceiverRequest(Connection source, SMPPPacket packet, SMSSenderListener listener){
		this.source = source;
		this.packet = packet;
		this.listener = listener;
	}

	public Connection getSource() {
		return source;
	}

	public SMPPPacket getPacket() {
		return packet;
	}

	public SMSSenderListener getListener() {
		return listener;
	}	
}
