package com.lumatagroup.expression.driver.SMPP;

import ie.omk.smpp.message.SMPPPacket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a Delivery Receipt from the SMSC
 * @author fduclos
 *
 */
public class DeliveryReceipt {
	private static Logger logger = LoggerFactory.getLogger(DeliveryReceipt.class);
	// attributes of delivery receipt
	private static final String DELREC_ID = "id";
	private static final String DELREC_SUB = "sub";
	private static final String DELREC_DLVRD = "dlvrd";
	private static final String DELREC_STAT = "stat";
	private static final String DELREC_ERR = "err";
	private static final String DELREC_TEXT = "Text";

	private String id;
	private Integer submitted;
	private Integer delivered;
	private Integer finalStatus;
	private String error;
	private String text;

	public DeliveryReceipt(String formattedDeliveryReceipt) {
		try {
			/*
			 * id:IIIIIIIIII sub:SSS dlvrd:DDD submit date:YYMMDDhhmm done
			 * date:YYMMDDhhmm stat:DDDDDDD err:E Text: ..........
			 */
			id = getDeliveryReceiptValue(DeliveryReceipt.DELREC_ID, formattedDeliveryReceipt);
			submitted = getDeliveryReceiptIntValue(DeliveryReceipt.DELREC_SUB, formattedDeliveryReceipt);
			delivered = getDeliveryReceiptIntValue(DeliveryReceipt.DELREC_DLVRD, formattedDeliveryReceipt);
			finalStatus = getDeliverReceiptState(getDeliveryReceiptValue(DeliveryReceipt.DELREC_STAT, formattedDeliveryReceipt));
			error = getDeliveryReceiptValue(DeliveryReceipt.DELREC_ERR, formattedDeliveryReceipt);
			text = getDeliveryReceiptTextValue(formattedDeliveryReceipt);
		} catch(Exception e) {
			logger.warn("DeliveryReceipt("+formattedDeliveryReceipt+") failed to initialize due to "+e);
			id = null;
			submitted = null;
			delivered = null;
			finalStatus = null;
			error = null;
			text = null;
		}
	}
	/**
	 * @return Returns the delivered.
	 */
	public Integer getDelivered() {
		return delivered;
	}


	/**
	 * @return Returns the error.
	 */
	public String getError() {
		return error;
	}

	/**
	 * @return Returns the finalStatus.
	 */
	public Integer getFinalStatus() {
		return finalStatus;
	}

	/**
	 * @return Returns the id.
	 */
	public String getId() {
		return id;
	}

	/**
	 * @return Returns the id as hex (lower case).
	 */
	public String getIdAsHex() {
		if (id == null) return id;
		else return Long.toHexString(new Long(id));
	}
	
	/**
	 * @return Returns the submitted.
	 */
	public Integer getSubmitted() {
		return submitted;
	}

	/**
	 * @return Returns the text.
	 */
	public String getText() {
		return text;
	}

	@Override
	public String toString() {
		/*
		 * id:IIIIIIIIII sub:SSS dlvrd:DDD submit date:YYMMDDhhmm done
		 * date:YYMMDDhhmm stat:DDDDDDD err:E Text: . . . . . . . . .
		 */
		StringBuffer sBuf = new StringBuffer(120);
		sBuf.append(DELREC_ID + ":" + id+" ("+getIdAsHex()+")");
		sBuf.append(" ");
		sBuf.append(DELREC_SUB + ":" + intToString(submitted, 3));
		sBuf.append(" ");
		sBuf.append(DELREC_DLVRD + ":" + intToString(delivered, 3));
		sBuf.append(" ");
		sBuf.append(DELREC_STAT + ":" + finalStatus);
		sBuf.append(" ");
		sBuf.append(DELREC_ERR + ":" + error);
		sBuf.append(" ");
		sBuf.append(DELREC_TEXT.toLowerCase() + ":" + text);
		return sBuf.toString();
	}

	/**
	 * Create String representation of integer. Preceding 0 will be add as
	 * needed.
	 *
	 * @param value is the value.
	 * @param digit is the digit should be shown.
	 * @return the String representation of int value.
	 */
	private static String intToString(Integer value, int digit) {
		if (value == null) return "0";
		StringBuffer sBuf = new StringBuffer(digit);
		sBuf.append(Integer.toString(value));
		while (sBuf.length() < digit) {
			sBuf.insert(0, "0");
		}
		return sBuf.toString();
	}

	public static Integer getDeliverReceiptState(String state) {
		if (state == null) return null;
		if (state.equals("DELIVRD")) {
			return SMPPPacket.SM_STATE_DELIVERED;
		} else if (state.equals("ENROUTE")) {
			return SMPPPacket.SM_STATE_EN_ROUTE;
		} else if (state.equals("EXPIRED")) {
			return SMPPPacket.SM_STATE_EXPIRED;
		} else if (state.equals("DELETED")) {
			return SMPPPacket.SM_STATE_DELETED;
		} else if (state.equals("UNDELIV")) {
			return SMPPPacket.SM_STATE_UNDELIVERABLE;
		} else if (state.equals("ACCEPTD")) {
			return SMPPPacket.SM_STATE_ACCEPTED;
		} else if (state.equals("REJECTD")) {
			return 8;
		} else if (state.equals("UNKNOWN")) {
			return SMPPPacket.SM_STATE_INVALID;
		} else {
			return null;
		}
	}

	/**
	 * Get the delivery receipt attribute value.
	 *
	 * @param attrName is the attribute name.
	 * @param source the original source id:IIIIIIIIII sub:SSS dlvrd:DDD submit
	 *        date:YYMMDDhhmm done date:YYMMDDhhmm stat:DDDDDDD err:E
	 *        Text:....................
	 * @return the value of specified attribute.
	 * @throws IndexOutOfBoundsException
	 */
	private static String getDeliveryReceiptValue(String attrName, String source)
	throws IndexOutOfBoundsException {
		String tmpAttr = attrName + ":";
		int startIndex = source.indexOf(tmpAttr);
		if (startIndex < 0)
			return null;
		startIndex = startIndex + tmpAttr.length();
		int endIndex = source.indexOf(" ", startIndex);
		if (endIndex > 0)
			return source.substring(startIndex, endIndex);
		return source.substring(startIndex);
	}
	
	private static Integer getDeliveryReceiptIntValue(String attrName, String source)
	throws IndexOutOfBoundsException {
		String tmpAttr = attrName + ":";
		int startIndex = source.indexOf(tmpAttr);
		if (startIndex < 0)
			return null;
		startIndex = startIndex + tmpAttr.length();
		int endIndex = source.indexOf(" ", startIndex);
		if (endIndex > 0)
			return new Integer(source.substring(startIndex, endIndex));
		return new Integer(source.substring(startIndex));
	}
	

	/**
	 * @param source
	 * @throws IndexOutOfBoundsException
	 */
	private static String getDeliveryReceiptTextValue(String source) {
		String tmpAttr = DeliveryReceipt.DELREC_TEXT + ":";
		int startIndex = source.indexOf(tmpAttr);
		if (startIndex < 0) {
			tmpAttr = DeliveryReceipt.DELREC_TEXT.toLowerCase() + ":";
			startIndex = source.indexOf(tmpAttr);
		}
		if (startIndex < 0) {
			return null;
		}
		startIndex = startIndex + tmpAttr.length();
		return source.substring(startIndex);
	}

}
