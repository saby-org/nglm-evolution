package com.lumatagroup.expression.driver.SMPP;

import ie.omk.smpp.Address;
import ie.omk.smpp.SMPPException;
import ie.omk.smpp.SMPPRuntimeException;
import ie.omk.smpp.message.DeliverSM;
import ie.omk.smpp.message.SMPPPacket;
import ie.omk.smpp.message.SubmitSM;
import ie.omk.smpp.message.SubmitSMResp;
import ie.omk.smpp.message.tlv.Tag;
import ie.omk.smpp.util.AlphabetEncoding;
import ie.omk.smpp.util.DefaultAlphabetEncoding;
import ie.omk.smpp.util.EncodingFactory;
import ie.omk.smpp.util.HPRoman8Encoding;
import ie.omk.smpp.util.MessageEncoding;
import ie.omk.smpp.util.PacketStatus;
import ie.omk.smpp.util.UCS2Encoding;
import ie.omk.smpp.util.UTF16Encoding;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;

import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.DeliveryManagerForNotifications;
import com.evolving.nglm.evolution.DeliveryManagerForNotifications.MessageStatus;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.EvolutionEngineEvent;
import com.evolving.nglm.evolution.EvolutionEngineEventDeclaration;
import com.evolving.nglm.evolution.INotificationRequest;
import com.evolving.nglm.evolution.MONotificationEvent;
import com.evolving.nglm.evolution.SMSNotificationManager.SMSNotificationManagerRequest;
import com.evolving.nglm.evolution.NotificationManager.NotificationManagerRequest;
import com.lumatagroup.expression.driver.SMPP.SMPPConnection.SubmitSMCorrectionDeliveryRequest;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SubscriberIDService.SubscriberIDServiceException;
import com.evolving.nglm.core.SystemTime;

/**
 * Recommended reading: http://www.developershome.com/sms/gsmAlphabet.asp
 * <p>
 * Keep also an eye on a forked version of SMPP API: http://www.mobicents.org/incubator/smpp/intro.html
 * @author oaudo
 *
 */
//@formatter:off
public class SimpleSMSSender extends SMSSenderListener {
//@formatter:on

  public enum SMPP_FEEDBACK_STATUS{
    SMS_FEEDBACK_STATUS_NO_SMSC_CONNECTION("NO_SMSC_CONNECTION"),
    SMS_FEEDBACK_STATUS_SENT("SENT"),
    SMS_FEEDBACK_STATUS_ACCEPTED("ACCEPTED"),
    SMS_FEEDBACK_STATUS_DELIVERED("DELIVERED"),
    SMS_FEEDBACK_STATUS_ENROUTE("ENROUTE"),
    SMS_FEEDBACK_STATUS_EXPIRED("EXPIRED"),
    SMS_FEEDBACK_STATUS_DELETED("DELETED"),
    SMS_FEEDBACK_STATUS_UNDELIVERABLE("UNDELIVERABLE"),
    SMS_FEEDBACK_STATUS_QUEUE_FULL("QUEUE_FULL"),
    SMS_FEEDBACK_STATUS_INVALID("INVALID"),
    SMS_FEEDBACK_STATUS_UNKNOWN("UNKNWON");
    
    private String value;
    
    private SMPP_FEEDBACK_STATUS(String value){
        this.value = value;
    }
    
    public String getValue(){
        return value;
    }
}
  
    private static final Logger logger = LoggerFactory.getLogger(SimpleSMSSender.class);
	protected final int MAX_MESSAGE_LENGTH = 140; // the maximum short message length is 140 bytes, depending on the encoding it can carray 160 characters
	protected final int MAX_MSG_CHAR_LENGTH = 160; // the maximum character message length
	protected final int UDH_LENGTH = 6; // we could use 16-bit instead of 8-bit reference number => 7 bytes for UDH header instead of 6

	protected int nextSARRefNum = 0;

	protected final String name;
	private DeliveryManagerForNotifications smsNotificationManager;

	protected final String source_addr;
	protected final Integer source_addr_ton;
	protected final Integer source_addr_npi;
	protected final Integer dest_addr_ton;
	protected final Integer dest_addr_npi;
	protected MessageEncoding encoding;
	protected String encoding_charset; // for example, "UTF-8"
	protected final boolean data_packing;
	protected final Integer expiration_period;
	protected final boolean support_sar;
	protected final boolean support_udh;
	protected final String handle_submit_sm_response_in_multi_part;
	protected final Integer dest_addr_subunit;
	protected final String service_type;
	protected final String sms_MO_event_name;
	protected final String sms_MO_channel_name;
	protected Constructor<? extends MONotificationEvent> sms_MO_class_constructor = null;
	

	private final String DELIVERY_RECEIPT_DEC = "dec";
	private final String MULTI_PART_SUBMIT_SM_HANDLE_ALL = "all";
	private final String MULTI_PART_SUBMIT_SM_HANDLE_FIRST = "first";
	private final String MULTI_PART_SUBMIT_SM_HANDLE_LAST = "last";

	protected Integer delay_on_queue_full = 60; // define the delay in sec to resend SMS on queue full error OR throttling error. 0 means no resend. Default is so 60 sec.
	protected String deliveryReceiptDecodingDecimalHexa = null;
	protected boolean isSendToPayload;

	protected Integer midnight_expiry_smooth_hours = null; // EFOGC-5387 defines the number of hours after midnight during which smoothing the exact 24:00:00 messages expiry

	public static final String SMPP_STAT_DELIVERSM_OK = "DELIVERSM_OK";
	protected static final String SMPP_STAT_DELIVERSM_KO = "DELIVERSM_KO";
	public static final String SMPP_STAT_SUBMITSMRESP_OK = "SUBMITSMRESP_OK";
	public static final String SMPP_STAT_SUBMITSMRESP_QFULL = "SUBMITSMRESP_QFULL";
	public static final String SMPP_STAT_SUBMITSMRESP_KO = "SUBMITSMRESP_KO";
	protected static final String SMPP_STAT_SUBMITSM_OK = "SUBMITSM_OK";
	protected static final String SMPP_STAT_SUBMITSM_KO = "SUBMITSM_KO";

	//@formatter:off
	public SimpleSMSSender(DeliveryManagerForNotifications smsNotificationManager,
	                       String name,
	                       SMPPConnection conn,
	                       String source_addr,
	                       String source_addr_ton,
	                       String source_addr_npi,
	                       String dest_addr_ton,
	                       String dest_addr_npi,
	                       String data_coding,
	                       String load_hproman8_as_data_coding_0, // in order to use, for dataCoding = 0 the HPRoman8 format
	                       String load_utf16_as_data_coding_8, // in order to use, for dataCoding = 8 the UTF16 format
	                       String data_packing,
	                       String encoding_charset,
	                       String expiration_period,
	                       String delay_on_queue_full,
	                       String support_sar,
	                       String support_udh,
	                       String handle_submit_sm_response_in_multi_part, // "all" (default), "first" or "last"
	                       String dest_addr_subunit,
	                       String service_type,
	                       String deliveryReceiptDecodingDecimalHexa,
	                       String isSendToPayload,
	                       String midnight_expiry_smooth_hours, // EFOGC-5387 defines the number of hours after midnight during which smoothing the exact 24:00:00 messages expiry
	                       String sms_MO_event_name,
	                       String sms_MO_channel_name
	                       ) throws NumberFormatException, UnsupportedEncodingException {
	//@formatter:on

		super(conn);

		this.smsNotificationManager = smsNotificationManager;
		
		if(delay_on_queue_full!=null && delay_on_queue_full.trim().length() > 0){
			try{
				this.delay_on_queue_full = Integer.parseInt(delay_on_queue_full.trim());
			}catch (NumberFormatException e){
				if(logger.isWarnEnabled()){
					logger.warn("SimpleSMSSender.SimpleSMSSender: can not parse "+delay_on_queue_full+" conf, will use the default one "+this.delay_on_queue_full+" sec");
				}
			}
		}
		if (logger.isInfoEnabled()) {
			if(this.delay_on_queue_full==0){
				logger.info("SimpleSMSSender.SimpleSMSSender: delay_on_queue_full configured to " + this.delay_on_queue_full + ", will not resend SMS on QUEUE_FULL or THROTTLING error");
			}else{
				logger.info("SimpleSMSSender.SimpleSMSSender: delay_on_queue_full configured, will resend SMS after " + this.delay_on_queue_full + "sec on QUEUE_FULL or THROTTLING error");
			}
		}

		this.deliveryReceiptDecodingDecimalHexa = deliveryReceiptDecodingDecimalHexa;
		if(deliveryReceiptDecodingDecimalHexa != null){
			if (logger.isInfoEnabled()) {
				logger.info("SimpleSMSSender.SimpleSMSSender: decode deliver SM message id (from sms body) as " + this.deliveryReceiptDecodingDecimalHexa);
			}
		}

		if (conn == null || source_addr_npi == null || dest_addr_ton == null || dest_addr_npi == null || data_coding == null /* || expiration_period == null ||*/) {
			throw new NullPointerException("Missing argument for SimpleSMSSender constructor");
		}

		this.conn.setListener(this);

		this.name = name;
		this.source_addr = source_addr; // can be null
		if(source_addr_ton != null){
			this.source_addr_ton = Integer.parseInt(source_addr_ton);
		}
		else {
			this.source_addr_ton = null;
		}
		this.source_addr_npi = Integer.parseInt(source_addr_npi);
		this.dest_addr_ton = Integer.parseInt(dest_addr_ton);
		this.dest_addr_npi = Integer.parseInt(dest_addr_npi);

		// handle encoding stuff
		if(load_hproman8_as_data_coding_0 != null && load_hproman8_as_data_coding_0.trim().toLowerCase().equals("true")){
			EncodingFactory.getInstance().addEncoding(new HPRoman8Encoding());
			if (logger.isInfoEnabled()) {
				logger.info("SimpleSMSSender.SimpleSMSSender: Load HPRoman8Encoding");
			}
		}
		else {
			EncodingFactory.getInstance().addEncoding(new BinaryEncoding());
			if (logger.isInfoEnabled()) {
				logger.info("SimpleSMSSender.SimpleSMSSender: Load BinaryEncoding");
			}
		}

		if(load_utf16_as_data_coding_8 != null && (load_utf16_as_data_coding_8.trim().toLowerCase().endsWith("big_endian"))){
			if(load_utf16_as_data_coding_8.trim().toLowerCase().startsWith("not_")){ // for not_big_endian
				EncodingFactory.getInstance().addEncoding(new UTF16Encoding(false));
				if (logger.isInfoEnabled()) {
					logger.info("SimpleSMSSender.SimpleSMSSender: Load UTF16 with big_endian");
				}
			}
			else {
				EncodingFactory.getInstance().addEncoding(new UTF16Encoding(true)); //for big_endian
				if (logger.isInfoEnabled()) {
					logger.info("SimpleSMSSender.SimpleSMSSender: Load UTF16 without big_endian");
				}

			}
		}
		
		//add flashsms encoding
		EncodingFactory.getInstance().addEncoding(new UCS2Encoding());

		MessageEncoding encoding = EncodingFactory.getInstance().getEncoding(Integer.parseInt(data_coding));
		if (encoding == null) {
			logger.warn("SimpleSMSSender.ctor: Cannot find encoding class of data_coding="+data_coding+", use default encoding if available");
			this.encoding = EncodingFactory.getInstance().getDefaultAlphabet();
		} else {
			this.encoding = encoding;
		}

		if (logger.isDebugEnabled()) logger.debug("SimpleSMSSender.ctor default JVM charset "+Charset.defaultCharset().name());
		if (encoding_charset != null && encoding_charset.length() > 0) {
			this.encoding_charset = encoding_charset;
		} else {
			this.encoding_charset = null;
		}
		if (this.encoding instanceof AlphabetEncoding) {
			logger.warn("SimpleSMSSender.ctor: data_coding="+data_coding+" corresponds to charset "+((AlphabetEncoding)this.encoding).getCharset()+" while encoding_charset="+this.encoding_charset+". Override");
			this.encoding_charset = ((AlphabetEncoding)this.encoding).getCharset(); // may override
		}
		if (this.encoding_charset != null) {
			// do basic encoding test:
			String test = "simple test message";
			if (logger.isDebugEnabled()) logger.debug("SimpleSMSSender.ctor test message encoded using the charset "+this.encoding_charset);
			if (test.getBytes(this.encoding_charset) == null) {
				logger.warn("SimpleSMSSender.ctor bad charset "+this.encoding_charset);
			} else {
				if (logger.isTraceEnabled()) logger.trace("SimpleSMSSender.ctor encoding test successful");
			}
		}

		// DATA PACKING IS NOT SUPPORTED YET
		if (data_packing != null && data_packing.length() > 0) {
			this.data_packing = Boolean.parseBoolean(data_packing);
			if (this.data_packing && this.encoding.getEncodingLength() != 7) {
				logger.warn("SimpleSMSSender.ctor: packing is enabled while encoding length is "+this.encoding.getEncodingLength()+". This is not recommended");
			}
		} else {
			this.data_packing = false; // default
		}
		if (logger.isInfoEnabled())	logger.info("SimpleSMSSender.ctor: data_coding="+data_coding+", data_packing="+data_packing+", encoding class "+this.encoding.getClass().getName()+", "+this.encoding.getEncodingLength()+" bits, encoding_charset "+this.encoding_charset);

		if (expiration_period == null || expiration_period.trim().length() == 0) {
			this.expiration_period = 0;
		} else {
			this.expiration_period = Integer.parseInt(expiration_period);
		}

		if (support_sar != null && support_sar.trim().length() > 0 && Boolean.parseBoolean(support_sar) == true) {
			this.support_sar = true;
			this.isSendToPayload = false;
		} else {
			this.support_sar = false; // default
		}

		if (support_udh != null && support_udh.trim().length() > 0 && Boolean.parseBoolean(support_udh) == true) {
			this.support_udh = true;
		} else {
			this.support_udh = false; // default
		}

		if (this.support_sar && this.support_udh) {
			logger.warn("SimpleSMSSender.ctor sar and udh should not be supported at the same time for sms concatenation");
		}

		if(handle_submit_sm_response_in_multi_part != null){
			if(handle_submit_sm_response_in_multi_part.trim().toLowerCase().equals(MULTI_PART_SUBMIT_SM_HANDLE_ALL)){
				this.handle_submit_sm_response_in_multi_part = MULTI_PART_SUBMIT_SM_HANDLE_ALL; // useful by example to lowercase everything...
			}
			else if(handle_submit_sm_response_in_multi_part.trim().toLowerCase().equals(MULTI_PART_SUBMIT_SM_HANDLE_FIRST)){
				this.handle_submit_sm_response_in_multi_part = MULTI_PART_SUBMIT_SM_HANDLE_FIRST; // useful by example to lowercase everything...
			}
			else if(handle_submit_sm_response_in_multi_part.trim().toLowerCase().equals(MULTI_PART_SUBMIT_SM_HANDLE_LAST)){
				this.handle_submit_sm_response_in_multi_part = MULTI_PART_SUBMIT_SM_HANDLE_LAST; // useful by example to lowercase everything...
			}
			else {
				this.handle_submit_sm_response_in_multi_part = MULTI_PART_SUBMIT_SM_HANDLE_LAST; // default...
			}
		}
		else {
			this.handle_submit_sm_response_in_multi_part = MULTI_PART_SUBMIT_SM_HANDLE_LAST; // default...
		}
		if(logger.isInfoEnabled()){
			logger.info("SimpleSMSSender.ctor handle_submit_sm_response_in_multi_part set to  " + handle_submit_sm_response_in_multi_part);
		}

		if (this.data_packing && this.support_udh) {
			logger.warn("SimpleSMSSender.ctor packing and udh should not be supported at the same time");
		}

		if (dest_addr_subunit != null && (dest_addr_subunit.trim().length() > 0 )) {
			this.dest_addr_subunit = Integer.parseInt(dest_addr_subunit); // can be null
		} else {
			this.dest_addr_subunit = null;
		}

		if (service_type != null && !service_type.trim().isEmpty()) {
			this.service_type = service_type;
		} else {
			this.service_type = null;
		}
		if(isSendToPayload != null && !isSendToPayload.isEmpty() && Boolean.parseBoolean(isSendToPayload) == true){
			if(!this.support_sar){
				this.isSendToPayload = true;
			}else{
				this.isSendToPayload = false;
			}
		}else{
			this.isSendToPayload = false;
		}

		if(midnight_expiry_smooth_hours != null && !midnight_expiry_smooth_hours.trim().isEmpty()){
			try{
				this.midnight_expiry_smooth_hours = Integer.parseInt(midnight_expiry_smooth_hours.trim());
				if (logger.isInfoEnabled()) {
					logger.info("SimpleSMSSender.SimpleSMSSender midnight_expiry_smooth_hours set to " + this.midnight_expiry_smooth_hours);
				}
			}
			catch(NumberFormatException e){
				logger.warn("SimpleSMSSender.SimpleSMSSender Exception " + e.getClass().getName() + " while interpreting midnight_expiry_smooth_hours " + midnight_expiry_smooth_hours);
			}
		}		
		this.sms_MO_event_name = sms_MO_event_name;
		this.sms_MO_channel_name = sms_MO_channel_name;
	}

	public SMPPConnection getSMPPConnection() {
		return this.conn;
	}

	public String getName() {
		return this.name;
	}


	/**
	 * Send an sms to the smsc (on the form of a SubmitSM)
	 * <p>
	 * Synchronous call (up to the SMPP layer)
	 * @param sms
	 * @param receipt
	 */
	public boolean sendSMS(INotificationRequest deliveryRequest, String text, String desination, String sender, boolean receipt, boolean flashsms){

		//DialogManagerMessage sms = expandedsms.getOriginalMsg();
		logger.info("SimpleSMSSender.sendSMS("+text+")");

		if (!conn.isConnected()) {
			logger.info("SimpleSMSSender.sendSMS("+text+") cannot send SMS, connection is not established");
		}
		try {

			if (desination==null || desination.length() == 0) {
			    logger.warn("SimpleSMSSender.sendSMS("+text+") cannot send SMS "+text +" to unknown destination");
			    return false;
			}
			if (logger.isDebugEnabled()) {
                logger.debug("SimpleSMSSender.sendSMS "+text + " to destination " + desination);
            }
			if (text==null || text.length() == 0) {
				logger.warn("SimpleSMSSender.sendSMS("+text+") cannot send empty SMS "+text +" to "+desination);
				return false;
			}
			text = getCappedText(text);
			// if (this.check_sms_text && text.matches(NotifTagFormatter.TAG_PATTERN) || text.matches(NotifTagFormatter.EXPRESSION_PATTERN)) {
			// The above matching does not work...

			Address source = null;
			String tmp_source_addr = ((sender!=null && sender.trim().length()>0)?sender:source_addr);

			if(source_addr_ton != null){
				if (source_addr_ton != -1 && source_addr_npi != -1 && tmp_source_addr != null && !tmp_source_addr.isEmpty()) {
					if (logger.isTraceEnabled()) logger.trace("SimpleSMSSender.sendSMS source_addr_ton="+source_addr_ton+", source_addr_npi="+source_addr_npi+", source_addr="+tmp_source_addr);
					source = new Address(source_addr_ton, source_addr_npi, tmp_source_addr);
				}
			}
			else {
				// let compute the value...
				if(tmp_source_addr != null){
					try{
						@SuppressWarnings("unused")
						long sourceAddressLong = Long.parseLong(tmp_source_addr);
						// no exception, so this is a number so ton = 3
						source = new Address(3, source_addr_npi, tmp_source_addr);
					}
					catch(NumberFormatException e){
						// means this is not a number but a simple string, so let use the ton = 5;
						source = new Address(5, source_addr_npi, tmp_source_addr);
					}
				}
			}


			logger.info("SimpleSMSSender.sendSMS dest_addr_ton="+dest_addr_ton+", dest_addr_npi="+dest_addr_npi+", dest_addr="+desination);
			Address destination = new Address(dest_addr_ton, dest_addr_npi, desination);

			byte[] message = null;
			//Cant be multiple flashsms
			if(flashsms && !this.support_sar && !this.support_udh) {
			  encoding = new UCS2Encoding();
			}
			if (encoding instanceof AlphabetEncoding) {
			  if (logger.isTraceEnabled()) logger.trace("SimpleSMSSender.sendSMS encode with alphabet charset "+((AlphabetEncoding)encoding).getCharset());
			  // The AlphabetEncoding applies its own conversion algo and charset
			  message = ((AlphabetEncoding)encoding).encodeString(text);
			} else {
			  if (encoding_charset != null) {
			    if (logger.isTraceEnabled()) logger.trace("SimpleSMSSender.sendSMS encode with charset "+encoding_charset);
			    message = text.getBytes(encoding_charset);
			  } else {
			    message = text.getBytes();
			    if (logger.isTraceEnabled()) logger.trace("SimpleSMSSender.sendSMS encode with default charset");
			  }
			}
			
			Calendar c = SystemTime.getCalendar();
	        Date expiryDateTimeStamp = null;
			if(this.expiration_period != 0) {
			  c.add(Calendar.HOUR_OF_DAY, this.expiration_period);
			  expiryDateTimeStamp = c.getTime();
			}
			
			logger.info("SimpleSMSSender.sendSMS expiration period of "+this.expiration_period+" hours, set the expiration date to "+expiryDateTimeStamp);
			//sms.setExpiration_timestamp(d[0]); // mostly for SMSC

			if (!this.support_sar && !this.support_udh) {
				sendSubmitSM(deliveryRequest, text,source, destination, message, encoding, expiryDateTimeStamp, receipt, dest_addr_subunit, isSendToPayload, flashsms);
			} else {
				sendMultipleSubmitSM(deliveryRequest, text,source, destination, message, encoding, expiryDateTimeStamp, receipt, dest_addr_subunit, isSendToPayload, flashsms);
			}

			return true;
		} catch (Throwable e) {
			// SMS could not been sent, do not increment the attempt counter, this is not a SMPP failure, but a connection or configuration failure
			logger.info("SimpleSMSSender.sendSMS Error when sending a sms "+e, e);
			return false;
		}
	}


	protected String getCappedText(final String text) {
		if (!this.support_sar && !this.support_udh && text.length() > MAX_MSG_CHAR_LENGTH && !this.isSendToPayload) {
			String tmp = text.substring(0, MAX_MSG_CHAR_LENGTH);
			if (logger.isTraceEnabled()) logger.trace("SimpleSMSSender.getCappedText("+text+") return "+tmp);
			return tmp;
		} else {
			if (logger.isTraceEnabled()) logger.trace("SimpleSMSSender.getCappedText("+text+") return "+text);
			return text;
		}
	}

	/** Return the max supported length in bytes */
	private int getMaxSegmentLength() {
		if (this.support_udh) {
			// The UDR header takes some part: 6 or 7 bytes
			return MAX_MESSAGE_LENGTH-UDH_LENGTH; // 153 7-bit character parts => 134 bytes
		} else {
			return MAX_MESSAGE_LENGTH; // bytes
		}
	}

	protected synchronized int getNextSARRefNum () {
		if (UDH_LENGTH == 7) {
		    if (nextSARRefNum>=65535) { //2 byte positive integer
		    	nextSARRefNum = 0;
		    }
		} else {
			if (nextSARRefNum>=255) { //1 byte positive integer
		    	nextSARRefNum = 0;
		    }
		}
	    return ++nextSARRefNum;
	}



	/**
	 * According to the GSM specification one message (SMS) cannot exceed 160 septets or 140 octets in length.
	 * However, sometimes it is necessary to send more information than that in one message.
	 *
	 * <br>For GSM networks, the concatenation related optional parameters (<b>sar_msg_ref_num</b>, <b>sar_total_segments</b>,
	 * <b>sar_segment_seqnum</b>) or port addressing related optional parameters (<b>source_port</b>,
	 * <b>destination_port</b>) cannot be used in conjunction with encoded UDH in the short_message
	 * (user data) field. This means that the above listed optional parameters cannot be used if the UDH Indicator
	 * flag is set (via SMPPPacket.setEsmClass()) with 0x40). Only use UDH (via UserData or otherwise) if you're stuck with no TLV support.
	 *
	 * <p>To send a multipart SMS messages through a SMPP server, you have to add the same UDH as in the previous chapter.
	 * Some providers allows you to send multipart messages without the need to encode this header.
	 * This is done using the so called TLV parameters which are extra options which can be used from version 3.4 of
	 * the SMPP protocol. You only have to split the message into parts and reserve 5 bytes per messagedata field,
	 * because the SMPP provider will add the UDH header for you.
	 *
	 * <p>UDH 5 bytes encoding:
	 * <ul>
	 * <li>00: Information Element Identifier: Concatenated short message, 8bit reference number
	 * <li>03: Information Element Data Length (always 03 for this UDH)
	 * <li>A4: Information Element Data: Concatenated short message reference, should be same for all parts of a message
	 * <li>03: Information Element Data: Total number of parts
	 * <li>01: Information Element Data: Number of this part (x/Total), starting at 1
	 * <li>135 bytes of message data
	 * </ul>
	 *
	 * <br>SMPP SAR (Segmentation and Reassembly) optional parameters, sent together with the submit_sm packet.
	 * <ul>
	 * <li><b>sar_msg_ref_num</b>: The reference number for a particular concatenated short message. Integer over 2 bytes
	 * starting at 1 and up to 65536. When present, the PDU must also contain the <b>sar_total_segments</b>
	 * and <b>sar_segment_seqnum</b> parameters.
	 * <li><b>sar_total_segments</b>: Indicates the total number of short messages within the concatenated short message.
	 * Integer over 1 byte starting at 1 and up to 255. When present, the PDU must also contain the <b>sar_msg_ref_num</b>
	 * and <b>sar_segment_seqnum</b> parameters.
	 * <li><b>sar_segment_seqnum</b>: Indicates the sequence number of a particular short message fragment within the
	 * concatenated short message. Integer over 1 byte starting at 1 and up to 255. The value shall start at 1 and increment
	 * by one for every message sent within the concatenated short message. When present, the PDU must also contain the
	 * <b>sar_total_segments</b> and <b>sar_msg_ref_num</b> parameters.
	 * </ul>
	 * <p>These optional arguments are only defined in SMPP 3.4 and this is not supported by all operators' smscs yet.
	 */
	protected int sendMultipleSubmitSM(INotificationRequest deliveryRequest, final String text,final Address source, final Address destination, final byte[] message, final MessageEncoding encoding, Date expiryTime, final boolean receipt, final Integer dest_addr_subunit
										, final boolean isSendToPayload, boolean flashsms) throws IOException, SMPPException, SMPPRuntimeException {
		boolean trace = logger.isTraceEnabled();
		// http://www.activexperts.com/xmstoolkit/sms/multipart/
		// http://www.ashishpaliwal.com/blog/2009/01/smpp-sending-long-sms-through-smpp/
		// http://sourceforge.net/p/smppapi/discussion/84650/thread/73ceac01
		// http://cfg11n.blogspot.com/2010_06_01_archive.html
		// http://en.wikipedia.org/wiki/Concatenated_SMS

		// The maximum length when using UDH is 153, not 154, since the UDH header
		// takes 6 or 7 bytes which is more than 6 characters.
		final int parts = (message.length<MAX_MESSAGE_LENGTH ? 1 : (int)Math.ceil(((double)message.length) / getMaxSegmentLength()));
		if (trace) logger.trace("SimpleSMSSender.sendMultipleSubmitSM("+text+", "+source+", "+destination+", ..., "+encoding+", "+receipt+", "+dest_addr_subunit+") parts = "+parts+" = ("+message.length+"<"+MAX_MESSAGE_LENGTH+" ? 1 : (int)Math.ceil("+((double)message.length) / getMaxSegmentLength()+"))");

		if (parts == 1) return sendSubmitSM(deliveryRequest, text, source, destination, message, encoding, expiryTime, receipt, dest_addr_subunit, isSendToPayload, flashsms);

		final int SARRefNum = getNextSARRefNum();
		final int dstPos = MAX_MESSAGE_LENGTH-getMaxSegmentLength();
		for (int i=0;i<parts;i++) {
			try {
				int srcPos = Math.max(0, i*getMaxSegmentLength());
				int size;
				int len;
				ByteBuffer packet;
				if (i==parts-1) {
		        	// last part
		        	len = message.length - (i*getMaxSegmentLength());
		        	size = message.length - (i*getMaxSegmentLength()) + dstPos;
				 } else {
		        	// first or next parts
					len = getMaxSegmentLength();
					size = MAX_MESSAGE_LENGTH;
		        }
				packet = ByteBuffer.allocate(size);

				SubmitSM sm = (SubmitSM) conn.getConnection().newInstance(SMPPPacket.SUBMIT_SM);
				// Set the sequence number that will be sent by the connection
				// This is a different implementation as {@see ie.omk.smpp.util.DefaultSequenceScheme}
				//
				// The SMPP v3.4 tells it's an Integer over 4 bytes starting at
				// 1 and up to 2147483647 (> 2 milliards), which maps the Java int / Integer type
				final int seqId = super.getSMPPConnection().referenceMessageId(deliveryRequest, text, true, i, parts-1); // simple message index for the current connection, not the E4O (i.e. notif bean) message id)
				if (trace) logger.trace("SimpleSMSSender.sendMultipleSubmitSM("+seqId+", "+text+", "+source+", "+destination+", ..., "+encoding+", "+receipt+", "+dest_addr_subunit+") parts = "+parts+" = ("+message.length+"<"+MAX_MESSAGE_LENGTH+" ? 1 : (int)Math.ceil("+((double)message.length) / getMaxSegmentLength()+"))");
				sm.setSequenceNum(seqId); // Different sequence number for all Submit_SM

				if (this.service_type != null) {
					sm.setServiceType(this.service_type);
				}

				if (dest_addr_subunit != null) {
					// The subcomponent in the destination device for which	the user data is intended.
					// The recommended way to specify GSM message class control is by specifying the relevant setting in this optional parameter
					// It is used to route messages when received by a mobile station, for example to a smart card in the mobile station or to an external device connected to the mobile station.
					// 0x00 = Unknown (default)
					// 0x01 = MS Display
					// 0x02 = Mobile Equipment
					// 0x03 = Smart Card 1 (expected to be SIM if a SIM exists in the MS)
					// 0x04 = External Unit 1
					sm.setOptionalParameter(Tag.DEST_ADDR_SUBUNIT/*0x05*/, dest_addr_subunit);
				}

				if (source != null) {
					sm.setSource(source);
				}
				if (destination != null) {
					sm.setDestination(destination);
				}

				byte[] tmp_message;

				if (this.support_udh) {
					// Including UDH in message decreases the number of bytes which can be transmitted in single PDU
					// http://en.wikipedia.org/wiki/Concatenated_SMS
					// http://onesec.googlecode.com/svn/trunk/onesec-rven/src/main/java/org/onesec/raven/sms/sm/SMTextFactory.java
					// http://memoirniche.wordpress.com/2010/04/10/smpp-submit-pdu/
					// http://mobiletidings.com/2009/02/18/combining-sms-messages/
					// https://smppapi.svn.sourceforge.net/svnroot/smppapi/smppapi/trunk/src/main/java/com/adenki/smpp/gsm/UserDataImpl.java
					if (trace) logger.trace("SimpleSMSSender.sendMultipleSubmitSM("+text+", "+source+", "+destination+", ..., "+encoding+", "+receipt+", "+dest_addr_subunit+") ["+i+"] udh_msg_ref_num="+SARRefNum+", udh_total_segments="+parts+", udh_segment_seqnum="+(i+1));
					sm.setEsmClass(0x40); // To indicate that a UDH is present we need to set bit 6 (0x40) => UDH Indicator = 1
					// store the header in the body, at the beginning of the message body
					packet.put((byte) (UDH_LENGTH-1)); // Field 1 (1 octet): Length of User Data Header (UDL), in this case 06.
					if (UDH_LENGTH == 7) {
						packet.put((byte) 8); // Field 2 (1 octet): Information Element Identifier, equal to 08 (Concatenated short messages, 16-bit reference number)
						packet.put((byte) 4); // Field 3 (1 octet): Length of the header in bytes, excluding the first two fields; equal to 04
						packet.putShort((short) SARRefNum); // Field 4 (2 octets): 0000-FFFF, CSMS reference number, must be same for all the SMS parts in the CSMS
					} else {
						packet.put((byte) 0); // Field 2 (1 octet): Information Element Identifier, equal to 00 (Concatenated short messages, 8-bit reference number)
						packet.put((byte) 3); // Field 3 (1 octet): Length of the header in bytes, excluding the first two fields; equal to 03
						packet.put((byte) SARRefNum); // Field 4 (1 octet): 00-FF, CSMS reference number, must be same for all the SMS parts in the CSMS
					}
					packet.put((byte) parts); // Field 5 (1 octet): 00-FF, total number of parts. The value shall remain constant for every short message which makes up the concatenated short message. If the value is zero then the receiving entity shall ignore the whole information element
					packet.put((byte) (i+1)); // Field 6 (1 octet): 00-FF, this part's number in the sequence. The value shall start at 1 and increment for every short message which makes up the concatenated short message. If the value is zero or greater than the value in Field 5 then the receiving entity shall ignore the whole information element. [ETSI Specification: GSM 03.40 Version 5.3.0: July 1996]
		            // here we may use 16-bit instead of 8-bit reference number in order to reduce the probability that two different concatenated messages are sent with identical reference numbers to a receiver.
				}
				if (this.support_udh && encoding.getEncodingLength() == 7) { // returns 7 or 8
					// GSM-7 encoded characters always start on a septet boundary, so if you have a UDH you might have to add some padding
	            	// bits to have you text start at a septet boundary.
	            	// up to 6 bits of zeros need to be inserted at the start of the "real" message body
	            	int bits_size = UDH_LENGTH*8; // in bits, bits_size = number of octets x bit size of octets
	            	int padding_bits_size = (bits_size%7>0?7-(bits_size%7):0);
	            	if (padding_bits_size > 0) {
	            		if (trace) logger.trace("SimpleSMSSender.sendMultipleSubmitSM("+text+", "+source+", "+destination+", ..., "+encoding+", "+receipt+", "+dest_addr_subunit+") ["+i+"] "+padding_bits_size+" bits of padding");
	            		// make a copy of the sample of the message we want to padd
	            		tmp_message = new byte[len];
	            		System.arraycopy(message, srcPos, tmp_message, 0, len);
	            		// do the padding
	            		tmp_message = paddTo7bits(tmp_message, padding_bits_size);
	            		// as we manipulate the sample copy, no need to add the offset
	            		srcPos = 0;
	            		if (tmp_message.length > len) {
	            			int new_capacity = packet.capacity()+(tmp_message.length-len);
	            			if (trace) logger.trace("SimpleSMSSender.sendMultipleSubmitSM("+text+", "+source+", "+destination+", ..., "+encoding+", "+receipt+", "+dest_addr_subunit+") ["+i+"] allocate a bigger buffer "+packet.capacity()+" => "+new_capacity);
	            			// in case packet.array() is too small to carry tmp_message, make a bigger one
	            			ByteBuffer tmp_packet = ByteBuffer.allocate(new_capacity);
	            			// reset the buffer counters
	            			packet.flip();
	            			// copy the content, remaining bytes are 0x0
	            			tmp_packet.put(packet.array());
	            			// replace the reference
	            			packet = tmp_packet;
	            			// set the new length to copy
	            			len = tmp_message.length;
	            		}
	            	} else {
	            		if (trace) logger.trace("SimpleSMSSender.sendMultipleSubmitSM("+text+", "+source+", "+destination+", ..., "+encoding+", "+receipt+", "+dest_addr_subunit+") ["+i+"] no padding");
	            		tmp_message = message;
	            	}
				} else {
					tmp_message = message;
				}

				// transfer the len bytes of the message part in the message body
		        System.arraycopy(tmp_message, srcPos, packet.array(), dstPos, len);
	            sm.setMessage(packet.array(), encoding);

				if (this.support_sar) {
					// optional parameters at the end of the message
					if (trace) logger.trace("SimpleSMSSender.sendMultipleSubmitSM("+text+", "+source+", "+destination+", ..., "+encoding+", "+receipt+", "+dest_addr_subunit+") ["+i+"] sar_msg_ref_num="+SARRefNum+", sar_total_segments="+parts+", sar_segment_seqnum="+(i+1));
					sm.setOptionalParameter(Tag.SAR_MSG_REF_NUM, new Integer(SARRefNum));
			        sm.setOptionalParameter(Tag.SAR_TOTAL_SEGMENTS, new Integer(parts));
			        sm.setOptionalParameter(Tag.SAR_SEGMENT_SEQNUM, new Integer(i+1));
				}

	            /*
			     * Set the expiry time of the message. If the message is not delivered by
			     * time 'd', it will be cancelled and never delivered to it's destination.
			     * [SMPP 3.4]
			     * The validity_period parameter indicates the SMSC expiration time, after which the message
				 * SHOULD be discarded if not delivered to the destination. It can be defined in absolute time format
				 * or relative time format.
			     */
//				if(midnight_expiry_smooth_hours != null){
//					expiryTime = computeSmoothExpiryDate(expiryTime, midnight_expiry_smooth_hours);
//				}
//				sm.setExpiryTime(expiryTime);
				if (receipt) {
					// configuration generally request a receipt
					// let filter the need of receipt for each part based on the configuration
					if((this.handle_submit_sm_response_in_multi_part.equals(MULTI_PART_SUBMIT_SM_HANDLE_ALL))
							||
							(i == 0 && this.handle_submit_sm_response_in_multi_part.equals(MULTI_PART_SUBMIT_SM_HANDLE_FIRST))
							||
							(i == parts-1 && this.handle_submit_sm_response_in_multi_part.equals(MULTI_PART_SUBMIT_SM_HANDLE_LAST))){
						sm.setRegistered(1); // tell we need to receive delivery message for all parts
						if(logger.isTraceEnabled()){
							logger.trace("SimpleSMSSender.sendMultipleSubmitSM request IdReceipt for sms " + text + " and part number (0 to n-1) " + i + " on total (N-1) parts: " + parts);
						}
					}
					else {
						if(logger.isTraceEnabled()){
							logger.trace("SimpleSMSSender.sendMultipleSubmitSM DON'T request IdReceipt for sms " + text + " and part number (0 to n-1) " + i + " on total (N-1) parts: " + parts);
						}
					}
				}
		        // Send the SMS (thread-safe), return null in async mode
				conn.getConnection().sendRequest(sm);
			} catch(IOException e) {
				logger.warn("SimpleSMSSender.sendMultipleSubmitSM("+text+", "+source+", "+destination+", ..., "+encoding+", "+receipt+", "+dest_addr_subunit+") failed to send part "+i+"/"+parts+" due to "+e);
				throw e; // re-throw
			} catch(SMPPException e) {
				logger.warn("SimpleSMSSender.sendMultipleSubmitSM("+text+", "+source+", "+destination+", ..., "+encoding+", "+receipt+", "+dest_addr_subunit+") failed to send part "+i+"/"+parts+" due to "+e);
				throw e; // re-throw
			} catch(SMPPRuntimeException e) {
				logger.warn("SimpleSMSSender.sendMultipleSubmitSM("+text+", "+source+", "+destination+", ..., "+encoding+", "+receipt+", "+dest_addr_subunit+") failed to send part "+i+"/"+parts+" due to "+e);
				throw e; // re-throw
			}
		}
		if (trace) logger.trace("SimpleSMSSender.sendMultipleSubmitSM("+text+", "+source+", "+destination+", ..., "+encoding+", "+receipt+", "+dest_addr_subunit+") "+parts+" parts were sent for "+message.length+" bytes");
		return parts;
	}

	protected int sendSubmitSM(INotificationRequest deliveryRequest, final String text, final Address source, final Address destination, final byte[] message, final MessageEncoding encoding, Date expiryTime, final boolean receipt,
								final Integer dest_addr_subunit, final boolean isSendToPayload, boolean flashsms) throws IOException, SMPPException, SMPPRuntimeException {
		final int id = super.getSMPPConnection().referenceMessageId(deliveryRequest,text, false, 0 /*useless for simple sms*/, 0 /*useless for simple sms */); // simple message index for the current connection, not the E4O (i.e. notif bean) message id)
		logger.info("SimpleSMSSender.sendSubmitSM("+id+", "+source+", "+destination+", ..., "+encoding+", "+receipt+", "+dest_addr_subunit+ ", "+isSendToPayload+")");
		SubmitSM sm = (SubmitSM) conn.getConnection().newInstance(SMPPPacket.SUBMIT_SM);
		// Set the sequence number that will be sent by the connection
		// This is a different implementation as {@see ie.omk.smpp.util.DefaultSequenceScheme}
		//
		// The SMPP v3.4 tells it's an Integer over 4 bytes starting at
		// 1 and up to 2147483647 (> 2 milliards), which maps the Java int / Integer type
		sm.setSequenceNum(id);

		if (this.service_type != null) {
			sm.setServiceType(this.service_type);
		}
		
	    if (source != null) {
            sm.setSource(source);
        }
        if (destination != null) {
            sm.setDestination(destination);
        }

		if (dest_addr_subunit != null) {
			// The subcomponent in the destination device for which	the user data is intended.
			// The recommended way to specify GSM message class control is by specifying the relevant setting in this optional parameter
			// It is used to route messages when received by a mobile station, for example to a smart card in the mobile station or to an external device connected to the mobile station.
			// 0x00 = Unknown (default)
			// 0x01 = MS Display
			// 0x02 = Mobile Equipment
			// 0x03 = Smart Card 1 (expected to be SIM if a SIM exists in the MS)
			// 0x04 = External Unit 1
			sm.setOptionalParameter(Tag.DEST_ADDR_SUBUNIT/*0x05*/, dest_addr_subunit);
		}

		//To handle the payload functionality. Note: To send the message on payload then isSendToPayload should be true and supportSar should be false.
		if(isSendToPayload && !this.support_sar){
			if(logger.isDebugEnabled()){
				logger.debug("Setting the message payload property to send the message to payload.");
			}
			sm.setOptionalParameter(Tag.MESSAGE_PAYLOAD, message);
		}else{
			sm.setMessage(message, encoding);
			
			if(flashsms) {
			  sm.setDataCoding(24);
			  sm.setEsmClass(3);
			}
		}
		/*
	     * Set the expiry time of the message. If the message is not delivered by
	     * time 'd', it will be cancelled and never delivered to it's destination.
	     * [SMPP 3.4]
	     * The validity_period parameter indicates the SMSC expiration time, after which the message
		 * SHOULD be discarded if not delivered to the destination. It can be defined in absolute time format
		 * or relative time format.
	     */

		if(midnight_expiry_smooth_hours != null){
			expiryTime = computeSmoothExpiryDate(expiryTime, midnight_expiry_smooth_hours);
		}
		
		sm.setExpiryTime(expiryTime);
		if (receipt) {
			sm.setRegistered(1); // tell we need to receive delivery message
		}

		// Send the SMS (thread-safe), return null in async mode
		conn.getConnection().sendRequest(sm);
		return 1;
	}

	private Date computeSmoothExpiryDate(Date expiryTime, Integer nbHoursSmoothing){
		// first check if the expiryDate maps 00:00:00
		Calendar c = Calendar.getInstance();
		if (logger.isDebugEnabled()) {
			logger.debug("SimpleSMSSender.computeSmoothExpiryDate Received time " + expiryTime + " configured nbHoursSmoothing " + nbHoursSmoothing);
		}
		c.setTime(expiryTime);
		if(nbHoursSmoothing != null && nbHoursSmoothing > 0){
			if(c.get(Calendar.HOUR_OF_DAY) == 0){
				if(c.get(Calendar.MINUTE) == 0){
					if(c.get(Calendar.SECOND) == 0){
						// this is a date to change !!
						// so compute number of seconds in the next nbHoursSmoothing hours:
						int nbSeconds = nbHoursSmoothing * 3600;
						// now let choose one second
						int secondsDelay = (int)(Math.random()*nbSeconds);
						// let add this delay to the Calendar
						c.add(Calendar.SECOND,  secondsDelay);
					}
				}
			}
		}
		if (logger.isDebugEnabled()) {
			logger.debug("SimpleSMSSender.computeSmoothExpiryDate incoming time " + expiryTime + " new Time " + c.getTime());
		}
		return c.getTime();
	}


	/**
     * Pack a byte array according to the GSM bit-packing algorithm.
     * The GSM specification defines a simple compression mechanism for its
     * default alphabet to pack more message characters into a smaller space.
     * Since the alphabet only contains 128 symbols, each one can be represented
     * in 7 bits. The packing algorithm squeezes the bits for each symbol
     * "down" into the preceeding byte (so bit 7 of the first byte actually
     * contains bit 0 of the second symbol in a default alphabet string, bits
     * 6 and 7 in the second byte contain bits 0 and 1 of the third symbol etc.)
     * Since the maximum short message length is 140 <b>bytes</b>, you save
     * one bit per byte using the default alphabet giving you a total of
     * 140 + (140 / 8) = 160 characters to use. This is where the 160 character
     * limit comes from in SMPP packets.
     * <p>
     * Having said all that, most SMSCs do <b>NOT</b> use the packing
     * algorithm when communicating over TCP/IP. They either use a full
     * 8-bit alphabet such as ASCII or Latin-1, or they accept the default
     * alphabet in its unpacked form. As such, you will be unlikely to
     * need this method.
     *
     * @param unpacked The unpacked byte array.
     * @return A new byte array containing the bytes in their packed form.
     */
	protected byte[] pack(byte[] unpacked) {
		byte[] packed = DefaultAlphabetEncoding.getInstance().pack(unpacked);
		if (logger.isTraceEnabled()) logger.trace("SimpleSMSSender.pack pack message from "+unpacked.length+" to "+packed.length+" bytes");
		return packed;
	}

	protected byte[] paddTo7bits(final byte[] src, final int padding)
	{
		int bits_size = 0;
		int x = 0;
		try {
			bits_size = src.length*8;
			x = (bits_size+padding)/7;
			final byte[] dst = new byte[x];
			int bc=0;
			for(int i=0;i<src.length;i++)
			{
				byte b = src[i];
				dst[bc++] |= (byte)(b >> padding);
				if (bc < x) {
					dst[bc] |= (byte)(b << (8-padding));
				}
			}
			return dst;
		} catch(Exception e) {
			logger.warn("SimpleSMSSender.paddTo7bits failed to add "+padding+" bits ("+src.length+", "+bits_size+", "+x+") due to "+e);
			return src;
		}
	}

	/**
	 * @return the number of milliseconds before next second
	 */
	protected long getMilliSecondsBeforeNextSecond() {
		return 1000L - (System.currentTimeMillis() % 1000L);
	}

  @Override
  public void onSubmitSmResp(SubmitSMResp packet)
  {
    String packetSequenceNumber = ""+packet.getSequenceNum();
    SubmitSMCorrectionDeliveryRequest smsCorrelation = super.getSMPPConnection().getReferencedMessageId(packetSequenceNumber);
    if(smsCorrelation == null){
        logger.info("SimpleSMSSender.onSubmitSmResp:  (May be NORMAL) packet.getCommandStatus():"+packet.getCommandStatus()+" missing seqnum: "+packetSequenceNumber);
        return;
    }
    else{
        logger.info("SimpleSMSSender.onSubmitSmResp: packet.getCommandStatus():"+packet.getCommandStatus()+" ExpandedMsg : "+smsCorrelation.toString());
         super.getSMPPConnection().deleteReferencedMessageId(packetSequenceNumber);
    }

    switch (packet.getCommandStatus()) {
        case PacketStatus.OK:

            // take in account this submit SM response only if simple sms or the good part answer of the SMS...
            if(!smsCorrelation.isMultiPart()
                    ||
                    (smsCorrelation.isMultiPart() && this.handle_submit_sm_response_in_multi_part.equals(MULTI_PART_SUBMIT_SM_HANDLE_ALL))
                    ||
                    (smsCorrelation.isMultiPart() && smsCorrelation.getPartOrdinal() == 0 && this.handle_submit_sm_response_in_multi_part.equals(MULTI_PART_SUBMIT_SM_HANDLE_FIRST))
                    ||
                    (smsCorrelation.isMultiPart() && smsCorrelation.getPartOrdinal() == smsCorrelation.getTotalPartsNumber() && this.handle_submit_sm_response_in_multi_part.equals(MULTI_PART_SUBMIT_SM_HANDLE_LAST))
                    ){

                String messageId = packet.getMessageId();
                while(messageId != null && messageId.length() > 1 && messageId.startsWith("0")){
                    // remove the 0, FDuclos 20130207 for Digicel
                    messageId = messageId.substring(1);
                }
                if(messageId != null){
                    messageId = messageId.toLowerCase();
                }
                logger.info("SimpleSMSSender.onSubmitSmResp: seqnum: "+packetSequenceNumber+", idreceipt: "+ messageId);

                
                
                Boolean receiptRequired = null;
                if(smsCorrelation.getDeliveryRequest() instanceof SMSNotificationManagerRequest) {
                  receiptRequired = ((SMSNotificationManagerRequest)smsCorrelation.getDeliveryRequest()).getConfirmationExpected();
                }
                else {
                  receiptRequired = (Boolean) ((NotificationManagerRequest)smsCorrelation.getDeliveryRequest()).getNotificationParameters().get("node.parameter.confirmationexpected");
                }            
                
                if(receiptRequired == null || receiptRequired.booleanValue() == false) 
                  {
                    // now DR required, let complete the request now
                    completeDeliveryRequest(smsCorrelation.getDeliveryRequest(), messageId, MessageStatus.DELIVERED, DeliveryStatus.Acknowledged, PacketStatusUtils.getMessage(packet.getCommandStatus()));
                  }
                else 
                  {
                    // deliveryRequest expected
                    updateDeliveryRequest(smsCorrelation.getDeliveryRequest(), messageId, MessageStatus.DELIVERED, DeliveryStatus.Acknowledged, PacketStatusUtils.getMessage(packet.getCommandStatus())); 
                  }                
                logger.info("Feedback Call for Accept Handler for messageId "+ messageId + " SimpleSMSSender " + this.hashCode());

            }
            else {
                //we just ignore this submit sm response as it one intermediate response of a multi part SMS not to be taken into account
                if(logger.isTraceEnabled()){
                    logger.trace("SimpleSMSSender.onSubmitSmResp: Skip SubmitSM response (internal part of a Multi Part SMS " +packetSequenceNumber);
                }
            }
        break;
        // error message from which we will resend after conf "delay_on_queue_full" seconds (SMSC temporary unable to send message)
        case PacketStatus.MESSAGE_QUEUE_FULL:
          if (logger.isWarnEnabled()) {
            logger.info("SimpleSMSSender.onSubmitSmResp: Message Queue Full for sms "+ packetSequenceNumber+" will try to resend in "+this.delay_on_queue_full+" sec");
          }
          completeDeliveryRequest(smsCorrelation.getDeliveryRequest(), packet.getMessageId(), MessageStatus.QUEUE_FULL, DeliveryStatus.Failed,  PacketStatusUtils.getMessage(packet.getCommandStatus()));
          break;
        case PacketStatus.THROTTLING_ERROR:
            if (logger.isWarnEnabled()) {
                        logger.info("SimpleSMSSender.onSubmitSmResp: Throttling Error for sms "+ packetSequenceNumber+" will try to resend in "+this.delay_on_queue_full+" sec");
            }
            completeDeliveryRequest(smsCorrelation.getDeliveryRequest(), packet.getMessageId(), MessageStatus.THROTTLING, DeliveryStatus.Failed,  PacketStatusUtils.getMessage(packet.getCommandStatus()));

            logger.info("Feedback Call for Accept Handler for messageId "+packet.getMessageId() + " SimpleSMSSender " + this.hashCode());
            break;
        default:
            logger.info("SimpleSMSSender.onSubmitSmResp: Unknown Status "+ packet.getCommandStatus()+" ("+PacketStatusUtils.getMessage(packet.getCommandStatus())+")");
//            if(smsCorrelation.getMessageContent().getDialogManagerMessage().getIdentifier().getMessageId() != null){
//                originalMessageId = "" + smsCorrelation.getExpandedMessageContent().getDialogManagerMessage().getIdentifier().getMessageId().toString();
//            }
//            else {
//                originalMessageId = null;
//                if (logger.isDebugEnabled()) {
//                    logger.debug("SimpleSMSSender.onSubmitSmResp No message Id " + packet + " " + packet.getMessageStatus());
//                }
//            }
            completeDeliveryRequest(smsCorrelation.getDeliveryRequest(), packet.getMessageId(), MessageStatus.UNKNOWN, DeliveryStatus.Failed, PacketStatusUtils.getMessage(packet.getCommandStatus()));
            
            logger.info("Feedback Call for Accept Handler for messageId "+packet.getMessageId() + "SimpleSMSSender "+ this.hashCode());
        break;
    }
    
  }
  
  private void completeDeliveryRequest(INotificationRequest smsNotif, String messageId, MessageStatus status, DeliveryStatus deliveryStatus, String returnCodeDetails){
    smsNotif.setCorrelator(messageId);
    smsNotif.setDeliveryStatus(deliveryStatus);
    smsNotif.setMessageStatus(status);
    smsNotif.setReturnCode(status.getReturnCode());
    smsNotif.setReturnCodeDetails(returnCodeDetails);
    smsNotificationManager.completeDeliveryRequest(smsNotif);
  }
  
  private void updateDeliveryRequest(INotificationRequest smsNotif, String messageId, MessageStatus status, DeliveryStatus deliveryStatus, String returnCodeDetails){
    smsNotif.setCorrelator(messageId);
    smsNotif.setDeliveryStatus(deliveryStatus);
    smsNotif.setMessageStatus(status);
    smsNotif.setReturnCode(status.getReturnCode());
    smsNotif.setReturnCodeDetails(returnCodeDetails);
    smsNotificationManager.updateDeliveryRequest(smsNotif);
  }
  
  @Override
  public void onDeliverSm(DeliverSM packet)
  {

    logger.info("SimpleSMSSender.onDeliverSm() : execution started... ");
    try{

            logger.info("SimpleSMSSender.onDeliverSM: " + packet+" "+(packet!=null?packet.getSequenceNum():"")+" "+(packet!=null?"("+packet.getCommandStatus()+","+packet.getMessageStatus()+")":""));
        
        if (packet == null) {
            logger.error("SimpleSMSSender.onDeliverSM: empty DeliverSM");
            return;
        }
        int seqNum = packet.getSequenceNum();
        logger.info("packet.getCommandStatus():"+packet.getCommandStatus());
        switch (packet.getCommandStatus()) {
        case PacketStatus.OK:
            if (logger.isTraceEnabled()) {
                logger.trace("SimpleSMSSender.onDeliverSm: packet Status OK");
            }
            Integer messageStatus = (Integer)packet.getOptionalParameter(Tag.MESSAGE_STATE);
            String messageId = (String)packet.getOptionalParameter(Tag.RECEIPTED_MESSAGE_ID);


                logger.info("SimpleSMSSender.onDeliverSm: messageStatus == "+messageStatus+" and messageid == "+messageId);

            if (messageStatus == null && messageId == null) {
                    logger.info("SimpleSMSSender.onDeliverSm: messageStatus == null and messageid == null");
                // attempt to read the "message" field
                String message = packet.getMessageText();
                    if (logger.isTraceEnabled()) {
                        logger.trace("SimpleSMSSender.onDeliverSm: text " + message);
                    }
                if (message != null && message.length() > 0) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("SimpleSMSSender.onDeliverSm: message length > 0");
                    }
                    DeliveryReceipt dr = new DeliveryReceipt(message);
                    // stat on 7 bytes, the final status of the message.
                    messageStatus = dr.getFinalStatus();
                    // id on 10 bytes, the message ID allocated to the message by the SMSC when originally submitted, to encode in hex.
                    if(deliveryReceiptDecodingDecimalHexa == null
                            || !deliveryReceiptDecodingDecimalHexa.toLowerCase().equals(DELIVERY_RECEIPT_DEC)){
                        messageId = dr.getIdAsHex();
                    }
                    else {
                        messageId = dr.getId();
                    }

                    if (logger.isTraceEnabled()) {
                        logger.trace("SimpleSMSSender.onDeliverSM: manage to get "+dr.toString()+" from DeliverSM "+seqNum+" message field '"+message+"'");
                    }
                } else {
                    logger.warn("SimpleSMSSender.onDeliverSM DeliverSM has missing message field");
                }
            }else if(messageId != null){
                if(deliveryReceiptDecodingDecimalHexa == null
                        || !deliveryReceiptDecodingDecimalHexa.toLowerCase().equals(DELIVERY_RECEIPT_DEC)){
                    messageId = Long.toHexString(new Long(messageId));
                    if (logger.isInfoEnabled()) {
                        logger.info("SimpleSMSSender.onDeliverSM: convert messageId dec to hex "+messageId);
                    }
                }
            }
            if (messageId != null && messageStatus != null) {
                messageId = ("" + messageId).toLowerCase();
                
                // remove the starting 0 if present...
                // 160413 retrofit of E4O Master (fduclos 141106 problem encountered at Digicel Jamaica)
                while(messageId != null && messageId.length() > 1 && messageId.startsWith("0")){
                	messageId = messageId.substring(1);
                }


                String tmp = PacketStatusUtils.getStatus(messageStatus);
                if (tmp.equals("UNKNOWN")) {
                    logger.info("SimpleSMSSender.onDeliverSM: unknown messageStatus: "+ messageStatus);
                }
                    logger.info("SimpleSMSSender.onDeliverSM: Statut of seqNum: "+seqNum+", idreceipt: "+messageId
                            + " = " + messageStatus+" ("+tmp+")");
                
                MessageStatus evolutionCode = null;
                if(tmp != null){
                  evolutionCode = MessageStatus.fromExternalRepresentation(tmp);
                }
                if(evolutionCode != null){
                  sendFeedback(messageId, evolutionCode.getReturnCode());
                }else{
                  sendFeedback(messageId, messageStatus);
                }
            } else {

                // Handling MO message
                    logger.info("SimpleSMSSender.onDeliverSM: ESMClass " + packet.getEsmClass());
               

                if(packet.getEsmClass() == SMPPPacket.ESME_ROK){
                    logger.info("SimpleSMSSender.onDeliverSm: MO packet received via SMSC at driver.");
                    try {
                      // build a MO message and drop it into the good queue
                      if(sms_MO_event_name != null) {
                        EvolutionEngineEventDeclaration eventDeclaration = Deployment.getEvolutionEngineEvents().get(sms_MO_event_name);
                        // the MO must be handled...
                        if(sms_MO_class_constructor == null) {
                          // must check the MO class and keep in memory its constructor
                          if(eventDeclaration == null) {
                            logger.warn("SimpleSMSSender.onDeliverSm No Event Declaration for eventName " + eventDeclaration);
                            return;
                          }
                          else {
                            Class<? extends MONotificationEvent> moClass = null;
                            try {
                              moClass = (Class<? extends MONotificationEvent>)Class.forName(eventDeclaration.getEventClassName());
                            }
                            catch(Exception e) {
                              logger.warn("SimpleSMSSender.onDeliverSm Exception while retrieving class " + eventDeclaration.getEventClassName(), e);
                              return;
                            }
                            if(!MONotificationEvent.class.isAssignableFrom(moClass) || !EvolutionEngineEvent.class.isAssignableFrom(moClass)) {
                              // the given class does not respect the good interface
                              logger.warn("SimpleSMSSender.onDeliverSm The class " + eventDeclaration.getEventClassName() + " does not implement interface " + MONotificationEvent.class.getName() + " and/or " + EvolutionEngineEvent.class.getName());
                              return;
                            }
                            try {
                              sms_MO_class_constructor = moClass.getConstructor(new Class[] {});
                            }
                            catch(Exception e) {
                              logger.warn("SimpleSMSSender.onDeliverSm Exception while retrieving empty constructor of class " + eventDeclaration.getEventClassName(), e);
                              return;
                            }
                          }
                        }
                        MONotificationEvent moEvent = null;
                        try {
                          moEvent = sms_MO_class_constructor.newInstance(new Object[] {});
                        }
                        catch(Exception e) {
                          logger.warn("SimpleSMSSender.onDeliverSm Exception, Could not instanciate class " + eventDeclaration.getEventClassName(), e);
                          return;
                        }
                        String subscriberID = resolveSubscriberID(packet.getSource().getAddress());
                        if(subscriberID == null) {
                          logger.warn("SimpleSMSSender.onDeliverSm SUBSCRIBER_NOT_FOUND " + packet.getSource().getAddress());
                          return;
                        }
                        String destination = packet.getDestination()!=null ? packet.getDestination().getAddress() : null;
                        if(destination == null || destination.trim().equals("")) {
                          logger.warn("SimpleSMSSender.onDeliverSm DESTINATION_NOT_FOUND " + destination);
                          return;
                        }
                        
                        String body = packet.getMessageText();
                        if(body == null) {
                          logger.warn("SimpleSMSSender.onDeliverSm MO body is null " + body);
                          return;
                        }
                        
                        moEvent.fillWithMOInfos(subscriberID, new Date(), sms_MO_channel_name, packet.getSource()!=null ? packet.getSource().getAddress() : null, destination, body);
                        // now drop it into the good topic
                        SMSSenderFactory.kafkaProducer.send(new ProducerRecord<byte[], byte[]>(eventDeclaration.getEventTopic(), StringKey.serde().serializer().serialize(eventDeclaration.getEventTopic(), new StringKey(subscriberID)), eventDeclaration.getEventSerde().serializer().serialize(eventDeclaration.getEventTopic(), (EvolutionEngineEvent)moEvent)));
                      }
                    } catch (Exception e) {
                        logger.warn("SMSSender.onDeliverSM MO handling failed due to "+e,e);
                    }
                }else{
                    logger.warn("SMSSender.onDeliverSM Recieved unknown type of message, which is not handled.");
                }
            }
            break;
        default:
            logger.info("SMSSender.onDeliverSM "+seqNum+" unknown status "+ packet.getCommandStatus()+" ("+PacketStatusUtils.getMessage(packet.getCommandStatus())+")");
        }
        try {
                logger.info("SimpleSMSSender.onDeliverSM attempt to ack DeliverSM "+seqNum);
            conn.getConnection().ackDeliverSm(packet);
        } catch (IOException e) {
            logger.warn("SimpleSMSSender.onDeliverSM Exception while sending DeliverSM "+seqNum+" response "+e,e);
            }
        }

        catch(Exception e){
            e.printStackTrace();
            logger.info("SimpleSMSSender.onDeliverSm: Exception " + e.getClass().getName() + " while handling delivery receipt ", e);
            
        }
  }
  
  private void sendFeedback(String messageId, int smppPacketStatus){
    logger.info("SimpleSMSSender.sendFeedback("+messageId+", "+smppPacketStatus+") got a response, processing feedback");
    HashMap<String,Object> correlatorUpdateRecord = new HashMap<String,Object>();
    correlatorUpdateRecord.put("result", smppPacketStatus);
    JSONObject correlatorUpdate = JSONUtilities.encodeObject(correlatorUpdateRecord);
    smsNotificationManager.submitCorrelatorUpdateDeliveryRequest(messageId, correlatorUpdate);
  }

  @Override
  public void onSubmitSM(SubmitSM packet)
  {
    logger.info("SimpleSMSSender.onSubmitSM() execution started..");
    logger.warn("SimpleSMSSender.onSubmitSM: should not happen");
  }
  
  
  /****************************************
  *
  *  resolveSubscriberID
  *
  ****************************************/
  
  private String resolveSubscriberID(String customerID)
  {
    String result = null;
    try
      {
        result = SMSSenderFactory.subscriberIDService.getSubscriberID(Deployment.getGetCustomerAlternateID(), customerID);
      } catch (SubscriberIDServiceException e)
      {
        logger.error("SubscriberIDServiceException can not resolve subscriberID for {} error is {}", customerID, e.getMessage());
      }
    return result;
  }
}
