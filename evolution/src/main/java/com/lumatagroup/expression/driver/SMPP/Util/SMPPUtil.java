package com.lumatagroup.expression.driver.SMPP.Util;

import java.util.Calendar;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lumatagroup.expression.driver.SMPP.SMPPConnection;

public class SMPPUtil {
	
	public enum SMPP_CONFIGS{
		name,
		isMO,
		isMT,
		address,
		port,
		connection_type,
		system_id,
		password,
		system_type,
		time_between_reconnection,
		time_between_connection_check,
		smpp_receiver_thread_number,
		source_addr,
		source_addr_ton,
		source_addr_npi,
		dest_addr_ton,
		dest_addr_npi,
		data_coding,
		load_HP_roman8_as_data_coding0,
		load_UTF16_as_data_coding8,
		data_packing,
		encoding_charset,
		expiration_period,
		delay_on_queue_full,
		support_sar,
		support_udh,
		handle_submit_sm_response_in_multi_part,
		dest_addr_subunit,
		service_type,
		delivery_receipt_decoding_hex_dec,
		is_send_to_payload,
		magic_number_configuration, 
		midnight_expiry_smooth_hours,
		sms_MO_event_name,
		sms_MO_channel_name
	}
	
	private static final Logger logger = LoggerFactory.getLogger(SMPPConnection.class);
	private static final int FIRST_DATE_OF_THE_WEEK = -1;
	private static final int MINIMAL_DAYS_IN_FIRST_WEEK = 4;
	public static  Date  getFeedBackDate(){
		return new Date();
	}
	public static String convertString(Object obj){
		String returnValue=null;
		if(obj!=null){
			returnValue=String.valueOf(obj);
		}
		return returnValue;
	}
	
	public static Calendar getCurrent() {	
		Calendar c = Calendar.getInstance();
		c.setMinimalDaysInFirstWeek(MINIMAL_DAYS_IN_FIRST_WEEK);
		c.setFirstDayOfWeek(FIRST_DATE_OF_THE_WEEK);
		return c;
	}
}
