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
		max_per_sec,
		interval_retry,
		support_sar,
		support_udh,
		handle_submit_sm_response_in_multi_part,
		dest_addr_subunit,
		service_type,
		delivery_receipt_decoding_hex_dec,
		is_send_to_payload,
		magic_number_configuration, 
		midnight_expiry_smooth_hours;
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
	
	/**
	 * Return an array of Date.
	 * <p>First date is for internal use, next attempt cannot be before that date, unless SMSC says so
	 * <p>Second is for the SMSC, the date the SMS is "canceled" on SMSC side
	 */
	public static Date[] getExpirationTimestamps(final Calendar cal, Integer period/*can be 0*/, Date max_expiration_timestamp, boolean one_attempt_per_day, int runningStartTimeInSeconds) {
		if (!one_attempt_per_day) {
			if (period > 0 || max_expiration_timestamp == null) {
				cal.add(Calendar.HOUR_OF_DAY, period);
			} else {
				// At Telma, we have the case of sms sent day-1 at 16h00, flushed at 06h30 day-2 cos expired (> 10h) and DeliverSM received day-2 at 09h00 !!
				// Thus they don't support SMPP validity_period parameter and we need to keep the sms long enough to receive their DeliverSM
				if (logger.isDebugEnabled()) logger.debug("SMSBeanManager.getExpirationTimestamps("+period+") no expiration period, set the expiration date to "+max_expiration_timestamp);
				cal.setTime(max_expiration_timestamp);
			}
			if (max_expiration_timestamp != null && cal.getTime().after(max_expiration_timestamp)) {
				if (logger.isDebugEnabled()) logger.debug("SMSBeanManager.getExpirationTimestamps("+period+") maximum reached, set the expiration date to "+max_expiration_timestamp);
				cal.setTime(max_expiration_timestamp);
			}
			if (logger.isTraceEnabled()) logger.trace("SMSBeanManager.getExpirationTimestamps("+period+") return "+cal.getTime());
			return new Date[]{cal.getTime(),cal.getTime()};
		} else {
			Calendar cal_ext = (Calendar)cal.clone();
			if (period > 0 || max_expiration_timestamp == null) {
				cal.add(Calendar.DAY_OF_YEAR, 1); // tomorrow
				cal.set(Calendar.HOUR_OF_DAY, 0); // used to get tomorrow's date, but at midnight
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				cal.add(Calendar.SECOND, runningStartTimeInSeconds);
				if (period > 0) {
					cal_ext.add(Calendar.HOUR_OF_DAY, period);
				} else {
					cal_ext = (Calendar)cal.clone();
				}
			} else {
				if (logger.isDebugEnabled()) logger.debug("SMSBeanManager.getExpirationTimestamps("+period+") no expiration period, set the expiration date to "+max_expiration_timestamp);
				cal.setTime(max_expiration_timestamp);
				cal_ext.setTime(max_expiration_timestamp);
			}
			if (max_expiration_timestamp != null) {
				if (cal.getTime().after(max_expiration_timestamp)) {
					if (logger.isDebugEnabled()) logger.debug("SMSBeanManager.getExpirationTimestamps("+period+") maximum reached, set the expiration date to "+max_expiration_timestamp);
					cal.setTime(max_expiration_timestamp);
				}
				if (cal_ext.getTime().after(max_expiration_timestamp)) {
					if (logger.isDebugEnabled()) logger.debug("SMSBeanManager.getExpirationTimestamps("+period+") maximum reached, set the expiration date to "+max_expiration_timestamp);
					cal_ext.setTime(max_expiration_timestamp);
				}
			}
			if (logger.isTraceEnabled()) logger.trace("SMSBeanManager.getExpirationTimestamps("+period+") return "+cal.getTime()+" // "+cal_ext.getTime());
			return new Date[]{cal.getTime(),cal_ext.getTime()};
		}
	}
	
	public static Calendar getCurrent() {	
		Calendar c = Calendar.getInstance();
		c.setMinimalDaysInFirstWeek(MINIMAL_DAYS_IN_FIRST_WEEK);
		c.setFirstDayOfWeek(FIRST_DATE_OF_THE_WEEK);
		return c;
	}
}
