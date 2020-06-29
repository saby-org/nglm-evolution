package com.lumatagroup.expression.driver.SMTP.util;

import java.util.HashMap;
import java.util.Map;

import com.lumatagroup.expression.driver.SMTP.MailSenderFactory;
import com.lumatagroup.expression.driver.SMTP.constants.SMTPConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Conf {
	private static Logger logger = LoggerFactory.getLogger(Conf.class);

	private static int openFeedbackNumOfRetry;
	private static int urlClickedRetry;
	private static int urlClickedRetryDelay;
	private static String urlEmailBounced;
	private static String  urlEmailOpened;
	private static String urlApiKey;
	private static String urlClickedUrl;
	private static String csvloggerDateTimeFormat;
	private static int startEndBonusTime;
	private static String timeZone = SMTPConstants.DEFAULT_TIMEZONE;
	private static String thirdPartyRequestApiDateFormat = SMTPConstants.THIRD_PARTY_REQUEST_API_DATE_FORMAT_DEFAULT;
	private static int httpRequestTimeoutSecs = SMTPConstants.HTTP_REQUEST_API_TIMEOUT_DEFAULT_VAL;
	private static String lockName;
	private static long pollingInterval = SMTPConstants.POLLING_INTERVAL_DEFAULT;
	private static Map<String, Object> configurationMap= new HashMap<String, Object>();
	private static String mailSMTPHost;
	private static String userName;
	private static int initialDurationDays = SMTPConstants.DURATION_DAYS_DEFAULT;
	private static String sqlDateFormat = SMTPConstants.SQL_DATE_FORMAT_DEFAULT;
	private static int dateId = SMTPConstants.DATE_ID_DEFAULT;
	private static String dateTableName = SMTPConstants.DATE_TABLE_NAME_DEFAULT;
	private static long lockGracePeriod = SMTPConstants.LOCK_GRACE_PERIOD_DEFAULT;

	public static void setConf(Map<String, Object> configMap) throws Exception {
	  
        configurationMap = configMap;
		
		if(configurationMap!=null && configurationMap.size()>=1){
			openFeedbackNumOfRetry = configurationMap.get(SMTPConstants.FEEDBACK_OPEN_NO_OF_RETRY)!=null?(Integer)configurationMap.get(SMTPConstants.FEEDBACK_OPEN_NO_OF_RETRY):SMTPConstants.OPENED_FEEDBACK_NUM_RETRY_DEFAULT_VAL;
			urlClickedRetry = configurationMap.get(SMTPConstants.CURL_RA_NUM_RETRY)!=null?(Integer)configurationMap.get(SMTPConstants.CURL_RA_NUM_RETRY):SMTPConstants.URL_CLICKED_RETRY_DEFAULT_VAL;
			urlClickedRetryDelay = configurationMap.get(SMTPConstants.CURL_RA_DELAY)!=null?(Integer)configurationMap.get(SMTPConstants.CURL_RA_DELAY):SMTPConstants.URL_CLICKED_RETRY_DELAY_DEFAULT_VAL;
			
			urlEmailBounced = SMTPUtil.convertString(configurationMap.get(SMTPConstants.DYN_URLS_EMAIL_BOUNCED));
			urlEmailOpened = SMTPUtil.convertString(configurationMap.get(SMTPConstants.DYN_URLS_EMAIL_OPENED));
			urlApiKey = SMTPUtil.convertString(configurationMap.get(SMTPConstants.DYN_API_KEY));
			urlClickedUrl = SMTPUtil.convertString(configurationMap.get(SMTPConstants.DYN_URLS_CLICKED_URL));
			
			timeZone = SMTPUtil.convertString(configurationMap.get(SMTPConstants.DYN_TIMEZONE)) == null ? SMTPConstants.DEFAULT_TIMEZONE : SMTPUtil.convertString(configurationMap.get(SMTPConstants.DYN_TIMEZONE));
			thirdPartyRequestApiDateFormat= SMTPUtil.convertString(configurationMap.get(SMTPConstants.THIRD_PARTY_REQUEST_API_DATE_FORMAT)) == null ? SMTPConstants.THIRD_PARTY_REQUEST_API_DATE_FORMAT_DEFAULT : SMTPUtil.convertString(configurationMap.get(SMTPConstants.THIRD_PARTY_REQUEST_API_DATE_FORMAT));
			csvloggerDateTimeFormat = SMTPUtil.convertString(configurationMap.get(SMTPConstants.CSV_LOG_DATE_TIME_FORMAT)) == null ? SMTPConstants.CSV_LOG_DATE_TIME_FORMAT_DEFAULT : SMTPUtil.convertString(configurationMap.get(SMTPConstants.CSV_LOG_DATE_TIME_FORMAT));
			startEndBonusTime = SMTPUtil.convertInt(configurationMap.get(SMTPConstants.START_END_BONUS_TIME)) == null ? SMTPConstants.DEFAULT_END_TIME_BONUS : SMTPUtil.convertInt(configurationMap.get(SMTPConstants.START_END_BONUS_TIME)).intValue();
			httpRequestTimeoutSecs = SMTPUtil.convertInt(configurationMap.get(SMTPConstants.HTTP_REQUEST_API_TIMEOUT)) == null ? SMTPConstants.HTTP_REQUEST_API_TIMEOUT_DEFAULT_VAL : SMTPUtil.convertInt(configurationMap.get(SMTPConstants.HTTP_REQUEST_API_TIMEOUT)).intValue();			
			lockName = SMTPUtil.convertString(configurationMap.get(SMTPConstants.LOCK_NAME_PROP)) == null ? SMTPConstants.LOCK_NAME_DEFAULT : SMTPUtil.convertString(configurationMap.get(SMTPConstants.LOCK_NAME_PROP));
			pollingInterval = SMTPUtil.convertLong(configurationMap.get(SMTPConstants.POLLING_INTERVAL_PROP)) == null ? SMTPConstants.POLLING_INTERVAL_DEFAULT : SMTPUtil.convertLong(configurationMap.get(SMTPConstants.POLLING_INTERVAL_PROP)).longValue();
			mailSMTPHost = (String)configurationMap.get(SMTPConstants.MAIL_SMTP_HOST);
			userName = (String)configurationMap.get(SMTPConstants.USER_NAME);			
			initialDurationDays = SMTPUtil.convertInt(configurationMap.get(SMTPConstants.DURATION_DAYS_PROP)) == null ? SMTPConstants.DURATION_DAYS_DEFAULT : SMTPUtil.convertInt(configurationMap.get(SMTPConstants.DURATION_DAYS_PROP)).intValue();
			sqlDateFormat = SMTPUtil.convertString(configurationMap.get(SMTPConstants.SQL_DATE_FORMAT_PROP)) == null ? SMTPConstants.SQL_DATE_FORMAT_DEFAULT : SMTPUtil.convertString(configurationMap.get(SMTPConstants.SQL_DATE_FORMAT_PROP));
			dateId = SMTPUtil.convertInt(configurationMap.get(SMTPConstants.DATE_ID_PROP)) == null ? SMTPConstants.DATE_ID_DEFAULT : SMTPUtil.convertInt(configurationMap.get(SMTPConstants.DATE_ID_PROP)).intValue();
			lockGracePeriod = SMTPUtil.convertLong(configurationMap.get(SMTPConstants.LOCK_GRACE_PERIOD_PROP)) == null ? SMTPConstants.LOCK_GRACE_PERIOD_DEFAULT : SMTPUtil.convertLong(configurationMap.get(SMTPConstants.LOCK_GRACE_PERIOD_PROP)).longValue();
			dateTableName = SMTPUtil.convertString(configurationMap.get(SMTPConstants.DATE_TABLE_NAME_PROP)) == null ? SMTPConstants.DATE_TABLE_NAME_DEFAULT: SMTPUtil.convertString(configurationMap.get(SMTPConstants.DATE_TABLE_NAME_PROP));
		}else{
			logger.error("Configuration Map does not have parameter values......");
			throw new Exception("Configuration Map does not have parameter values......" + configurationMap);
		}	

	}

	public static MailSenderFactory createMailSenderFactory() {
		return new MailSenderFactory(configurationMap);
	}

	public static int getOpenFeedbackNumOfRetry() {
		return openFeedbackNumOfRetry;
	}

	public static int getUrlClickedRetry() {
		return urlClickedRetry;
	}

	public static int getUrlClickedRetryDelay() {
		return urlClickedRetryDelay;
	}

	public static String getUrlEmailBounced() {
		return urlEmailBounced;
	}

	public static String getUrlEmailOpened() {
		return urlEmailOpened;
	}

	public static String getUrlApiKey() {
		return urlApiKey;
	}

	public static String getUrlClickedUrl() {
		return urlClickedUrl;
	}

	public static String getCsvloggerDateTimeFormat() {
		return csvloggerDateTimeFormat;
	}

	public static int getStartEndBonusTime() {
		return startEndBonusTime;
	}

	public static String getTimeZone() {
		return timeZone;
	}

	public static String getThirdPartyRequestApiDateFormat() {
		return thirdPartyRequestApiDateFormat;
	}

	public static int getHttpRequestTimeoutSecs() {
		return httpRequestTimeoutSecs;
	}

	public static String getLockName() {
		return lockName;
	}

	public static long getPollingInterval() {
		return pollingInterval;
	}
	
	public static String getMailSMTPHost() {
		return mailSMTPHost;
	}

	public static String getUserName() {
		return userName;
	}

	public static int getInitialDurationDays() {
		return initialDurationDays;
	}

	public static String getSqlDateFormat() {
		return sqlDateFormat;
	}

	public static int getDateId() {
		return dateId;
	}

	public static String getDateTableName() {
		return dateTableName;
	}

	public static long getLockGracePeriod() {
		return lockGracePeriod;
	}
	
}

