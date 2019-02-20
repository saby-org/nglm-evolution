package com.lumatagroup.expression.driver.SMTP.constants;

/**
 * 
 * @author Bhavishya
 *
 */
public final class SMTPConstants {
	public static final String MAIL_SMTP_HOST = "mail.smtp.host";
	public static final String MAIL_SMTP_SOCKETFACTORY_PORT = "mail.smtp.socketFactory.port";
	public static final String MAIL_SMTP_SOCKETFACTORY_CLASS = "mail.smtp.socketFactory.class";
	public static final String MAIL_SMTP_AUTH = "mail.smtp.auth";
	public static final String SENDER_EMAIL_ADDRESS = "sender.email.address";
	public static final String EMAIL_SUBJECT = "emailSubject";
	public static final String USER_NAME="user.name";
	public static final String PASSWORD="password";
	public static final String SMTP_PROTOCOL="smtp.protocol";
	public static final String DRIVER_SESSION_DEBUG_FLAG="smtp.driver.session.debug.flag";
	public static final String REPLY_TO_EMAIL_ADDRESS="reply.to.email.address";
	public static final String CONNECTION_TIMEOUT_VAL = "driver.smtp.connection.timeout";
	public static final String CURL_RA_NUM_RETRY="feedback.generic.polling.retry.attempts";
	public static final String CURL_RA_DELAY="feedback.generic.polling.retry.delay";
	public static final String DYN_URLS_EMAIL_DELIVERY = "dyn.url.emailDelivery";
	public static final String DYN_URLS_EMAIL_OPENED = "dyn.url.emailOpened";
	public static final String DYN_URLS_EMAIL_BOUNCED = "dyn.url.emailBounced";
	public static final String DYN_URLS_CLICKED_URL = "dyn.url.clickedUrl";
	public static final String DYN_API_KEY = "dyn.apiKey";
	public static final String DYN_TIMEZONE = "dyn.timezone";	
	public static final String DEFAULT_TIMEZONE = "UTC";
	public static final String THIRD_PARTY_REQUEST_API_DATE_FORMAT_DEFAULT = "yyyy-MM-dd'T'HH:mm:ss";
	public static final String THIRD_PARTY_REQUEST_API_DATE_FORMAT = "dyn.api.call.date.format";
	public static final String CSV_LOG_DATE_TIME_FORMAT = "csv.log.date.format";
	public static final String CSV_LOG_DATE_TIME_FORMAT_DEFAULT = "yyyy-MM-dd HH:mm:ss";
	public static final String START_END_BONUS_TIME = "dyn.api.call.time.margin";
	public static final int DEFAULT_END_TIME_BONUS = 600000;
	public static final String FEEDBACK_OPEN_NO_OF_RETRY = "feedback.polling.retry.attempts";
	public static final int URL_CLICKED_RETRY_DEFAULT_VAL = 5;
	public static final int URL_CLICKED_RETRY_DELAY_DEFAULT_VAL = 600000;
	public static final int OPENED_FEEDBACK_NUM_RETRY_DEFAULT_VAL = 5;
	public static final String HTTP_REQUEST_API_TIMEOUT = "http.connection.timeout.secs";
	public static final int HTTP_REQUEST_API_TIMEOUT_DEFAULT_VAL = 20;
	public static final String SMTP_DRIVER_CONNECTION_CHECKING_TIME = "smtp.driver.connection.checking.time.delay";
	public static final int DEFAULT_SMTP_DRIVER_CONNECTION_CHECKING_TIME_VAL = 5000;
	public static final String HTML_CONTENT_CHARSET = "html.content.charset";
	public static final String SUBJECT_CHARSET = "subject.charset";
//	public static final String EMAIL_DELIVERED = "Message Delivered";
//	public static final String EMAIL_ACCEPTED = "Message Accepted";
//	public static final String EMAIL_ERROR = "Message ERROR";
//	public static final String EMAIL_INVALID = "Message INVALID";
//	public static final String EMAIL_RETRY = "Message Retry";
	public static final String LOCK_NAME_PROP = "lock.name";
	public static final String LOCK_NAME_DEFAULT = "ndm_lock";
	public static final String POLLING_INTERVAL_PROP = "polling.interval.secs";
	public static final long POLLING_INTERVAL_DEFAULT = 3600; // 1 hour
	public static final String DURATION_DAYS_PROP = "initial.duration.days";
	public static final int DURATION_DAYS_DEFAULT = 30; // one month
	public static final String SQL_DATE_FORMAT_PROP = "sql.date.format";
	public static final String SQL_DATE_FORMAT_DEFAULT = "yyyy-MM-dd HH:mm:ss";
	public static final String DATE_ID_PROP = "date.id";
	public static final int DATE_ID_DEFAULT = 1;
	public static final String LOCK_GRACE_PERIOD_PROP = "lock.grace.period.minutes";
	public static final long LOCK_GRACE_PERIOD_DEFAULT = 10;
	public static String DATE_TABLE_NAME_PROP = "table.name.date";
	public static String DATE_TABLE_NAME_DEFAULT = "dialog_manager.smtp_driver_date";
	
	//Need to remove the to email address constants
	//public static final String TO_EMAIL = "bhavishya.soni@lumatagroup.com";
	
	public static final String initial_duration_days = "1";
	public static final String subject_charset = "UTF-8";
	public static final String feedback_generic_polling_retry_delay = "150000";
	public static final String smtp_driver_connection_checking_time_delay = "20000";
	public static final String feedback_polling_initial_try_with_delay = "false";
	public static final String polling_interval_secs = "30";
	public static final String dyn_timezone = "GMT";
	public static final String dyn_api_call_date_format = "yyyy-MM-dd'T'HH";
	public static final String html_content_charset = "utf-8";
	public static final String driver_smtp_connection_timeout = "180000";
	public static final String feedback_generic_polling_retry_attempts = "10";
	public static final String date_id = "1";
	public static final String feedback_polling_retry_attempts = "20";
	public static final String table_name_date = "dialog_manager.smtp_driver_date";
	public static final String dyn_url_emailBounced = "http";
	public static final String dyn_url_clickedUrl = "http";
	public static final String dyn_api_call_time_margin = "3600000";
	public static final String mail_smtp_socketFactory_class = "javax.net.ssl.SSLSocketFactory";
	public static final String http_connection_timeout_secs = "60";
	public static final String smtp_protocol = "SMTP";
	public static final String dyn_url_emailDelivery = "http";
	public static final String mail_smtp_auth = "true";
	public static final String mail_smtp_host = "smtp.dynect.net";
	public static final String dyn_url_emailOpened = "http";
	public static final String name = "smtp_3rdparty";
	public static final String smtp_driver_session_debug_flag = "true";
	public static final String lock_grace_period_minutes = "10";
	public static final String sql_date_format = "yyyy-MM-dd HH";
	public static final String mail_smtp_socketFactory_port = "25";
	public static final String feedback_polling_try_constant_delay = "150000";
	
}
