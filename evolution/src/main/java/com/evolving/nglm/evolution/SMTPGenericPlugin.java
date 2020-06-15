/*****************************************************************************
*
*  SMTPPlugin.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.MailNotificationManager.MailNotificationManagerRequest;
import com.evolving.nglm.evolution.NotificationManager.NotificationManagerRequest;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.DeliveryManagerForNotifications.MessageStatus;
import com.evolving.nglm.core.JSONUtilities;

import com.lumatagroup.expression.driver.SMTP.FeedbackThread;
import com.lumatagroup.expression.driver.SMTP.MailSenderFactory;
import com.lumatagroup.expression.driver.SMTP.SimpleEmailSender;
import com.lumatagroup.expression.driver.SMTP.constants.SMTPConstants;
import com.lumatagroup.expression.driver.dyn.NotificationStatus;

public class SMTPGenericPlugin implements NotificationInterface
{
  /*****************************************
  *
  *  logger
  *
  *****************************************/

  private static final Logger logger = LoggerFactory.getLogger(SMTPGenericPlugin.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private SimpleEmailSender[] senders = null;
  private int currentSenderIndex = 0; 
  private DeliveryManagerForNotifications mailNotificationManager;
  private FeedbackThread ft;

  /*****************************************
  *
  *  init
  *
  *****************************************/

  @Override public void init(DeliveryManagerForNotifications deliveryManagerForNotifications, JSONObject mailNotifSharedConfiguration)
  {
    logger.info("SMTP Driver init()");

    this.mailNotificationManager = deliveryManagerForNotifications;

    Map<String, Object> config = new HashMap<String, Object>();

    String initial_duration_days = JSONUtilities.decodeString(mailNotifSharedConfiguration, "initial.duration.days", false);
    String subject_charset = JSONUtilities.decodeString(mailNotifSharedConfiguration, "subject.charset", false);
    String feedback_generic_polling_retry_delay = JSONUtilities.decodeString(mailNotifSharedConfiguration, "feedback.generic.polling.retry.delay", false);
    String user_name = JSONUtilities.decodeString(mailNotifSharedConfiguration, "user.name", true);
    String smtp_driver_connection_checking_time_delay = JSONUtilities.decodeString(mailNotifSharedConfiguration, "smtp.driver.connection.checking.time.delay", false);
    String password = JSONUtilities.decodeString(mailNotifSharedConfiguration, "password", true);
    String feedback_polling_initial_try_with_delay = JSONUtilities.decodeString(mailNotifSharedConfiguration, "feedback.polling.initial.try.with.delay", false);
    String polling_interval_secs = JSONUtilities.decodeString(mailNotifSharedConfiguration, "polling.interval.secs", false);
    String dyn_timezone = JSONUtilities.decodeString(mailNotifSharedConfiguration, "dyn.timezone", false);
    String dyn_api_call_date_format = JSONUtilities.decodeString(mailNotifSharedConfiguration, "dyn.api.call.date.format", false);
    String html_content_charset = JSONUtilities.decodeString(mailNotifSharedConfiguration, "html.content.charset", false);
    String driver_smtp_connection_timeout = JSONUtilities.decodeString(mailNotifSharedConfiguration, "driver.smtp.connection.timeout", false);
    String feedback_generic_polling_retry_attempts = JSONUtilities.decodeString(mailNotifSharedConfiguration, "feedback.generic.polling.retry.attempts", false);
    String date_id = JSONUtilities.decodeString(mailNotifSharedConfiguration, "date.id", false);
    String reply_to_email_address = JSONUtilities.decodeString(mailNotifSharedConfiguration, "reply.to.email.address", true);
    String feedback_polling_retry_attempts = JSONUtilities.decodeString(mailNotifSharedConfiguration, "feedback.polling.retry.attempts", false);
    String table_name_date = JSONUtilities.decodeString(mailNotifSharedConfiguration, "table.name.date", false);
    String dyn_url_emailBounced = JSONUtilities.decodeString(mailNotifSharedConfiguration, "dyn.url.emailBounced", false);
    String dyn_url_clickedUrl = JSONUtilities.decodeString(mailNotifSharedConfiguration, "dyn.url.clickedUrl", false);
    String dyn_api_call_time_margin = JSONUtilities.decodeString(mailNotifSharedConfiguration, "dyn.api.call.time.margin", false);
    String mail_smtp_socketFactory_class = JSONUtilities.decodeString(mailNotifSharedConfiguration, "mail.smtp.socketFactory.class", false);
    String http_connection_timeout_secs = JSONUtilities.decodeString(mailNotifSharedConfiguration, "http.connection.timeout.secs", false);
    String smtp_protocol = JSONUtilities.decodeString(mailNotifSharedConfiguration, "smtp.protocol", false);
    String dyn_url_emailDelivery = JSONUtilities.decodeString(mailNotifSharedConfiguration, "dyn.url.emailDelivery", false);
    String mail_smtp_auth = JSONUtilities.decodeString(mailNotifSharedConfiguration, "mail.smtp.auth", true);
    String mail_smtp_host = JSONUtilities.decodeString(mailNotifSharedConfiguration, "mail.smtp.host", true);
    String sender_email_address = JSONUtilities.decodeString(mailNotifSharedConfiguration, "sender.email.address", true);
    String dyn_apiKey = JSONUtilities.decodeString(mailNotifSharedConfiguration, "dyn.apiKey", true);
    String dyn_url_emailOpened = JSONUtilities.decodeString(mailNotifSharedConfiguration, "dyn.url.emailOpened", false);
    String name = JSONUtilities.decodeString(mailNotifSharedConfiguration, "name", false);
    String smtp_driver_session_debug_flag = JSONUtilities.decodeString(mailNotifSharedConfiguration, "smtp.driver.session.debug.flag", false);
    String lock_grace_period_minutes = JSONUtilities.decodeString(mailNotifSharedConfiguration, "lock.grace.period.minutes", false);
    String sql_date_format = JSONUtilities.decodeString(mailNotifSharedConfiguration, "sql.date.format", false);
    String mail_smtp_socketFactory_port = JSONUtilities.decodeString(mailNotifSharedConfiguration, "mail.smtp.socketFactory.port", false);
    String feedback_polling_try_constant_delay = JSONUtilities.decodeString(mailNotifSharedConfiguration, "feedback.polling.try.constant.delay", false);
    boolean usingFakeEmulator = JSONUtilities.decodeBoolean(mailNotifSharedConfiguration, "using.fake.emulator", Boolean.FALSE);

    if(initial_duration_days == null) { 
      initial_duration_days= SMTPConstants.initial_duration_days;
    } 
    config.put("initial.duration.days", initial_duration_days);
    if(subject_charset == null) { 
      subject_charset= SMTPConstants.subject_charset;
    } 
    config.put("subject.charset", subject_charset);
    if(feedback_generic_polling_retry_delay == null) { 
      feedback_generic_polling_retry_delay= SMTPConstants.feedback_generic_polling_retry_delay;
    } 
    config.put("feedback.generic.polling.retry.delay", feedback_generic_polling_retry_delay);
    config.put("user.name", user_name);
    if(smtp_driver_connection_checking_time_delay == null) { 
      smtp_driver_connection_checking_time_delay= SMTPConstants.smtp_driver_connection_checking_time_delay;
    } 
    config.put("smtp.driver.connection.checking.time.delay", smtp_driver_connection_checking_time_delay); 
    config.put("password", password);
    if(feedback_polling_initial_try_with_delay == null) { 
      feedback_polling_initial_try_with_delay= SMTPConstants.feedback_polling_initial_try_with_delay;
    } 
    config.put("feedback.polling.initial.try.with.delay", feedback_polling_initial_try_with_delay);
    if(polling_interval_secs == null) { 
      polling_interval_secs= SMTPConstants.polling_interval_secs;
    } 
    config.put("polling.interval.secs", polling_interval_secs);
    if(dyn_timezone == null) { 
      dyn_timezone= SMTPConstants.dyn_timezone;
    } 
    config.put("dyn.timezone", dyn_timezone);
    if(dyn_api_call_date_format == null) { 
      dyn_api_call_date_format= SMTPConstants.dyn_api_call_date_format;
    } 
    config.put("dyn.api.call.date.format", dyn_api_call_date_format);
    if(html_content_charset == null) { 
      html_content_charset= SMTPConstants.html_content_charset;
    } 
    config.put("html.content.charset", html_content_charset);
    if(driver_smtp_connection_timeout == null) { 
      driver_smtp_connection_timeout= SMTPConstants.driver_smtp_connection_timeout;
    } 
    config.put("driver.smtp.connection.timeout", driver_smtp_connection_timeout);
    if(feedback_generic_polling_retry_attempts == null) { 
      feedback_generic_polling_retry_attempts= SMTPConstants.feedback_generic_polling_retry_attempts;
    } 
    config.put("feedback.generic.polling.retry.attempts", feedback_generic_polling_retry_attempts);
    if(date_id == null) { 
      date_id= SMTPConstants.date_id;
    } 
    config.put("date.id", date_id); 
    config.put("reply.to.email.address", reply_to_email_address);
    if(feedback_polling_retry_attempts == null) { 
      feedback_polling_retry_attempts= SMTPConstants.feedback_polling_retry_attempts;
    } 
    config.put("feedback.polling.retry.attempts", feedback_polling_retry_attempts);
    if(table_name_date == null) { 
      table_name_date= SMTPConstants.table_name_date;
    } 
    config.put("table.name.date", table_name_date);
    if(dyn_url_emailBounced == null) { 
      dyn_url_emailBounced= SMTPConstants.dyn_url_emailBounced;
    } 
    config.put("dyn.url.emailBounced", dyn_url_emailBounced);
    if(dyn_url_clickedUrl == null) { 
      dyn_url_clickedUrl= SMTPConstants.dyn_url_clickedUrl;
    } 
    config.put("dyn.url.clickedUrl", dyn_url_clickedUrl);
    if(dyn_api_call_time_margin == null) { 
      dyn_api_call_time_margin= SMTPConstants.dyn_api_call_time_margin;
    } 
    config.put("dyn.api.call.time.margin", dyn_api_call_time_margin);
    if(mail_smtp_socketFactory_class == null) { 
      mail_smtp_socketFactory_class= SMTPConstants.mail_smtp_socketFactory_class;
    } 
    config.put("mail.smtp.socketFactory.class", mail_smtp_socketFactory_class);
    if(http_connection_timeout_secs == null) { 
      http_connection_timeout_secs= SMTPConstants.http_connection_timeout_secs;
    } 
    config.put("http.connection.timeout.secs", http_connection_timeout_secs);
    if(smtp_protocol == null) { 
      smtp_protocol= SMTPConstants.smtp_protocol;
    } 
    config.put("smtp.protocol", smtp_protocol);
    if(dyn_url_emailDelivery == null) { 
      dyn_url_emailDelivery= SMTPConstants.dyn_url_emailDelivery;
    } 
    config.put("dyn.url.emailDelivery", dyn_url_emailDelivery);
    if(mail_smtp_auth == null) { 
      mail_smtp_auth= SMTPConstants.mail_smtp_auth;
    } 
    config.put("mail.smtp.auth", mail_smtp_auth);
    if(mail_smtp_host == null) { 
      mail_smtp_host= SMTPConstants.mail_smtp_host;
    } 
    config.put("mail.smtp.host", mail_smtp_host);
    config.put("sender.email.address", sender_email_address);
    config.put("dyn.apiKey", dyn_apiKey);
    if(dyn_url_emailOpened == null) { 
      dyn_url_emailOpened= SMTPConstants.dyn_url_emailOpened;
    } 
    config.put("dyn.url.emailOpened", dyn_url_emailOpened);
    if(name == null) { 
      name= SMTPConstants.name;
    } 
    config.put("name", name);
    if(smtp_driver_session_debug_flag == null) { 
      smtp_driver_session_debug_flag= SMTPConstants.smtp_driver_session_debug_flag;
    } 
    config.put("smtp.driver.session.debug.flag", smtp_driver_session_debug_flag);
    if(lock_grace_period_minutes == null) { 
      lock_grace_period_minutes= SMTPConstants.lock_grace_period_minutes;
    } 
    config.put("lock.grace.period.minutes", lock_grace_period_minutes);
    if(sql_date_format == null) { 
      sql_date_format= SMTPConstants.sql_date_format;
    } 
    config.put("sql.date.format", sql_date_format);
    if(mail_smtp_socketFactory_port == null) { 
      mail_smtp_socketFactory_port= SMTPConstants.mail_smtp_socketFactory_port;
    } 
    config.put("mail.smtp.socketFactory.port", mail_smtp_socketFactory_port);
    if(feedback_polling_try_constant_delay == null) { 
      feedback_polling_try_constant_delay= SMTPConstants.feedback_polling_try_constant_delay;
    } 
    config.put("feedback.polling.try.constant.delay", feedback_polling_try_constant_delay);

    logger.info("Configuration for SMTPGenericPlugin : " + config);
    MailSenderFactory mailSenderFactory = new MailSenderFactory(config);
    try
    {
      mailSenderFactory.init(deliveryManagerForNotifications);
      senders = mailSenderFactory.getEmailSenders();  
      if(senders == null || senders.length==0)
        {
          // TrapSenderUtils.sendTrap(SnmpTrapSeverity.WARNING, SnmpTrapType.COMMUNICATION_SERVER_CONNECTION, "SMTP3rdPartyConnection", "SMTPConnection to 3rdParty SMTPServer Loading Fail on host "+getSystemInfo());
          throw new Exception("SMTPGenericPlugin no senders : " + senders);
        }
        else
        {
          // TrapSenderUtils.sendTrap(SnmpTrapSeverity.CLEAR, SnmpTrapType.COMMUNICATION_SERVER_CONNECTION, "SMTP3rdPartyConnection", "SMTPConnection to 3rdParty SMTPServer Loaded Successfully on host "+getSystemInfo());
          logger.info("SMTP Driver Loaded Successfully.");
        }
    }
    catch(Exception ex)
      {
        logger.error("Exception occured in SMTPDriver.init() : "+ex);
        throw new RuntimeException("SMTP Driver Load Finished : Driver loading Failure. ");
      }

    ft = new FeedbackThread(deliveryManagerForNotifications, usingFakeEmulator);
    ft.start();    
  }

  /*****************************************
  *
  *  send
  *
  *****************************************/

  @Override public void send(INotificationRequest mailNotificationRequest)
  {
    if (logger.isDebugEnabled()) logger.debug("START: SMTPDriver.send() execution.");

    logger.info("Consuming the email message " + mailNotificationRequest.toString());

    if (senders != null && senders.length == currentSenderIndex)
      {
        currentSenderIndex = 0;
      }
    if(senders != null && senders.length > 0)
      {
        try
          {
            NotificationManagerRequest deliveryRequest = (NotificationManagerRequest) mailNotificationRequest;
            Map<String,String> resolvedParameters = deliveryRequest.getResolvedParameters(mailNotificationManager.getSubscriberMessageTemplateService());
            String body = resolvedParameters.get("node.parameter.body");
            String toEmail = deliveryRequest.getDestination();
            String fromAddress =  deliveryRequest.getDeliveryRequestSource(); // TODO check
            boolean confirmationExpected = false ; // TODO : where to get it ?
            senders[currentSenderIndex].sendEmail(mailNotificationRequest, body, toEmail, fromAddress, confirmationExpected);
          }
        catch (Exception mEx)
          {
            logger.error("Exception occured in SMTPDriver.send()."+mEx.getMessage(), mEx);
            if (logger.isDebugEnabled()) logger.debug("Setting it for retry...");
            senders[currentSenderIndex].completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MessageStatus.ERROR, DeliveryStatus.Failed, NotificationStatus.ERROR.toString());
          }
      }
    else
      {
        if (logger.isDebugEnabled()) logger.debug("No Email Sender, can't send the email, return an error STATUS");
        senders[currentSenderIndex].completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MessageStatus.ERROR, DeliveryStatus.Failed, NotificationStatus.ERROR.toString());
        try
          {
            Thread.sleep(1000);
          }
        catch (InterruptedException e)
          {
            if (logger.isDebugEnabled()) logger.debug("Sleep interrupted");
          }
      }
    if (logger.isDebugEnabled()) logger.debug("END: SMTPDriver.send() execution.");

  }

  /*****************************************
  *
  *  sendConnectionStatusTrap
  *
  *****************************************/

  public void sendConnectionStatusTrap(boolean isConnected, String msg)
  {
    String okMessage = "SMTPConnection to 3rdParty SMTPServer is up on host ";
    String errorMessage = "SMTPConnection is to 3rdParty SMTPServer down on host ";

    if(isConnected && msg!=null)
      {
        okMessage = msg;
      }

    if(!isConnected && msg!=null)
      {
        errorMessage = msg;
      }

    //  if(isConnected)
    //      TrapSenderUtils.sendTrap(SnmpTrapSeverity.CLEAR, SnmpTrapType.COMMUNICATION_SERVER_CONNECTION, "SMTP3rdPartyConnection", okMessage+getSystemInfo());
    //  else
    //      TrapSenderUtils.sendTrap(SnmpTrapSeverity.WARNING, SnmpTrapType.COMMUNICATION_SERVER_CONNECTION, "SMTP3rdPartyConnection", errorMessage+getSystemInfo());

  }

  /*****************************************
  *
  *  getHost
  *
  *****************************************/

  private String getHost()
  {
    try
      {
        return InetAddress.getLocalHost().getHostName();
      }
    catch (UnknownHostException e)
      {
        logger.warn("Not able to retrieve host name",e);
      }
    return "UNKNOWN HOST";
  }

  /*****************************************
  *
  *  getSystemInfo
  *
  *****************************************/

  private String getSystemInfo()
  {
    return getHost()+" on date "+SystemTime.getCurrentTime();
  }
}
