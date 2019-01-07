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

import com.evolving.nglm.evolution.MailNotificationManager.MAILMessageStatus;
import com.evolving.nglm.evolution.MailNotificationManager.MailNotificationManagerRequest;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;

import com.lumatagroup.expression.driver.SMTP.FeedbackThread;
import com.lumatagroup.expression.driver.SMTP.MailSenderFactory;
import com.lumatagroup.expression.driver.SMTP.SimpleEmailSender;
import com.lumatagroup.expression.driver.dyn.NotificationStatus;

public class SMTPPlugin implements MailNotificationInterface
{
  /*****************************************
  *
  *  logger
  *
  *****************************************/

  private static final Logger logger = LoggerFactory.getLogger(SMTPPlugin.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private SimpleEmailSender[] senders = null;
  private int currentSenderIndex = 0; 
  private MailNotificationManager mailNotificationManager;
  private FeedbackThread ft;

  /*****************************************
  *
  *  init
  *
  *****************************************/

  @Override public void init(MailNotificationManager mailNotificationManager, JSONObject smsNotifSharedConfiguration, String smsNotifSpecificConfiguration, String pluginName)
  {
    logger.info("SMTP Driver Loading Start...");

    this.mailNotificationManager = mailNotificationManager;

    Map<String, Object> config = new HashMap<String, Object>();

    config.put("initial.duration.days","1");
    config.put("subject.charset","UTF-8");
    config.put("feedback.generic.polling.retry.delay","150000");
    config.put("user.name","mael.raffin@lumatagroup.com");
    config.put("smtp.driver.connection.checking.time.delay","20000");
    config.put("password","Dyn4B@HaMas!");
    config.put("feedback.polling.initial.try.with.delay","false");
    config.put("polling.interval.secs","30");
    config.put("dyn.timezone","GMT");
    config.put("dyn.api.call.date.format","yyyy-MM-dd'T'HH");
    config.put("html.content.charset","utf-8");
    config.put("driver.smtp.connection.timeout","180000");
    config.put("feedback.generic.polling.retry.attempts","10");
    config.put("date.id","1");
    config.put("reply.to.email.address","aaronboyd1992@hotmail.com");
    config.put("feedback.polling.retry.attempts","20");
    config.put("table.name.date","dialog_manager.smtp_driver_date");
    config.put("dyn.url.emailBounced","http");
    config.put("dyn.url.clickedUrl","http");
    config.put("dyn.api.call.time.margin","3600000");
    config.put("mail.smtp.socketFactory.class","javax.net.ssl.SSLSocketFactory");
    config.put("http.connection.timeout.secs","60");
    config.put("smtp.protocol","SMTP");
    config.put("dyn.url.emailDelivery","http");
    config.put("mail.smtp.auth","true");
    config.put("mail.smtp.host","smtp.dynect.net");
    config.put("sender.email.address","aaronboyd1992@hotmail.com");
    config.put("dyn.apiKey","9971b469b09cd134705f4acb7e5d66f2");
    config.put("dyn.url.emailOpened","http");
    config.put("name","smtp_3rdparty");
    config.put("smtp.driver.session.debug.flag","true");
    config.put("lock.grace.period.minutes","10");
    config.put("sql.date.format","yyyy-MM-dd HH");
    config.put("mail.smtp.socketFactory.port","25");
    config.put("feedback.polling.try.constant.delay","150000");

    MailSenderFactory mailSenderFactory = new MailSenderFactory(config);
    try
      {
        mailSenderFactory.init(mailNotificationManager);
        senders = mailSenderFactory.getEmailSenders();  
        if(senders == null || senders.length==0)
          {
            //          TrapSenderUtils.sendTrap(SnmpTrapSeverity.WARNING, 
            //                  SnmpTrapType.COMMUNICATION_SERVER_CONNECTION, "SMTP3rdPartyConnection", "SMTPConnection to 3rdParty SMTPServer Loading Fail on host "+getSystemInfo());
            throw new Exception("SMTP Driver Load Finished : Driver loading Failure. SMTPConnection to 3rdParty SMTPServer Loading Fail on host ");
          }
        else
          {
            logger.info("SMTP Driver Loaded Successfully.");
            //          TrapSenderUtils.sendTrap(SnmpTrapSeverity.CLEAR, 
            //                  SnmpTrapType.COMMUNICATION_SERVER_CONNECTION, "SMTP3rdPartyConnection", "SMTPConnection to 3rdParty SMTPServer Loaded Successfully on host "+getSystemInfo());
            if (logger.isInfoEnabled())
              {
                if (logger.isInfoEnabled())
                  {
                    logger.info("SMTPDriver3rdPartyNDM.init SMTP3rdPartyConnection SMTPConnection to 3rdParty SMTPServer Loaded Successfully on host ");
                  }                   
              }
          }
      }
    catch(Exception ex)
      {
        logger.error("Exception occured in SMTPDriver.asyncCall() : "+ex);
        throw new RuntimeException("SMTP Driver Load Finished : Driver loading Failure. ");
      }

    ft = new FeedbackThread(mailNotificationManager);
    ft.start();    
  }

  /*****************************************
  *
  *  send
  *
  *****************************************/

  @Override public void send(MailNotificationManagerRequest mailNotificationRequest)
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
            senders[currentSenderIndex].sendEmail(mailNotificationRequest);
          }
        catch (Exception mEx)
          {
            logger.error("Exception occured in SMTPDriver.send()."+mEx.getMessage(), mEx);
            if (logger.isDebugEnabled()) logger.debug("Setting it for retry...");
            senders[currentSenderIndex].completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.ERROR, DeliveryStatus.Failed, NotificationStatus.ERROR.toString());
          }
      }
    else
      {
        if (logger.isDebugEnabled()) logger.debug("No Email Sender, can't send the email, return an error STATUS");
        senders[currentSenderIndex].completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.ERROR, DeliveryStatus.Failed, NotificationStatus.ERROR.toString());
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
    return getHost()+" on date "+new Date();
  }
}
