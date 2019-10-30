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

import javax.mail.MessagingException;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.MailNotificationManager.MAILMessageStatus;
import com.evolving.nglm.evolution.MailNotificationManager.MailNotificationManagerRequest;
import com.evolving.nglm.evolution.PushNotificationManager.PushMessageStatus;
import com.evolving.nglm.evolution.PushNotificationManager.PushNotificationManagerRequest;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.core.JSONUtilities;

import com.lumatagroup.expression.driver.SMTP.FeedbackThread;
import com.lumatagroup.expression.driver.SMTP.MailSenderFactory;
import com.lumatagroup.expression.driver.SMTP.SimpleEmailSender;
import com.lumatagroup.expression.driver.SMTP.constants.SMTPConstants;
import com.lumatagroup.expression.driver.dyn.NotificationStatus;
import com.lumatagroup.expression.driver.push.PushSenderFactory;
import com.lumatagroup.expression.driver.push.SimplePushSender;

public class PushPlugin implements PushNotificationInterface
{
  /*****************************************
  *
  *  logger
  *
  *****************************************/

  private static final Logger logger = LoggerFactory.getLogger(PushPlugin.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private PushNotificationManager pushNotificationManager;
  private SimplePushSender sender;

  /*****************************************
  *
  *  init
  *
  *****************************************/

  @Override public void init(PushNotificationManager pushNotificationManager, JSONObject pushNotifConfiguration, String pluginName)
  {
    logger.info("Push Driver Loading Start...");

    //
    //  manager
    //

    this.pushNotificationManager = pushNotificationManager;

    //
    //  get senders
    //

    PushSenderFactory pushSenderFactory = new PushSenderFactory();
    pushSenderFactory.init(pushNotificationManager);
    sender = pushSenderFactory.getPushSender();  
    if(sender == null)
      {
        throw new RuntimeException("Push Driver Load Finished : Driver loading Failure");
      }

    logger.info("Push Driver Loading Start DONE");
  }

  /*****************************************
  *
  *  send
  *
  *****************************************/

  @Override public void send(PushNotificationManagerRequest pushNotificationRequest)
  {
    if (logger.isDebugEnabled()) logger.debug("Push Driver : send("+pushNotificationRequest+") ...");
    
    if (sender != null)
      {
        try
          {
            sender.sendPush(pushNotificationRequest);
          } catch (MessagingException e)
          {
            logger.error("Exception occured in PushDriver.send()."+e.getMessage(), e);
            sender.completeDeliveryRequest(pushNotificationRequest, pushNotificationRequest.getDeliveryRequestID(), PushMessageStatus.ERROR, DeliveryStatus.Failed, NotificationStatus.ERROR.toString());
          }
      }
    
    if (logger.isDebugEnabled()) logger.debug("END: SMTPDriver.send("+pushNotificationRequest+") DONE");

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
