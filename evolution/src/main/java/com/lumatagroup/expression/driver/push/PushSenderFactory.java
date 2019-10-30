package com.lumatagroup.expression.driver.push;

import java.util.Map;

import org.apache.log4j.Logger;

import com.evolving.nglm.evolution.PushNotificationManager;


public class PushSenderFactory {
  private static Logger logger = Logger.getLogger(PushSenderFactory.class);
  private SimplePushSender sender = null;

  /**
   * PushSenderFactory constructor
   */
  public PushSenderFactory() {
    logger.debug("PushSenderFactory.constructor done");
  }

  /**
   * init() method used to create push driver connection.
   */
  public void init(PushNotificationManager pushNotificationManager) {
    logger.debug("MailSenderFactory.init() method execution ...");
    sender = new SimplePushSender(pushNotificationManager);
    logger.debug("MailSenderFactory.init() method execution done");
  }

  public SimplePushSender getPushSender() 
  {
    return sender;
  }
}
