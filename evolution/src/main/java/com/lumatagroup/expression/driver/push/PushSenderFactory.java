package com.lumatagroup.expression.driver.push;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.DeliveryManagerForNotifications;


public class PushSenderFactory {
  private static Logger logger = LoggerFactory.getLogger(PushSenderFactory.class);
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
  public void init(DeliveryManagerForNotifications pushNotificationManager) {
    logger.debug("PushSenderFactory.init() method execution ...");
    sender = new SimplePushSender(pushNotificationManager);
    logger.debug("PushSenderFactory.init() method execution done");
  }

  public SimplePushSender getPushSender() 
  {
    return sender;
  }
}
