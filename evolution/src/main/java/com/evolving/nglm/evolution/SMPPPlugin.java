/*****************************************************************************
*
*  SMPPPlugin.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.SMSNotificationManager.SMSNotificationManagerRequest;
import com.lumatagroup.expression.driver.SMPP.SMSSenderFactory;
import com.lumatagroup.expression.driver.SMPP.SimpleSMSSender;
import com.lumatagroup.expression.driver.SMPP.configuration.SMSC;
import com.lumatagroup.expression.driver.SMPP.Util.SMPPUtil.SMPP_CONFIGS;
import com.evolving.nglm.core.JSONUtilities;

public class SMPPPlugin implements SMSNotificationInterface
{
  /*****************************************
  *
  *  logger
  *
  *****************************************/

  private static final Logger log = LoggerFactory.getLogger(SMPPPlugin.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private SimpleSMSSender sender = null;

  /*****************************************
  *
  *  init
  *
  *****************************************/

  public void init(SMSNotificationManager smsNotificationManager, JSONObject notificationPluginConfiguration, String notificationPluginSpecificConfiguration, String pluginName)
  {
    String smscHost = JSONUtilities.decodeString(notificationPluginConfiguration, "smscConnection", true);
    String username = JSONUtilities.decodeString(notificationPluginConfiguration, "username", true);
    String password = JSONUtilities.decodeString(notificationPluginConfiguration, "password", true);
    String connectionType = JSONUtilities.decodeString(notificationPluginConfiguration, "connectionType", true);
    log.info("SMPP Plugin init; smscHost="+smscHost+ ", username="+username+ ", password="+password + ", connectionType="+connectionType);

    if(smscHost == null || username == null || password == null || connectionType == null)
      {
        throw new RuntimeException("Bad SMSC Server configuration; smscHost="+smscHost+ ", username="+username+ ", password="+password + ", connectionType="+connectionType);
      }

    String[] split = smscHost.split(":");
    String host = split[0];
    int port = 0;
    try
      {
        port = Integer.parseInt(split[1]);
      }
    catch(NumberFormatException e)
      {
        throw new RuntimeException("Bad SMSC port " + port, e);
      }

    SMSSenderFactory smsSenderFactory = null ;
    SMSC config = new SMSC(pluginName);
    config.addProperty(SMPP_CONFIGS.name.toString(), pluginName);
    config.addProperty(SMPP_CONFIGS.address.toString(), host);
    config.addProperty(SMPP_CONFIGS.port.toString(), String.valueOf(port));
    config.addProperty(SMPP_CONFIGS.connection_type.toString(), connectionType);
    config.addProperty(SMPP_CONFIGS.system_id.toString(), username);
    config.addProperty(SMPP_CONFIGS.password.toString(), password);
    config.addProperty(SMPP_CONFIGS.system_type.toString(), "SMPP");
    config.addProperty(SMPP_CONFIGS.time_between_reconnection.toString(), String.valueOf(1000));
    config.addProperty(SMPP_CONFIGS.time_between_connection_check.toString(), String.valueOf(1000));
    //        config.addProperty(SMPP_CONFIGS.smpp_receiver_thread_number.toString(), value);
    config.addProperty(SMPP_CONFIGS.source_addr.toString(), String.valueOf(162122));
    config.addProperty(SMPP_CONFIGS.source_addr_ton.toString(), String.valueOf(0));
    config.addProperty(SMPP_CONFIGS.source_addr_npi.toString(), String.valueOf(1));
    config.addProperty(SMPP_CONFIGS.dest_addr_ton.toString(), String.valueOf(0));
    config.addProperty(SMPP_CONFIGS.dest_addr_npi.toString(), String.valueOf(0));
    config.addProperty(SMPP_CONFIGS.data_coding.toString(), String.valueOf(3));
    config.addProperty(SMPP_CONFIGS.encoding_charset.toString(), "ISO8859_1");
    config.addProperty(SMPP_CONFIGS.expiration_period.toString(), String.valueOf(2));
    config.addProperty(SMPP_CONFIGS.delay_on_queue_full.toString(), String.valueOf(60));

    smsSenderFactory = new SMSSenderFactory(config);

    smsSenderFactory.init(smsNotificationManager);
    if(smsSenderFactory.getSMSSenders() == null || (smsSenderFactory.getSMSSenders() != null && smsSenderFactory.getSMSSenders().length == 0))
      {
        log.info("SMPP Driver Load NOT Successfully: no sender created");
      }
    else
      {
        log.info("SMPP Driver Load Successfully");
        sender = smsSenderFactory.getSMSSenders()[0];
      }
  }

  /*****************************************
  *
  *  send
  *
  *****************************************/

  public void send(SMSNotificationManagerRequest deliveryRequest)
  {
    String text = deliveryRequest.getText();
    String destination = deliveryRequest.getDestination();
    String source = deliveryRequest.getSource();

    if(sender == null)
      {
        throw new RuntimeException("SMPPPlugin.send("+deliveryRequest+") sender is null, no smsc");
      }
    else
      {
        if(sender.sendSMS(deliveryRequest, text, destination, source, true))
          {
            log.info("SMPP Driver message sent successfully");
          }
      }
  }
}
