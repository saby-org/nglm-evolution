/*****************************************************************************
*
*  SMPPPlugin.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Map;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.NotificationManager.NotificationManagerRequest;
import com.lumatagroup.expression.driver.SMPP.SMSSenderFactory;
import com.lumatagroup.expression.driver.SMPP.SimpleSMSSender;
import com.lumatagroup.expression.driver.SMPP.configuration.SMSC;
import com.lumatagroup.expression.driver.SMPP.Util.SMPPUtil.SMPP_CONFIGS;
import com.evolving.nglm.core.JSONUtilities;

public class SMPPGenericPlugin implements NotificationInterface
{
  /*****************************************
  *
  *  logger
  *
  *****************************************/

  private static final Logger log = LoggerFactory.getLogger(SMPPGenericPlugin.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private NotificationManager notificationManager = null;
  private SimpleSMSSender sender = null;

  /*****************************************
  *
  *  init
  *
  *****************************************/

  public void init(DeliveryManagerForNotifications notificationManager, JSONObject notificationPluginConfiguration)
  {
    //
    //  smsNotificationManager
    //

    this.notificationManager = (NotificationManager) notificationManager;

    //
    //  attributes
    //  

    String connection_name = JSONUtilities.decodeString(notificationPluginConfiguration, "smsc_connection_name", true);
    String smscHost = JSONUtilities.decodeString(notificationPluginConfiguration, "smsc_connection", true);
    String username = JSONUtilities.decodeString(notificationPluginConfiguration, "username", true);
    String password = JSONUtilities.decodeString(notificationPluginConfiguration, "password", true);
    String connection_type = JSONUtilities.decodeString(notificationPluginConfiguration, "connection_type", true);
    String system_type = JSONUtilities.decodeString(notificationPluginConfiguration, "system_type", false);
    String time_between_reconnection  = JSONUtilities.decodeString(notificationPluginConfiguration, "time_between_reconnection", false);
    String time_between_connection_check  = JSONUtilities.decodeString(notificationPluginConfiguration, "time_between_connection_check", false);
    String source_addr  = JSONUtilities.decodeString(notificationPluginConfiguration, "source_addr", false);
    String source_addr_ton  = JSONUtilities.decodeString(notificationPluginConfiguration, "source_addr_ton", false);
    String source_addr_npi  = JSONUtilities.decodeString(notificationPluginConfiguration, "source_addr_npi", false);
    String dest_addr_ton = JSONUtilities.decodeString(notificationPluginConfiguration, "dest_addr_ton", false);
    String dest_addr_npi = JSONUtilities.decodeString(notificationPluginConfiguration, "dest_addr_npi", false);
    String data_coding = JSONUtilities.decodeString(notificationPluginConfiguration, "data_coding", false);
    String encoding_charset = JSONUtilities.decodeString(notificationPluginConfiguration, "encoding_charset", false);
    String expiration_period = JSONUtilities.decodeString(notificationPluginConfiguration, "expiration_period", false);
    String delay_on_queue_full = JSONUtilities.decodeString(notificationPluginConfiguration, "delay_on_queue_full", false);
    String smpp_receiver_thread_number = JSONUtilities.decodeString(notificationPluginConfiguration, "smpp_receiver_thread_number", false);
    String load_HP_roman8_as_data_coding0 = JSONUtilities.decodeString(notificationPluginConfiguration, "load_HP_roman8_as_data_coding0", false);
    String load_UTF16_as_data_coding8 = JSONUtilities.decodeString(notificationPluginConfiguration, "load_UTF16_as_data_coding8", false);
    String data_packing = JSONUtilities.decodeString(notificationPluginConfiguration, "data_packing", false);
    String support_sar = JSONUtilities.decodeString(notificationPluginConfiguration, "support_sar", false);
    String support_udh = JSONUtilities.decodeString(notificationPluginConfiguration, "support_udh", false);
    String handle_submit_sm_response_in_multi_part = JSONUtilities.decodeString(notificationPluginConfiguration, "handle_submit_sm_response_in_multi_part", false);
    String dest_addr_subunit = JSONUtilities.decodeString(notificationPluginConfiguration, "dest_addr_subunit", false);
    String service_type = JSONUtilities.decodeString(notificationPluginConfiguration, "service_type", false);
    String delivery_receipt_decoding_hex_dec = JSONUtilities.decodeString(notificationPluginConfiguration, "delivery_receipt_decoding_hex_dec", false);
    String is_send_to_payload = JSONUtilities.decodeString(notificationPluginConfiguration, "is_send_to_payload", false);
    String sms_MO_event_name = JSONUtilities.decodeString(notificationPluginConfiguration, "sms_MO_event_name", false);
    String sms_MO_channel_name = JSONUtilities.decodeString(notificationPluginConfiguration, "sms_MO_channel_name", false);
    
    //
    //  log
    //
    
    log.info("SMPPGenericPlugin init; smscHost="+smscHost+ ", username="+username+ ", password="+password + ", connectionType="+connection_type);

    //
    //  validate
    //

    if (smscHost == null || username == null || password == null || connection_type == null)
      {
        throw new RuntimeException("Bad SMSC Server configuration; smscHost="+smscHost+ ", username="+username+ ", password="+password + ", connectionType="+connection_type);
      }

    //
    // validate smscHost
    //

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

    //
    //  factory
    //

    SMSSenderFactory smsSenderFactory = null ;
    SMSC config = new SMSC(connection_name);
    config.addProperty(SMPP_CONFIGS.name.toString(), connection_name);
    config.addProperty(SMPP_CONFIGS.address.toString(), host);
    config.addProperty(SMPP_CONFIGS.port.toString(), String.valueOf(port));
    config.addProperty(SMPP_CONFIGS.connection_type.toString(), connection_type);
    config.addProperty(SMPP_CONFIGS.system_id.toString(), username);
    config.addProperty(SMPP_CONFIGS.password.toString(), password);
    config.addProperty(SMPP_CONFIGS.system_type.toString(), system_type != null ? system_type : "SMPP");
    config.addProperty(SMPP_CONFIGS.time_between_reconnection.toString(), time_between_reconnection != null ? time_between_reconnection : String.valueOf(1000));
    config.addProperty(SMPP_CONFIGS.time_between_connection_check.toString(), time_between_connection_check != null ? time_between_connection_check : String.valueOf(1000));
    config.addProperty(SMPP_CONFIGS.source_addr.toString(), source_addr != null ? source_addr : String.valueOf(162122));
    config.addProperty(SMPP_CONFIGS.source_addr_ton.toString(), source_addr_ton != null ? source_addr_ton : String.valueOf(0));
    config.addProperty(SMPP_CONFIGS.source_addr_npi.toString(), source_addr_npi != null ? source_addr_npi : String.valueOf(1));
    config.addProperty(SMPP_CONFIGS.dest_addr_ton.toString(), dest_addr_ton != null ? dest_addr_ton : String.valueOf(0));
    config.addProperty(SMPP_CONFIGS.dest_addr_npi.toString(), dest_addr_npi != null ? dest_addr_npi : String.valueOf(0));
    config.addProperty(SMPP_CONFIGS.data_coding.toString(), data_coding != null ? data_coding : String.valueOf(3));
    config.addProperty(SMPP_CONFIGS.encoding_charset.toString(), encoding_charset != null ? encoding_charset : "ISO8859_1");
    config.addProperty(SMPP_CONFIGS.expiration_period.toString(), expiration_period != null ? expiration_period : String.valueOf(2));
    config.addProperty(SMPP_CONFIGS.delay_on_queue_full.toString(), delay_on_queue_full != null ? delay_on_queue_full : String.valueOf(60));
    if (smpp_receiver_thread_number != null) config.addProperty(SMPP_CONFIGS.smpp_receiver_thread_number.toString(), smpp_receiver_thread_number);
    if (load_HP_roman8_as_data_coding0 != null) config.addProperty(SMPP_CONFIGS.load_HP_roman8_as_data_coding0.toString(), load_HP_roman8_as_data_coding0);
    if (load_UTF16_as_data_coding8 != null) config.addProperty(SMPP_CONFIGS.load_UTF16_as_data_coding8.toString(), load_UTF16_as_data_coding8);
    if (data_packing != null) config.addProperty(SMPP_CONFIGS.data_packing.toString(), data_packing);
    if (support_sar != null) config.addProperty(SMPP_CONFIGS.support_sar.toString(), support_sar);
    if (support_udh != null) config.addProperty(SMPP_CONFIGS.support_udh.toString(), support_udh);
    if (handle_submit_sm_response_in_multi_part != null) config.addProperty(SMPP_CONFIGS.handle_submit_sm_response_in_multi_part.toString(), handle_submit_sm_response_in_multi_part);
    if (dest_addr_subunit != null) config.addProperty(SMPP_CONFIGS.dest_addr_subunit.toString(), dest_addr_subunit);
    if (service_type != null) config.addProperty(SMPP_CONFIGS.service_type.toString(), service_type);
    if (delivery_receipt_decoding_hex_dec != null) config.addProperty(SMPP_CONFIGS.delivery_receipt_decoding_hex_dec.toString(), delivery_receipt_decoding_hex_dec);
    if (is_send_to_payload != null) config.addProperty(SMPP_CONFIGS.is_send_to_payload.toString(), is_send_to_payload);    
    if (sms_MO_event_name != null)  config.addProperty(SMPP_CONFIGS.sms_MO_event_name.toString(), sms_MO_event_name);  
    if (sms_MO_channel_name != null)  config.addProperty(SMPP_CONFIGS.sms_MO_channel_name.toString(), sms_MO_event_name);  
    smsSenderFactory = new SMSSenderFactory(config);
    smsSenderFactory.init(this.notificationManager,this);
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

  public void send(INotificationRequest deliveryNotificationRequest)
  {
    
    NotificationManagerRequest deliveryRequest = (NotificationManagerRequest)deliveryNotificationRequest;
    Map<String,String> resolvedParameters = deliveryRequest.getResolvedParameters(this.notificationManager.getSubscriberMessageTemplateService());
    String text = resolvedParameters.get("sms.body");
        
    String destination = deliveryRequest.getDestination();
    
    String source = deliveryRequest.getSourceAddressParam();
    // replace here for stats (NOTE : this is duplicated from SimpleSMSSender as well, to clean once we have just generic channel)
    if(source==null||source.trim().isEmpty()){
      source = sender.get_source_addr();
      deliveryRequest.setSourceAddressParam(source);
    }
    Boolean receiptExpected = (Boolean) deliveryRequest.getNotificationParameters().get("node.parameter.confirmationexpected");
    Boolean flashSMS = (Boolean) deliveryRequest.getNotificationParameters().get("node.parameter.flashsms");
    if(sender == null)
      {
        throw new RuntimeException("SMPPGenericPlugin.send("+deliveryRequest+") sender is null, no smsc");
      }
    else
      {
        if(sender.sendSMS(deliveryRequest, text, destination, source, receiptExpected != null ? receiptExpected : false, flashSMS != null ? flashSMS : false))
          {
            if(log.isDebugEnabled()) log.debug("SMPPGenericPlugin.send : deliveryRequest "+deliveryRequest.getDeliveryRequestID()+" sent to "+destination);
          }
        else
          {
            log.info("SMPPGenericPlugin.send : issue when sending deliveryRequest "+deliveryRequest.getDeliveryRequestID()+" to "+destination);
            deliveryNotificationRequest.setDeliveryStatus(DeliveryManager.DeliveryStatus.Failed);
            deliveryNotificationRequest.setReturnCode(DeliveryManagerForNotifications.MessageStatus.UNKNOWN.getReturnCode());
            deliveryNotificationRequest.setMessageStatus(DeliveryManagerForNotifications.MessageStatus.UNKNOWN);
            deliveryNotificationRequest.setReturnCodeDetails("issue while sending");
            this.notificationManager.completeDeliveryRequest(deliveryNotificationRequest);
          }
      }
  }
}
