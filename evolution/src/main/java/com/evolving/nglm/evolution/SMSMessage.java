/*****************************************************************************
*
*  SMSMessage.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class SMSMessage extends SubscriberMessage
{
  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema schema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("sms_message");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schema = schemaBuilder.build();
  }

  //
  //  serde
  //

  private static ConnectSerde<SMSMessage> serde = new ConnectSerde<SMSMessage>(schema, false, SMSMessage.class, SMSMessage::pack, SMSMessage::unpack);
  public static Object pack(Object value) { return packCommon(schema, value); }
  public static SMSMessage unpack(SchemaAndValue schemaAndValue) { return new SMSMessage(schemaAndValue); }
  public SMSMessage(SchemaAndValue schemaAndValue) { super(schemaAndValue); }

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SMSMessage> serde() { return serde; }

  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  public SMSMessage(Object smsMessageJSON, String communicationChannelID, SubscriberMessageTemplateService subscriberMessageTemplateService, CriterionContext criterionContext, int tenantID) throws GUIManagerException
  {
    super(smsMessageJSON, communicationChannelID, createMap(), subscriberMessageTemplateService, criterionContext, tenantID);
  }
  
  private static Map<String, Boolean> createMap() {
    Map<String,Boolean> myMap = new HashMap<String, Boolean>();
    myMap.put("messageText", true);
    return myMap;
  }
  
}
