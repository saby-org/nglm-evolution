/*****************************************************************************
*
*  EmailMessage.java
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

public class EmailMessage extends SubscriberMessage
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
    schemaBuilder.name("email_message");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schema = schemaBuilder.build();
  }

  //
  //  serde
  //

  private static ConnectSerde<EmailMessage> serde = new ConnectSerde<EmailMessage>(schema, false, EmailMessage.class, EmailMessage::pack, EmailMessage::unpack);
  public static Object pack(Object value) { return packCommon(schema, value); }
  public static EmailMessage unpack(SchemaAndValue schemaAndValue) { return new EmailMessage(schemaAndValue); }
  public EmailMessage(SchemaAndValue schemaAndValue) { super(schemaAndValue); }

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<EmailMessage> serde() { return serde; }

  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  public EmailMessage(Object emailMessageJSON, String communicationChannelID, SubscriberMessageTemplateService subscriberMessageTemplateService, CriterionContext criterionContext) throws GUIManagerException
  {
    super(emailMessageJSON, communicationChannelID, createMap(), subscriberMessageTemplateService, criterionContext);
  }
  
  private static Map<String, Boolean> createMap() {
    Map<String,Boolean> myMap = new HashMap<String, Boolean>();
    myMap.put("subject", true);
    myMap.put("htmlBody", true);
    myMap.put("textBody", true);
    return myMap;
  }
  
}
