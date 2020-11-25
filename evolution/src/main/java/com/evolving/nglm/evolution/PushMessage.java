/*****************************************************************************
*
*  PushMessage.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.HashMap;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class PushMessage extends SubscriberMessage
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
    schemaBuilder.name("push_message");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schema = schemaBuilder.build();
  }

  //
  //  serde
  //

  private static ConnectSerde<PushMessage> serde = new ConnectSerde<PushMessage>(schema, false, PushMessage.class, PushMessage::pack, PushMessage::unpack);
  public static Object pack(Object value) { return packCommon(schema, value); }
  public static PushMessage unpack(SchemaAndValue schemaAndValue) { return new PushMessage(schemaAndValue); }
  public PushMessage(SchemaAndValue schemaAndValue) { super(schemaAndValue); }

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<PushMessage> serde() { return serde; }

  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  public PushMessage(Object pushMessageJSON, String communicationChannelID, SubscriberMessageTemplateService subscriberMessageTemplateService, CriterionContext criterionContext) throws GUIManagerException
  {
    super(pushMessageJSON, communicationChannelID, new HashMap<String, Boolean>(), subscriberMessageTemplateService, criterionContext);
    
  }
  /*****************************************
  *
  *  toString
  *
  *****************************************/

  @Override
  public String toString()
  {
    return "PushMessage ["
        + "subscriberMessageTemplateID=" + getSubscriberMessageTemplateID() 
        + ", parameterTags()=" + getParameterTags() 
        + ", dialogMessages()=" + getDialogMessages() 
        + "]";
  }
  
}
