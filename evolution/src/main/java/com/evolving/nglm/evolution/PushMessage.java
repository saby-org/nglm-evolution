/*****************************************************************************
*
*  PushMessage.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.text.Format;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PushMessage
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("messageText", DialogMessage.schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<PushMessage> serde = new ConnectSerde<PushMessage>(schema, false, PushMessage.class, PushMessage::pack, PushMessage::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<PushMessage> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private DialogMessage messageText = null;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public DialogMessage getMessageText() { return messageText; }
  
  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  public PushMessage(JSONArray messagesJSON, CriterionContext criterionContext) throws GUIManagerException
  {
    this.messageText = new DialogMessage(messagesJSON, "messageText", criterionContext);
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  private PushMessage(DialogMessage messageText)
  {
    this.messageText = messageText;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  public PushMessage(PushMessage pushMessage)
  {
    this.messageText = new DialogMessage(pushMessage.getMessageText());
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    PushMessage pushMessage = (PushMessage) value;
    Struct struct = new Struct(schema);
    struct.put("messageText", DialogMessage.pack(pushMessage.getMessageText()));
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static PushMessage unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    DialogMessage messageText = DialogMessage.unpack(new SchemaAndValue(schema.field("messageText").schema(), valueStruct.get("messageText")));

    //
    //  return
    //

    return new PushMessage(messageText);
  }

  /*****************************************
  *
  *  resolve
  *
  *****************************************/

  public String resolveX(SubscriberEvaluationRequest subscriberEvaluationRequest) { return messageText.resolveX(subscriberEvaluationRequest); }
}
