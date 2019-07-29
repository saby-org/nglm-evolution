/*****************************************************************************
*
*  EmailMessage.java
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

public class EmailMessage
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("subject", DialogMessage.schema());
    schemaBuilder.field("htmlBody", DialogMessage.schema());
    schemaBuilder.field("textBody", DialogMessage.schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<EmailMessage> serde = new ConnectSerde<EmailMessage>(schema, false, EmailMessage.class, EmailMessage::pack, EmailMessage::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<EmailMessage> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private DialogMessage subject = null;
  private DialogMessage htmlBody = null;
  private DialogMessage textBody = null;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public DialogMessage getSubject() { return subject; }
  public DialogMessage getHTMLBody() { return htmlBody; }
  public DialogMessage getTextBody() { return textBody; }
  
  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  public EmailMessage(JSONArray messagesJSON, CriterionContext criterionContext) throws GUIManagerException
  {
    this.subject = new DialogMessage(messagesJSON, "subject", criterionContext);
    this.htmlBody = new DialogMessage(messagesJSON, "htmlBody", criterionContext);
    this.textBody = new DialogMessage(messagesJSON, "textBody", criterionContext);
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  private EmailMessage(DialogMessage subject, DialogMessage htmlBody, DialogMessage textBody)
  {
    this.subject = subject;
    this.htmlBody = htmlBody;
    this.textBody = textBody;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  public EmailMessage(EmailMessage emailMessage)
  {
    this.subject = new DialogMessage(emailMessage.getSubject());
    this.htmlBody = new DialogMessage(emailMessage.getHTMLBody());
    this.textBody = new DialogMessage(emailMessage.getTextBody());
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    EmailMessage emailMessage = (EmailMessage) value;
    Struct struct = new Struct(schema);
    struct.put("subject", DialogMessage.pack(emailMessage.getSubject()));
    struct.put("htmlBody", DialogMessage.pack(emailMessage.getHTMLBody()));
    struct.put("textBody", DialogMessage.pack(emailMessage.getTextBody()));
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static EmailMessage unpack(SchemaAndValue schemaAndValue)
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
    DialogMessage subject = DialogMessage.unpack(new SchemaAndValue(schema.field("subject").schema(), valueStruct.get("subject")));
    DialogMessage htmlBody = DialogMessage.unpack(new SchemaAndValue(schema.field("htmlBody").schema(), valueStruct.get("htmlBody")));
    DialogMessage textBody = DialogMessage.unpack(new SchemaAndValue(schema.field("textBody").schema(), valueStruct.get("textBody")));

    //
    //  return
    //

    return new EmailMessage(subject, htmlBody, textBody);
  }

  /*****************************************
  *
  *  resolve
  *
  *****************************************/

  public String resolveSubject(SubscriberEvaluationRequest subscriberEvaluationRequest) { return subject.resolveX(subscriberEvaluationRequest); }
  public String resolveHTMLBody(SubscriberEvaluationRequest subscriberEvaluationRequest) { return htmlBody.resolveX(subscriberEvaluationRequest); }
  public String resolveTextBody(SubscriberEvaluationRequest subscriberEvaluationRequest) { return textBody.resolveX(subscriberEvaluationRequest); }
}
