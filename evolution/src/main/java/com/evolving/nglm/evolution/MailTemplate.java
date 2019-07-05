/*****************************************************************************
*
*  MailTemplate.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MailTemplate extends SubscriberMessageTemplate
{
  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  serde
  //

  private static ConnectSerde<MailTemplate> serde = new ConnectSerde<MailTemplate>(SubscriberMessageTemplate.schema(), false, MailTemplate.class, MailTemplate::pack, MailTemplate::unpack);
  public static Object pack(Object value) { return SubscriberMessageTemplate.pack(value); }
  public static MailTemplate unpack(SchemaAndValue schemaAndValue) { return new MailTemplate(schemaAndValue); }
  public MailTemplate(SchemaAndValue schemaAndValue) { super(schemaAndValue); }

  //
  //  accessor
  //

  public static Schema schema() { return SubscriberMessageTemplate.schema(); }
  public static ConnectSerde<MailTemplate> serde() { return serde; }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getMailTemplateID() { return getGUIManagedObjectID(); }
  public String getMailTemplateName() { return getGUIManagedObjectName(); }
  public DialogMessage getSubject() { return super.getDialogMessages().get(0); }
  public DialogMessage getHTMLBody() { return super.getDialogMessages().get(1); }
  public DialogMessage getTextBody() { return super.getDialogMessages().get(2); }
  
  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  public MailTemplate(JSONObject jsonRoot, long epoch, GUIManagedObject existingTemplateUnchecked) throws GUIManagerException
  {
    super(jsonRoot, GUIManagedObjectType.MailMessageTemplate, Arrays.asList("subject", "htmlBody", "textBody"), epoch, existingTemplateUnchecked);
  }
  
  /*****************************************
  *
  *  resolve
  *
  *****************************************/

  public String resolveSubject(SubscriberEvaluationRequest subscriberEvaluationRequest) { return getSubject().resolve(subscriberEvaluationRequest); }
  public String resolveHTMLBody(SubscriberEvaluationRequest subscriberEvaluationRequest) { return getHTMLBody().resolve(subscriberEvaluationRequest); }
  public String resolveTextBody(SubscriberEvaluationRequest subscriberEvaluationRequest) { return getTextBody().resolve(subscriberEvaluationRequest); }
}
