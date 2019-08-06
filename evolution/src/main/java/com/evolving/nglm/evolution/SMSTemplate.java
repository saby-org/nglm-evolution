/*****************************************************************************
*
*  SMSTemplate.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import org.apache.kafka.connect.data.Field;
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

public class SMSTemplate extends SubscriberMessageTemplate
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
    schemaBuilder.name("sms_template");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schema = schemaBuilder.build();
  }

  //
  //  serde
  //

  private static ConnectSerde<SMSTemplate> serde = new ConnectSerde<SMSTemplate>(schema, false, SMSTemplate.class, SMSTemplate::pack, SMSTemplate::unpack);
  public static Object pack(Object value) { return SubscriberMessageTemplate.packCommon(schema, value); }
  public static SMSTemplate unpack(SchemaAndValue schemaAndValue) { return new SMSTemplate(schemaAndValue); }
  public SMSTemplate(SchemaAndValue schemaAndValue) { super(schemaAndValue); }

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SMSTemplate> serde() { return serde; }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getSMSTemplateID() { return getGUIManagedObjectID(); }
  public String getSMSTemplateName() { return getGUIManagedObjectName(); }
  public DialogMessage getMessageText() { return super.getDialogMessages().get(0); }
  
  //
  //  abstract
  //

  @Override public String getTemplateType() { return "sms"; }
  @Override public List<String> getDialogMessageFields() { return Arrays.asList("messageText"); }

  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  public SMSTemplate(JSONObject jsonRoot, long epoch, GUIManagedObject existingTemplateUnchecked) throws GUIManagerException
  {
    super(jsonRoot, GUIManagedObjectType.SMSMessageTemplate, epoch, existingTemplateUnchecked);
  }
}
