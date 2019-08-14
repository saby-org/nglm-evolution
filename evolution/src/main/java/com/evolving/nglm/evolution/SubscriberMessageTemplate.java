/*****************************************************************************
*
*  SubscriberMessageTemplate.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class SubscriberMessageTemplate extends GUIManagedObject
{
  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema commonSchema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("subscriber_message_template");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(GUIManagedObject.commonSchema().version(),1));
    for (Field field : GUIManagedObject.commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("dialogMessages", SchemaBuilder.array(DialogMessage.schema()).schema());
    schemaBuilder.field("readOnlyCopyID", Schema.OPTIONAL_STRING_SCHEMA);
    commonSchema = schemaBuilder.build();
  };

  //
  //  accessor
  //

  public static Schema commonSchema() { return commonSchema; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private List<DialogMessage> dialogMessages = new ArrayList<DialogMessage>();
  private String readOnlyCopyID;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getSubscriberMessageTemplateID() { return getGUIManagedObjectID(); }
  public String getSubscriberMessageTemplateName() { return getGUIManagedObjectName(); }
  public List<DialogMessage> getDialogMessages() { return dialogMessages; }
  public String getReadOnlyCopyID() { return readOnlyCopyID; }

  //
  //  abstract
  //

  public abstract String getTemplateType();
  public abstract List<String> getDialogMessageFields();

  /*****************************************
  *
  *  setters
  *
  *****************************************/
      
  public void setReadOnlyCopyID(String readOnlyCopyID) { this.readOnlyCopyID = readOnlyCopyID; }
  
  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  protected SubscriberMessageTemplate(JSONObject jsonRoot, GUIManagedObjectType messageTemplateType, long epoch, GUIManagedObject existingSubscriberMessageTemplateUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, messageTemplateType, (existingSubscriberMessageTemplateUnchecked != null) ? existingSubscriberMessageTemplateUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingSubscriberMessageTemplate
    *
    *****************************************/

    SubscriberMessageTemplate existingSubscriberMessageTemplate = (existingSubscriberMessageTemplateUnchecked != null && existingSubscriberMessageTemplateUnchecked instanceof SubscriberMessageTemplate) ? (SubscriberMessageTemplate) existingSubscriberMessageTemplateUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.readOnlyCopyID = null;

    /*****************************************
    *
    *  messages
    *
    *****************************************/

    //
    //  messagesJSON
    //

    JSONArray messagesJSON = JSONUtilities.decodeJSONArray(jsonRoot, "message", true);

    //
    //  messageText
    //

    if (messagesJSON.size() > 0)
      {
        for (String dialogMessageField : getDialogMessageFields())
          {
            dialogMessages.add(new DialogMessage(messagesJSON, dialogMessageField, CriterionContext.Profile));
          }
      }

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingSubscriberMessageTemplate))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  newReadOnlyCopy
  *
  *****************************************/

  public static SubscriberMessageTemplate newReadOnlyCopy(SubscriberMessageTemplate subscriberMessageTemplate, SubscriberMessageTemplateService subscriberMessageTemplateService) throws GUIManagerException
  {
    //
    //  construct JSON representation
    //

    JSONObject templateJSON = subscriberMessageTemplateService.getJSONRepresentation(subscriberMessageTemplate);
    Map<String,Object> readOnlyCopyJSON = new HashMap<String,Object>(templateJSON);
    readOnlyCopyJSON.put("id", "readonly-" + subscriberMessageTemplateService.generateSubscriberMessageTemplateID());
    readOnlyCopyJSON.put("effectiveStartDate", null);
    readOnlyCopyJSON.put("effectiveEndDate", null);
    readOnlyCopyJSON.put("readOnly", true);
    readOnlyCopyJSON.put("internalOnly", true);
    readOnlyCopyJSON.put("active", true);
    JSONObject readOnlyCopy = JSONUtilities.encodeObject(readOnlyCopyJSON);
    
    //
    //  readOnlyCopy
    //

    SubscriberMessageTemplate result = null;
    if (subscriberMessageTemplate instanceof SMSTemplate) result = new SMSTemplate(readOnlyCopy, 0L, null);
    if (subscriberMessageTemplate instanceof MailTemplate) result = new MailTemplate(readOnlyCopy, 0L, null);
    if (subscriberMessageTemplate instanceof PushTemplate) result = new PushTemplate(readOnlyCopy, 0L, null);
    if (result == null) throw new ServerRuntimeException("illegal subscriberMessageTemplate");

    //
    //  return
    //
    
    return result;
  }

  /*****************************************
  *
  *  newInternalTemplate
  *
  *****************************************/

  public static SubscriberMessageTemplate newInternalTemplate(SubscriberMessage subscriberMessage, SubscriberMessageTemplateService subscriberMessageTemplateService) throws GUIManagerException
  {
    //
    //  construct JSON representation
    //

    JSONObject internalSubscriberMessageTemplate = new JSONObject();
    internalSubscriberMessageTemplate.put("id", "hardcoded-" + subscriberMessageTemplateService.generateSubscriberMessageTemplateID());
    internalSubscriberMessageTemplate.put("effectiveStartDate", null);
    internalSubscriberMessageTemplate.put("effectiveEndDate", null);
    internalSubscriberMessageTemplate.put("readOnly", true);
    internalSubscriberMessageTemplate.put("internalOnly", true);
    internalSubscriberMessageTemplate.put("active", true);
    internalSubscriberMessageTemplate.put("message", new JSONArray());

    //
    //  new template
    //

    SubscriberMessageTemplate result = null;
    if (subscriberMessage instanceof SMSMessage) result = new SMSTemplate(internalSubscriberMessageTemplate, 0L, null);
    if (subscriberMessage instanceof EmailMessage) result = new MailTemplate(internalSubscriberMessageTemplate, 0L, null);
    if (subscriberMessage instanceof PushMessage) result = new PushTemplate(internalSubscriberMessageTemplate, 0L, null);
    if (result == null) throw new ServerRuntimeException("illegal subscriberMessage");

    //
    //  add dialogMessages
    //

    for (DialogMessage dialogMessage : subscriberMessage.getDialogMessages())
      {
        result.getDialogMessages().add(new DialogMessage(dialogMessage));
      }

    //
    //  return
    //

    return result;
  }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  protected SubscriberMessageTemplate(SchemaAndValue schemaAndValue)
  {
    //
    //  super
    //

    super(schemaAndValue);

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
    List<DialogMessage> dialogMessages = unpackDialogMessages(schema.field("dialogMessages").schema(), (List<Object>) valueStruct.get("dialogMessages"));
    String readOnlyCopyID = valueStruct.getString("readOnlyCopyID");

    //
    //  return
    //

    this.dialogMessages = dialogMessages;
    this.readOnlyCopyID = readOnlyCopyID;
  }

  /*****************************************
  *
  *  unpackDialogMessages
  *
  *****************************************/

  private static List<DialogMessage> unpackDialogMessages(Schema schema, List<Object> value)
  {
    //
    //  get schema
    //

    Schema dialogMessageSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<DialogMessage> result = new ArrayList<DialogMessage>();
    List<Object> valueArray = (List<Object>) value;
    for (Object dialogMessage : valueArray)
      {
        result.add(DialogMessage.unpack(new SchemaAndValue(dialogMessageSchema, dialogMessage)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  packCommon
  *
  *****************************************/

  protected static Object packCommon(Schema schema, Object value)
  {
    SubscriberMessageTemplate subscriberMessageTemplate = (SubscriberMessageTemplate) value;
    Struct struct = new Struct(schema);
    GUIManagedObject.packCommon(struct, subscriberMessageTemplate);
    struct.put("dialogMessages", packDialogMessages(subscriberMessageTemplate.getDialogMessages()));
    struct.put("readOnlyCopyID", subscriberMessageTemplate.getReadOnlyCopyID());
    return struct;
  }

  /*****************************************
  *
  *  packDialogMessages
  *
  *****************************************/

  private static List<Object> packDialogMessages(List<DialogMessage> dialogMessages)
  {
    List<Object> result = new ArrayList<Object>();
    for (DialogMessage dialogMessage : dialogMessages)
      {
        result.add(DialogMessage.pack(dialogMessage));
      }
    return result;
  }

  /*****************************************
  *
  *  getParameterTags
  *
  *****************************************/

  public List<CriterionField> getParameterTags()
  {
    return resolveParameterTags(dialogMessages);
  }

  /*****************************************
  *
  *  resolveParameterTags
  *
  *****************************************/

  public static List<CriterionField> resolveParameterTags(List<DialogMessage> dialogMessages)
  {
    List<CriterionField> parameterTags = new ArrayList<CriterionField>();
    Set<String> parameterTagIDs = new HashSet<String>();
    for (DialogMessage dialogMessage : dialogMessages)
      {
        for (CriterionField parameterTag : dialogMessage.getParameterTags())
          {
            if (! parameterTagIDs.contains(parameterTag.getID()))
              {
                parameterTags.add(parameterTag);
                parameterTagIDs.add(parameterTag.getID());
              }
          }
      }
    return parameterTags;
  }

  /*****************************************
  *
  *  getLanguages
  *
  *****************************************/

  public List<String> getLanguages()
  {
    Set<String> languages = new HashSet<String>();
    for (DialogMessage dialogMessage : dialogMessages)
      {
        for (String languageName : dialogMessage.getMessageTextByLanguage().keySet())
          {
            String languageID = Deployment.getSupportedLanguageID(languageName);
            if (languageID != null)
              {
                languages.add(languageID);
              }
          }
      }
    return Collections.<String>list(Collections.enumeration(languages));
  }

  /*****************************************
  *
  *  getReadOnlyCopy
  *
  *****************************************/

  public SubscriberMessageTemplate getReadOnlyCopy(EvolutionEventContext evolutionEventContext)
  {
    SubscriberMessageTemplate result;
    if (! getReadOnly() && getReadOnlyCopyID() != null)
      {
        result = evolutionEventContext.getSubscriberMessageTemplateService().getActiveSubscriberMessageTemplate(getReadOnlyCopyID(), evolutionEventContext.now());
      }
    else if (getReadOnly())
      {
        result = this;
      }
    else
      {
        result = null;
      }
    return result;
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(SubscriberMessageTemplate existingSubscriberMessageTemplate)
  {
    if (existingSubscriberMessageTemplate != null && existingSubscriberMessageTemplate.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingSubscriberMessageTemplate.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(dialogMessages, existingSubscriberMessageTemplate.getDialogMessages());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
}
