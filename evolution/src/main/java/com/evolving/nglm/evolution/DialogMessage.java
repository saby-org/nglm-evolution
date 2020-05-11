/*****************************************************************************
*
*  DialogMessage.java
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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DialogMessage
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
    schemaBuilder.name("dialog_message");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("messageTextByLanguage", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).name("dialog_message_text").schema());
    schemaBuilder.field("contextTags", SchemaBuilder.array(CriterionField.schema()).defaultValue(new ArrayList<CriterionField>()).schema());
    schemaBuilder.field("parameterTags", SchemaBuilder.array(CriterionField.schema()).defaultValue(new ArrayList<CriterionField>()).schema());
    schemaBuilder.field("allTags", SchemaBuilder.array(CriterionField.schema()).defaultValue(new ArrayList<CriterionField>()).schema());
    
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<DialogMessage> serde = new ConnectSerde<DialogMessage>(schema, false, DialogMessage.class, DialogMessage::pack, DialogMessage::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<DialogMessage> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private Map<String,String> messageTextByLanguage = new HashMap<String,String>();
  private List<CriterionField> contextTags = new ArrayList<CriterionField>();
  private List<CriterionField> parameterTags = new ArrayList<CriterionField>();
  private List<CriterionField> allTags = new ArrayList<CriterionField>();

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public Map<String,String> getMessageTextByLanguage() { return messageTextByLanguage; }
  public List<CriterionField> getContextTags() { return contextTags; }
  public List<CriterionField> getParameterTags() { return parameterTags; }
  public List<CriterionField> getAllTags() { return allTags; }

  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  public DialogMessage(JSONArray messagesJSON, String messageTextAttribute, boolean mandatory, CriterionContext criterionContext) throws GUIManagerException
  {
    Map<CriterionField,String> tagReplacements = new HashMap<CriterionField,String>();
    for (int i=0; i<messagesJSON.size(); i++)
      {
        /*****************************************
        *
        *  messageJSON
        *
        *****************************************/

        JSONObject messageJSON = (JSONObject) messagesJSON.get(i);

        /*****************************************
        *
        *  language
        *
        *****************************************/

        SupportedLanguage supportedLanguage = Deployment.getSupportedLanguages().get(JSONUtilities.decodeString(messageJSON, "languageID", true));
        String language = (supportedLanguage != null) ? supportedLanguage.getName() : null;
        if (language == null) throw new GUIManagerException("unsupported language", JSONUtilities.decodeString(messageJSON, "languageID", true));

        /*****************************************
        *
        *  unprocessedMessageText
        *
        *****************************************/

        String unprocessedMessageText = JSONUtilities.decodeString(messageJSON, messageTextAttribute, mandatory);

        /*****************************************
        *
        *  find tags
        *
        *****************************************/

        String messageText = null;
        if(unprocessedMessageText != null){
          Pattern p = Pattern.compile("\\{(.*?)\\}");
          Matcher m = p.matcher(unprocessedMessageText);
          Map<String,String> rawTagReplacements = new HashMap<String,String>();
          while (m.find())
            {
              /*****************************************
              *
              *  resolve reference
              *
              *****************************************/

              //
              //  criterionField
              //

              String rawTag = m.group();
              String criterionFieldName = m.group(1).trim();
              CriterionField criterionField = criterionContext.getCriterionFields().get(criterionFieldName);
              boolean parameterTag = false;
              if (criterionField == null)
                {
                  criterionField = new CriterionField(criterionFieldName, messageTextAttribute);
                  parameterTag = true;
                }

              //
              //  valid data type
              //

              switch (criterionField.getFieldDataType())
              {
                case IntegerCriterion:
                case DoubleCriterion:
                case StringCriterion:
                case StringSetCriterion:
                case BooleanCriterion:
                case DateCriterion:
                  break;

                default:
                  throw new GUIManagerException("unsupported tag type", criterionFieldName);  
              }

            /*****************************************
            *
            *  generate MessageFormat replacement
            *
            *****************************************/

            if (tagReplacements.get(criterionField) == null)
              {
                StringBuilder replacement = new StringBuilder();
                replacement.append("{");
                replacement.append(allTags.size());
                replacement.append("}");
                tagReplacements.put(criterionField, replacement.toString());
                allTags.add(criterionField);
                if (parameterTag)
                  parameterTags.add(criterionField);
                else
                  contextTags.add(criterionField);
              }
            rawTagReplacements.put(rawTag, tagReplacements.get(criterionField));
          }

        /*****************************************
        *
        *  replace tags
        *
        *****************************************/

        messageText = unprocessedMessageText;
        for (String rawTag : rawTagReplacements.keySet())
          {
            messageText = messageText.replace(rawTag, rawTagReplacements.get(rawTag));
          }
        }
        
        /*****************************************
        *
        *  messageTextByLanguage
        *
        *****************************************/
        
        messageTextByLanguage.put(language, messageText);
      }
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  private DialogMessage(Map<String,String> messageTextByLanguage, List<CriterionField> contextTags, List<CriterionField> parameterTags, List<CriterionField> allTags)
  {
    this.messageTextByLanguage = messageTextByLanguage;
    this.contextTags = contextTags;
    this.parameterTags = parameterTags;
    this.allTags = allTags;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  public DialogMessage(DialogMessage dialogMessage)
  {
    this.messageTextByLanguage = new HashMap<String,String>(dialogMessage.getMessageTextByLanguage());
    this.contextTags = new ArrayList<CriterionField>(dialogMessage.getContextTags());
    this.parameterTags = new ArrayList<CriterionField>(dialogMessage.getParameterTags());
    this.allTags = new ArrayList<CriterionField>(dialogMessage.getAllTags());
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    DialogMessage dialogMessage = (DialogMessage) value;
    Struct struct = new Struct(schema);
    struct.put("messageTextByLanguage", dialogMessage.getMessageTextByLanguage());
    struct.put("contextTags", packTags(dialogMessage.getContextTags()));
    struct.put("parameterTags", packTags(dialogMessage.getParameterTags()));
    struct.put("allTags", packTags(dialogMessage.getAllTags()));
    return struct;
  }

  /*****************************************
  *
  *  packTags
  *
  *****************************************/

  private static List<Object> packTags(List<CriterionField> tags)
  {
    List<Object> result = new ArrayList<Object>();
    for (CriterionField criterionField : tags)
      {
        result.add(CriterionField.pack(criterionField));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static DialogMessage unpack(SchemaAndValue schemaAndValue)
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
    Map<String,String> messageTextByLanguage = (Map<String,String>) valueStruct.get("messageTextByLanguage");
    List<CriterionField> contextTags = (schemaVersion >= 2) ? unpackTags(schema.field("contextTags").schema(), (List<Object>) valueStruct.get("contextTags")) : unpackTags(schema.field("tags").schema(), (List<Object>) valueStruct.get("tags"));
    List<CriterionField> parameterTags = (schemaVersion >= 2) ? unpackTags(schema.field("parameterTags").schema(), (List<Object>) valueStruct.get("parameterTags")) : new ArrayList<CriterionField>();
    List<CriterionField> allTags = (schemaVersion >= 2) ? unpackTags(schema.field("allTags").schema(), (List<Object>) valueStruct.get("allTags")) : unpackTags(schema.field("tags").schema(), (List<Object>) valueStruct.get("tags"));

    //
    //  return
    //

    return new DialogMessage(messageTextByLanguage, contextTags, parameterTags, allTags);
  }

  /*****************************************
  *
  *  unpackTags
  *
  *****************************************/

  private static List<CriterionField> unpackTags(Schema schema, List<Object> value)
  {
    //
    //  get schema for EvaluationCriterion
    //

    Schema criterionFieldSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<CriterionField> result = new ArrayList<CriterionField>();
    List<Object> valueArray = (List<Object>) value;
    for (Object criterionField : valueArray)
      {
        result.add(CriterionField.unpack(new SchemaAndValue(criterionFieldSchema, criterionField)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  resolve
  *
  *****************************************/

  public String resolveX(SubscriberEvaluationRequest subscriberEvaluationRequest)
  {
    /*****************************************
    *
    *  resolve language and messageText
    *
    *****************************************/
    
    //
    //  subscriber language
    //

    CriterionField subscriberLanguage = CriterionContext.Profile.getCriterionFields().get("subscriber.language");
    String languageID = (String) subscriberLanguage.retrieve(subscriberEvaluationRequest);
    String language = (languageID != null && Deployment.getSupportedLanguages().get(languageID) != null) ? Deployment.getSupportedLanguages().get(languageID).getName() : Deployment.getBaseLanguage();

    //
    //  message text
    //

    String messageText = (language != null) ? messageTextByLanguage.get(language) : null;

    //
    //  use base language (if necessary)
    //

    if (messageText == null)
      {
        language = Deployment.getBaseLanguage();
        messageText = messageTextByLanguage.get(language);
      }
    
    /*****************************************
    *
    *  tags
    *
    *****************************************/

    Locale messageLocale = new Locale(language, Deployment.getBaseCountry());
    MessageFormat formatter = null;
    Object[] messageTags = new Object[this.allTags.size()];
    for (int i=0; i<this.allTags.size(); i++)
      {
        //
        //  criterionField
        //

        CriterionField tag = this.allTags.get(i);

        //
        //  retrieve value
        //
        
        Object tagValue = tag.retrieve(subscriberEvaluationRequest);

        //
        //  resolve formatDataType
        //

        CriterionDataType formatDataType = resolveFormatDataType(tag.getFieldDataType(), tagValue);

        //
        //  formatter for tag
        //

        formatter = new MessageFormat("{0" + tag.resolveTagFormat(formatDataType) + "}", messageLocale);
        for (Format format : formatter.getFormats())
          {
            if (format instanceof SimpleDateFormat)
              {
                ((SimpleDateFormat) format).setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
              }
          }

        //
        //  formatted tag
        //

        Object[] tagValues = new Object[1];
        tagValues[0] = tagValue;
        String formattedTag = formatter.format(tagValues);
        
        //
        //  truncate (if necessary)
        //

        int maxLength = tag.resolveTagMaxLength(formatDataType);
        String resolvedTag = formattedTag;
        if (formattedTag.length() > maxLength)
          {
            switch (formatDataType)
              {
                case StringCriterion:
                case StringSetCriterion:
                  resolvedTag = formattedTag.substring(0, maxLength);
                  break;

                default:
                  StringBuilder invalidTag = new StringBuilder();
                  for (int j=0; j<maxLength; j++) invalidTag.append("X");
                  resolvedTag = invalidTag.toString();
                  break;
              }
          }
        
        //
        //  resolved tag
        //

        messageTags[i] = resolvedTag;
      }

    /*****************************************
    *
    *  format result
    *
    *****************************************/

    formatter = new MessageFormat(messageText, messageLocale);
    String resolvedMessage = formatter.format(messageTags);

    /*****************************************
    *
    *  return
    *
    ****************************************/

    return resolvedMessage;
  }

  /*****************************************
  *
  *  resolveMessageTags
  *
  *****************************************/

  protected List<String> resolveMessageTags(SubscriberEvaluationRequest subscriberEvaluationRequest, String language)
  {
    /*****************************************
    *
    *  tags
    *
    *****************************************/

    Locale messageLocale = new Locale(language, Deployment.getBaseCountry());
    MessageFormat formatter = null;
    List<String> messageTags = new ArrayList<String>();
    for (int i=0; i<this.allTags.size(); i++)
      {
        //
        //  criterionField
        //

        CriterionField tag = this.allTags.get(i);

        //
        //  retrieve value
        //
        
        subscriberEvaluationRequest.getMiscData().put("tagJourneyNodeParameterName", (String)tag.getJSONRepresentation().get("tagJourneyNodeParameterName"));
        Object tagValue = tag.retrieve(subscriberEvaluationRequest);

        //
        //  resolve formatDataType
        //

        CriterionDataType formatDataType = resolveFormatDataType(tag.getFieldDataType(), tagValue);

        //
        //  formatter for tag
        //

        formatter = new MessageFormat("{0" + tag.resolveTagFormat(formatDataType) + "}", messageLocale);
        for (Format format : formatter.getFormats())
          {
            if (format instanceof SimpleDateFormat)
              {
                ((SimpleDateFormat) format).setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
              }
          }

        //
        //  formatted tag
        //

        Object[] tagValues = new Object[1];
        tagValues[0] = tagValue;
        String formattedTag = formatter.format(tagValues);
        
        //
        //  truncate (if necessary)
        //

        int maxLength = tag.resolveTagMaxLength(formatDataType);
        String resolvedTag = formattedTag;
        if (formattedTag.length() > maxLength)
          {
            switch (formatDataType)
              {
                case StringCriterion:
                case StringSetCriterion:
                  resolvedTag = formattedTag.substring(0, maxLength);
                  break;

                default:
                  StringBuilder invalidTag = new StringBuilder();
                  for (int j=0; j<maxLength; j++) invalidTag.append("X");
                  resolvedTag = invalidTag.toString();
                  break;
              }
          }
        
        //
        //  resolved tag
        //

        messageTags.add(resolvedTag);
      }

    /*****************************************
    *
    *  return
    *
    ****************************************/

    return messageTags;
  }

  /*****************************************
  *
  *  resolveFormatDataType
  *
  *****************************************/

  private CriterionDataType resolveFormatDataType(CriterionDataType fieldDataType, Object tagValue)
  {
    CriterionDataType formatDataType = fieldDataType;
    switch (formatDataType)
      {
        case StringCriterion:
          if (tagValue instanceof Integer) formatDataType = CriterionDataType.IntegerCriterion;
          if (tagValue instanceof Double) formatDataType = CriterionDataType.DoubleCriterion;
          if (tagValue instanceof Date) formatDataType = CriterionDataType.DateCriterion;
          break;
      }
    return formatDataType;
  }

  /*****************************************
  *
  *  resolve
  *
  *****************************************/

  public String resolve(String language, List<String> messageTags)
  {
    String text = null;
    if (messageTextByLanguage.get(language) != null)
      {
        Locale messageLocale = new Locale(language, Deployment.getBaseCountry());
        MessageFormat formatter = new MessageFormat(messageTextByLanguage.get(language), messageLocale);
        text = formatter.format(messageTags.toArray());
      }
    return text;
  }

  /*****************************************
  *
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof DialogMessage)
      {
        DialogMessage dialogMessage = (DialogMessage) obj;
        result = true;
        result = result && Objects.equals(messageTextByLanguage, dialogMessage.getMessageTextByLanguage());
        result = result && Objects.equals(contextTags, dialogMessage.getContextTags());
        result = result && Objects.equals(parameterTags, dialogMessage.getParameterTags());
      }
    return result;
  }

  /*****************************************
  *
  *  hashCode
  *
  *****************************************/

  public int hashCode()
  {
    return messageTextByLanguage.hashCode();
  }
  
  /*****************************************
  *
  *  toString
  *
  *****************************************/

  @Override
  public String toString()
  {
    return "DialogMessage [messageTextByLanguage=" + messageTextByLanguage 
        + ", contextTags=" + contextTags 
        + ", parameterTags=" + parameterTags 
        + ", allTags=" + allTags + "]";
  }


}
