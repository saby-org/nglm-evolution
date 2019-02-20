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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("messageTextByLanguage", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA));
    schemaBuilder.field("tags", SchemaBuilder.array(CriterionField.schema()));
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
  private List<CriterionField> tags = new ArrayList<CriterionField>();

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public Map<String,String> getMessageTextByLanguage() { return messageTextByLanguage; }
  public  List<CriterionField> getTags() { return tags; }
  
  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  public DialogMessage(JSONArray messagesJSON, String messageTextAttribute, CriterionContext criterionContext) throws GUIManagerException
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

        SupportedLanguage supportedLanaguage = Deployment.getSupportedLanguages().get(JSONUtilities.decodeString(messageJSON, "languageID", true));
        String language = (supportedLanaguage != null) ? supportedLanaguage.getName() : null;
        if (language == null) throw new GUIManagerException("unsupported language", JSONUtilities.decodeString(messageJSON, "languageID", true));

        /*****************************************
        *
        *  unprocessedMessageText
        *
        *****************************************/

        String unprocessedMessageText = JSONUtilities.decodeString(messageJSON, messageTextAttribute, true);

        /*****************************************
        *
        *  find tags
        *
        *****************************************/

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
            if (criterionField == null) throw new GUIManagerException("unsupported tag", criterionFieldName);

            //
            //  valid data type
            //

            switch (criterionField.getFieldDataType())
              {
                case IntegerCriterion:
                case DoubleCriterion:
                case StringCriterion:
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
                replacement.append(tags.size());
                replacement.append("}");
                tagReplacements.put(criterionField, replacement.toString());
                tags.add(criterionField);
              }
            rawTagReplacements.put(rawTag, tagReplacements.get(criterionField));
          }

        /*****************************************
        *
        *  replace tags
        *
        *****************************************/

        String messageText = unprocessedMessageText;
        for (String rawTag : rawTagReplacements.keySet())
          {
            messageText = messageText.replace(rawTag, rawTagReplacements.get(rawTag));
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

  private DialogMessage(Map<String,String> messageTextByLanguage, List<CriterionField> tags)
  {
    this.messageTextByLanguage = messageTextByLanguage;
    this.tags = tags;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  public DialogMessage(DialogMessage dialogMessage)
  {
    this.messageTextByLanguage = new HashMap<String,String>(dialogMessage.getMessageTextByLanguage());
    this.tags = new ArrayList<CriterionField>(dialogMessage.getTags());
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
    struct.put("tags", packTags(dialogMessage.getTags()));
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
    List<CriterionField> tags = unpackTags(schema.field("tags").schema(), (List<Object>) valueStruct.get("tags"));

    //
    //  return
    //

    return new DialogMessage(messageTextByLanguage, tags);
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

  public String resolve(SubscriberEvaluationRequest subscriberEvaluationRequest)
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
    String language = (String) subscriberLanguage.retrieve(subscriberEvaluationRequest);

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
    Object[] messageTags = new Object[this.tags.size()];
    for (int i=0; i<this.tags.size(); i++)
      {
        //
        //  criterionField
        //

        CriterionField tag = this.tags.get(i);

        //
        //  retrieve value
        //
        
        Object tagValue = tag.retrieve(subscriberEvaluationRequest);

        //
        //  formatter for tag
        //

        formatter = new MessageFormat("{0" + tag.resolveTagFormat() + "}", messageLocale);
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

        int maxLength = tag.resolveTagMaxLength();
        String resolvedTag = formattedTag;
        if (formattedTag.length() > maxLength)
          {
            switch (tag.getFieldDataType())
              {
                case StringCriterion:
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
}
