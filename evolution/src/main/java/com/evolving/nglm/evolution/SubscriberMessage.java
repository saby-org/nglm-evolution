/*****************************************************************************
*
*  SubscriberMessage.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public abstract class SubscriberMessage
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
    schemaBuilder.name("subscriber_message");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("subscriberMessageTemplateID", Schema.STRING_SCHEMA);
    schemaBuilder.field("parameterTags", SimpleParameterMap.schema());
    schemaBuilder.field("dialogMessages", SchemaBuilder.map(Schema.STRING_SCHEMA, DialogMessage.schema()).name("message_dialog_messages").schema());
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

  private String subscriberMessageTemplateID = null;
  private SimpleParameterMap parameterTags = new SimpleParameterMap();
  private Map<String, DialogMessage> dialogMessages = new HashMap<String, DialogMessage>();

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getSubscriberMessageTemplateID() { return subscriberMessageTemplateID; }
  public SimpleParameterMap getParameterTags() { return parameterTags; }
  public Map<String, DialogMessage> getDialogMessages() { return dialogMessages; }
  
  /****************************************
  *
  *  setters
  *
  *****************************************/

  public void setSubscriberMessageTemplateID(String subscriberMessageTemplateID) { this.subscriberMessageTemplateID = subscriberMessageTemplateID; }

  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  protected SubscriberMessage(Object subscriberMessageJSON, Map<String, Boolean> dialogMessageFields, SubscriberMessageTemplateService subscriberMessageTemplateService, CriterionContext criterionContext) throws GUIManagerException
  {
    /*****************************************
    *
    *  case 1:  subscriberMessageJSON is a reference to a template
    *
    *****************************************/

    if (subscriberMessageJSON instanceof JSONObject)
      {
        //
        //  messageJSON
        //

        JSONObject messageJSON = (JSONObject) subscriberMessageJSON;

        //
        //  template
        //

        subscriberMessageTemplateID = JSONUtilities.decodeString(messageJSON, "templateID", true);
        SubscriberMessageTemplate subscriberMessageTemplate = subscriberMessageTemplateService.getActiveSubscriberMessageTemplate(subscriberMessageTemplateID, SystemTime.getCurrentTime());
        if (subscriberMessageTemplate == null) throw new GUIManagerException("unknown subscriberMessageTemplate", subscriberMessageTemplateID);

        //
        //  parameterTags
        //
        
        parameterTags = decodeParameterTags(JSONUtilities.decodeJSONArray(messageJSON, "parameterTags", new JSONArray()), subscriberMessageTemplate, criterionContext);
      }

    /*****************************************
    *
    *  case 2:  subscriberMessageJSON is a hard-coded message
    *
    *****************************************/
    
    if (subscriberMessageJSON instanceof JSONArray)
      {
        //
        //  messagesJSON
        //

        JSONArray messagesJSON = (JSONArray) subscriberMessageJSON;

        //
        //  messageText
        //

        dialogMessages = new HashMap<String, DialogMessage>();
        for (String dialogMessageField : dialogMessageFields.keySet())
          {
            boolean mandatory = dialogMessageFields.get(dialogMessageField);
            dialogMessages.put(dialogMessageField, new DialogMessage(messagesJSON, dialogMessageField, dialogMessageField, mandatory, criterionContext));
          }
      }
  }
  
  
  public SubscriberMessage(ArrayList<DialogMessage> genericNotificationFields)
  {
    this.dialogMessages = new HashMap<>();
    for(DialogMessage notificationField : genericNotificationFields) {
      dialogMessages.put(notificationField.getNotificationParameterName(), notificationField);
    }
  }

  /*****************************************
  *
  *  decodeParameterTags
  *
  *****************************************/

  private SimpleParameterMap decodeParameterTags(JSONArray jsonArray, SubscriberMessageTemplate subscriberMessageTemplate, CriterionContext criterionContext) throws GUIManagerException
  {
    /*****************************************
    *
    *  parameterTagsByID
    *
    *****************************************/

    Map<String,CriterionField> parameterTagsByID = new HashMap<String,CriterionField>();
    for (CriterionField parameterTag : subscriberMessageTemplate.getParameterTags())
      {
        parameterTagsByID.put(parameterTag.getID(), parameterTag);
      }

    /*****************************************
    *
    *  decode
    *
    *****************************************/

    SimpleParameterMap parameterTags = new SimpleParameterMap();
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject parameterJSON = (JSONObject) jsonArray.get(i);
        String parameterID = JSONUtilities.decodeString(parameterJSON, "parameterName", true);
        CriterionField parameter = parameterTagsByID.get(parameterID);
        if (parameter == null) throw new GUIManagerException("unknown parameterTag", parameterID);
        if (! Journey.isExpressionValuedParameterValue(parameterJSON))
          {
            switch (parameter.getFieldDataType())
              {
                case StringCriterion:
                  parameterTags.put(parameterID, JSONUtilities.decodeString(parameterJSON, "value", false));
                  break;

                default:
                  throw new GUIManagerException("unsupported parameterTag type", parameterID);
              }
          }
        else
          {
            ParameterExpression parameterExpressionValue = new ParameterExpression(JSONUtilities.decodeJSONObject(parameterJSON, "value", true), criterionContext);
            parameterTags.put(parameterID, parameterExpressionValue);
            switch (parameterExpressionValue.getType())
              {
                case IntegerExpression:
                case DoubleExpression:
                case StringExpression:
                case BooleanExpression:
                case DateExpression:
                  break;

                default:
                  throw new GUIManagerException("unsupported parameterTag expression type", parameterID);
              }
          }
      }

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return parameterTags;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  protected SubscriberMessage(SchemaAndValue schemaAndValue)
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
    String subscriberMessageTemplateID = valueStruct.getString("subscriberMessageTemplateID");
    SimpleParameterMap parameterTags = SimpleParameterMap.unpack(new SchemaAndValue(schema.field("parameterTags").schema(), valueStruct.get("parameterTags")));    
    Map<String,DialogMessage> dialogMessages = unpackDialogMessages(schema.field("dialogMessages").schema(), (Map<String,Object>) valueStruct.get("dialogMessages"));
    
    //
    //  return
    //

    this.subscriberMessageTemplateID = subscriberMessageTemplateID;
    this.parameterTags = parameterTags;
  }


  /*****************************************
  *
  *  unpackDialogMessages
  *
  *****************************************/

  private static Map<String,DialogMessage> unpackDialogMessages(Schema schema, Map<String,Object> dialogMessages)
  {
    //
    //  get schema
    //

    Schema dialogMessageSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    Map<String,DialogMessage> result = new HashMap<String,DialogMessage>();
    for (String dialogMessageFieldName : dialogMessages.keySet())
      {
        DialogMessage dialogMessage = DialogMessage.unpack(new SchemaAndValue(dialogMessageSchema, dialogMessages.get(dialogMessageFieldName)));
        result.put(dialogMessageFieldName, dialogMessage);
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

  public static Object packCommon(Schema schema, Object value)
  {
    SubscriberMessage subscriberMessage = (SubscriberMessage) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberMessageTemplateID", subscriberMessage.getSubscriberMessageTemplateID());
    struct.put("parameterTags", SimpleParameterMap.pack(subscriberMessage.getParameterTags()));
    struct.put("dialogMessages", packDialogMessages(subscriberMessage.getDialogMessages()));
    return struct;
  }

  /*****************************************
  *
  *  packDialogMessages
  *
  *****************************************/

  private static Map<String,Object> packDialogMessages(Map<String,DialogMessage> dialogMessages)
  {
    Map<String,Object> result = new HashMap<String,Object>();
    for (String dialogMessageFieldName : dialogMessages.keySet())
      {
        DialogMessage communicationChannelParameter = dialogMessages.get(dialogMessageFieldName);
        result.put(dialogMessageFieldName,DialogMessage.pack(communicationChannelParameter));
      }
    return result;
  }

  /*****************************************
  *
  *  resolveTemplate
  *
  *****************************************/

  protected SubscriberMessageTemplate resolveTemplate(EvolutionEventContext evolutionEventContext)
  {
    return evolutionEventContext.getSubscriberMessageTemplateService().getActiveSubscriberMessageTemplate(getSubscriberMessageTemplateID(), evolutionEventContext.now());
  }
}
