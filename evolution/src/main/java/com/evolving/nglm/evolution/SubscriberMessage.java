/*****************************************************************************
*
*  SubscriberMessage.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public abstract class SubscriberMessage
{
  
  private static final Logger log = LoggerFactory.getLogger(SubscriberMessage.class);
  
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(3));
    schemaBuilder.field("subscriberMessageTemplateID", Schema.STRING_SCHEMA);
    schemaBuilder.field("communicationChannelID", Schema.OPTIONAL_STRING_SCHEMA); // A enlever
    schemaBuilder.field("parameterTags", SimpleParameterMap.schema());
    schemaBuilder.field("dialogMessages", SchemaBuilder.map(Schema.STRING_SCHEMA, DialogMessage.schema()).name("message_dialog_messages").schema());
    schemaBuilder.field("tenantID", Schema.INT16_SCHEMA);
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
  private String communicationChannelID = null;
  private SimpleParameterMap parameterTags = new SimpleParameterMap();
  private Map<String, DialogMessage> dialogMessages = new HashMap<String, DialogMessage>();
  private int tenantID;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getSubscriberMessageTemplateID() { return subscriberMessageTemplateID; }
  public String getCommunicationChannelID() { return communicationChannelID; }
  public SimpleParameterMap getParameterTags() { return parameterTags; }
  public Map<String, DialogMessage> getDialogMessages() { return dialogMessages; }
  public int getTenantID() { return tenantID; }
  
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

  protected SubscriberMessage(Object subscriberMessageJSON, String communicationChannelID, Map<String, Boolean> dialogMessageFields, SubscriberMessageTemplateService subscriberMessageTemplateService, CriterionContext criterionContext) throws GUIManagerException
  {
    this.communicationChannelID = communicationChannelID;
    this.tenantID = tenantID;
    
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
        
        parameterTags = decodeParameterTags(JSONUtilities.decodeJSONArray(messageJSON, "macros", new JSONArray()), subscriberMessageTemplate, criterionContext, tenantID);
      }
    
    /*****************************************
    *
    *  case 2:  subscriberMessageJSON: in case of OLD SMS inline messages and Generic inLine messages
    *          
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
            dialogMessages.put(dialogMessageField, new DialogMessage(messagesJSON, dialogMessageField, mandatory, criterionContext, true, tenantID));
          }
      }
  }


  /*****************************************
  *
  *  decodeParameterTags
  *
  *****************************************/

  private SimpleParameterMap decodeParameterTags(JSONArray jsonArray, SubscriberMessageTemplate subscriberMessageTemplate, CriterionContext criterionContext, int tenantID) throws GUIManagerException
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

    List<String> contextIDs = subscriberMessageTemplate.getContextTags().stream().map(CriterionField::getID).collect(Collectors.toList());
    SimpleParameterMap parameterTags = new SimpleParameterMap();
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject parameterJSON = (JSONObject) jsonArray.get(i);
        String parameterID = JSONUtilities.decodeString(parameterJSON, "templateValue", true);
        
        //
        //  ignore contexts - no need to respect the context tags
        //
        
        if (contextIDs.contains(parameterID)) continue;
        CriterionField parameter = parameterTagsByID.get(parameterID);
        if (parameter == null)
          {
            log.error("parameterID {} not found in parameterTagsByID {} and parameterJSON is {}", parameterID, parameterTagsByID, parameterJSON);
            throw new GUIManagerException("unknown parameterTag", parameterID);
          }
        
        if (! Journey.isExpressionValuedParameterValue(parameterJSON))
          {
            switch (parameter.getFieldDataType())
              {
                case StringCriterion:
                  
                  // parameter value is either a GUI entered String, either the reference to a criterion Field
                  String parameterValue = JSONUtilities.decodeString(parameterJSON, "campaignValue", false);
                  
                  // check if the value refers a criterion or a simple String...
                  CriterionField tagCriterionField = criterionContext.getCriterionFields(tenantID).get(parameterValue);
                  
                  if(tagCriterionField != null) {
                    ParameterExpression parameterExpression = new ParameterExpression(parameterValue, null, criterionContext, tenantID);
                    parameterTags.put(parameterID, parameterExpression);
                  }
                  else {
                    parameterTags.put(parameterID, JSONUtilities.decodeString(parameterJSON, "campaignValue", false));
                  }
                  break;

                default:
                  throw new GUIManagerException("unsupported parameterTag type", parameterID);
              }
          }
        else
          {
            ParameterExpression parameterExpressionValue = new ParameterExpression(JSONUtilities.decodeJSONObject(parameterJSON, "value", true), criterionContext, tenantID);
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
    String communicationChannelID = schema.field("communicationChannelID") != null ? valueStruct.getString("communicationChannelID") : null;
    SimpleParameterMap parameterTags = SimpleParameterMap.unpack(new SchemaAndValue(schema.field("parameterTags").schema(), valueStruct.get("parameterTags")));    
    Map<String,DialogMessage> dialogMessages = unpackDialogMessages(schema.field("dialogMessages").schema(), (Map<String,Object>) valueStruct.get("dialogMessages"));
    int tenantID = schema.field("tenantID") != null ? valueStruct.getInt16("tenantID") : 1; // by default tenant 1
    
    //
    //  return
    //

    this.subscriberMessageTemplateID = subscriberMessageTemplateID;
    this.communicationChannelID = communicationChannelID;
    this.parameterTags = parameterTags;
    this.tenantID = tenantID;
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
    struct.put("communicationChannelID", subscriberMessage.getCommunicationChannelID());
    struct.put("parameterTags", SimpleParameterMap.pack(subscriberMessage.getParameterTags()));
    struct.put("dialogMessages", packDialogMessages(subscriberMessage.getDialogMessages()));
    struct.put("tenantID", subscriberMessage.getTenantID());
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
