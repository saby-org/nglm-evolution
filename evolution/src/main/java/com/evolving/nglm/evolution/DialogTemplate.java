/*****************************************************************************
*
*  Template.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.HashMap;
import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class DialogTemplate extends SubscriberMessageTemplate
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
    schemaBuilder.name("dialog_template");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(SubscriberMessageTemplate.commonSchema().version(),1));
    for (Field field : SubscriberMessageTemplate.commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("communicationChannelID", Schema.STRING_SCHEMA);
    schema = schemaBuilder.build();
  }

  //
  //  serde
  //

  private static ConnectSerde<DialogTemplate> serde = new ConnectSerde<DialogTemplate>(schema, false, DialogTemplate.class, DialogTemplate::pack, DialogTemplate::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<DialogTemplate> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String communicationChannelID;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getDialogTemplateID() { return getGUIManagedObjectID(); }
  public String getDialogTemplateName() { return getGUIManagedObjectName(); }
  public String getCommunicationChannelID() { return communicationChannelID; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public DialogTemplate(SchemaAndValue schemaAndValue, String communicationChannelID)
  {
    super(schemaAndValue);
    this.communicationChannelID = communicationChannelID;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    DialogTemplate dialogTemplate = (DialogTemplate) value;
    Struct struct = new Struct(schema);
    SubscriberMessageTemplate.packCommon(struct, dialogTemplate);
    struct.put("communicationChannelID", dialogTemplate.getCommunicationChannelID());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/
  
  public static DialogTemplate unpack(SchemaAndValue schemaAndValue) 
  { 
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion2(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String communicationChannelID = valueStruct.getString("communicationChannelID");
    
    //
    //  return
    //

    return new DialogTemplate(schemaAndValue, communicationChannelID);

  }

  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  public DialogTemplate(JSONObject jsonRoot, long epoch, GUIManagedObject existingTemplateUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, GUIManagedObjectType.DialogTemplate, epoch, existingTemplateUnchecked);
    
    /*****************************************
    *
    *  existingSegmentationDimension
    *
    *****************************************/

    DialogTemplate existingDialogTemplate = (existingTemplateUnchecked != null && existingTemplateUnchecked instanceof DialogTemplate) ? (DialogTemplate) existingTemplateUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.communicationChannelID = JSONUtilities.decodeString(jsonRoot, "communicationChannelID", true);

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingDialogTemplate))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  abstract
  *
  *****************************************/

  @Override public String getTemplateType() { return "dialogTemplate"; } // should be useless for generic DialogTemplate

  @Override public void retrieveDialogMessageFields(JSONObject jsonRoot) throws GUIManagerException
  {
    /*****************************************
    *
    *  attributes
    *
    *****************************************/
    
    this.communicationChannelID = JSONUtilities.decodeString(jsonRoot, "communicationChannelID", false);

    this.dialogMessageFields = new HashMap<String, Boolean>();
    Date now = SystemTime.getCurrentTime();
    CommunicationChannel communicationChannel = Deployment.getCommunicationChannels().get(communicationChannelID);
    if(communicationChannel == null)
      {
        throw new GUIManagerException("unknown communication channel", communicationChannelID);
      }
    if(communicationChannel.getParameters() != null)
      {
        for(String communicationChannelParameter : communicationChannel.getParameters().keySet())
          {
            CriterionField criterionField = communicationChannel.getParameters().get(communicationChannelParameter);
            dialogMessageFields.put(communicationChannelParameter, criterionField.getMandatoryParameter()); 
          }
      }
  }
  
  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(DialogTemplate existingDialogTemplate)
  {
    if (existingDialogTemplate != null && existingDialogTemplate.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingDialogTemplate.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(getDialogMessages(), existingDialogTemplate.getDialogMessages());
        epochChanged = epochChanged || ! Objects.equals(getDialogMessageFields(), existingDialogTemplate.getDialogMessageFields());
        epochChanged = epochChanged || ! Objects.equals(communicationChannelID, existingDialogTemplate.getCommunicationChannelID());
        return epochChanged;
      }
    else
      {
        return true;
      }
   }

}
