/*****************************************************************************
*
*  PushTemplate.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Date;
import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class PushTemplate extends SubscriberMessageTemplate
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
    schemaBuilder.name("push_template");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(SubscriberMessageTemplate.commonSchema().version(),1));
    for (Field field : SubscriberMessageTemplate.commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("communicationChannelID", Schema.STRING_SCHEMA);
    schema = schemaBuilder.build();
  }

  //
  //  serde
  //

  private static ConnectSerde<PushTemplate> serde = new ConnectSerde<PushTemplate>(schema, false, PushTemplate.class, PushTemplate::pack, PushTemplate::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<PushTemplate> serde() { return serde; }

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

  public String getPushTemplateID() { return getGUIManagedObjectID(); }
  public String getPushTemplateName() { return getGUIManagedObjectName(); }
  public String getCommunicationChannelID() { return communicationChannelID; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public PushTemplate(SchemaAndValue schemaAndValue, String communicationChannelID)
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
    PushTemplate pushTemplate = (PushTemplate) value;
    Struct struct = new Struct(schema);
    SubscriberMessageTemplate.packCommon(struct, pushTemplate);
    struct.put("communicationChannelID", pushTemplate.getCommunicationChannelID());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/
  
  public static PushTemplate unpack(SchemaAndValue schemaAndValue) 
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

    return new PushTemplate(schemaAndValue, communicationChannelID);

  }

  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  public PushTemplate(CommunicationChannelService communicationChannelService, JSONObject jsonRoot, long epoch, GUIManagedObject existingTemplateUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(communicationChannelService, jsonRoot, GUIManagedObjectType.PushMessageTemplate, epoch, existingTemplateUnchecked);
    
    /*****************************************
    *
    *  existingSegmentationDimension
    *
    *****************************************/

    PushTemplate existingPushTemplate = (existingTemplateUnchecked != null && existingTemplateUnchecked instanceof PushTemplate) ? (PushTemplate) existingTemplateUnchecked : null;

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

    if (epochChanged(existingPushTemplate))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  abstract
  *
  *****************************************/

  @Override public String getTemplateType() { return "push"; }

  @Override public void retrieveDialogMessageFields(CommunicationChannelService communicationChannelService, JSONObject jsonRoot) throws GUIManagerException
  {
    /*****************************************
    *
    *  attributes
    *
    *****************************************/
    
    this.communicationChannelID = JSONUtilities.decodeString(jsonRoot, "communicationChannelID", true);

    this.dialogMessageFields = new ArrayList<String>();
    Date now = SystemTime.getCurrentTime();
    CommunicationChannel communicationChannel = communicationChannelService.getActiveCommunicationChannel(communicationChannelID, now);
    if(communicationChannel == null)
      {
        throw new GUIManagerException("unknown communication channel", communicationChannelID);
      }
    if(communicationChannel.getParameters() != null)
      {
        for(String communicationChannelParameter : communicationChannel.getParameters().keySet())
          {
            dialogMessageFields.add(communicationChannelParameter); 
          }
      }
  }
  
  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(PushTemplate existingPushTemplate)
  {
    if (existingPushTemplate != null && existingPushTemplate.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingPushTemplate.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(getDialogMessages(), existingPushTemplate.getDialogMessages());
        epochChanged = epochChanged || ! Objects.equals(getDialogMessageFields(), existingPushTemplate.getDialogMessageFields());
        epochChanged = epochChanged || ! Objects.equals(communicationChannelID, existingPushTemplate.getCommunicationChannelID());
        return epochChanged;
      }
    else
      {
        return true;
      }
   }

}
