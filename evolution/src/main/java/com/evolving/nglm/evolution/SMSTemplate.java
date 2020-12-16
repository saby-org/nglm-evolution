/*****************************************************************************
*
*  SMSTemplate.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.HashMap;
import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

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
  public DialogMessage getMessageText() { return super.getDialogMessages().get("messageText"); }
  
  //
  //  abstract
  //

  @Override public String getTemplateType() { return "sms"; }
  @Override public void retrieveDialogMessageFields(JSONObject jsonRoot) throws GUIManagerException 
  { 
    this.dialogMessageFields = new HashMap<String, Boolean>();
    dialogMessageFields.put("messageText", true);
  }

  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  public SMSTemplate(JSONObject jsonRoot, long epoch, GUIManagedObject existingTemplateUnchecked, int tenantID) throws GUIManagerException
  {
    super(jsonRoot, GUIManagedObjectType.SMSMessageTemplate, epoch, existingTemplateUnchecked, tenantID);
    
    /*****************************************
    *
    *  existingSegmentationDimension
    *
    *****************************************/

    SMSTemplate existingTemplate = (existingTemplateUnchecked != null && existingTemplateUnchecked instanceof SMSTemplate) ? (SMSTemplate) existingTemplateUnchecked : null;

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingTemplate))
      {
        this.setEpoch(epoch);
      }    
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(SMSTemplate existingSubscriberMessageTemplate)
  {
    if (existingSubscriberMessageTemplate != null && existingSubscriberMessageTemplate.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingSubscriberMessageTemplate.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(getDialogMessages(), existingSubscriberMessageTemplate.getDialogMessages());
        epochChanged = epochChanged || ! Objects.equals(getDialogMessageFields(), existingSubscriberMessageTemplate.getDialogMessageFields());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
}
