/*****************************************************************************
*
*  MailTemplate.java
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

public class MailTemplate extends SubscriberMessageTemplate
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
    schemaBuilder.name("mail_template");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schema = schemaBuilder.build();
  }

  //
  //  serde
  //

  private static ConnectSerde<MailTemplate> serde = new ConnectSerde<MailTemplate>(schema, false, MailTemplate.class, MailTemplate::pack, MailTemplate::unpack);
  public static Object pack(Object value) { return SubscriberMessageTemplate.packCommon(schema, value); }
  public static MailTemplate unpack(SchemaAndValue schemaAndValue) { return new MailTemplate(schemaAndValue); }
  public MailTemplate(SchemaAndValue schemaAndValue) { super(schemaAndValue); }

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<MailTemplate> serde() { return serde; }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getMailTemplateID() { return getGUIManagedObjectID(); }
  public String getMailTemplateName() { return getGUIManagedObjectName(); }
  public DialogMessage getSubject() { return super.getDialogMessages().get(0/*"subject"*/); }
  public DialogMessage getHTMLBody() { return super.getDialogMessages().get(1/*"htmlBody"*/); }
  public DialogMessage getTextBody() { return super.getDialogMessages().get(2/*"textBody"*/); }
  
  //
  //  abstract
  //

  @Override public String getTemplateType() { return "mail"; }
  @Override public void retrieveDialogMessageFields(CommunicationChannelService communicationChannelService, JSONObject jsonRoot) throws GUIManagerException 
  { 
    this.dialogMessageFields = new HashMap<String, Boolean>();
    dialogMessageFields.put("subject", true);
    dialogMessageFields.put("htmlBody", true);
    dialogMessageFields.put("textBody", true); 
  }

  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  public MailTemplate(CommunicationChannelService communicationChannelService, JSONObject jsonRoot, long epoch, GUIManagedObject existingTemplateUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/
    
    super(communicationChannelService, jsonRoot, GUIManagedObjectType.MailMessageTemplate, epoch, existingTemplateUnchecked);
    
    /*****************************************
    *
    *  existingSegmentationDimension
    *
    *****************************************/

    MailTemplate existingTemplate = (existingTemplateUnchecked != null && existingTemplateUnchecked instanceof MailTemplate) ? (MailTemplate) existingTemplateUnchecked : null;

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
  *  resolve
  *
  *****************************************/

  public String resolveSubject(SubscriberEvaluationRequest subscriberEvaluationRequest) { return getSubject().resolveX(subscriberEvaluationRequest); }
  public String resolveHTMLBody(SubscriberEvaluationRequest subscriberEvaluationRequest) { return getHTMLBody().resolveX(subscriberEvaluationRequest); }
  public String resolveTextBody(SubscriberEvaluationRequest subscriberEvaluationRequest) { return getTextBody().resolveX(subscriberEvaluationRequest); }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  protected boolean epochChanged(MailTemplate existingSubscriberMessageTemplate)
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
