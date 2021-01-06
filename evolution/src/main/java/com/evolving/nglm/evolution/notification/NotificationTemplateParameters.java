/*****************************************************************************
*
*  NotificationTemplateParameters.java
*
*****************************************************************************/

package com.evolving.nglm.evolution.notification;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.CriterionContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.SubscriberMessage;
import com.evolving.nglm.evolution.SubscriberMessageTemplateService;
import com.evolving.nglm.evolution.Tenant;

public class NotificationTemplateParameters extends SubscriberMessage
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
    schemaBuilder.name("notification_template_parameters");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schema = schemaBuilder.build();
  }

  //
  //  serde
  //

  private static ConnectSerde<NotificationTemplateParameters> serde = new ConnectSerde<NotificationTemplateParameters>(schema, false, NotificationTemplateParameters.class, NotificationTemplateParameters::pack, NotificationTemplateParameters::unpack);
  public static Object pack(Object value) { return packCommon(schema, value); }
  public static NotificationTemplateParameters unpack(SchemaAndValue schemaAndValue) { return new NotificationTemplateParameters(schemaAndValue); }
  public NotificationTemplateParameters(SchemaAndValue schemaAndValue) { super(schemaAndValue); }

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<NotificationTemplateParameters> serde() { return serde; }

  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  public NotificationTemplateParameters(Object pushMessageJSON, String communicationChannelID,  Map<String, Boolean> dialogMessageFieldsMandatory, SubscriberMessageTemplateService subscriberMessageTemplateService, CriterionContext criterionContext) throws GUIManagerException
  {
    super(pushMessageJSON, communicationChannelID, dialogMessageFieldsMandatory, subscriberMessageTemplateService, criterionContext);
    
  }
  /*****************************************
  *
  *  toString
  *
  *****************************************/

  @Override
  public String toString()
  {
    return "NotificationTemplateParameters ["
        + "subscriberMessageTemplateID=" + getSubscriberMessageTemplateID() 
        + ", parameterTags()=" + getParameterTags() 
        + ", dialogMessages()=" + getDialogMessages() 
        + "]";
  }
  
}
