/*****************************************************************************
*
*  NotficationGenericMessage.java
*
*****************************************************************************/

package com.evolving.nglm.evolution.notification;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.CriterionContext;
import com.evolving.nglm.evolution.DialogMessage;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.SubscriberMessage;
import com.evolving.nglm.evolution.SubscriberMessageTemplateService;

public class NotificationGenericMessage extends SubscriberMessage
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
    schemaBuilder.name("notification_generic_message");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schema = schemaBuilder.build();
  }

  //
  //  serde
  //

  private static ConnectSerde<NotificationGenericMessage> serde = new ConnectSerde<NotificationGenericMessage>(schema, false, NotificationGenericMessage.class, NotificationGenericMessage::pack, NotificationGenericMessage::unpack);
  public static Object pack(Object value) { return packCommon(schema, value); }
  public static NotificationGenericMessage unpack(SchemaAndValue schemaAndValue) { return new NotificationGenericMessage(schemaAndValue); }
  public NotificationGenericMessage(SchemaAndValue schemaAndValue) { super(schemaAndValue); }

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<NotificationGenericMessage> serde() { return serde; }

  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  public NotificationGenericMessage(Object pushMessageJSON, SubscriberMessageTemplateService subscriberMessageTemplateService, CriterionContext criterionContext) throws GUIManagerException
  {
    super(pushMessageJSON, new HashMap<String, Boolean>(), subscriberMessageTemplateService, criterionContext);
    
  }
  public NotificationGenericMessage(ArrayList<DialogMessage> genericNotificationFields)
    {
      super(genericNotificationFields);
    }
  /*****************************************
  *
  *  toString
  *
  *****************************************/

  @Override
  public String toString()
  {
    return "NotficationGenericMessage ["
        + "subscriberMessageTemplateID=" + getSubscriberMessageTemplateID() 
        + ", parameterTags()=" + getParameterTags() 
        + ", dialogMessages()=" + getDialogMessages() 
        + "]";
  }
  
}
