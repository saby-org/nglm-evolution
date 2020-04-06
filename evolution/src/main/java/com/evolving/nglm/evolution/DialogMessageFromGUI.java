/*****************************************************************************
*
*  DialogMessageFromGUI.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.HashMap;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class DialogMessageFromGUI extends SubscriberMessage
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
    schemaBuilder.name("dialog_message_from_gui");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schema = schemaBuilder.build();
  }

  //
  //  serde
  //

  private static ConnectSerde<DialogMessageFromGUI> serde = new ConnectSerde<DialogMessageFromGUI>(schema, false, DialogMessageFromGUI.class, DialogMessageFromGUI::pack, DialogMessageFromGUI::unpack);
  public static Object pack(Object value) { return packCommon(schema, value); }
  public static DialogMessageFromGUI unpack(SchemaAndValue schemaAndValue) { return new DialogMessageFromGUI(schemaAndValue); }
  public DialogMessageFromGUI(SchemaAndValue schemaAndValue) { super(schemaAndValue); }

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<DialogMessageFromGUI> serde() { return serde; }

  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  public DialogMessageFromGUI(Object dialogMessageJSON, SubscriberMessageTemplateService subscriberMessageTemplateService, CriterionContext criterionContext) throws GUIManagerException
  {
    super(dialogMessageJSON, new HashMap<String, Boolean>(), subscriberMessageTemplateService, criterionContext);
    
  }
  /*****************************************
  *
  *  toString
  *
  *****************************************/

  @Override
  public String toString()
  {
    return "DialogMessageFromGUI ["
        + "subscriberMessageTemplateID=" + getSubscriberMessageTemplateID() 
        + ", parameterTags()=" + getParameterTags() 
        + ", dialogMessages()=" + getDialogMessages() 
        + "]";
  }
  
}
