/*****************************************************************************
*
*  Template.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class MailTemplate extends GUIManagedObject
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
    schemaBuilder.name("email_template");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("message", EmailMessage.schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<MailTemplate> serde = new ConnectSerde<MailTemplate>(schema, false, MailTemplate.class, MailTemplate::pack, MailTemplate::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<MailTemplate> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private EmailMessage message;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getMailTemplateID() { return getGUIManagedObjectID(); }
  public String getMailTemplateName() { return getGUIManagedObjectName(); }
  public EmailMessage getMessage() { return message; }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public MailTemplate(SchemaAndValue schemaAndValue, EmailMessage message)
  {
    super(schemaAndValue);
    this.message = message;
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    MailTemplate template = (MailTemplate) value;
    Struct struct = new Struct(schema);
    packCommon(struct, template);
    struct.put("message", EmailMessage.pack(template.getMessage()));
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static MailTemplate unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    EmailMessage message = EmailMessage.unpack(new SchemaAndValue(schema.field("message").schema(), valueStruct.get("message")));
    
    //
    //  return
    //

    return new MailTemplate(schemaAndValue, message);
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public MailTemplate(JSONObject jsonRoot, long epoch, GUIManagedObject existingTemplateUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingTemplateUnchecked != null) ? existingTemplateUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingTemplate
    *
    *****************************************/

    MailTemplate existingTemplate = (existingTemplateUnchecked != null && existingTemplateUnchecked instanceof MailTemplate) ? (MailTemplate) existingTemplateUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.message = new EmailMessage(JSONUtilities.decodeJSONArray(jsonRoot, "message", true), CriterionContext.Profile);

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

  private boolean epochChanged(MailTemplate existingTemplate)
  {
    if (existingTemplate != null && existingTemplate.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingTemplate.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(message, existingTemplate.getMessage());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
}
