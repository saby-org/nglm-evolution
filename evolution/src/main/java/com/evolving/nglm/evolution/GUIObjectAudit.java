/*****************************************************************************
*
*  GUIObjectAudit.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.API;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.Date;

public class GUIObjectAudit
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
    schemaBuilder.name("gui_action_audit");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("userID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("api", Schema.STRING_SCHEMA);
    schemaBuilder.field("newObject", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("objectID", Schema.STRING_SCHEMA);
    schemaBuilder.field("submittedObject", GUIManagedObject.commonSerde().optionalSchema());
    schemaBuilder.field("apiDate", Timestamp.builder().schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<GUIObjectAudit> serde = new ConnectSerde<GUIObjectAudit>(schema, false, GUIObjectAudit.class, GUIObjectAudit::pack, GUIObjectAudit::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<GUIObjectAudit> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String userID;
  private String api;
  private boolean newObject;
  private String objectID;
  private GUIManagedObject submittedObject;
  private Date apiDate;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //

  public String getUserID() { return userID; }
  public String getAPI() { return api; }
  public boolean getNewObject() { return newObject; }
  public String getObjectID() { return objectID; }
  public GUIManagedObject getSubmittedObject() { return submittedObject; }
  public Date getAPIDate() { return apiDate;  }

  /*****************************************
  *
  *  constructor -- standard/unpack
  *
  *****************************************/

  public GUIObjectAudit(String userID, String api, boolean newObject, String objectID, GUIManagedObject submittedObject, Date apiDate)
  {
    this.userID = userID;
    this.api = api;
    this.newObject = newObject;
    this.objectID = objectID;
    this.submittedObject = submittedObject;
    this.apiDate = apiDate;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    GUIObjectAudit guiObjectAudit = (GUIObjectAudit) value;
    Struct struct = new Struct(schema);
    struct.put("userID", guiObjectAudit.getUserID());
    struct.put("api", guiObjectAudit.getAPI());
    struct.put("newObject", guiObjectAudit.getNewObject());
    struct.put("objectID", guiObjectAudit.getObjectID());
    struct.put("submittedObject", GUIManagedObject.commonSerde().packOptional(guiObjectAudit.getSubmittedObject()));
    struct.put("apiDate", guiObjectAudit.getAPIDate());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static GUIObjectAudit unpack(SchemaAndValue schemaAndValue)
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
    String userID = valueStruct.getString("userID");
    String api = valueStruct.getString("api");
    boolean newObject = valueStruct.getBoolean("newObject");
    String objectID = valueStruct.getString("objectID");
    GUIManagedObject submittedObject = GUIManagedObject.commonSerde().unpackOptional(new SchemaAndValue(schema.field("submittedObject").schema(), valueStruct.get("submittedObject")));
    Date apiDate = (Date) valueStruct.get("apiDate");
    
    //
    //  return
    //

    return new GUIObjectAudit(userID, api, newObject, objectID, submittedObject, apiDate);
  }
}
