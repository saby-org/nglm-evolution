/*****************************************************************************
*
*  Tenant.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class Tenant extends GUIManagedObject
{
  /*****************************************
  *
  *  singletonID
  *
  *****************************************/

  public static final String singletonID = "base";

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
    schemaBuilder.name("tenant");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("languageID", Schema.STRING_SCHEMA);
    schemaBuilder.field("effectiveTenantID", Schema.INT16_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<Tenant> serde = new ConnectSerde<Tenant>(schema, false, Tenant.class, Tenant::pack, Tenant::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<Tenant> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/
  private int effectiveTenantID;
  private String languageID;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getLanguageID() { return languageID; }
  public int getEffectiveTenantID() { return effectiveTenantID; }

  /*****************************************
  *
  *  constructor
   * @throws GUIManagerException 
  *
  *****************************************/

  public Tenant(JSONObject guiManagedObjectJson, int effectiveTenantID, String languageID)
  {
    super(guiManagedObjectJson, 1, 0); // 0 because in the point of view of the system, this GUIManagedObject is shared among all tenants (administrator)
    this.languageID = languageID;
    this.effectiveTenantID = effectiveTenantID;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Tenant(SchemaAndValue schemaAndValue, int effectiveTenantID, String languageID)
  {
    super(schemaAndValue);
    this.languageID = languageID;
    this.effectiveTenantID = effectiveTenantID;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    Tenant tenant = (Tenant) value;
    Struct struct = new Struct(schema);
    packCommon(struct, tenant);
    struct.put("languageID", tenant.getLanguageID());
    struct.put("effectiveTenantID", tenant.getEffectiveTenantID());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static Tenant unpack(SchemaAndValue schemaAndValue)
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
    String languageID = valueStruct.getString("languageID");
    int effectiveTenantID = valueStruct.getInt16("effectiveTenantID");
    

    //
    //  return
    //

    return new Tenant(schemaAndValue, effectiveTenantID, languageID);
  }
}
