/*****************************************************************************
*
*  DynamicEventDeclarations.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

// TODO EVPRO-99 must we split per tenant ???
public class DynamicEventDeclarations extends GUIManagedObject
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
    schemaBuilder.name("dynamic_event_declarations");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("dynamicEventDeclarations", SchemaBuilder.map(Schema.STRING_SCHEMA, DynamicEventDeclaration.schema()).name("dynamic_event_declarations_field").schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<DynamicEventDeclarations> serde = new ConnectSerde<DynamicEventDeclarations>(schema, false, DynamicEventDeclarations.class, DynamicEventDeclarations::pack, DynamicEventDeclarations::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<DynamicEventDeclarations> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private Map<String, DynamicEventDeclaration> dynamicEventDeclarations;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public Map<String, DynamicEventDeclaration> getDynamicEventDeclarations() { return dynamicEventDeclarations; }

  /*****************************************
  *
  *  constructor
   * @throws GUIManagerException 
  *
  *****************************************/

  public DynamicEventDeclarations(JSONObject guiManagedObjectJson, Map<String, DynamicEventDeclaration> dynamicEventDeclarations, int tenantID)
  {
    super(guiManagedObjectJson, 1, tenantID);
    this.dynamicEventDeclarations = dynamicEventDeclarations;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public DynamicEventDeclarations(SchemaAndValue schemaAndValue, Map<String, DynamicEventDeclaration> dynamicEventDeclarations)
  {
    super(schemaAndValue);
    this.dynamicEventDeclarations = dynamicEventDeclarations;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    DynamicEventDeclarations dynamicEventDeclarations = (DynamicEventDeclarations) value;
    Struct struct = new Struct(schema);
    packCommon(struct, dynamicEventDeclarations);
    struct.put("dynamicEventDeclarations", packDynamicEventDeclarations(dynamicEventDeclarations.getDynamicEventDeclarations()));
    return struct;
  }
  
  /****************************************
  *
  *  packDynamicEventDeclarations
  *
  ****************************************/

  private static Map<String,Object> packDynamicEventDeclarations(Map<String, DynamicEventDeclaration> dynamicEventDeclarations)
  {
    Map<String,Object> result = new LinkedHashMap<String,Object>();
    for (String dynamicEventDeclarationName : dynamicEventDeclarations.keySet())
      {
        DynamicEventDeclaration dynamicEventDeclaration = dynamicEventDeclarations.get(dynamicEventDeclarationName);
        result.put(dynamicEventDeclarationName, DynamicEventDeclaration.pack(dynamicEventDeclaration));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static DynamicEventDeclarations unpack(SchemaAndValue schemaAndValue)
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
    Map<String,DynamicEventDeclaration> dynamicEventDeclarations = unpackDynamicEventDeclarations(schema.field("dynamicEventDeclarations").schema(), (Map<String,Object>) valueStruct.get("dynamicEventDeclarations"));
    

    //
    //  return
    //

    return new DynamicEventDeclarations(schemaAndValue, dynamicEventDeclarations);
  }
  
  /*****************************************
  *
  *  unpackDynamicEventDeclarations
  *
  *****************************************/

  private static Map<String,DynamicEventDeclaration> unpackDynamicEventDeclarations(Schema schema, Map<String,Object> dynamicEventDeclarations)
  {
    Map<String,DynamicEventDeclaration> result = new LinkedHashMap<String,DynamicEventDeclaration>();
    for (String dynamicEventDeclarationName : dynamicEventDeclarations.keySet())
      {
        DynamicEventDeclaration dynamicEventDeclaration = DynamicEventDeclaration.unpack(new SchemaAndValue(schema.valueSchema(), dynamicEventDeclarations.get(dynamicEventDeclarationName)));
        result.put(dynamicEventDeclarationName, dynamicEventDeclaration);
      }
    return result;
  }
}
