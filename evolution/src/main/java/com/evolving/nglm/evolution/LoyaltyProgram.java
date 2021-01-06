/*****************************************************************************
 *
 *  LoyaltyProgram.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public abstract class LoyaltyProgram extends GUIManagedObject
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  LoyaltyProgramType
  //

  public enum LoyaltyProgramType
  {
    POINTS("POINTS"),
//    BADGES("BADGES"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private LoyaltyProgramType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static LoyaltyProgramType fromExternalRepresentation(String externalRepresentation) { for (LoyaltyProgramType enumeratedValue : LoyaltyProgramType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  //
  //  LoyaltyProgramOperation
  //

  public enum LoyaltyProgramOperation
  {
    Optin("opt-in"),
    Optout("opt-out"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private LoyaltyProgramOperation(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static LoyaltyProgramOperation fromExternalRepresentation(String externalRepresentation) { for (LoyaltyProgramOperation enumeratedValue : LoyaltyProgramOperation.values()) { if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  /*****************************************
   *
   *  schema
   *
   *****************************************/

  //
  //  schema
  //

  private static Schema commonSchema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("loyalty_program");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(GUIManagedObject.commonSchema().version(),1));
    for (Field field : GUIManagedObject.commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("loyaltyProgramType", Schema.STRING_SCHEMA);
    schemaBuilder.field("loyaltyProgramDescription", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("characteristics", SchemaBuilder.array(CatalogCharacteristicInstance.schema()).optional().schema());
    commonSchema = schemaBuilder.build();
  };

  //
  //  accessor
  //

  public static Schema commonSchema() { return commonSchema; }

  /*****************************************
   *
   *  data
   *
   *****************************************/

  private LoyaltyProgramType loyaltyProgramType = null;
  private String loyaltyProgramDescription = null;
  private Set<CatalogCharacteristicInstance> characteristics = null;

  /*****************************************
   *
   *  accessors
   *
   *****************************************/

  public String getLoyaltyProgramID() { return getGUIManagedObjectID(); }
  public String getLoyaltyProgramName() { return getGUIManagedObjectName(); }
  public String getLoyaltyProgramDisplay() { return getGUIManagedObjectDisplay(); }
  public LoyaltyProgramType getLoyaltyProgramType() { return loyaltyProgramType; }
  public String getLoyaltyProgramDescription() { return loyaltyProgramDescription; }
  public Set<CatalogCharacteristicInstance> getCharacteristics() { return characteristics; }

  /*****************************************
   *
   *  constructor -- unpack
   *
   *****************************************/

  public LoyaltyProgram(SchemaAndValue schemaAndValue)
  {
    //
    //  superclass
    //
    
    super(schemaAndValue);
    
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
    LoyaltyProgramType loyaltyProgramType = LoyaltyProgramType.fromExternalRepresentation(valueStruct.getString("loyaltyProgramType"));
    String loyaltyProgramDescription = valueStruct.getString("loyaltyProgramDescription");
    Set<CatalogCharacteristicInstance> characteristics = unpackLoyaltyProgramCharacteristics(schema.field("characteristics").schema(), valueStruct.get("characteristics"));

    //
    //  return
    //
    
    this.loyaltyProgramType = loyaltyProgramType;
    this.loyaltyProgramDescription = loyaltyProgramDescription;
    this.characteristics = characteristics;
  }

  /*****************************************
  *
  *  unpackLoyaltyProgramCharacteristics
  *
  *****************************************/

  private static Set<CatalogCharacteristicInstance> unpackLoyaltyProgramCharacteristics(Schema schema, Object value)
  {
    //
    //  get schema for LoyaltyProgramCharacteristics
    //

    Schema propertySchema = schema.valueSchema();
    
    //
    //  unpack
    //

    Set<CatalogCharacteristicInstance> result = new HashSet<CatalogCharacteristicInstance>();
    List<Object> valueArray = (List<Object>) value;
    for (Object property : valueArray)
      {
        result.add(CatalogCharacteristicInstance.unpack(new SchemaAndValue(propertySchema, property)));
      }

    //
    //  return
    //

    return result;
  }
  
  /*****************************************
   *
   *  packCommon
   *
   *****************************************/

  public static Object packCommon(Struct struct, LoyaltyProgram loyaltyProgram)
  {
    GUIManagedObject.packCommon(struct, loyaltyProgram);
    struct.put("loyaltyProgramType", loyaltyProgram.getLoyaltyProgramType().getExternalRepresentation());
    struct.put("loyaltyProgramDescription", loyaltyProgram.getLoyaltyProgramDescription());
    struct.put("characteristics", packLoyaltyProgramCharacteristics(loyaltyProgram.getCharacteristics()));
    return struct;
  }
  
  /****************************************
  *
  *  packCatalogCharacteristics
  *
  ****************************************/

  private static List<Object> packLoyaltyProgramCharacteristics(Set<CatalogCharacteristicInstance> catalogCharacteristics)
  {
    List<Object> result = new ArrayList<Object>();
    for (CatalogCharacteristicInstance catalogCharacteristic : catalogCharacteristics)
      {
        result.add(CatalogCharacteristicInstance.pack(catalogCharacteristic));
      }
    return result;
  }
  
  /*****************************************
   *
   *  constructor -- JSON
   *
   *****************************************/

  public LoyaltyProgram(JSONObject jsonRoot, long epoch, GUIManagedObject existingLoyaltyProgramUnchecked, CatalogCharacteristicService catalogCharacteristicService, int tenantID) throws GUIManagerException
  {
    /*****************************************
     *
     *  super
     *
     *****************************************/

    super(jsonRoot, (existingLoyaltyProgramUnchecked != null) ? existingLoyaltyProgramUnchecked.getEpoch() : epoch, tenantID);

    /*****************************************
     *
     *  attributes
     *
     *****************************************/
    
    this.loyaltyProgramType = LoyaltyProgramType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "loyaltyProgramType", true));
    this.loyaltyProgramDescription = JSONUtilities.decodeString(jsonRoot, "loyaltyProgramDescription", false);
    this.characteristics = decodeLoyaltyProgramCharacteristics(JSONUtilities.decodeJSONArray(jsonRoot, "characteristics", false), catalogCharacteristicService);
    
  }
  
  /*****************************************
  *
  *  decodeCatalogCharacteristics
  *
  *****************************************/

  private Set<CatalogCharacteristicInstance> decodeLoyaltyProgramCharacteristics(JSONArray jsonArray, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
  {
    Set<CatalogCharacteristicInstance> result = new HashSet<CatalogCharacteristicInstance>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new CatalogCharacteristicInstance((JSONObject) jsonArray.get(i), catalogCharacteristicService));
          }
      }
    return result;
  }

  /*****************************************
  *
  *  validation
  *
  *****************************************/
  
  public abstract boolean validate() throws GUIManagerException;
  

  
}
