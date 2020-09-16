/*****************************************************************************
*
*  CatalogCharacteristic.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SchemaUtilities;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import org.json.simple.JSONObject;

import java.util.Objects;

@GUIDependencyDef(objectType = "CatalogCharacteristic", serviceClass = CatalogCharacteristicService.class, dependencies = {  })
public class CatalogCharacteristic extends GUIManagedObject
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
    schemaBuilder.name("catalogcharacteristic");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),2));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("dataType", Schema.STRING_SCHEMA);
    schemaBuilder.field("catalogCharacteristicUnitID", SchemaBuilder.string().optional().defaultValue(null).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<CatalogCharacteristic> serde = new ConnectSerde<CatalogCharacteristic>(schema, false, CatalogCharacteristic.class, CatalogCharacteristic::pack, CatalogCharacteristic::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<CatalogCharacteristic> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private CriterionDataType dataType;
  private String catalogCharacteristicUnitID;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getCatalogCharacteristicID() { return getGUIManagedObjectID(); }
  public String getCatalogCharacteristicName() { return getGUIManagedObjectName(); }
  public CriterionDataType getDataType() { return dataType; }
  public String getCatalogCharacteristicUnitID() { return catalogCharacteristicUnitID; }

  //
  //  setters
  //

  private void setCatalogCharacteristicUnitID(String catalogCharacteristicUnitID) { this.catalogCharacteristicUnitID = catalogCharacteristicUnitID; }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public CatalogCharacteristic(SchemaAndValue schemaAndValue, CriterionDataType dataType, String catalogCharacteristicUnitID)
  {
    super(schemaAndValue);
    this.dataType = dataType;
    this.catalogCharacteristicUnitID = catalogCharacteristicUnitID;
  }
                
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    CatalogCharacteristic catalogCharacteristic = (CatalogCharacteristic) value;
    Struct struct = new Struct(schema);
    packCommon(struct, catalogCharacteristic);
    struct.put("dataType", catalogCharacteristic.getDataType().getExternalRepresentation());
    struct.put("catalogCharacteristicUnitID", catalogCharacteristic.getCatalogCharacteristicUnitID());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static CatalogCharacteristic unpack(SchemaAndValue schemaAndValue)
  {
    /****************************************
    *
    *  data
    *
    ****************************************/

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    /****************************************
    *
    *  unpack
    *
    ****************************************/

    Struct valueStruct = (Struct) value;
    CriterionDataType dataType = CriterionDataType.fromExternalRepresentation((String) valueStruct.get("dataType"));
    String catalogCharacteristicUnitID = (schemaVersion >= 2) ? (String) valueStruct.get("catalogCharacteristicUnitID") : null;
    
    //
    //  result
    //

    CatalogCharacteristic result = new CatalogCharacteristic(schemaAndValue, dataType, catalogCharacteristicUnitID);

    //
    //  version 1 compatibility
    //
    
    if (schemaVersion < 2)
      {
        result.setCatalogCharacteristicUnitID(JSONUtilities.decodeString(result.getJSONRepresentation(), "unit", false));
      }

    /****************************************
    *
    *  validate
    *
    ****************************************/

    if (result.getCatalogCharacteristicUnitID() != null && ! Deployment.getCatalogCharacteristicUnits().containsKey(result.getCatalogCharacteristicUnitID()))
      {
        throw new SerializationException("unknown unit for catalog characteristic " + result.getCatalogCharacteristicID());
      }
    
    /****************************************
    *
    *  return
    *
    ****************************************/

    return result;
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public CatalogCharacteristic(JSONObject jsonRoot, long epoch, GUIManagedObject existingCatalogCharacteristicUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingCatalogCharacteristicUnchecked != null) ? existingCatalogCharacteristicUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingCatalogCharacteristic
    *
    *****************************************/

    CatalogCharacteristic existingCatalogCharacteristic = (existingCatalogCharacteristicUnchecked != null && existingCatalogCharacteristicUnchecked instanceof CatalogCharacteristic) ? (CatalogCharacteristic) existingCatalogCharacteristicUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    CriterionDataType baseDataType = CriterionDataType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "dataType", true));
    boolean allowMultipleValues = JSONUtilities.decodeBoolean(jsonRoot, "allowMultipleValues", Boolean.FALSE);
    switch (baseDataType)
      {
        case IntegerCriterion:
          this.dataType = allowMultipleValues ? CriterionDataType.IntegerSetCriterion : CriterionDataType.IntegerCriterion;
          break;

        case StringCriterion:
          this.dataType = allowMultipleValues ? CriterionDataType.StringSetCriterion : CriterionDataType.StringCriterion;
          break;
          
        case DoubleCriterion:
            this.dataType = allowMultipleValues ? CriterionDataType.DoubleSetCriterion : CriterionDataType.DoubleCriterion;
            break;

        default:
          this.dataType = baseDataType;
          break;
      }
    String catalogCharacteristicUnitID = JSONUtilities.decodeString(jsonRoot, "unit", false);

    /*****************************************
    *
    *  validate
    *
    *****************************************/

    if (getRawEffectiveStartDate() != null) throw new GUIManagerException("unsupported start date", JSONUtilities.decodeString(jsonRoot, "effectiveStartDate", false));
    if (getRawEffectiveEndDate() != null) throw new GUIManagerException("unsupported end date", JSONUtilities.decodeString(jsonRoot, "effectiveEndDate", false));
    if (catalogCharacteristicUnitID != null && ! Deployment.getCatalogCharacteristicUnits().containsKey(catalogCharacteristicUnitID)) throw new GUIManagerException("unknown unit", catalogCharacteristicUnitID);

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingCatalogCharacteristic))
      {
        this.setEpoch(epoch);
      }
  }
  
  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(CatalogCharacteristic existingCatalogCharacteristic)
  {
    if (existingCatalogCharacteristic != null && existingCatalogCharacteristic.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingCatalogCharacteristic.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(dataType, existingCatalogCharacteristic.getDataType());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
}
