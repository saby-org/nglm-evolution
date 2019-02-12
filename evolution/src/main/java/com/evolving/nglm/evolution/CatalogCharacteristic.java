/*****************************************************************************
*
*  CatalogCharacteristic.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("dataType", Schema.STRING_SCHEMA);
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

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getCatalogCharacteristicID() { return getGUIManagedObjectID(); }
  public String getCatalogCharacteristicName() { return getGUIManagedObjectName(); }
  public CriterionDataType getDataType() { return dataType; }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public CatalogCharacteristic(SchemaAndValue schemaAndValue, CriterionDataType dataType)
  {
    super(schemaAndValue);
    this.dataType = dataType;
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
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static CatalogCharacteristic unpack(SchemaAndValue schemaAndValue)
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
    CriterionDataType dataType = CriterionDataType.fromExternalRepresentation((String) valueStruct.get("dataType"));
    
    //
    //  return
    //

    return new CatalogCharacteristic(schemaAndValue, dataType);
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

    this.dataType = CriterionDataType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "dataType", true));

    /*****************************************
    *
    *  validate
    *
    *****************************************/

    if (getRawEffectiveStartDate() != null) throw new GUIManagerException("unsupported start date", JSONUtilities.decodeString(jsonRoot, "effectiveStartDate", false));
    if (getRawEffectiveEndDate() != null) throw new GUIManagerException("unsupported end date", JSONUtilities.decodeString(jsonRoot, "effectiveEndDate", false));

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
