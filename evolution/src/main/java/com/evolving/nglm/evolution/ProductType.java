/*****************************************************************************
*
*  ProductType.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import com.evolving.nglm.core.JSONUtilities;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

public class ProductType extends GUIManagedObject
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
    schemaBuilder.name("producttype");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("catalogCharacteristics", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<ProductType> serde = new ConnectSerde<ProductType>(schema, false, ProductType.class, ProductType::pack, ProductType::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<ProductType> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private List<String> catalogCharacteristics;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getProductTypeID() { return getGUIManagedObjectID(); }
  public List<String> getCatalogCharacteristics() { return catalogCharacteristics; }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public ProductType(SchemaAndValue schemaAndValue, List<String> catalogCharacteristics)
  {
    super(schemaAndValue);
    this.catalogCharacteristics = catalogCharacteristics;
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    ProductType productType = (ProductType) value;
    Struct struct = new Struct(schema);
    packCommon(struct, productType);
    struct.put("catalogCharacteristics", productType.getCatalogCharacteristics());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static ProductType unpack(SchemaAndValue schemaAndValue)
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
    List<String> catalogCharacteristics = (List<String>) valueStruct.get("catalogCharacteristics");
    
    //
    //  return
    //

    return new ProductType(schemaAndValue, catalogCharacteristics);
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ProductType(JSONObject jsonRoot, long epoch, GUIManagedObject existingProductTypeUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingProductTypeUnchecked != null) ? existingProductTypeUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingProductType
    *
    *****************************************/

    ProductType existingProductType = (existingProductTypeUnchecked != null && existingProductTypeUnchecked instanceof ProductType) ? (ProductType) existingProductTypeUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.catalogCharacteristics = decodeCatalogCharacteristics(JSONUtilities.decodeJSONArray(jsonRoot, "catalogCharacteristics", true));

    /*****************************************
    *
    *  validate
    *
    *****************************************/

    // none yet

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingProductType))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  decodeCatalogCharacteristics
  *
  *****************************************/

  private List<String> decodeCatalogCharacteristics(JSONArray jsonArray) throws GUIManagerException
  {
    List<String> catalogCharacteristics = new ArrayList<String>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject catalogCharacteristicJSON = (JSONObject) jsonArray.get(i);
        catalogCharacteristics.add(JSONUtilities.decodeString(catalogCharacteristicJSON, "catalogCharacteristicID", true));
      }
    return catalogCharacteristics;
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(ProductType existingProductType)
  {
    if (existingProductType != null && existingProductType.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingProductType.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(catalogCharacteristics, existingProductType.getCatalogCharacteristics());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
  
  /*****************************************
  *
  *  validate
  *
  *****************************************/

  public void validate(CatalogCharacteristicService catalogCharacteristicService, Date date) throws GUIManagerException
  {
    /*****************************************
    *
    *  validate catalog characteristics exist and are active
    *
    *****************************************/

    for (String catalogCharacteristicID : catalogCharacteristics)
      {
        CatalogCharacteristic catalogCharacteristic = catalogCharacteristicService.getActiveCatalogCharacteristic(catalogCharacteristicID, date);
        if (catalogCharacteristic == null) throw new GUIManagerException("unknown catalog characteristic", catalogCharacteristicID);
      }
  }
}
