/*****************************************************************************
*
*  ProductTypeInstance.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class ProductTypeInstance
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(ProductTypeInstance.class);

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
    //
    //  schema
    //
    
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("product_type");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("productTypeID", Schema.STRING_SCHEMA);
    schemaBuilder.field("catalogCharacteristics", SchemaBuilder.array(CatalogCharacteristicInstance.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  accessor
  //

  public static Schema schema() { return schema; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String productTypeID;
  private Set<CatalogCharacteristicInstance> catalogCharacteristics;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private ProductTypeInstance(String productTypeID, Set<CatalogCharacteristicInstance> catalogCharacteristics)
  {
    this.productTypeID = productTypeID;
    this.catalogCharacteristics = catalogCharacteristics;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  ProductTypeInstance(JSONObject jsonRoot, CatalogCharacteristicService catalogCharacteristicService, int tenantID) throws GUIManagerException
  {
    this.productTypeID = JSONUtilities.decodeString(jsonRoot, "productTypeID", true);
    this.catalogCharacteristics = decodeCatalogCharacteristics(JSONUtilities.decodeJSONArray(jsonRoot, "catalogCharacteristics", false), catalogCharacteristicService, tenantID);
  }

  /*****************************************
  *
  *  decodeProductCatalogCharacteristics
  *
  *****************************************/

  private Set<CatalogCharacteristicInstance> decodeCatalogCharacteristics(JSONArray jsonArray, CatalogCharacteristicService catalogCharacteristicService, int tenantID) throws GUIManagerException
  {
    Set<CatalogCharacteristicInstance> result = new HashSet<CatalogCharacteristicInstance>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new CatalogCharacteristicInstance((JSONObject) jsonArray.get(i), catalogCharacteristicService, tenantID));
          }
      }
    return result;
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getProductTypeID() { return productTypeID; }
  public Set<CatalogCharacteristicInstance> getCatalogCharacteristics() { return catalogCharacteristics; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<ProductTypeInstance> serde()
  {
    return new ConnectSerde<ProductTypeInstance>(schema, false, ProductTypeInstance.class, ProductTypeInstance::pack, ProductTypeInstance::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    ProductTypeInstance productType = (ProductTypeInstance) value;
    Struct struct = new Struct(schema);
    struct.put("productTypeID", productType.getProductTypeID());
    struct.put("catalogCharacteristics", packCatalogCharacteristics(productType.getCatalogCharacteristics()));
    return struct;
  }

  /****************************************
  *
  *  packCatalogCharacteristics
  *
  ****************************************/

  private static List<Object> packCatalogCharacteristics(Set<CatalogCharacteristicInstance> catalogCharacteristics)
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
  *  unpack
  *
  *****************************************/

  public static ProductTypeInstance unpack(SchemaAndValue schemaAndValue)
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
    String productTypeID = valueStruct.getString("productTypeID");
    Set<CatalogCharacteristicInstance> catalogCharacteristics = unpackProductCatalogCharacteristics(schema.field("catalogCharacteristics").schema(), valueStruct.get("catalogCharacteristics"));
    
    //
    //  return
    //

    return new ProductTypeInstance(productTypeID, catalogCharacteristics);
  }

  /*****************************************
  *
  *  unpackProductCatalogCharacteristics
  *
  *****************************************/

  private static Set<CatalogCharacteristicInstance> unpackProductCatalogCharacteristics(Schema schema, Object value)
  {
    //
    //  get schema for ProductCatalogCharacteristic
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
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof ProductTypeInstance)
      {
        ProductTypeInstance productType = (ProductTypeInstance) obj;
        result = true;
        result = result && Objects.equals(productTypeID, productType.getProductTypeID());
        result = result && Objects.equals(catalogCharacteristics, productType.getCatalogCharacteristics());
      }
    return result;
  }

  /*****************************************
  *
  *  hashCode
  *
  *****************************************/

  public int hashCode()
  {
    return productTypeID.hashCode();
  }
}
