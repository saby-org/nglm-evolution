/*****************************************************************************
*
*  Product.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import com.evolving.nglm.core.JSONUtilities;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.Objects;
import java.util.Set;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

public class Product extends GUIManagedObject
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
    schemaBuilder.name("product");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("supplierID", Schema.STRING_SCHEMA);
    schemaBuilder.field("productTypes", SchemaBuilder.array(ProductTypeInstance.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<Product> serde = new ConnectSerde<Product>(schema, false, Product.class, Product::pack, Product::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<Product> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String supplierID;
  private Set<ProductTypeInstance> productTypes; 

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getProductID() { return getGUIManagedObjectID(); }
  public String getSupplierID() { return supplierID; }
  public Set<ProductTypeInstance> getProductTypes() { return productTypes;  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Product(SchemaAndValue schemaAndValue, String supplierID, Set<ProductTypeInstance> productTypes)
  {
    super(schemaAndValue);
    this.supplierID = supplierID;
    this.productTypes = productTypes;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    Product product = (Product) value;
    Struct struct = new Struct(schema);
    packCommon(struct, product);
    struct.put("supplierID", product.getSupplierID());
    struct.put("productTypes", packProductTypes(product.getProductTypes()));
    return struct;
  }
  
  /****************************************
  *
  *  packProductTypes
  *
  ****************************************/

  private static List<Object> packProductTypes(Set<ProductTypeInstance> productTypes)
  {
    List<Object> result = new ArrayList<Object>();
    for (ProductTypeInstance productType : productTypes)
      {
        result.add(ProductTypeInstance.pack(productType));
      }
    return result;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static Product unpack(SchemaAndValue schemaAndValue)
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
    String supplierID = (String) valueStruct.get("supplierID");
    Set<ProductTypeInstance> productTypes = unpackProductTypes(schema.field("productTypes").schema(), valueStruct.get("productTypes"));
    
    //
    //  return
    //

    return new Product(schemaAndValue, supplierID, productTypes);
  }
  
  /*****************************************
  *
  *  unpackProductTypes
  *
  *****************************************/

  private static Set<ProductTypeInstance> unpackProductTypes(Schema schema, Object value)
  {
    //
    //  get schema for ProductType
    //

    Schema productTypeSchema = schema.valueSchema();

    //
    //  unpack
    //

    Set<ProductTypeInstance> result = new HashSet<ProductTypeInstance>();
    List<Object> valueArray = (List<Object>) value;
    for (Object productType : valueArray)
      {
        result.add(ProductTypeInstance.unpack(new SchemaAndValue(productTypeSchema, productType)));
      }

    //
    //  return
    //

    return result;
  }


  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public Product(JSONObject jsonRoot, long epoch, GUIManagedObject existingProductUnchecked, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingProductUnchecked != null) ? existingProductUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingProduct
    *
    *****************************************/

    Product existingProduct = (existingProductUnchecked != null && existingProductUnchecked instanceof Product) ? (Product) existingProductUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.supplierID = JSONUtilities.decodeString(jsonRoot, "supplierID", true);
    this.productTypes = decodeProductTypes(JSONUtilities.decodeJSONArray(jsonRoot, "productTypes", true), catalogCharacteristicService);

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

    if (epochChanged(existingProduct))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(Product existingProduct)
  {
    if (existingProduct != null && existingProduct.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingProduct.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(supplierID, existingProduct.getSupplierID());
        epochChanged = epochChanged || ! Objects.equals(productTypes, existingProduct.getProductTypes());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
  
  /*****************************************
  *
  *  decodeProductTypes
  *
  *****************************************/

  private Set<ProductTypeInstance> decodeProductTypes(JSONArray jsonArray, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
  {
    Set<ProductTypeInstance> result = new HashSet<ProductTypeInstance>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new ProductTypeInstance((JSONObject) jsonArray.get(i)));
          }
      }
    return result;
  }
  
  /*****************************************
  *
  *  validateSuppliers
  *
  *****************************************/

  public void validateSupplier(SupplierService supplierService, Date date) throws GUIManagerException
  {
    /*****************************************
    *
    *  retrieve supplier
    *
    *****************************************/

    Supplier supplier = supplierService.getActiveSupplier(supplierID, date);

    /*****************************************
    *
    *  validate the supplier exists and is active
    *
    *****************************************/

    if (supplier == null) throw new GUIManagerException("unknown supplier", supplierID);
  }
}
