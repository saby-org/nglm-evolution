/*****************************************************************************
*
*  Product.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SchemaUtilities;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;

import org.json.simple.JSONObject;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.Objects;
import java.util.Date;

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

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getProductID() { return getGUIManagedObjectID(); }
  public String getSupplierID() { return supplierID; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Product(SchemaAndValue schemaAndValue, String supplierID)
  {
    super(schemaAndValue);
    this.supplierID = supplierID;
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
    return struct;
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
    
    //
    //  return
    //

    return new Product(schemaAndValue, supplierID);
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public Product(JSONObject jsonRoot, long epoch, GUIManagedObject existingProductUnchecked) throws GUIManagerException
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
        return epochChanged;
      }
    else
      {
        return true;
      }
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
