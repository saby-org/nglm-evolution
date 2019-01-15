/*****************************************************************************
*
*  OfferProduct.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;

public class OfferProduct
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(OfferProduct.class);

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
    schemaBuilder.name("offer_product");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("productID", Schema.STRING_SCHEMA);
    schemaBuilder.field("quantity", Schema.OPTIONAL_INT32_SCHEMA);
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

  private String productID;
  private Integer quantity;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private OfferProduct(String productID, Integer quantity)
  {
    this.productID = productID;
    this.quantity = quantity;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  public OfferProduct(JSONObject jsonRoot) {
    this.productID = JSONUtilities.decodeString(jsonRoot, "productID", true);
    this.quantity = JSONUtilities.decodeInteger(jsonRoot, "quantity", false);
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getProductID() { return productID; }
  public Integer getQuantity() { return quantity; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<OfferProduct> serde()
  {
    return new ConnectSerde<OfferProduct>(schema, false, OfferProduct.class, OfferProduct::pack, OfferProduct::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    OfferProduct offerProduct = (OfferProduct) value;
    Struct struct = new Struct(schema);
    struct.put("productID", offerProduct.getProductID());
    struct.put("quantity", offerProduct.getQuantity());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static OfferProduct unpack(SchemaAndValue schemaAndValue)
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
    String productID = valueStruct.getString("productID");
    Integer quantity = valueStruct.getInt32("quantity");
    
    //
    //  return
    //

    return new OfferProduct(productID, quantity);
  }

  /*****************************************
  *
  *  getJSONRepresentation
  *
  *****************************************/

  public JSONObject getJSONRepresentation(){
    Map<String, Object> data = new HashMap<String, Object>();
    data.put("productID", this.getProductID());
    data.put("quantity", this.getQuantity());
    return JSONUtilities.encodeObject(data);
  }
  
  /*****************************************
  *
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof OfferProduct)
      {
        OfferProduct offerProduct = (OfferProduct) obj;
        result = true;
        result = result && Objects.equals(productID, offerProduct.getProductID());
        result = result && Objects.equals(quantity, offerProduct.getQuantity());
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
    return productID.hashCode();
  }
}
