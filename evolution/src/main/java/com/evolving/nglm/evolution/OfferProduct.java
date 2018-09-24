/*****************************************************************************
*
*  OfferProduct.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.ServerRuntimeException;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import com.rii.utilities.SystemTime;

import java.nio.charset.StandardCharsets;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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

  OfferProduct(JSONObject jsonRoot) throws GUIManagerException
  {
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
