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

import org.json.simple.JSONObject;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class OfferPrice
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
    schemaBuilder.name("offerPrice");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("amount", Schema.INT64_SCHEMA);
    schemaBuilder.field("supportedCurrencyID", Schema.STRING_SCHEMA);
    schemaBuilder.field("providerID", Schema.STRING_SCHEMA);
    schemaBuilder.field("paymentMeanID", Schema.STRING_SCHEMA);
    schema = schemaBuilder.optional().build();
  };

  //
  //  serde
  //

  private static ConnectSerde<OfferPrice> serde = new ConnectSerde<OfferPrice>(schema, false, OfferPrice.class, OfferPrice::pack, OfferPrice::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<OfferPrice> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private long amount;
  private String supportedCurrencyID;
  private String providerID;
  private String paymentMeanID;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public long getAmount() { return amount; };
  public String getSupportedCurrencyID() { return supportedCurrencyID; };
  public String getProviderID() { return providerID; };
  public String getPaymentMeanID() { return paymentMeanID; };
    
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public OfferPrice(SchemaAndValue schemaAndValue, long amount, String supportedCurrencyID, String providerID, String paymentMeanID)
  {
    this.amount = amount;
    this.supportedCurrencyID = supportedCurrencyID;
    this.providerID = providerID;
    this.paymentMeanID = paymentMeanID;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    OfferPrice offerPrice = (OfferPrice) value;
    Struct struct = new Struct(schema);
    struct.put("amount", offerPrice.getAmount());
    struct.put("supportedCurrencyID", offerPrice.getSupportedCurrencyID());
    struct.put("providerID", offerPrice.getProviderID());
    struct.put("paymentMeanID", offerPrice.getPaymentMeanID());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static OfferPrice unpack(SchemaAndValue schemaAndValue)
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
    long amount = valueStruct.getInt64("amount");
    String supportedCurrencyID = valueStruct.getString("supportedCurrencyID");
    String providerID = valueStruct.getString("providerID");
    String paymentMeanID = valueStruct.getString("paymentMeanID");
        
    //
    //  return
    //

    return new OfferPrice(schemaAndValue, amount, supportedCurrencyID, providerID, paymentMeanID);
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public OfferPrice(JSONObject jsonRoot) throws GUIManagerException
  {  
    this.amount = JSONUtilities.decodeLong(jsonRoot, "amount", true);
    this.supportedCurrencyID = JSONUtilities.decodeString(jsonRoot, "supportedCurrencyID", true);
    this.providerID = JSONUtilities.decodeString(jsonRoot, "providerID", true);
    this.paymentMeanID = JSONUtilities.decodeString(jsonRoot, "paymentMeanID", true);
  }
}
