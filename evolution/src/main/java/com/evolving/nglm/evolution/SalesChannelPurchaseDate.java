package com.evolving.nglm.evolution;

import java.util.Date;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

public class SalesChannelPurchaseDate
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
    schemaBuilder.name("saleschannel_purchasedate");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(0));
    schemaBuilder.field("salesChannelID", Schema.STRING_SCHEMA);
    schemaBuilder.field("purchaseDate", Timestamp.builder().schema());
    schema = schemaBuilder.build();
  };
  
  public static Schema schema() { return schema; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String salesChannelID;
  private Date purchaseDate;
  
  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  public SalesChannelPurchaseDate(String salesChannelID, Date purchaseDate)
  {
    this.salesChannelID = salesChannelID;
    this.purchaseDate = purchaseDate;
  }
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getSalesChannelID() { return salesChannelID; }
  public Date getPurchaseDate() { return purchaseDate; }
  
  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<SalesChannelPurchaseDate> serde()
  {
    return new ConnectSerde<SalesChannelPurchaseDate>(schema, false, SalesChannelPurchaseDate.class, SalesChannelPurchaseDate::pack, SalesChannelPurchaseDate::unpack);
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    SalesChannelPurchaseDate salesChannelPurchaseDate = (SalesChannelPurchaseDate) value;
    Struct struct = new Struct(schema);
    struct.put("salesChannelID", salesChannelPurchaseDate.getSalesChannelID());
    struct.put("purchaseDate", salesChannelPurchaseDate.getPurchaseDate());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static SalesChannelPurchaseDate unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;
    Struct valueStruct = (Struct) value;

    String salesChannelID = valueStruct.getString("salesChannelID");
    Date purchaseDate = (Date) valueStruct.get("purchaseDate");

    //
    //  construct
    //

    return new SalesChannelPurchaseDate(salesChannelID, purchaseDate);
  }

}
