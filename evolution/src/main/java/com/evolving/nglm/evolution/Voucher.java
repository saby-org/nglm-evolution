/*****************************************************************************
*
*  Voucher.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class Voucher extends GUIManagedObject
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
    schemaBuilder.name("voucher");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("description", Schema.STRING_SCHEMA);
    schemaBuilder.field("supplierID", Schema.STRING_SCHEMA);
    schemaBuilder.field("voucherTypeId", Schema.STRING_SCHEMA);
    schemaBuilder.field("unitaryCost", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("recommendedPrice", Schema.OPTIONAL_INT32_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<Voucher> serde = new ConnectSerde<Voucher>(schema, false, Voucher.class, Voucher::pack, Voucher::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<Voucher> serde() { return serde; }

 /*****************************************
  *
  *  data
  *
  *****************************************/

  private String description;
  private String supplierID;
  private String voucherTypeId;
  private Integer unitaryCost;
  private Integer recommendedPrice;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getVoucherID() { return getGUIManagedObjectID(); }
  public String getVoucherName() { return getGUIManagedObjectName(); }
  public String getDescription() { return description; }
  public String getSupplierID() { return supplierID; }
  public String getVoucherTypeId() { return voucherTypeId; }
  public Integer getUnitaryCost() { return unitaryCost; }
  public Integer getRecommendedPrice() { return recommendedPrice; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Voucher(SchemaAndValue schemaAndValue, String description, String supplierID, String voucherTypeId, Integer unitaryCost, Integer recommendedPrice)
  {
    super(schemaAndValue);
    this.description = description;
    this.supplierID = supplierID;
    this.voucherTypeId = voucherTypeId;
    this.unitaryCost = unitaryCost;
    this.recommendedPrice = recommendedPrice;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  private Voucher(Voucher voucher)
  {
    super(voucher.getJSONRepresentation(), voucher.getEpoch());
    this.description = voucher.getDescription();
    this.supplierID = voucher.getSupplierID();
    this.voucherTypeId = voucher.getVoucherTypeId();
    this.unitaryCost = voucher.getUnitaryCost();
    this.recommendedPrice = voucher.getRecommendedPrice();
  }

  /*****************************************
  *
  *  copy
  *
  *****************************************/

  public Voucher copy()
  {
    return new Voucher(this);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    Voucher voucher = (Voucher) value;
    Struct struct = new Struct(schema);
    packCommon(struct, voucher);
    struct.put("description", voucher.getDescription());
    struct.put("supplierID", voucher.getSupplierID());
    struct.put("voucherTypeId", voucher.getVoucherTypeId());
    struct.put("unitaryCost", voucher.getUnitaryCost());
    struct.put("recommendedPrice", voucher.getRecommendedPrice());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static Voucher unpack(SchemaAndValue schemaAndValue)
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
    String description = valueStruct.getString("description");
    String supplierID = valueStruct.getString("supplierID");
    String voucherTypeId = valueStruct.getString("voucherTypeId");
    Integer unitaryCost = valueStruct.getInt32("unitaryCost");
    Integer recommendedPrice = valueStruct.getInt32("recommendedPrice");

    //
    //  return
    //

    return new Voucher(schemaAndValue, description, supplierID, voucherTypeId, unitaryCost, recommendedPrice);
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public Voucher(JSONObject jsonRoot, long epoch, GUIManagedObject existingVoucherUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingVoucherUnchecked != null) ? existingVoucherUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingVoucher
    *
    *****************************************/

    Voucher existingVoucher = (existingVoucherUnchecked != null && existingVoucherUnchecked instanceof Voucher) ? (Voucher) existingVoucherUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.description = JSONUtilities.decodeString(jsonRoot, "description", true);
    this.supplierID = JSONUtilities.decodeString(jsonRoot, "supplierID", true);
    this.voucherTypeId = JSONUtilities.decodeString(jsonRoot, "voucherTypeId", true);
    this.unitaryCost = JSONUtilities.decodeInteger(jsonRoot, "unitaryCost", false);
    this.recommendedPrice = JSONUtilities.decodeInteger(jsonRoot, "recommendedPrice", false);

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingVoucher))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(Voucher existingVoucher)
  {
    if (existingVoucher != null && existingVoucher.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingVoucher.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(description, existingVoucher.getDescription());
        epochChanged = epochChanged || ! Objects.equals(supplierID, existingVoucher.getSupplierID());
        epochChanged = epochChanged || ! Objects.equals(voucherTypeId, existingVoucher.getVoucherTypeId());
        epochChanged = epochChanged || ! Objects.equals(unitaryCost, existingVoucher.getUnitaryCost());
        epochChanged = epochChanged || ! Objects.equals(recommendedPrice, existingVoucher.getRecommendedPrice());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }

}
