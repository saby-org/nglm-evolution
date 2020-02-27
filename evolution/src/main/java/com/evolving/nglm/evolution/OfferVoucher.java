package com.evolving.nglm.evolution;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import org.apache.kafka.connect.data.*;
import org.json.simple.JSONObject;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class OfferVoucher {

  private static Schema schema = null;
  static {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("offer_voucher");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("voucherID", Schema.STRING_SCHEMA);
    schemaBuilder.field("quantity", Schema.OPTIONAL_INT32_SCHEMA);
    schema = schemaBuilder.build();
  }

  public static Schema schema() { return schema; }

  public static Object pack(Object value) {
    OfferVoucher offerVoucher = (OfferVoucher) value;
    Struct struct = new Struct(schema);
    struct.put("voucherID", offerVoucher.getVoucherID());
    struct.put("quantity", offerVoucher.getQuantity());
    return struct;
  }

  public static OfferVoucher unpack(SchemaAndValue schemaAndValue) {
    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;
    Struct valueStruct = (Struct) value;
    String voucherID = valueStruct.getString("voucherID");
    Integer quantity = valueStruct.getInt32("quantity");
    return new OfferVoucher(voucherID, quantity);
  }

  private String voucherID;
  private Integer quantity;

  //field for purchase transaction only, never stored and show for Offer conf
  private String fileID;
  private String voucherCode;
  private Date voucherExpiryDate;

  private OfferVoucher(String voucherID, Integer quantity) {
    this.voucherID = voucherID;
    this.quantity = quantity;
  }

  public OfferVoucher(JSONObject jsonRoot) {
    this.voucherID = JSONUtilities.decodeString(jsonRoot, "voucherID", true);
    this.fileID = JSONUtilities.decodeString(jsonRoot, "fileID", false);
    this.quantity = JSONUtilities.decodeInteger(jsonRoot, "quantity", false);
    this.voucherCode = JSONUtilities.decodeString(jsonRoot, "voucherCode", false);
    this.voucherExpiryDate = JSONUtilities.decodeDate(jsonRoot,"voucherExpiryDate",false);
  }

  public OfferVoucher(OfferVoucher offerVoucher){
    this.voucherID=offerVoucher.getVoucherID();
    this.fileID = offerVoucher.getFileID();
    this.quantity=offerVoucher.getQuantity();
    this.voucherCode=offerVoucher.getVoucherCode();
    this.voucherExpiryDate=offerVoucher.getVoucherExpiryDate();
  }

  public String getVoucherID() { return voucherID; }
  public Integer getQuantity() { return quantity; }
  public void setQuantity(Integer quantity) { this.quantity=quantity; }

  //field for purchase transaction only, never stored and show for Offer conf
  public String getFileID() { return fileID; }
  public String getVoucherCode() { return voucherCode; }
  public Date getVoucherExpiryDate() { return voucherExpiryDate; }
  public void setFileID(String fileID) { this.fileID=fileID; }
  public void setVoucherCode(String voucherCode) { this.voucherCode=voucherCode; }
  public void setVoucherExpiryDate(Date voucherExpiryDate) { this.voucherExpiryDate=voucherExpiryDate; }


  public JSONObject getJSONRepresentationForPurchaseTransaction(){
    Map<String, Object> data = new HashMap<String, Object>();
    data.put("voucherID", this.getVoucherID());
    data.put("fileID", this.getFileID());
    data.put("quantity", this.getQuantity());
    data.put("voucherCode", this.getVoucherCode());
    data.put("voucherExpiryDate", this.getVoucherExpiryDate());
    return JSONUtilities.encodeObject(data);
  }
  
  public boolean equals(Object obj) {
    boolean result = false;
    if (obj instanceof OfferVoucher) {
        OfferVoucher offerVoucher = (OfferVoucher) obj;
        result = true;
        result = result && Objects.equals(voucherID, offerVoucher.getVoucherID());
        result = result && Objects.equals(quantity, offerVoucher.getQuantity());
    }
    return result;
  }

  public int hashCode()
  {
    return voucherID.hashCode();
  }

}
