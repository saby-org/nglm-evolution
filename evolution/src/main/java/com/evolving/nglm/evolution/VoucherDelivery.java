package com.evolving.nglm.evolution;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import org.apache.kafka.connect.data.*;
import org.json.simple.JSONObject;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class VoucherDelivery  {

  public enum VoucherStatus {
    Available("available"),
    Delivered("delivered"),
    Redeemed("redeemed"),
    Expired("expired"),
    Cancelled("cancelled"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private VoucherStatus(String externalRepresentation) { this.externalRepresentation = externalRepresentation;}
    public String getExternalRepresentation() { return externalRepresentation; }
    public static VoucherStatus fromExternalRepresentation(String externalRepresentation) { for (VoucherStatus enumeratedValue : VoucherStatus.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  private static Schema schema = null;
  static {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("voucher_delivered");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("voucherID", Schema.STRING_SCHEMA);
    schemaBuilder.field("fileID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("voucherCode", Schema.STRING_SCHEMA);
    schemaBuilder.field("voucherStatus", Schema.STRING_SCHEMA);
    schemaBuilder.field("voucherExpiryDate", Timestamp.builder().optional().schema());
    schema = schemaBuilder.build();
  }

  public static Schema schema() { return schema; }

  private String voucherID;
  private String fileID;
  private String voucherCode;
  private VoucherStatus voucherStatus;
  private Date voucherExpiryDate;

  public String getVoucherID() { return voucherID; }
  public String getFileID() {return fileID; }
  public String getVoucherCode() { return voucherCode; }
  public VoucherStatus getVoucherStatus() { return voucherStatus; }
  public Date getVoucherExpiryDate() { return voucherExpiryDate; }

  public void setVoucherStatus(VoucherStatus voucherStatus) { this.voucherStatus=voucherStatus; }

  public static Object pack(Object value) {
    VoucherDelivery voucherDelivery = (VoucherDelivery) value;
    Struct struct = new Struct(schema);
    struct.put("voucherID", voucherDelivery.getVoucherID());
    struct.put("fileID", voucherDelivery.getFileID());
    struct.put("voucherCode", voucherDelivery.getVoucherCode());
    struct.put("voucherStatus", voucherDelivery.getVoucherStatus().getExternalRepresentation());
    struct.put("voucherExpiryDate", voucherDelivery.getVoucherExpiryDate());
    return struct;
  }

  public static VoucherDelivery unpack(SchemaAndValue schemaAndValue) {
    Object value = schemaAndValue.value();
    Struct valueStruct = (Struct) value;
    String voucherID = valueStruct.getString("voucherID");
    String fileID = valueStruct.getString("fileID");
    String voucherCode = valueStruct.getString("voucherCode");
    VoucherStatus voucherStatus = VoucherStatus.fromExternalRepresentation(valueStruct.getString("voucherStatus"));
    Date voucherExpiryDate = (Date)valueStruct.get("voucherExpiryDate");
    return new VoucherDelivery(voucherID,fileID,voucherCode,voucherStatus,voucherExpiryDate);
  }

  public VoucherDelivery(String voucherID, String fileID, String voucherCode, VoucherStatus voucherStatus, Date voucherExpiryDate)
  {
    this.voucherID = voucherID;
    this.fileID = fileID;
    this.voucherCode = voucherCode;
    this.voucherStatus = voucherStatus;
    this.voucherExpiryDate = voucherExpiryDate;
  }
  
  public VoucherDelivery(JSONObject jsonRoot)
  {
    this.voucherID = JSONUtilities.decodeString(jsonRoot, "voucherID", true);
    this.fileID = JSONUtilities.decodeString(jsonRoot, "fileID", false);
    this.voucherCode = JSONUtilities.decodeString(jsonRoot, "voucherCode", true);;
    this.voucherExpiryDate = JSONUtilities.decodeDate(jsonRoot, "voucherExpiryDate", false);
  }
  
  public JSONObject getJSONPresentation()
  {
    Map<String, Object> jsonMap = new LinkedHashMap<String, Object>();
    jsonMap.put("voucherID", getVoucherID());
    jsonMap.put("fileID", getFileID());
    jsonMap.put("voucherCode", getVoucherCode());
    jsonMap.put("voucherExpiryDate", getVoucherExpiryDate());
    return JSONUtilities.encodeObject(jsonMap);
  }

  @Override
  public String toString() {
    return "VoucherDelivery{" +
            "voucherID='" + voucherID + '\'' +
            ", fileID='" + fileID + '\'' +
            ", voucherCode='" + voucherCode + '\'' +
            ", voucherStatus=" + voucherStatus +
            ", voucherExpiryDate=" + voucherExpiryDate +
            '}';
  }

}
