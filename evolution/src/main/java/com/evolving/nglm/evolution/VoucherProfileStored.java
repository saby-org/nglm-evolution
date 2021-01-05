package com.evolving.nglm.evolution;

import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.VoucherDelivery.VoucherStatus;
import com.evolving.nglm.evolution.retention.Cleanable;
import com.evolving.nglm.evolution.retention.RetentionService;
import org.apache.kafka.connect.data.*;

import java.time.Duration;
import java.util.Date;

public class VoucherProfileStored implements Cleanable {

  private static Schema voucherProfileStoredSchema = null;
  static {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("voucher_profile_stored");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("voucherID", Schema.STRING_SCHEMA);
    schemaBuilder.field("fileID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("voucherCode", Schema.STRING_SCHEMA);
    schemaBuilder.field("voucherStatus", Schema.STRING_SCHEMA);
    schemaBuilder.field("voucherExpiryDate", Timestamp.builder().schema());
    schemaBuilder.field("voucherDeliveryDate", Timestamp.builder().schema());
    schemaBuilder.field("voucherRedeemDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("offerID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("eventID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("moduleID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("featureID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("origin", Schema.OPTIONAL_STRING_SCHEMA);
    voucherProfileStoredSchema = schemaBuilder.build();
  }

  public static Schema voucherProfileStoredSchema() { return voucherProfileStoredSchema; }

  public static Object pack(Object value) {
    VoucherProfileStored voucherProfileStored = (VoucherProfileStored) value;
    Struct struct = new Struct(voucherProfileStoredSchema);
    struct.put("voucherID",voucherProfileStored.getVoucherID());
    struct.put("fileID",voucherProfileStored.getFileID());
    struct.put("voucherCode",voucherProfileStored.getVoucherCode());
    struct.put("voucherStatus",voucherProfileStored.getVoucherStatus().getExternalRepresentation());
    struct.put("voucherExpiryDate",voucherProfileStored.getVoucherExpiryDate());
    struct.put("voucherDeliveryDate",voucherProfileStored.getVoucherDeliveryDate());
    struct.put("voucherRedeemDate",voucherProfileStored.getVoucherRedeemDate());
    struct.put("offerID",voucherProfileStored.getOfferID());
    struct.put("eventID",voucherProfileStored.getEventID());
    struct.put("moduleID",voucherProfileStored.getModuleID());
    struct.put("featureID",voucherProfileStored.getFeatureID());
    struct.put("origin",voucherProfileStored.getOrigin());
    return struct;
  }

  public static VoucherProfileStored unpack(SchemaAndValue schemaAndValue) {
    Object value = schemaAndValue.value();
    Struct valueStruct = (Struct) value;
    String voucherID = valueStruct.getString("voucherID");
    String fileID = valueStruct.getString("fileID");
    String voucherCode = valueStruct.getString("voucherCode");
    VoucherStatus voucherStatus = VoucherStatus.fromExternalRepresentation(valueStruct.getString("voucherStatus"));
    Date voucherExpiryDate = (Date)valueStruct.get("voucherExpiryDate");
    Date voucherDeliveryDate = (Date)valueStruct.get("voucherDeliveryDate");
    Date voucherRedeemDate = (Date)valueStruct.get("voucherRedeemDate");
    String offerID = valueStruct.getString("offerID");
    String eventID = valueStruct.getString("eventID");
    String moduleID = valueStruct.getString("moduleID");
    String featureID = valueStruct.getString("featureID");
    String origin = valueStruct.getString("origin");
    return new VoucherProfileStored(voucherID,fileID,voucherCode,voucherStatus,voucherExpiryDate,voucherDeliveryDate,voucherRedeemDate,offerID,eventID,moduleID,featureID,origin);
  }

  private String voucherID;
  private String fileID;
  private String voucherCode;
  private VoucherStatus voucherStatus;
  private Date voucherExpiryDate;
  private Date voucherDeliveryDate;
  private Date voucherRedeemDate;
  private String offerID;
  private String eventID;
  private String moduleID;
  private String featureID;
  private String origin;

  public String getVoucherID() { return voucherID; }
  public String getFileID() { return fileID; }
  public String getVoucherCode() { return voucherCode; }
  public VoucherStatus getVoucherStatus() { return voucherStatus; }
  public Date getVoucherExpiryDate() { return voucherExpiryDate; }
  public Date getVoucherDeliveryDate() { return voucherDeliveryDate; }
  public Date getVoucherRedeemDate() { return voucherRedeemDate; }
  public String getOfferID() { return offerID; }
  public String getEventID() { return eventID; }
  public String getModuleID() { return moduleID; }
  public String getFeatureID() { return featureID; }
  public String getOrigin() { return origin; }

  public void setVoucherStatus(VoucherStatus voucherStatus) { this.voucherStatus = voucherStatus; }
  public void setVoucherExpiryDate(Date voucherExpiryDate) { this.voucherExpiryDate = voucherExpiryDate; }
  public void setVoucherRedeemDate(Date voucherRedeemDate) { this.voucherRedeemDate = voucherRedeemDate; }

  @Override public Date getExpirationDate(RetentionService retentionService, int tenantID) { return getVoucherExpiryDate(); }
  @Override public Duration getRetention(RetentionType type, RetentionService retentionService, int tenantID) {
    switch (type){
      case KAFKA_DELETION:
        return Duration.ofDays(Deployment.getKafkaRetentionDaysExpiredVouchers());
    }
    return null;
  }


  public VoucherProfileStored(String voucherID, String fileID, String voucherCode, VoucherStatus voucherStatus, Date voucherExpiryDate, Date voucherDeliveryDate, Date voucherRedeemDate, String offerID, String eventID, String moduleID, String featureID, String origin) {
    this.voucherID = voucherID;
    this.fileID = fileID;
    this.voucherCode = voucherCode;
    this.voucherStatus = voucherStatus;
    this.voucherExpiryDate = voucherExpiryDate;
    this.voucherDeliveryDate = voucherDeliveryDate;
    this.voucherRedeemDate = voucherRedeemDate;
    this.offerID = offerID;
    this.eventID = eventID;
    this.moduleID = moduleID;
    this.featureID = featureID;
    this.origin = origin;
  }

  @Override
  public String toString() {
    return "VoucherProfileStored{" +
            "voucherID='" + voucherID + '\'' +
            ", fileID='" + fileID + '\'' +
            ", voucherCode='" + voucherCode + '\'' +
            ", voucherStatus=" + voucherStatus +
            ", voucherExpiryDate=" + voucherExpiryDate +
            ", voucherDeliveryDate=" + voucherDeliveryDate +
            ", voucherRedeemDate=" + voucherRedeemDate +
            ", offerID='" + offerID + '\'' +
            ", eventID='" + eventID + '\'' +
            ", moduleID='" + moduleID + '\'' +
            ", featureID='" + featureID + '\'' +
            ", origin='" + origin + '\'' +
            '}';
  }

}
