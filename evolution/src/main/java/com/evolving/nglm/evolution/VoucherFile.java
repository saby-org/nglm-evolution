package com.evolving.nglm.evolution;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import org.apache.kafka.connect.data.*;
import org.json.simple.JSONObject;

import java.util.Date;
import java.util.List;
import java.util.Objects;

public class VoucherFile {

  private static Schema schema = null;
  static {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("voucher_file");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("fileId", Schema.STRING_SCHEMA);
    schemaBuilder.field("codeFormatId", Schema.STRING_SCHEMA);
    schemaBuilder.field("expiryDate", Timestamp.builder().optional().schema());
    schema = schemaBuilder.build();
  }

  public static Schema schema() { return schema; }

  private String fileId;
  private String codeFormatId;
  private Date expiryDate;//!!SHOULD BE NULL IF NONE!! this is how we know in Purchase the expirationDate should be a "relative validity period"

  // this is stored with voucher conf
  private Date relativeLatestExpiryDate;//stored the max expiry date for relative expiry date only (if no expiryDate set for the file, expiryDate is the relative expiry period from voucherType)
  private VoucherService.VoucherFileStats voucherFileStats;//stats about this voucher file

  public String getFileId() { return fileId; }
  public String getCodeFormatId() { return codeFormatId; }
  public Date getExpiryDate() { return expiryDate; }

  public Date getRelativeLatestExpiryDate() { return relativeLatestExpiryDate; }
  public VoucherService.VoucherFileStats getVoucherFileStats() { return voucherFileStats; }
  public void setRelativeLatestExpiryDate(Date relativeLatestExpiryDate) { this.relativeLatestExpiryDate = relativeLatestExpiryDate; }
  public void setVoucherFileStats(VoucherService.VoucherFileStats voucherFileStats){ this.voucherFileStats=voucherFileStats; }

  public static Object pack(Object value) {
    VoucherFile voucherFile = (VoucherFile) value;
    Struct struct = new Struct(schema);
    struct.put("fileId", voucherFile.getFileId());
    struct.put("codeFormatId", voucherFile.getCodeFormatId());
    struct.put("expiryDate", voucherFile.getExpiryDate());
    return struct;
  }

  public static VoucherFile unpack(SchemaAndValue schemaAndValue) {
    Struct valueStruct = (Struct)schemaAndValue.value();
    String fileId = valueStruct.getString("fileId");
    String codeFormatId = valueStruct.getString("codeFormatId");
    Date expiryDate = (Date) valueStruct.get("expiryDate");
    return new VoucherFile(fileId, codeFormatId, expiryDate);
  }


  private VoucherFile(String fileId, String codeFormatId, Date expiryDate) {
    this.fileId = fileId;
    this.codeFormatId = codeFormatId;
    this.expiryDate = expiryDate;
  }

  public VoucherFile(JSONObject jsonRoot, List<VoucherFile> allPreviousVoucherFiles,VoucherPersonal voucher, VoucherType voucherType){
    this.fileId = JSONUtilities.decodeString(jsonRoot,"fileId",true);
    this.codeFormatId = JSONUtilities.decodeString(jsonRoot,"codeFormatId",true);
    this.expiryDate = GUIManagedObject.parseDateField(JSONUtilities.decodeString(jsonRoot,"expiryDate",false));
    // if no expiryDate coming from GUI with file, we have a relative expiryDate from voucherType expiryPeriod
    // this is just an attempt to give smallest expiry period before biggest (we order ES request by date)
    // but note, we wont keep that behavior consistent if VoucherType expiry period is modified, this is too much complexity to track this change
    this.relativeLatestExpiryDate = EvolutionUtilities.addTime(voucher.getEffectiveEndDate(),voucherType.getValidity().getPeriodQuantity(),voucherType.getValidity().getPeriodType(),Deployment.getDeployment(tenantID).getBaseTimeZone(),voucherType.getValidity().getRoundDown()? EvolutionUtilities.RoundingSelection.RoundDown:EvolutionUtilities.RoundingSelection.NoRound);
  }

  @Override
  public String toString() {
    return "VoucherFile{" +
            "fileId='" + fileId + '\'' +
            ", codeFormatId='" + codeFormatId + '\'' +
            ", expiryDate=" + expiryDate +
            ", relativeLatestExpiryDate=" + relativeLatestExpiryDate +
            ", voucherFileStats=" + voucherFileStats +
            '}';
  }

  @Override
  public boolean equals(Object obj) {
    boolean result = false;
    if(obj instanceof VoucherFile){
      VoucherFile voucherFile = (VoucherFile) obj;
      result = true;
      result = result && Objects.equals(getFileId(),voucherFile.getFileId());
      result = result && Objects.equals(getCodeFormatId(),voucherFile.getCodeFormatId());
      result = result && Objects.equals(getExpiryDate(),voucherFile.getExpiryDate());
    }
    return result;
  }

}
