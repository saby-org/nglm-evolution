package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.StockMonitor.StockableItem;
import org.apache.kafka.connect.data.*;
import org.json.simple.JSONObject;

import java.util.*;
import java.util.Date;

@GUIDependencyDef(objectType = "vouchershared", serviceClass = VoucherService.class, dependencies = {"supplier", "vouchertype", "workflow" })
public class VoucherShared extends Voucher implements StockableItem {

  private static Schema schema = null;
  static {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("voucher_shared");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(Voucher.commonSchema().version(),1));
    for (Field field : Voucher.commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("stock", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("sharedCode", Schema.STRING_SCHEMA);
    schemaBuilder.field("codeFormatId", Schema.STRING_SCHEMA);
    schema = schemaBuilder.build();
  }

  private static ConnectSerde<VoucherShared> serde = new ConnectSerde<VoucherShared>(schema, false, VoucherShared.class, VoucherShared::pack, VoucherShared::unpack);

  public static Schema schema() { return schema; }
  public static ConnectSerde<VoucherShared> serde() { return serde; }


  private Integer stock;
  private String sharedCode;
  private String codeFormatId;

  private String stockableItemID;

  public Integer getStock() { return stock; }
  public String getSharedCode() { return sharedCode; }
  public String getCodeFormatId() { return codeFormatId; }

  public String getStockableItemID() { return stockableItemID; }


  public static Object pack(Object value) {
    VoucherShared voucherShared = (VoucherShared) value;
    Struct struct = new Struct(schema);
    Voucher.packCommon(struct, voucherShared);
    struct.put("stock", voucherShared.getStock());
    struct.put("sharedCode", voucherShared.getSharedCode());
    struct.put("codeFormatId", voucherShared.getCodeFormatId());
    return struct;
  }

  public static VoucherShared unpack(SchemaAndValue schemaAndValue) {
    Object value = schemaAndValue.value();
    Struct valueStruct = (Struct) value;
    Integer stock = valueStruct.getInt32("stock");
    String sharedCode = valueStruct.getString("sharedCode");
    String codeFormatId = valueStruct.getString("codeFormatId");
    return new VoucherShared(schemaAndValue, stock, sharedCode, codeFormatId);
  }

  public VoucherShared(SchemaAndValue schemaAndValue, Integer stock, String sharedCode, String codeFormatId) {
    super(schemaAndValue);
    this.stock = stock;
    this.sharedCode = sharedCode;
    this.codeFormatId = codeFormatId;
    this.stockableItemID = "voucher-shared-" + getVoucherID();
  }

  public VoucherShared(JSONObject jsonRoot, long epoch, GUIManagedObject existingVoucherUnchecked, int tenantID) throws GUIManagerException {

    super(jsonRoot, epoch, existingVoucherUnchecked, tenantID);

    // not allow this type change
    if(existingVoucherUnchecked instanceof VoucherPersonal) throw new GUIManagerException("can not modify Personal to Shared Voucher type",existingVoucherUnchecked.getGUIManagedObjectDisplay());

    VoucherShared existingVoucherShared = (existingVoucherUnchecked != null && existingVoucherUnchecked instanceof VoucherShared) ? (VoucherShared) existingVoucherUnchecked : null;

    this.stock = JSONUtilities.decodeInteger(jsonRoot, "stock", false);
    this.sharedCode = JSONUtilities.decodeString(jsonRoot, "sharedCode", true);
    this.codeFormatId = JSONUtilities.decodeString(jsonRoot, "codeFormatId", true);
    this.stockableItemID = "voucher-shared-" + getVoucherID();

    if (epochChanged(existingVoucherShared)){
      this.setEpoch(epoch);
    }

  }

  private boolean epochChanged(VoucherShared existingProduct){
    if (existingProduct != null && existingProduct.getAccepted()){
      boolean epochChanged = false;
      epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingProduct.getGUIManagedObjectID());
      epochChanged = epochChanged || ! Objects.equals(stock, existingProduct.getStock());
      epochChanged = epochChanged || ! Objects.equals(sharedCode, existingProduct.getSharedCode());
      epochChanged = epochChanged || ! Objects.equals(codeFormatId, existingProduct.getCodeFormatId());
      return epochChanged;
    }else{
      return true;
    }
  }

  @Override
  public void validate(VoucherTypeService voucherTypeService, UploadedFileService uploadedFileService, Date now) throws GUIManagerException {
    commonValidate(voucherTypeService, now);
    // check type id is Shared
    if(!voucherTypeService.getActiveVoucherType(getVoucherTypeId(),now).getCodeType().equals(VoucherType.CodeType.Shared)) throw new GUIManagerException("wrong VoucherType for "+this.getClass().getSimpleName(),getVoucherTypeId());
  }
  
  @Override public Map<String, List<String>> getGUIDependencies(int tenantID)
  {
    Map<String, List<String>> result = new HashMap<String, List<String>>();
    List<String> wrkflowIDs = new ArrayList<String>();
    List<String> supplierIDs = new ArrayList<String>();
    List<String> vouchertypeIDs = new ArrayList<String>();
    
    if (getWorkflowID() != null && !getWorkflowID().isEmpty()) wrkflowIDs.add(getWorkflowID());
    supplierIDs.add(getSupplierID());
    vouchertypeIDs.add(getVoucherTypeId());
    
    result.put("workflow", wrkflowIDs);
    result.put("supplier", supplierIDs);
    result.put("vouchertype", vouchertypeIDs);
    return result;
  }

}
