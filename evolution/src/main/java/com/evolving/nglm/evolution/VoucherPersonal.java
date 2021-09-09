package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@GUIDependencyDef(objectType = "voucher", serviceClass = VoucherService.class, dependencies = {"supplier", "vouchertype", "workflow" })
public class VoucherPersonal extends Voucher {

  private static Schema schema = null;
  static {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("voucher_personal");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(Voucher.commonSchema().version(),1));
    for (Field field : Voucher.commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("voucherFiles", SchemaBuilder.array(VoucherFile.schema()).optional().schema());
    schema = schemaBuilder.build();
  }

  private static ConnectSerde<VoucherPersonal> serde = new ConnectSerde<VoucherPersonal>(schema, false, VoucherPersonal.class, VoucherPersonal::pack, VoucherPersonal::unpack);

  public static Schema schema() { return schema; }
  public static ConnectSerde<VoucherPersonal> serde() { return serde; }


  private List<VoucherFile> voucherFiles;
  public List<VoucherFile> getVoucherFiles() { return voucherFiles; }

  // not stored, but need to keep the previous voucherFiles, if there was, to check what changed compared with new one
  private List<VoucherFile> previousVoucherFiles=new ArrayList<>();
  public List<VoucherFile> getPreviousVoucherFiles() { return previousVoucherFiles; }

  public static Object pack(Object value) {
    VoucherPersonal voucherPersonal = (VoucherPersonal) value;
    Struct struct = new Struct(schema);
    Voucher.packCommon(struct, voucherPersonal);
    struct.put("voucherFiles", packVoucherFiles(voucherPersonal.getVoucherFiles()));
    return struct;
  }

  private static List<Object> packVoucherFiles(List<VoucherFile> voucherFiles){
    List<Object> result = new ArrayList<>();
    for(VoucherFile voucherFile:voucherFiles){
      result.add(VoucherFile.pack(voucherFile));
    }
    return result;
  }

  public static VoucherPersonal unpack(SchemaAndValue schemaAndValue) {
    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Struct valueStruct = (Struct) value;
    List<VoucherFile> voucherFiles = unpackVoucherFiles(schema.field("voucherFiles").schema(),valueStruct.get("voucherFiles"));
    return new VoucherPersonal(schemaAndValue, voucherFiles);
  }

  private static List<VoucherFile> unpackVoucherFiles(Schema schema, Object value){
    Schema voucherFileSchema = schema.valueSchema();
    List<VoucherFile> result = new ArrayList<>();
    List<Object> valueArray = (List<Object>) value;
    for(Object voucherFile:valueArray){
      result.add(VoucherFile.unpack(new SchemaAndValue(voucherFileSchema,voucherFile)));
    }
    return result;
  }

  public VoucherPersonal(SchemaAndValue schemaAndValue, List<VoucherFile> voucherFiles) {
    super(schemaAndValue);
    this.voucherFiles=voucherFiles;
  }

  public VoucherPersonal(JSONObject jsonRoot, long epoch, GUIManagedObject existingVoucherUnchecked, VoucherType voucherType, int tenantID) throws GUIManagerException {

    super(jsonRoot, GUIManagedObjectType.Voucher, epoch, existingVoucherUnchecked, tenantID);

    // not allow this type change
    if(existingVoucherUnchecked instanceof VoucherShared) throw new GUIManagerException("can not modify Shared to Personal Voucher type",existingVoucherUnchecked.getGUIManagedObjectDisplay());

    VoucherPersonal existingVoucherPersonal = (existingVoucherUnchecked != null && existingVoucherUnchecked instanceof VoucherPersonal) ? (VoucherPersonal) existingVoucherUnchecked : null;

    // not allow supplierID change
    if(existingVoucherPersonal!=null && !existingVoucherPersonal.getSupplierID().equals(this.getSupplierID())) throw new GUIManagerException("can not modify Supplier id",existingVoucherPersonal.getSupplierID());

    // save previous files conf to figure out what changed need to be processed there
    if(existingVoucherPersonal!=null && existingVoucherPersonal.getVoucherFiles()!=null){
      this.previousVoucherFiles=existingVoucherPersonal.getVoucherFiles();
    }

    this.voucherFiles=decodeVoucherFiles(JSONUtilities.decodeJSONArray(jsonRoot,"voucherFiles",false),voucherType, tenantID);

    if (epochChanged(existingVoucherPersonal)){
      this.setEpoch(epoch);
    }

  }

  private List<VoucherFile> decodeVoucherFiles(JSONArray jsonArray,VoucherType voucherType, int tenantID) throws GUIManagerException{
    List<VoucherFile> result = new ArrayList<>();
    if(jsonArray==null) return result;
    for(int i=0;i<jsonArray.size();i++){
      result.add(new VoucherFile((JSONObject)jsonArray.get(i),getPreviousVoucherFiles(),this,voucherType, tenantID));
    }
    return result;
  }

  private boolean epochChanged(VoucherPersonal existingVoucherPersonal){
    if (existingVoucherPersonal != null && existingVoucherPersonal.getAccepted()){
      boolean epochChanged = false;
      epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingVoucherPersonal.getGUIManagedObjectID());
      // note that List are equals if elements are in the same order, not sure yet if it is good or not
      epochChanged = epochChanged || ! Objects.equals(getVoucherFiles(), existingVoucherPersonal.getVoucherFiles());
      return epochChanged;
    }else{
      return true;
    }
  }

  @Override
  public void validate(VoucherTypeService voucherTypeService, UploadedFileService uploadedFileService, Date now) throws GUIManagerException {
    commonValidate(voucherTypeService, now);
    // check type id is Personal
    if(!voucherTypeService.getActiveVoucherType(getVoucherTypeId(),now).getCodeType().equals(VoucherType.CodeType.Personal)) throw new GUIManagerException("wrong VoucherType for "+this.getClass().getSimpleName(),getVoucherTypeId());
    // check files exists, and not twice there
    List<String> seenFileIds = new ArrayList<>();
    if(getVoucherFiles()!=null && !getVoucherFiles().isEmpty()){
      for(VoucherFile voucherFile:getVoucherFiles()){
        // duplicated file in conf, not good
        if(seenFileIds.contains(voucherFile.getFileId())) throw new GUIManagerException("duplicated uploaded file id ",voucherFile.getFileId());
        seenFileIds.add(voucherFile.getFileId());
      }
    }
  }

  @Override
  public JSONObject getJSONRepresentation() {
    // to insert file stats
    JSONObject jsonRoot = super.getJSONRepresentation();
    if(getVoucherFiles()==null || getVoucherFiles().isEmpty() || jsonRoot.get("voucherFiles")==null) return jsonRoot;
    JSONArray jsonList = (JSONArray)jsonRoot.get("voucherFiles");
    if(jsonList==null||jsonList.isEmpty()) return jsonRoot;
    for(VoucherFile voucherFile:getVoucherFiles()){
      if(voucherFile.getVoucherFileStats()==null)continue;
      for(Object jsonFile:jsonList.toArray()){
        if(((JSONObject)jsonFile).get("fileId").equals(voucherFile.getFileId())){
          ((JSONObject)jsonFile).put("importStatus",voucherFile.getVoucherFileStats().getImportStatus().getExternalRepresentation());
          ((JSONObject)jsonFile).put("initialLineInFile",voucherFile.getVoucherFileStats().getStockInFile());
          ((JSONObject)jsonFile).put("initialStock",voucherFile.getVoucherFileStats().getStockImported());
          ((JSONObject)jsonFile).put("remainingStock",voucherFile.getVoucherFileStats().getStockAvailable());
          ((JSONObject)jsonFile).put("expiredStock",voucherFile.getVoucherFileStats().getStockExpired());
          ((JSONObject)jsonFile).put("deliveredStock",voucherFile.getVoucherFileStats().getStockDelivered());
          ((JSONObject)jsonFile).put("redeemedStock",voucherFile.getVoucherFileStats().getStockRedeemed());
          break;
        }
      }
    }
    return jsonRoot;
  }
  
  @Override public Map<String, List<String>> getGUIDependencies(List<GUIService> guiServiceList, int tenantID)
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
  
  @Override public GUIManagedObjectType getGUIManagedObjectType()
  {
    return GUIManagedObjectType.Voucher;
  }

}
