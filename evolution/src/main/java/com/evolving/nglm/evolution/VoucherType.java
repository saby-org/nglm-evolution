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
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

@GUIDependencyDef(objectType = "vouchertype", serviceClass = VoucherTypeService.class, dependencies = { })
public class VoucherType extends OfferContentType {

  public enum CodeType {
    Personal("Personal"),
    Shared("Shared"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private CodeType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static CodeType fromExternalRepresentation(String externalRepresentation) { for (CodeType enumeratedValue : CodeType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  private static Schema schema = null;
  static {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("voucher_type");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(OfferContentType.commonSchema().version(),2));
    for (Field field : OfferContentType.commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("codeType", Schema.STRING_SCHEMA);
    schemaBuilder.field("transferable", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("validity", VoucherValidity.schema());
    schema = schemaBuilder.build();
  }

  private static ConnectSerde<VoucherType> serde = new ConnectSerde<VoucherType>(schema, false, VoucherType.class, VoucherType::pack, VoucherType::unpack);

  public static Schema schema() { return schema; }
  public static ConnectSerde<VoucherType> serde() { return serde; }

  private CodeType codeType;
  private boolean transferable;
  private VoucherValidity validity;

  public CodeType getCodeType() { return codeType; }
  public boolean getTransferable() { return transferable; }
  public VoucherValidity getValidity() { return validity; }


  public static Object pack(Object value) {
    VoucherType voucherType = (VoucherType) value;
    Struct struct = new Struct(schema);
    OfferContentType.packCommon(struct, voucherType);
    struct.put("codeType", voucherType.getCodeType().getExternalRepresentation());
    struct.put("transferable", voucherType.getTransferable());
    struct.put("validity", VoucherValidity.pack(voucherType.getValidity()));
    return struct;
  }

  public static VoucherType unpack(SchemaAndValue schemaAndValue) {
    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Struct valueStruct = (Struct) value;
    CodeType codeType = CodeType.fromExternalRepresentation(valueStruct.getString("codeType"));
    boolean transferable = valueStruct.getBoolean("transferable");
    VoucherValidity validity = VoucherValidity.unpack(new SchemaAndValue(schema.field("validity").schema(), valueStruct.get("validity")));
    return new VoucherType(schemaAndValue, codeType, transferable, validity);
  }


  public VoucherType(SchemaAndValue schemaAndValue, CodeType codeType, boolean transferable, VoucherValidity validity) {
    super(schemaAndValue);
    this.codeType = codeType;
    this.transferable = transferable;
    this.validity = validity;
  }

  public VoucherType(JSONObject jsonRoot, long epoch, GUIManagedObject existingVoucherTypeUnchecked, int tenantID) throws GUIManagerException {

    super(jsonRoot, epoch, existingVoucherTypeUnchecked, tenantID);

    VoucherType existingVoucherType = (existingVoucherTypeUnchecked != null && existingVoucherTypeUnchecked instanceof VoucherType) ? (VoucherType) existingVoucherTypeUnchecked : null;

    this.codeType = CodeType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "codeType", true));
    this.transferable = JSONUtilities.decodeBoolean(jsonRoot, "transferable", true);
    this.validity = new VoucherValidity(JSONUtilities.decodeJSONObject(jsonRoot, "validity"));

    if (epochChanged(existingVoucherType)) {
      this.setEpoch(epoch);
    }
  }

  private boolean epochChanged(VoucherType existingVoucherType) {
    if (existingVoucherType != null && existingVoucherType.getAccepted()) {
      boolean epochChanged = false;
      epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingVoucherType.getGUIManagedObjectID());
      epochChanged = epochChanged || ! Objects.equals(codeType, existingVoucherType.getCodeType());
      epochChanged = epochChanged || ! Objects.equals(transferable, existingVoucherType.getTransferable());
      epochChanged = epochChanged || ! Objects.equals(validity, existingVoucherType.getValidity());
      return epochChanged;
    } else {
      return true;
    }
  }

}
