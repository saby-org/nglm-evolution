/*****************************************************************************
*
*  VoucherType.java
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

public class VoucherType extends GUIManagedObject
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  public enum CodeType
  {
    Personal("Personal"),
    Shared("Shared"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private CodeType(String externalRepresentation) { this.externalRepresentation = externalRepresentation;}
    public String getExternalRepresentation() { return externalRepresentation; }
    public static CodeType fromExternalRepresentation(String externalRepresentation) { for (CodeType enumeratedValue : CodeType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

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
    schemaBuilder.name("voucher_type");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("codeType", Schema.STRING_SCHEMA);
    schemaBuilder.field("description", Schema.STRING_SCHEMA);
    schemaBuilder.field("transferable", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("validity", VoucherValidity.schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<VoucherType> serde = new ConnectSerde<VoucherType>(schema, false, VoucherType.class, VoucherType::pack, VoucherType::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<VoucherType> serde() { return serde; }

 /*****************************************
  *
  *  data
  *
  *****************************************/

  private CodeType codeType;
  private String description;
  private boolean transferable;
  private VoucherValidity validity;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getVoucherTypeID() { return getGUIManagedObjectID(); }
  public String getVoucherTypeName() { return getGUIManagedObjectName(); }
  public CodeType getCodeType() { return codeType; }
  public String getDescription() { return description; }
  public boolean getTransferable() { return transferable; }
  public VoucherValidity getValidity() { return validity; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public VoucherType(SchemaAndValue schemaAndValue, CodeType codeType, String description, boolean transferable, VoucherValidity validity)
  {
    super(schemaAndValue);
    this.codeType = codeType;
    this.description = description;
    this.transferable = transferable;
    this.validity = validity;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  private VoucherType(VoucherType voucherType)
  {
    super(voucherType.getJSONRepresentation(), voucherType.getEpoch());
    this.codeType = voucherType.getCodeType();
    this.description = voucherType.getDescription();
    this.transferable = voucherType.getTransferable();
    this.validity = voucherType.getValidity();
  }

  /*****************************************
  *
  *  copy
  *
  *****************************************/

  public VoucherType copy()
  {
    return new VoucherType(this);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    VoucherType voucherType = (VoucherType) value;
    Struct struct = new Struct(schema);
    packCommon(struct, voucherType);
    struct.put("codeType", voucherType.getCodeType().getExternalRepresentation());
    struct.put("description", voucherType.getDescription());
    struct.put("transferable", voucherType.getTransferable());
    struct.put("validity", VoucherValidity.pack(voucherType.getValidity()));
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static VoucherType unpack(SchemaAndValue schemaAndValue)
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
    CodeType codeType = CodeType.fromExternalRepresentation(valueStruct.getString("codeType"));
    String description = valueStruct.getString("description");
    boolean transferable = valueStruct.getBoolean("transferable");
    VoucherValidity validity = VoucherValidity.unpack(new SchemaAndValue(schema.field("validity").schema(), valueStruct.get("validity")));

    //
    //  return
    //

    return new VoucherType(schemaAndValue, codeType, description, transferable, validity);
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public VoucherType(JSONObject jsonRoot, long epoch, GUIManagedObject existingVoucherTypeUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingVoucherTypeUnchecked != null) ? existingVoucherTypeUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingVoucherType
    *
    *****************************************/

    VoucherType existingVoucherType = (existingVoucherTypeUnchecked != null && existingVoucherTypeUnchecked instanceof VoucherType) ? (VoucherType) existingVoucherTypeUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.codeType = CodeType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "codeType", true));
    this.description = JSONUtilities.decodeString(jsonRoot, "description", true);
    this.transferable = JSONUtilities.decodeBoolean(jsonRoot, "transferable", true);
    this.validity = new VoucherValidity(JSONUtilities.decodeJSONObject(jsonRoot, "validity"));

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingVoucherType))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(VoucherType existingVoucherType)
  {
    if (existingVoucherType != null && existingVoucherType.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingVoucherType.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(codeType, existingVoucherType.getCodeType());
        epochChanged = epochChanged || ! Objects.equals(description, existingVoucherType.getDescription());
        epochChanged = epochChanged || ! Objects.equals(transferable, existingVoucherType.getTransferable());
        epochChanged = epochChanged || ! Objects.equals(validity, existingVoucherType.getValidity());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }

}
