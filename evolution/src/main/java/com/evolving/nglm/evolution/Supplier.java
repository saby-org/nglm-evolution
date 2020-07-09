/*****************************************************************************
*
*  Supplier.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

@GUIDependencyDef(objectType = "supplier", serviceClass = SupplierService.class, dependencies = { })
public class Supplier extends GUIManagedObject
{
  
  
  /*****************************************
  *
  *  enum
  *
  *****************************************/
  
  public enum SupplierType
  {
    Internal("INTERNAL"),
    External("EXTERNAL"),
    Unknown("(unknown)");
    
    private String externalRepresentation;
    private SupplierType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static SupplierType fromExternalRepresentation(String externalRepresentation) { for (SupplierType enumeratedValue : SupplierType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) { return enumeratedValue; } } return Unknown; }
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
    schemaBuilder.name("supplier");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),2));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("supplierType", SchemaBuilder.string().optional().defaultValue("Internal").schema());
    schemaBuilder.field("userIDs", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<Supplier> serde = new ConnectSerde<Supplier>(schema, false, Supplier.class, Supplier::pack, Supplier::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<Supplier> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  
  private SupplierType supplierType;
  private List<String> userIDs;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getSupplierID() { return getGUIManagedObjectID(); }
  public SupplierType getSupplierType() { return supplierType; }
  public List<String> getUserIDs() { return userIDs; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Supplier(SchemaAndValue schemaAndValue, SupplierType supplierType, List<String> userIDs)
  {
    super(schemaAndValue);
    this.supplierType = supplierType;
    this.userIDs = userIDs;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    Supplier supplier = (Supplier) value;
    Struct struct = new Struct(schema);
    packCommon(struct, supplier);
    struct.put("supplierType", supplier.getSupplierType().getExternalRepresentation());
    struct.put("userIDs", supplier.getUserIDs());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static Supplier unpack(SchemaAndValue schemaAndValue)
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
    SupplierType supplierType = (schemaVersion >= 2) ? SupplierType.fromExternalRepresentation(valueStruct.getString("supplierType")) : SupplierType.Internal;
    List<String> userIDs = (schemaVersion >= 2) ? (List<String>) valueStruct.get("userIDs"):new ArrayList<String>();
    //
    //  return
    //

    return new Supplier(schemaAndValue, supplierType, userIDs);
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public Supplier(JSONObject jsonRoot, long epoch, GUIManagedObject existingSupplierUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingSupplierUnchecked != null) ? existingSupplierUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingSupplier
    *
    *****************************************/

    Supplier existingSupplier = (existingSupplierUnchecked != null && existingSupplierUnchecked instanceof Supplier) ? (Supplier) existingSupplierUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/
    
    this.supplierType = SupplierType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "supplierType", false));
    this.userIDs = decodeUsers(JSONUtilities.decodeJSONArray(jsonRoot, "userIDs", false));

    /*****************************************
    *
    *  validate
    *
    *****************************************/

    if (getRawEffectiveStartDate() != null) throw new GUIManagerException("unsupported start date", JSONUtilities.decodeString(jsonRoot, "effectiveStartDate", false));
    if (getRawEffectiveEndDate() != null) throw new GUIManagerException("unsupported end date", JSONUtilities.decodeString(jsonRoot, "effectiveEndDate", false));

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingSupplier))
      {
        this.setEpoch(epoch);
      }
  }
  
  /*****************************************
  *
  *  decodeIDs
  *
  *****************************************/

  private List<String> decodeUsers(JSONArray jsonArray)
  {
    List<String> userIDs = null;
    if (jsonArray != null)
      {
        userIDs = new ArrayList<String>();
        for (int i=0; i<jsonArray.size(); i++)
          {
            String ID = (String) jsonArray.get(i);
            userIDs.add(ID);
          }
      }
    return userIDs;
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(Supplier existingSupplier)
  {
    if (existingSupplier != null && existingSupplier.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingSupplier.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(getSupplierType(), existingSupplier.getSupplierType());
        epochChanged = epochChanged || ! Objects.equals(getUserIDs(), existingSupplier.getUserIDs());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
  
  @Override public Map<String, List<String>> getGUIDependencies()
  {
    Map<String, List<String>> result = new HashMap<String, List<String>>();
    return result;
  }
}
