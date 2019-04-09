/*****************************************************************************
*
*  PointType.java
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

public class PointType extends GUIManagedObject
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
    schemaBuilder.name("pointType");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("debitable", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("creditable", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("setable", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("validity", PointTypeValidity.schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<PointType> serde = new ConnectSerde<PointType>(schema, false, PointType.class, PointType::pack, PointType::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<PointType> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private boolean debitable;
  private boolean creditable;
  private boolean setable;
  private PointTypeValidity validity;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //

  public String getPointTypeID() { return getGUIManagedObjectID(); }
  public String getPointTypeName() { return getGUIManagedObjectName(); }
  public String getDisplay() { return getGUIManagedObjectName(); }
  public boolean getDebitable() { return debitable; }
  public boolean getCreditable() { return creditable; }
  public boolean getSetable() { return setable; }
  public PointTypeValidity getValidity(){ return validity; }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public PointType(SchemaAndValue schemaAndValue, boolean debitable, boolean creditable, boolean setable, PointTypeValidity validity)
  {
    super(schemaAndValue);
    this.debitable = debitable;
    this.creditable = creditable;
    this.setable = setable;
    this.validity = validity;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    PointType pointType = (PointType) value;
    Struct struct = new Struct(schema);
    packCommon(struct, pointType);
    struct.put("debitable", pointType.getDebitable());
    struct.put("creditable", pointType.getCreditable());
    struct.put("setable", pointType.getSetable());
    struct.put("validity", PointTypeValidity.pack(pointType.getValidity()));
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static PointType unpack(SchemaAndValue schemaAndValue)
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
    boolean debitable = valueStruct.getBoolean("debitable");
    boolean creditable = valueStruct.getBoolean("creditable");
    boolean setable = valueStruct.getBoolean("setable");
    PointTypeValidity validity = PointTypeValidity.unpack(new SchemaAndValue(schema.field("validity").schema(), valueStruct.get("validity")));

    //
    //  return
    //

    return new PointType(schemaAndValue, debitable, creditable, setable, validity);
  }
  
  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public PointType(JSONObject jsonRoot, long epoch, GUIManagedObject existingPointTypeUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingPointTypeUnchecked != null) ? existingPointTypeUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingPointType
    *
    *****************************************/

    PointType existingPointType = (existingPointTypeUnchecked != null && existingPointTypeUnchecked instanceof PointType) ? (PointType) existingPointTypeUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.debitable = JSONUtilities.decodeBoolean(jsonRoot, "debitable", true);
    this.creditable = JSONUtilities.decodeBoolean(jsonRoot, "creditable", true);
    this.setable = JSONUtilities.decodeBoolean(jsonRoot, "setable", true);
    this.validity = new PointTypeValidity(JSONUtilities.decodeJSONObject(jsonRoot, "validity"));

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingPointType))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(PointType pointType)
  {
    if (pointType != null && pointType.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getDebitable(), pointType.getDebitable());
        epochChanged = epochChanged || ! Objects.equals(getCreditable(), pointType.getCreditable());
        epochChanged = epochChanged || ! Objects.equals(getSetable(), pointType.getSetable());
        epochChanged = epochChanged || ! Objects.equals(getValidity(), pointType.getValidity());
        return epochChanged;
      }
    else
      {
        return true;
      }
   }

}
