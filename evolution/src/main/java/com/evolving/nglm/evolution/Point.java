/*****************************************************************************
*
*  Point.java
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

public class Point extends GUIManagedObject
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
    schemaBuilder.name("point");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("debitable", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("crebitable", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("setable", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("validity", PointValidity.schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<Point> serde = new ConnectSerde<Point>(schema, false, Point.class, Point::pack, Point::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<Point> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private boolean debitable;
  private boolean crebitable;
  private boolean setable;
  private PointValidity validity;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //

  public String getPointID() { return getGUIManagedObjectID(); }
  public String getPointName() { return getGUIManagedObjectName(); }
  public boolean getDebitable() { return debitable; }
  public boolean getCrebitable() { return crebitable; }
  public boolean getSetable() { return setable; }
  public PointValidity getValidity(){ return validity; }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Point(SchemaAndValue schemaAndValue, boolean debitable, boolean crebitable, boolean setable, PointValidity validity)
  {
    super(schemaAndValue);
    this.debitable = debitable;
    this.crebitable = crebitable;
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
    Point point = (Point) value;
    Struct struct = new Struct(schema);
    packCommon(struct, point);
    struct.put("debitable", point.getDebitable());
    struct.put("crebitable", point.getCrebitable());
    struct.put("setable", point.getSetable());
    struct.put("validity", PointValidity.pack(point.getValidity()));
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static Point unpack(SchemaAndValue schemaAndValue)
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
    boolean crebitable = valueStruct.getBoolean("crebitable");
    boolean setable = valueStruct.getBoolean("setable");
    PointValidity validity = unpackPointValidity(schema.field("validity").schema(), valueStruct.get("validity"));

    //
    //  return
    //

    return new Point(schemaAndValue, debitable, crebitable, setable, validity);
  }
  
  /*****************************************
  *
  *  unpackPointValidity
  *
  *****************************************/

  private static PointValidity unpackPointValidity(Schema schema, Object value)
  {
    //
    //  get schema for PointValidity
    //

    Schema validitySchema = schema.valueSchema();
    
    //
    //  unpack
    //

    return PointValidity.unpack(new SchemaAndValue(validitySchema, value));
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public Point(JSONObject jsonRoot, long epoch, GUIManagedObject existingPointUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingPointUnchecked != null) ? existingPointUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingPoint
    *
    *****************************************/

    Point existingPoint = (existingPointUnchecked != null && existingPointUnchecked instanceof Point) ? (Point) existingPointUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.debitable = JSONUtilities.decodeBoolean(jsonRoot, "debitable", true);
    this.crebitable = JSONUtilities.decodeBoolean(jsonRoot, "crebitable", true);
    this.setable = JSONUtilities.decodeBoolean(jsonRoot, "setable", true);
    this.validity = new PointValidity(JSONUtilities.decodeJSONObject(jsonRoot, "validity"));

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingPoint))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(Point point)
  {
    if (point != null && point.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getDebitable(), point.getDebitable());
        epochChanged = epochChanged || ! Objects.equals(getCrebitable(), point.getCrebitable());
        epochChanged = epochChanged || ! Objects.equals(getSetable(), point.getSetable());
        epochChanged = epochChanged || ! Objects.equals(getValidity(), point.getValidity());
        return epochChanged;
      }
    else
      {
        return true;
      }
   }

}
