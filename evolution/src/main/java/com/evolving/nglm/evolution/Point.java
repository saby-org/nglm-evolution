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
    schemaBuilder.field("creditable", Schema.BOOLEAN_SCHEMA);
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
  private boolean creditable;
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
  public String getDisplay() { return getGUIManagedObjectDisplay(); }
  public boolean getDebitable() { return debitable; }
  public boolean getCreditable() { return creditable; }
  public boolean getSetable() { return setable; }
  public PointValidity getValidity(){ return validity; }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Point(SchemaAndValue schemaAndValue, boolean debitable, boolean creditable, boolean setable, PointValidity validity)
  {
    super(schemaAndValue);
    this.debitable = debitable;
    this.creditable = creditable;
    this.setable = setable;
    this.validity = validity;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  private Point(Point point)
  {
    super(point.getJSONRepresentation(), point.getEpoch());
    this.debitable = point.getDebitable();
    this.creditable = point.getCreditable();
    this.setable = point.getSetable();
    this.validity = point.getValidity().copy();
  }

  /*****************************************
  *
  *  copy
  *
  *****************************************/

  public Point copy()
  {
    return new Point(this);
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
    struct.put("creditable", point.getCreditable());
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
    boolean creditable = valueStruct.getBoolean("creditable");
    boolean setable = valueStruct.getBoolean("setable");
    PointValidity validity = PointValidity.unpack(new SchemaAndValue(schema.field("validity").schema(), valueStruct.get("validity")));

    //
    //  return
    //

    return new Point(schemaAndValue, debitable, creditable, setable, validity);
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

    this.debitable = JSONUtilities.decodeBoolean(jsonRoot, "debitable", Boolean.TRUE);
    this.creditable = JSONUtilities.decodeBoolean(jsonRoot, "creditable", Boolean.TRUE);
    this.setable = JSONUtilities.decodeBoolean(jsonRoot, "setable", Boolean.FALSE);
    this.validity = new PointValidity(JSONUtilities.decodeJSONObject(jsonRoot, "validity", true));

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
        epochChanged = epochChanged || ! Objects.equals(getCreditable(), point.getCreditable());
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
