/*****************************************************************************
*
*  Score.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

@GUIDependencyDef(objectType = "score", serviceClass = PointService.class, dependencies = {})
public class Score extends Point
{
  
  //
  //  schema
  //

  private static Schema schema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("score");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(), 1));
    for (Field field : Point.schema().fields()) schemaBuilder.field(field.name(), field.schema());
    schema = schemaBuilder.build();
  };
  
  //
  //  serde
  //

  private static ConnectSerde<Score> serde = new ConnectSerde<Score>(schema, false, Score.class, Score::pack, Score::unpack);
  
  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<Score> scoreSerde() { return serde; }
  
  //
  //  Score constructor (same as point)
  //
  
  public Score(JSONObject jsonRoot, long epoch, GUIManagedObject existingPointUnchecked) throws GUIManagerException
  {
    super(jsonRoot, epoch, existingPointUnchecked);
    validate();
  }

  
  //
  // this property a score must have
  //
  
  private void validate() throws GUIManagerException
  {
    if (getEffectiveEndDate().before(NGLMRuntime.END_OF_TIME) || getEffectiveStartDate().after(NGLMRuntime.BEGINNING_OF_TIME)) throw new GUIManagerException("score should not have start/end date", "");
    if (!getCreditable() || !getDebitable()) throw new GUIManagerException("score should be both creditable and debitable", "");
    if (getValidity().getPeriodType() != TimeUnit.Year || getValidity().getPeriodQuantity() != 100) throw new GUIManagerException("score should have 100 years validity", "");
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    Score score = (Score) value;
    Struct struct = new Struct(schema);
    packCommon(struct, score);
    struct.put("debitable", score.getDebitable());
    struct.put("creditable", score.getCreditable());
    struct.put("setable", score.getSetable());
    struct.put("validity", PointValidity.pack(score.getValidity()));
    struct.put("label", score.getLabel());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static Score unpack(SchemaAndValue schemaAndValue)
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
    String label = valueStruct.getString("label");

    //
    //  return
    //

    return new Score(schemaAndValue, debitable, creditable, setable, validity, label);
  }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Score(SchemaAndValue schemaAndValue, boolean debitable, boolean creditable, boolean setable, PointValidity validity, String label)
  {
    super(schemaAndValue, debitable, creditable, setable, validity, label);
  }
  
}
