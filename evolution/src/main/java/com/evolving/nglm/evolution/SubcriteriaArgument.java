package com.evolving.nglm.evolution;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class SubcriteriaArgument
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
    schemaBuilder.name("subcriteria_argument");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(0));
    schemaBuilder.field("argumentExpression", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("argumentBaseTimeUnit", Schema.STRING_SCHEMA);
    schema = schemaBuilder.build();
  };
  
  //
  //  accessor
  //

  public static Schema schema() { return schema; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  //
  //  criterion
  //

  private String argumentExpression;
  private TimeUnit argumentBaseTimeUnit;
  
  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private SubcriteriaArgument(String argumentExpression, TimeUnit argumentBaseTimeUnit)
  {
    this.argumentExpression = argumentExpression;
    this.argumentBaseTimeUnit = argumentBaseTimeUnit;
  }
  
  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  public SubcriteriaArgument(JSONObject jsonRoot) throws GUIManagerException
  {
    //
    //  basic fields (all but argument)
    //

    this.argumentExpression = (jsonRoot != null) ? JSONUtilities.decodeString(jsonRoot, "expression", true) : null;
    this.argumentBaseTimeUnit = (jsonRoot != null) ? TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "timeUnit", "(unknown)")) : TimeUnit.Unknown;
  }
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getArgumentExpression() { return argumentExpression; }
  public TimeUnit getArgumentBaseTimeUnit() { return argumentBaseTimeUnit; }
  
  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<SubcriteriaArgument> serde()
  {
    return new ConnectSerde<SubcriteriaArgument>(schema, false, SubcriteriaArgument.class, SubcriteriaArgument::pack, SubcriteriaArgument::unpack);
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    SubcriteriaArgument criterion = (SubcriteriaArgument) value;
    Struct struct = new Struct(schema);
    struct.put("argumentExpression", criterion.getArgumentExpression());
    struct.put("argumentBaseTimeUnit", criterion.getArgumentBaseTimeUnit().getExternalRepresentation());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static SubcriteriaArgument unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;
    Struct valueStruct = (Struct) value;

    String argumentExpression = valueStruct.getString("argumentExpression");
    TimeUnit argumentBaseTimeUnit = TimeUnit.fromExternalRepresentation(valueStruct.getString("argumentBaseTimeUnit"));

    //
    //  construct
    //

    return new SubcriteriaArgument(argumentExpression, argumentBaseTimeUnit);
  }

}
