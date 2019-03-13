/*****************************************************************************
*
*  SegmentEligibility.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class PointValidity 
{

  /*****************************************
  *
  *  configuration
  *
  *****************************************/
  
  //
  //  ValidityType
  //

  public static enum ValidityType{
    FIXED("fixed"),
    VARIABLE("variable"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private ValidityType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static ValidityType fromExternalRepresentation(String externalRepresentation) { for (ValidityType enumeratedValue : ValidityType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }
  
  //
  //  TimeUnit
  //

  public enum PeriodType
  {
    Minutes("minutes"),
    Hours("hours"),
    Days("days"),
    Weeks("weeks"),
    Months("months"),
    Quarters("quarters"),
    Semesters("semesters"),
    Years("years"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private PeriodType(String externalRepresentation) { this.externalRepresentation = externalRepresentation;}
    public String getExternalRepresentation() { return externalRepresentation; }
    public static PeriodType fromExternalRepresentation(String externalRepresentation) { for (PeriodType enumeratedValue : PeriodType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }
  
  //
  //  PeriodRoundUp
  //
  
  public static enum PeriodRoundUp {
    BEGINNING_OF_CURRENT_PERIOD("begining of current period"),
    BEGINNING_OF_NEXT_PERIOD("begining of next period"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private PeriodRoundUp(String externalRepresentation) { this.externalRepresentation = externalRepresentation;}
    public String getExternalRepresentation() { return externalRepresentation; }
    public static PeriodRoundUp fromExternalRepresentation(String externalRepresentation) { for (PeriodRoundUp enumeratedValue : PeriodRoundUp.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
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
    //
    //  schema
    //
    
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("point_validity");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("type", Schema.STRING_SCHEMA);
    schemaBuilder.field("periodType", Schema.STRING_SCHEMA);
    schemaBuilder.field("periodQuantity", Schema.INT32_SCHEMA);
    schemaBuilder.field("periodRoundUp", Schema.OPTIONAL_STRING_SCHEMA);
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

  private ValidityType type;
  private PeriodType periodType;
  private int periodQuantity;
  private PeriodRoundUp periodRoundUp;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private PointValidity(ValidityType type, PeriodType periodType, int periodQuantity, PeriodRoundUp periodRoundUp)
  {
    this.type = type;
    this.periodType = periodType;
    this.periodQuantity = periodQuantity;
    this.periodRoundUp = periodRoundUp;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  PointValidity(JSONObject jsonRoot) throws GUIManagerException
  {
    this.type = ValidityType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "type", true));
    this.periodType = PeriodType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "periodType", true));
    this.periodQuantity = JSONUtilities.decodeInteger(jsonRoot, "periodQuantity", true);
    if(JSONUtilities.decodeString(jsonRoot, "periodRoundUp", false) != null){
      this.periodRoundUp = PeriodRoundUp.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "periodRoundUp", false));
    }
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public ValidityType getType() { return type; }
  public PeriodType getPeriodType() { return periodType; }
  public int getPeriodQuantity() { return periodQuantity; }
  public PeriodRoundUp getPeriodRoundUp() { return periodRoundUp; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<PointValidity> serde()
  {
    return new ConnectSerde<PointValidity>(schema, false, PointValidity.class, PointValidity::pack, PointValidity::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    PointValidity segment = (PointValidity) value;
    Struct struct = new Struct(schema);
    struct.put("type", segment.getType().getExternalRepresentation());
    struct.put("periodType", segment.getPeriodType().getExternalRepresentation());
    struct.put("periodQuantity", segment.getPeriodQuantity());
    if(segment.getPeriodRoundUp() != null){
      struct.put("periodRoundUp", segment.getPeriodRoundUp().getExternalRepresentation());
    }
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static PointValidity unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    ValidityType type = ValidityType.fromExternalRepresentation(valueStruct.getString("type"));
    PeriodType periodType = PeriodType.fromExternalRepresentation(valueStruct.getString("periodType"));
    int periodQuantity = valueStruct.getInt32("periodQuantity");
    PeriodRoundUp periodRoundUp = null;
    if(valueStruct.getString("periodRoundUp") != null){
      periodRoundUp = PeriodRoundUp.fromExternalRepresentation(valueStruct.getString("periodRoundUp"));
    }

    //
    //  return
    //

    return new PointValidity(type, periodType, periodQuantity, periodRoundUp);
  }
  
}
