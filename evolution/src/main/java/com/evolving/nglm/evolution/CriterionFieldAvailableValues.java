/*****************************************************************************
*
*  CriterionFieldAvailableValues.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class CriterionFieldAvailableValues extends GUIManagedObject
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
    schemaBuilder.name("criterionfieldavailablevalues");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("availableValues", SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA).name("criterion_field_available_values").schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<CriterionFieldAvailableValues> serde = new ConnectSerde<CriterionFieldAvailableValues>(schema, false, CriterionFieldAvailableValues.class, CriterionFieldAvailableValues::pack, CriterionFieldAvailableValues::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<CriterionFieldAvailableValues> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  //id,display
  Set<Pair<String,String>> availableValues;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //

  public String getCriterionFieldAvailableValuesID() { return getGUIManagedObjectID(); }
  public Set<Pair<String,String>> getAvailableValues() { return availableValues; }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public CriterionFieldAvailableValues(SchemaAndValue schemaAndValue, Set<Pair<String,String>> availableValues)
  {
    super(schemaAndValue);
    this.availableValues = availableValues;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  private CriterionFieldAvailableValues(CriterionFieldAvailableValues criterionFieldAvailableValues)
  {
    super(criterionFieldAvailableValues.getJSONRepresentation(), criterionFieldAvailableValues.getEpoch());
    this.availableValues = criterionFieldAvailableValues.getAvailableValues();
  }

  /*****************************************
  *
  *  copy
  *
  *****************************************/

  public CriterionFieldAvailableValues copy()
  {
    return new CriterionFieldAvailableValues(this);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    CriterionFieldAvailableValues criterionFieldAvailableValues = (CriterionFieldAvailableValues) value;
    Struct struct = new Struct(schema);
    packCommon(struct, criterionFieldAvailableValues);
    struct.put("availableValues", packCriterionFieldAvailableValues(criterionFieldAvailableValues.getAvailableValues()));
    return struct;
  }
  
  /****************************************
  *
  *  packCriterionFieldAvailableValues
  *
  ****************************************/

  private static Object packCriterionFieldAvailableValues(Set<Pair<String,String>> availableValues)
  {
    Map<Object, Object> result = new HashMap<Object, Object>();
    for (Pair<String,String> groupID : availableValues)
      {
        String id = groupID.getFirstElement();
        String display = groupID.getSecondElement();
        result.put(id, display);
      }
    return result;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static CriterionFieldAvailableValues unpack(SchemaAndValue schemaAndValue)
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
    Set<Pair<String,String>> availableValues = unpackCriterionFieldAvailableValues(valueStruct.get("availableValues"));

    //
    //  return
    //

    return new CriterionFieldAvailableValues(schemaAndValue, availableValues);
  }
  
  /*****************************************
  *
  *  unpackCriterionFieldAvailableValues
  *
  *****************************************/

  private static Set<Pair<String,String>> unpackCriterionFieldAvailableValues(Object value)
  {
    Set<Pair<String,String>> result = new HashSet<Pair<String,String>>();
    if (value != null)
      {
        Map<String, String> valueMap = (Map<String, String>) value;
        for (String packedGroupID : valueMap.keySet())
          {
            Pair<String,String> pair = new Pair<String,String>(packedGroupID, valueMap.get(packedGroupID));
            result.add(pair);
          }
      }
    return result;
  }
  
  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public CriterionFieldAvailableValues(JSONObject jsonRoot, long epoch, GUIManagedObject existingCriterionFieldAvailableValuesUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingCriterionFieldAvailableValuesUnchecked != null) ? existingCriterionFieldAvailableValuesUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingPoint
    *
    *****************************************/

    CriterionFieldAvailableValues existingCriterionFieldAvailableValues = (existingCriterionFieldAvailableValuesUnchecked != null && existingCriterionFieldAvailableValuesUnchecked instanceof CriterionFieldAvailableValues) ? (CriterionFieldAvailableValues) existingCriterionFieldAvailableValuesUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.availableValues = decodeCriterionFieldAvailableValues(JSONUtilities.decodeJSONArray(jsonRoot, "availableValues", new JSONArray()));

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingCriterionFieldAvailableValues))
      {
        this.setEpoch(epoch);
      }
  }
  
  /*****************************************
  *
  *  decodeCriterionFieldAvailableValues
  *
  *****************************************/
  
  private Set<Pair<String,String>> decodeCriterionFieldAvailableValues(JSONArray jsonArray)
  {
    Set<Pair<String,String>> availableValues = new HashSet<Pair<String,String>>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            JSONObject availableValuesJSON = (JSONObject) jsonArray.get(i);
            String id = JSONUtilities.decodeString(availableValuesJSON, "id", true);
            String display = JSONUtilities.decodeString(availableValuesJSON, "display", true);
            Pair<String, String> pair = new Pair<String, String>(id, display);
            availableValues.add(pair);
          }
      }
    return availableValues;
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(CriterionFieldAvailableValues criterionFieldAvailableValues)
  {
    if (criterionFieldAvailableValues != null && criterionFieldAvailableValues.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getAvailableValues(), criterionFieldAvailableValues.getAvailableValues());
        return epochChanged;
      }
    else
      {
        return true;
      }
   }
}
