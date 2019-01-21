/*****************************************************************************
*
*  JourneyObjectiveInstance.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.ServerRuntimeException;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import com.evolving.nglm.core.SystemTime;

import java.nio.charset.StandardCharsets;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class JourneyObjectiveInstance
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(JourneyObjectiveInstance.class);

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
    schemaBuilder.name("journey_objective_instance");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("journeyObjectiveID", Schema.STRING_SCHEMA);
    schemaBuilder.field("catalogCharacteristics", SchemaBuilder.array(CatalogCharacteristicInstance.schema()).schema());
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

  private String journeyObjectiveID;
  private Set<CatalogCharacteristicInstance> catalogCharacteristics;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private JourneyObjectiveInstance(String journeyObjectiveID, Set<CatalogCharacteristicInstance> catalogCharacteristics)
  {
    this.journeyObjectiveID = journeyObjectiveID;
    this.catalogCharacteristics = catalogCharacteristics;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  JourneyObjectiveInstance(JSONObject jsonRoot) throws GUIManagerException
  {
    this.journeyObjectiveID = JSONUtilities.decodeString(jsonRoot, "journeyObjectiveID", true);
    this.catalogCharacteristics = decodeCatalogCharacteristics(JSONUtilities.decodeJSONArray(jsonRoot, "catalogCharacteristics", false));
  }

  /*****************************************
  *
  *  decodeCatalogCharacteristics
  *
  *****************************************/

  private Set<CatalogCharacteristicInstance> decodeCatalogCharacteristics(JSONArray jsonArray) throws GUIManagerException
  {
    Set<CatalogCharacteristicInstance> result = new HashSet<CatalogCharacteristicInstance>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new CatalogCharacteristicInstance((JSONObject) jsonArray.get(i)));
          }
      }
    return result;
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getJourneyObjectiveID() { return journeyObjectiveID; }
  public Set<CatalogCharacteristicInstance> getCatalogCharacteristics() { return catalogCharacteristics; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<JourneyObjectiveInstance> serde()
  {
    return new ConnectSerde<JourneyObjectiveInstance>(schema, false, JourneyObjectiveInstance.class, JourneyObjectiveInstance::pack, JourneyObjectiveInstance::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    JourneyObjectiveInstance journeyJourneyObjective = (JourneyObjectiveInstance) value;
    Struct struct = new Struct(schema);
    struct.put("journeyObjectiveID", journeyJourneyObjective.getJourneyObjectiveID());
    struct.put("catalogCharacteristics", packCatalogCharacteristics(journeyJourneyObjective.getCatalogCharacteristics()));
    return struct;
  }

  /****************************************
  *
  *  packCatalogCharacteristics
  *
  ****************************************/

  private static List<Object> packCatalogCharacteristics(Set<CatalogCharacteristicInstance> catalogCharacteristics)
  {
    List<Object> result = new ArrayList<Object>();
    for (CatalogCharacteristicInstance catalogCharacteristic : catalogCharacteristics)
      {
        result.add(CatalogCharacteristicInstance.pack(catalogCharacteristic));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static JourneyObjectiveInstance unpack(SchemaAndValue schemaAndValue)
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
    String journeyObjectiveID = valueStruct.getString("journeyObjectiveID");
    Set<CatalogCharacteristicInstance> catalogCharacteristics = unpackCatalogCharacteristics(schema.field("catalogCharacteristics").schema(), valueStruct.get("catalogCharacteristics"));
    
    //
    //  return
    //

    return new JourneyObjectiveInstance(journeyObjectiveID, catalogCharacteristics);
  }

  /*****************************************
  *
  *  unpackJourneyCatalogCharacteristics
  *
  *****************************************/

  private static Set<CatalogCharacteristicInstance> unpackCatalogCharacteristics(Schema schema, Object value)
  {
    //
    //  get schema for JourneyCatalogCharacteristic
    //

    Schema propertySchema = schema.valueSchema();
    
    //
    //  unpack
    //

    Set<CatalogCharacteristicInstance> result = new HashSet<CatalogCharacteristicInstance>();
    List<Object> valueArray = (List<Object>) value;
    for (Object property : valueArray)
      {
        result.add(CatalogCharacteristicInstance.unpack(new SchemaAndValue(propertySchema, property)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof JourneyObjectiveInstance)
      {
        JourneyObjectiveInstance journeyJourneyObjective = (JourneyObjectiveInstance) obj;
        result = true;
        result = result && Objects.equals(journeyObjectiveID, journeyJourneyObjective.getJourneyObjectiveID());
        result = result && Objects.equals(catalogCharacteristics, journeyJourneyObjective.getCatalogCharacteristics());
      }
    return result;
  }

  /*****************************************
  *
  *  hashCode
  *
  *****************************************/

  public int hashCode()
  {
    return journeyObjectiveID.hashCode();
  }

}
