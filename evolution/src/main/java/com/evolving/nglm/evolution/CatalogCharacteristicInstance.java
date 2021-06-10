package com.evolving.nglm.evolution;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

/****************************************************************************
*
*  CatalogCharacteristicInstance
*
****************************************************************************/

public class CatalogCharacteristicInstance
{
  
  //
  //  logger
  //
  
  private static final Logger log = LoggerFactory.getLogger(CatalogCharacteristicInstance.class);

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
    schemaBuilder.name("offer_catalog_characteristic");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("catalogCharacteristicID", Schema.STRING_SCHEMA);
    schemaBuilder.field("value", ParameterMap.schema());
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

  private String catalogCharacteristicID;
  private ParameterMap value;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private CatalogCharacteristicInstance(String catalogCharacteristicID, ParameterMap value)
  {
    this.catalogCharacteristicID = catalogCharacteristicID;
    this.value = value;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  CatalogCharacteristicInstance(JSONObject jsonRoot, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
  {
    //
    //  catalog characteristic
    //

    this.catalogCharacteristicID = JSONUtilities.decodeString(jsonRoot, "catalogCharacteristicID", true);
    CatalogCharacteristic catalogCharacteristic = catalogCharacteristicService.getActiveCatalogCharacteristic(catalogCharacteristicID, SystemTime.getCurrentTime());
    CriterionDataType dataType = (catalogCharacteristic != null) ? catalogCharacteristic.getDataType() : CriterionDataType.Unknown;

    //
    //  parse value
    //

    Object value = null;
    switch (dataType)
      {
        case IntegerCriterion:
          value = JSONUtilities.decodeInteger(jsonRoot, "value", false);
          break;

        case DoubleCriterion:
          value = JSONUtilities.decodeDouble(jsonRoot, "value", false);
          break;

        case StringCriterion:
          value = JSONUtilities.decodeString(jsonRoot, "value", false);
          break;

        case DateCriterion:
          value = GUIManagedObject.parseDateField(JSONUtilities.decodeString(jsonRoot, "value", false));
          break;

        case BooleanCriterion:
          value = JSONUtilities.decodeBoolean(jsonRoot, "value", false);
          break;

        case StringSetCriterion:
          JSONArray jsonArrayString = JSONUtilities.decodeJSONArray(jsonRoot, "value", false);
          Set<Object> stringSetValue = new HashSet<Object>();
          for (int i=0; i<jsonArrayString.size(); i++)
            {
              stringSetValue.add(jsonArrayString.get(i));
            }
          value = stringSetValue;
          break;

        case IntegerSetCriterion:
          JSONArray jsonArrayInteger = JSONUtilities.decodeJSONArray(jsonRoot, "value", false);
          Set<Object> integerSetValue = new HashSet<Object>();
          for (int i=0; i<jsonArrayInteger.size(); i++)
            {
              integerSetValue.add(new Integer(((Number) jsonArrayInteger.get(i)).intValue()));
            }
          value = integerSetValue;
          break;
          
        case DoubleSetCriterion:
            JSONArray jsonArrayDouble = JSONUtilities.decodeJSONArray(jsonRoot, "value", false);
            Set<Object> doubleSetValue = new HashSet<Object>();
            for (int i=0; i<jsonArrayDouble.size(); i++)
              {
            	doubleSetValue.add(new Integer(((Number) jsonArrayDouble.get(i)).intValue()));
              }
            value = doubleSetValue;
            break;

        case TimeCriterion:
        case AniversaryCriterion:
        default:
          throw new GUIManagerException("unsupported catalogCharacteristic data type", catalogCharacteristicID);
      }

    //
    //  store in singleton parameterMap
    //
          
    this.value = new ParameterMap();
    this.value.put("value", value);
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getCatalogCharacteristicID() { return catalogCharacteristicID; }
  public Object getValue() { return value.get("value"); }
  private ParameterMap getParameterMap() { return value; }
  
  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<CatalogCharacteristicInstance> serde()
  {
    return new ConnectSerde<CatalogCharacteristicInstance>(schema, false, CatalogCharacteristicInstance.class, CatalogCharacteristicInstance::pack, CatalogCharacteristicInstance::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    CatalogCharacteristicInstance offerCatalogCharacteristic = (CatalogCharacteristicInstance) value;
    Struct struct = new Struct(schema);
    struct.put("catalogCharacteristicID", offerCatalogCharacteristic.getCatalogCharacteristicID());
    struct.put("value", ParameterMap.pack(offerCatalogCharacteristic.getParameterMap()));
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static CatalogCharacteristicInstance unpack(SchemaAndValue schemaAndValue)
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
    String catalogCharacteristicID = valueStruct.getString("catalogCharacteristicID");
    ParameterMap parameterMap = ParameterMap.unpack(new SchemaAndValue(schema.field("value").schema(), valueStruct.get("value")));

    //
    //  return
    //

    return new CatalogCharacteristicInstance(catalogCharacteristicID, parameterMap);
  }

  /*****************************************
  *
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof CatalogCharacteristicInstance)
      {
        CatalogCharacteristicInstance offerCatalogCharacteristic = (CatalogCharacteristicInstance) obj;
        result = true;
        result = result && Objects.equals(catalogCharacteristicID, offerCatalogCharacteristic.getCatalogCharacteristicID());
        result = result && Objects.equals(value, offerCatalogCharacteristic.getParameterMap());
      }
    return result;
  }
  
  /*****************************************
  *
  *  equalsNonRobustly
  *
  *****************************************/

  public boolean equalsNonRobustly(Object obj)
  {
    log.info("RAJ K equalsNonRobustly between this {} and obj {}", this, obj);
    boolean result = false;
    if (obj instanceof CatalogCharacteristicInstance)
      {
        CatalogCharacteristicInstance offerCatalogCharacteristic = (CatalogCharacteristicInstance) obj;
        result = true;
        result = result && Objects.equals(catalogCharacteristicID, offerCatalogCharacteristic.getCatalogCharacteristicID());
        log.info("RAJ K equalsNonRobustly catalogCharacteristicID match result is {}", result);
        if (result && getValue() instanceof Set)
          {
            
            Set<Object> thisValue = (Set<Object>) getValue();
            Set<Object> reqValue = (Set<Object>) offerCatalogCharacteristic.getValue();
            result = result && thisValue.stream().filter(reqValue::contains).count() > 0L;
            log.info("RAJ K equalsNonRobustly instanceof Set result is {}", result);
          }
        else if (result)
          {
            result = result && Objects.equals(value, offerCatalogCharacteristic.getParameterMap());
            log.info("RAJ K equalsNonRobustly normal result is {}", result);
          }
      }
    log.info("RAJ K equalsNonRobustly result is {}", result);
    return result;
  }

  /*****************************************
  *
  *  hashCode
  *
  *****************************************/

  public int hashCode()
  {
    return catalogCharacteristicID.hashCode();
  }
  
  @Override
  public String toString()
  {
    return "CatalogCharacteristicInstance [catalogCharacteristicID=" + catalogCharacteristicID + ", value=" + value.get("value") + "]";
  }
}
