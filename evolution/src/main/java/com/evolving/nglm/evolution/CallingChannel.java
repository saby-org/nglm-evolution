/*****************************************************************************
*
*  CallingChannel.java
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

@GUIDependencyDef(objectType = "callingChannel", serviceClass = CallingChannelService.class, dependencies = {  })
public class CallingChannel extends GUIManagedObject
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
    schemaBuilder.name("calling_channel");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),2));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("catalogCharacteristics", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<CallingChannel> serde = new ConnectSerde<CallingChannel>(schema, false, CallingChannel.class, CallingChannel::pack, CallingChannel::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<CallingChannel> serde() { return serde; }
  private List<String> catalogCharacteristics;

  /****************************************
  *
  *  data
  *
  ****************************************/

  // none currently

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getCallingChannelID() { return getGUIManagedObjectID(); }
  public String getDisplay() { return getGUIManagedObjectDisplay(); }
  public List<String> getCatalogCharacteristics() { return catalogCharacteristics; }
  
  

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public CallingChannel(SchemaAndValue schemaAndValue, List<String> catalogCharacteristics)
  {
    super(schemaAndValue);
    this.catalogCharacteristics = catalogCharacteristics;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    CallingChannel callingChannel = (CallingChannel) value;
    Struct struct = new Struct(schema);
    packCommon(struct, callingChannel);
    struct.put("catalogCharacteristics", callingChannel.getCatalogCharacteristics());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static CallingChannel unpack(SchemaAndValue schemaAndValue)
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
    List<String> catalogCharacteristics = (List<String>) valueStruct.get("catalogCharacteristics");
     
    //
    //  return
    //

    return new CallingChannel(schemaAndValue,catalogCharacteristics);
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public CallingChannel(JSONObject jsonRoot, long epoch, GUIManagedObject existingCallingChannelUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingCallingChannelUnchecked != null) ? existingCallingChannelUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingCallingChannel
    *
    *****************************************/

    CallingChannel existingCallingChannel = (existingCallingChannelUnchecked != null && existingCallingChannelUnchecked instanceof CallingChannel) ? (CallingChannel) existingCallingChannelUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    // none

    /*****************************************
    *
    *  validate
    *
    *****************************************/

    if (getRawEffectiveStartDate() != null) throw new GUIManagerException("unsupported start date", JSONUtilities.decodeString(jsonRoot, "effectiveStartDate", false));
    if (getRawEffectiveEndDate() != null) throw new GUIManagerException("unsupported end date", JSONUtilities.decodeString(jsonRoot, "effectiveEndDate", false));

    this.catalogCharacteristics = decodeCatalogCharacteristics(JSONUtilities.decodeJSONArray(jsonRoot, "catalogCharacteristics", true));


    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingCallingChannel))
      {
        this.setEpoch(epoch);
      }
  }
  
  private List<String> decodeCatalogCharacteristics(JSONArray jsonArray) throws GUIManagerException
  {
    List<String> catalogCharacteristics = new ArrayList<String>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject catalogCharacteristicJSON = (JSONObject) jsonArray.get(i);
        catalogCharacteristics.add(JSONUtilities.decodeString(catalogCharacteristicJSON, "catalogCharacteristicID", true));
      }
    return catalogCharacteristics;
  }
  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(CallingChannel existingCallingChannel)
  {
    if (existingCallingChannel != null && existingCallingChannel.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingCallingChannel.getGUIManagedObjectID());
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
   result.put("CatalogCharacteristic", getCatalogCharacteristics());
    return result;
  }
}
