/*****************************************************************************
*
*  DeliverableSource.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.NGLMRuntime;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;

public class DeliverableSource
{
  /*****************************************
  *
  *  standard formats
  *
  *****************************************/

  private static SimpleDateFormat standardDateFormat;
  static
  {
    standardDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSSXXX");
    standardDateFormat.setTimeZone(TimeZone.getTimeZone(Deployment.getSystemTimeZone())); // TODO EVPRO-99 use systemTimeZone instead of baseTimeZone, is it correct or should it be per tenant ???
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
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("deliverablesource");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("name", Schema.STRING_SCHEMA);
    schemaBuilder.field("display", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("active", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("effectiveStartDate", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("effectiveEndDate", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("fulfillmentProviderID", Schema.STRING_SCHEMA);
    schemaBuilder.field("externalAccountID", Schema.STRING_SCHEMA);
    schemaBuilder.field("unitaryCost", Schema.INT32_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<DeliverableSource> serde = new ConnectSerde<DeliverableSource>(schema, false, DeliverableSource.class, DeliverableSource::pack, DeliverableSource::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<DeliverableSource> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String id;
  private String name;
  private String display;
  private boolean active;
  private String effectiveStartDate;
  private String effectiveEndDate;
  private String fulfillmentProviderID;
  private String externalAccountID;
  private int unitaryCost;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getID() { return id; }
  public String getName() { return name; }
  public String getDisplay() { return display; }
  public boolean getActive() { return active; }
  String getEffectiveStartDate() { return effectiveStartDate; }
  String getEffectiveEndDate() { return effectiveEndDate; }
  public String getFulfillmentProviderID() { return fulfillmentProviderID; }
  public String getCommodityID() { return externalAccountID; }
  public int getUnitaryCost() { return unitaryCost; }

  /*****************************************
  *
  *  getDeliverableJSON
  *
  *****************************************/

  public JSONObject getDeliverableJSON()
  {
    HashMap<String,Object> deliverableJSON = new HashMap<String,Object>();
    deliverableJSON.put("id", id);
    deliverableJSON.put("name", name);
    deliverableJSON.put("display", display);
    deliverableJSON.put("active", active);
    deliverableJSON.put("effectiveStartDate", effectiveStartDate);
    deliverableJSON.put("effectiveEndDate", effectiveEndDate);
    deliverableJSON.put("fulfillmentProviderID", fulfillmentProviderID);
    deliverableJSON.put("externalAccountID", externalAccountID);
    deliverableJSON.put("unitaryCost", new Integer(unitaryCost));
    return JSONUtilities.encodeObject(deliverableJSON);
  }
  
  /*****************************************
  *
  *  setters
  *
  *****************************************/

  public void setID(String id) { this.id = id; }
  
  /*****************************************
  *
  *  constructor -- json
  *
  *****************************************/

  public DeliverableSource(JSONObject jsonRoot)
  {
    this.id = null;
    this.name = JSONUtilities.decodeString(jsonRoot, "name", true);
    this.display = JSONUtilities.decodeString(jsonRoot, "display", false);
    this.active = JSONUtilities.decodeBoolean(jsonRoot, "active", true);
    this.effectiveStartDate = JSONUtilities.decodeString(jsonRoot, "effectiveStartDate", false);
    this.effectiveEndDate = JSONUtilities.decodeString(jsonRoot, "effectiveEndDate", false);
    this.fulfillmentProviderID = JSONUtilities.decodeString(jsonRoot, "fulfillmentProviderID", true);
    this.externalAccountID = (jsonRoot.get("externalAccountID") != null) ? JSONUtilities.decodeString(jsonRoot, "externalAccountID", true) : JSONUtilities.decodeString(jsonRoot, "commodityID", true);
    this.unitaryCost = JSONUtilities.decodeInteger(jsonRoot, "unitaryCost", true);
  }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public DeliverableSource(String name, String display, boolean active, String effectiveStartDate, String effectiveEndDate, String fulfillmentProviderID, String externalAccountID, int unitaryCost)
  {
    this.id = null;
    this.name = name;
    this.display = display;
    this.active = active;
    this.effectiveStartDate = effectiveStartDate;
    this.effectiveEndDate = effectiveEndDate;
    this.fulfillmentProviderID = fulfillmentProviderID;
    this.externalAccountID = externalAccountID;
    this.unitaryCost = unitaryCost;
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    DeliverableSource deliverableSource = (DeliverableSource) value;
    Struct struct = new Struct(schema);
    struct.put("name", deliverableSource.getName());
    struct.put("display", deliverableSource.getDisplay());
    struct.put("active", deliverableSource.getActive());
    struct.put("effectiveStartDate", deliverableSource.getEffectiveStartDate());
    struct.put("effectiveEndDate", deliverableSource.getEffectiveEndDate());
    struct.put("fulfillmentProviderID", deliverableSource.getFulfillmentProviderID());
    struct.put("externalAccountID", deliverableSource.getCommodityID());
    struct.put("unitaryCost", deliverableSource.getUnitaryCost());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static DeliverableSource unpack(SchemaAndValue schemaAndValue)
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
    String name = valueStruct.getString("name");
    String display = valueStruct.getString("display");
    boolean active = valueStruct.getBoolean("active");
    String effectiveStartDate = valueStruct.getString("effectiveStartDate");
    String effectiveEndDate = valueStruct.getString("effectiveEndDate");
    String fulfillmentProviderID = valueStruct.getString("fulfillmentProviderID");
    String externalAccountID = (schemaVersion >= 2) ? valueStruct.getString("externalAccountID") : fulfillmentProviderID; 
    int unitaryCost = valueStruct.getInt32("unitaryCost");
    
    //
    //  return
    //

    return new DeliverableSource(name, display, active, effectiveStartDate, effectiveEndDate, fulfillmentProviderID, externalAccountID, unitaryCost);
  }
}
