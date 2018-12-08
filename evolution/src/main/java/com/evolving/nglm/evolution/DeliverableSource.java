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
    standardDateFormat.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("name", Schema.STRING_SCHEMA);
    schemaBuilder.field("display", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("valid", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("active", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("effectiveStartDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("effectiveEndDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("fulfillmentProviderID", Schema.STRING_SCHEMA);
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
  private boolean valid;
  private boolean active;
  private Date effectiveStartDate;
  private Date effectiveEndDate;
  private String fulfillmentProviderID;
  private int unitaryCost;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getID() { return id; }
  public String getName() { return name; }
  public String getDisplay() { return display; }
  public boolean getValid() { return valid; }
  public boolean getActive() { return active; }
  Date getEffectiveStartDate() { return (effectiveStartDate != null) ? effectiveStartDate : NGLMRuntime.BEGINNING_OF_TIME; }
  Date getEffectiveEndDate() { return (effectiveEndDate != null) ? effectiveEndDate : NGLMRuntime.END_OF_TIME; }
  public String getFulfillmentProviderID() { return fulfillmentProviderID; }
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
    deliverableJSON.put("valid", valid);
    deliverableJSON.put("active", active);
    deliverableJSON.put("effectiveStartDate", formatDateField(effectiveStartDate));
    deliverableJSON.put("effectiveEndDate", formatDateField(effectiveEndDate));
    deliverableJSON.put("fulfillmentProviderID", fulfillmentProviderID);
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
    this.valid = JSONUtilities.decodeBoolean(jsonRoot, "valid", Boolean.FALSE);
    this.active = JSONUtilities.decodeBoolean(jsonRoot, "active", Boolean.FALSE);
    this.effectiveStartDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "effectiveStartDate", false));
    this.effectiveEndDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "effectiveEndDate", false));
    this.fulfillmentProviderID = JSONUtilities.decodeString(jsonRoot, "fulfillmentProviderID", true);
    this.unitaryCost = JSONUtilities.decodeInteger(jsonRoot, "unitaryCost", true);
  }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public DeliverableSource(String name, String display, boolean valid, boolean active, Date effectiveStartDate, Date effectiveEndDate, String fulfillmentProviderID, int unitaryCost)
  {
    this.id = null;
    this.name = name;
    this.display = display;
    this.valid = valid;
    this.active = active;
    this.effectiveStartDate = effectiveStartDate;
    this.effectiveEndDate = effectiveEndDate;
    this.fulfillmentProviderID = fulfillmentProviderID;
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
    struct.put("valid", deliverableSource.getValid());
    struct.put("active", deliverableSource.getActive());
    struct.put("effectiveStartDate", deliverableSource.getEffectiveStartDate());
    struct.put("effectiveEndDate", deliverableSource.getEffectiveEndDate());
    struct.put("fulfillmentProviderID", deliverableSource.getFulfillmentProviderID());
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
    boolean valid = valueStruct.getBoolean("valid");
    boolean active = valueStruct.getBoolean("active");
    Date effectiveStartDate = (Date) valueStruct.get("effectiveStartDate");
    Date effectiveEndDate = (Date) valueStruct.get("effectiveEndDate");
    String fulfillmentProviderID = valueStruct.getString("fulfillmentProviderID");
    int unitaryCost = valueStruct.getInt32("unitaryCost");
    
    //
    //  return
    //

    return new DeliverableSource(name, display, valid, active, effectiveStartDate, effectiveEndDate, fulfillmentProviderID, unitaryCost);
  }
  
  /*****************************************
  *
  *  parseDateField
  *
  *****************************************/

  protected Date parseDateField(String stringDate) throws JSONUtilitiesException
  {
    Date result = null;
    try
      {
        if (stringDate != null && stringDate.trim().length() > 0)
          {
            synchronized (standardDateFormat)
              {
                result = standardDateFormat.parse(stringDate.trim());
              }
          }
      }
    catch (ParseException e)
      {
        throw new JSONUtilitiesException("parseDateField", e);
      }
    return result;
  }
  
  /*****************************************
  *
  *  formatDateField
  *
  *****************************************/

  protected static String formatDateField(Date date)
  {
    String result = null;
    if (date != null)
      {
        synchronized (standardDateFormat)
          {
            result = standardDateFormat.format(date);
          }
      }
    return result;
  }
}
