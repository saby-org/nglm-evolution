/*****************************************************************************
*
*  GUIManagedObject.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SchemaUtilities;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONObject;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.json.simple.parser.JSONParser;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;

public abstract class GUIManagedObject
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  protected static final Logger log = LoggerFactory.getLogger(GUIManagedObject.class);

  /*****************************************
  *
  *  standard formats
  *
  *****************************************/

  private static SimpleDateFormat standardDateFormat;
  static
  {
    standardDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSSXX");
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

  private static Schema commonSchema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("guimanager_managed_object");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("jsonRepresentation", Schema.STRING_SCHEMA);
    schemaBuilder.field("guiManagedObjectID", Schema.STRING_SCHEMA);
    schemaBuilder.field("epoch", Schema.INT64_SCHEMA);
    schemaBuilder.field("effectiveStartDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("effectiveEndDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("valid", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("active", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("name", Schema.STRING_SCHEMA);
    commonSchema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<GUIManagedObject> commonSerde;
  private static ConnectSerde<GUIManagedObject> incompleteObjectSerde;
  static
  {
    List<ConnectSerde<? extends GUIManagedObject>> guiManagedObjectSerdes = new ArrayList<ConnectSerde<? extends GUIManagedObject>>();
    guiManagedObjectSerdes.add(Journey.serde());
    guiManagedObjectSerdes.add(Offer.serde());
    guiManagedObjectSerdes.add(IncompleteObject.serde());
    commonSerde = new ConnectSerde<GUIManagedObject>("guiManagedObject", false, guiManagedObjectSerdes.toArray(new ConnectSerde[0]));
    incompleteObjectSerde = new ConnectSerde<GUIManagedObject>("guiManagedObjectIncomplete", false, IncompleteObject::unpack, guiManagedObjectSerdes.toArray(new ConnectSerde[0]));
  }

  //
  //  accessor
  //

  public static Schema commonSchema() { return commonSchema; }
  public static ConnectSerde<GUIManagedObject> commonSerde() { return commonSerde; }
  public static ConnectSerde<GUIManagedObject> incompleteObjectSerde() { return incompleteObjectSerde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private JSONObject jsonRepresentation;
  private String guiManagedObjectID;
  private long epoch;
  private Date effectiveStartDate;
  private Date effectiveEndDate;
  private boolean valid;
  private boolean active;
  private String name;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //
  
  public String getGUIManagedObjectID() { return guiManagedObjectID; }
  public long getEpoch() { return epoch; }
  public String getName() { return name; }

  //
  //  package protected
  //

  JSONObject getJSONRepresentation() { return jsonRepresentation; }
  Date getEffectiveStartDate() { return (effectiveStartDate != null) ? effectiveStartDate : NGLMRuntime.BEGINNING_OF_TIME; }
  Date getEffectiveEndDate() { return (effectiveEndDate != null) ? effectiveEndDate : NGLMRuntime.END_OF_TIME; }
  boolean getValid() { return valid; }
  boolean getActive() { return active; }

  //
  //  private
  //

  protected Date getRawEffectiveStartDate() { return effectiveStartDate; }
  protected Date getRawEffectiveEndDate() { return effectiveEndDate; }

  //
  //  calculated
  //

  public boolean getAccepted() { return ! (this instanceof IncompleteObject); }

  /*****************************************
  *
  *  setter
  *
  *****************************************/

  protected void setEpoch(long epoch)
  {
    this.epoch = epoch;
  }

  /*****************************************
  *
  *  packCommon
  *
  *****************************************/
  
  protected static void packCommon(Struct struct, GUIManagedObject guiManagedObject)
  {
    struct.put("jsonRepresentation", guiManagedObject.getJSONRepresentation().toString());
    struct.put("guiManagedObjectID", guiManagedObject.getGUIManagedObjectID());
    struct.put("epoch", guiManagedObject.getEpoch());
    struct.put("effectiveStartDate", guiManagedObject.getRawEffectiveStartDate());
    struct.put("effectiveEndDate", guiManagedObject.getRawEffectiveEndDate());
    struct.put("valid", guiManagedObject.getValid());
    struct.put("active", guiManagedObject.getActive());
    struct.put("name", guiManagedObject.getName());
  }

  /*****************************************
  *
  *  construct -- unpack
  *
  *****************************************/

  protected GUIManagedObject(SchemaAndValue schemaAndValue)
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
    JSONObject jsonRepresentation = parseRepresentation(valueStruct.getString("jsonRepresentation"));
    String guiManagedObjectID = valueStruct.getString("guiManagedObjectID");
    long epoch = valueStruct.getInt64("epoch");
    Date effectiveStartDate = (Date) valueStruct.get("effectiveStartDate");
    Date effectiveEndDate = (Date) valueStruct.get("effectiveEndDate");
    boolean valid = valueStruct.getBoolean("valid");
    boolean active = valueStruct.getBoolean("active");
    String name = valueStruct.getString("name");

    //
    //  return
    //

    this.jsonRepresentation = jsonRepresentation;
    this.guiManagedObjectID = guiManagedObjectID;
    this.epoch = epoch;
    this.effectiveStartDate = effectiveStartDate;
    this.effectiveEndDate = effectiveEndDate;
    this.valid = valid;
    this.active = active;
    this.name = name;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  public GUIManagedObject(JSONObject jsonRoot, String idField, long epoch)
  {
    this.jsonRepresentation = jsonRoot;
    this.guiManagedObjectID = JSONUtilities.decodeString(jsonRoot, idField, true);
    this.epoch = epoch;
    this.effectiveStartDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "effectiveStartDate", false));
    this.effectiveEndDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "effectiveEndDate", false));
    this.valid = JSONUtilities.decodeBoolean(jsonRoot, "valid", Boolean.FALSE);
    this.active = JSONUtilities.decodeBoolean(jsonRoot, "active", Boolean.FALSE);
    this.name = JSONUtilities.decodeString(jsonRoot, "name", "N/A"); 
  }

  /*****************************************
  *
  *  parseRepresentation
  *
  *****************************************/

  private static JSONObject parseRepresentation(String jsonString) throws JSONUtilitiesException
  {
    JSONObject result = null;
    try
      {
        result = (JSONObject) (new JSONParser()).parse(jsonString);
      }
    catch (org.json.simple.parser.ParseException e)
      {
        throw new JSONUtilitiesException("jsonRepresentation", e);
      }
    return result;
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
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof GUIManagedObject)
      {
        GUIManagedObject guiManagedObject = (GUIManagedObject) obj;
        result = true;
        result = result && Objects.equals(name, guiManagedObject.getName());
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
    return name.hashCode();
  }

  /****************************************
  *
  *  class IncompleteObject
  *
  ****************************************/

  public static class IncompleteObject extends GUIManagedObject
  {
    /*****************************************
    *
    *  schema -- rule
    *
    *****************************************/

    //
    //  schema
    //

    private static Schema schema = null;
    static
    {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      schemaBuilder.name("guimanager_incomplete");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
      for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<IncompleteObject> serde = new ConnectSerde<IncompleteObject>(schema, false, IncompleteObject.class, IncompleteObject::pack, IncompleteObject::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<IncompleteObject> serde() { return serde; }

    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    public IncompleteObject(SchemaAndValue schemaAndValue)
    {
      super(schemaAndValue);
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      //
      //  incompleteOffer
      //

      IncompleteObject incompleteObject = (IncompleteObject) value;

      //
      //  return
      //

      Struct struct = new Struct(schema);
      packCommon(struct, incompleteObject);
      return struct;
    }

    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static IncompleteObject unpack(SchemaAndValue schemaAndValue)
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

      //
      //  return
      //

      return new IncompleteObject(schemaAndValue);
    }

    /*****************************************
    *
    *  constructor -- external JSON
    *
    *****************************************/

    public IncompleteObject(JSONObject jsonRoot, String idField, long epoch)
    {
      super(jsonRoot, idField, epoch);
    }
  }
}
