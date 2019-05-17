/*****************************************************************************
*
*  GUIManagedObject.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONObject;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.json.simple.parser.JSONParser;

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
  *  enum
  *
  *****************************************/

  //
  //  GUIManagedObjectType
  //

  public enum GUIManagedObjectType
  {
    Journey("journey"),
    Campaign("campaign"),
    Other("other"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private GUIManagedObjectType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static GUIManagedObjectType fromExternalRepresentation(String externalRepresentation) { for (GUIManagedObjectType enumeratedValue : GUIManagedObjectType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }
  

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

  private static List<SimpleDateFormat> standardDateFormats;
  static
  {
    //
    //  standardDateFormats
    //
        
    standardDateFormats = new ArrayList<SimpleDateFormat>();
    standardDateFormats.add(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSSXXX"));
    standardDateFormats.add(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX"));

    //
    //  setTimeZone
    //

    for (SimpleDateFormat standardDateFormat : standardDateFormats)
      {
        standardDateFormat.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
      }
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
    schemaBuilder.field("guiManagedObjectName", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("guiManagedObjectType", Schema.STRING_SCHEMA);
    schemaBuilder.field("epoch", Schema.INT64_SCHEMA);
    schemaBuilder.field("effectiveStartDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("effectiveEndDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("readOnly", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("active", Schema.BOOLEAN_SCHEMA);
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
    guiManagedObjectSerdes.add(SegmentationDimensionEligibility.serde());
    guiManagedObjectSerdes.add(SegmentationDimensionFileImport.serde());
    guiManagedObjectSerdes.add(SegmentationDimensionRanges.serde());
    guiManagedObjectSerdes.add(Point.serde());
    guiManagedObjectSerdes.add(Offer.serde());
    guiManagedObjectSerdes.add(PresentationStrategy.serde());
    guiManagedObjectSerdes.add(ScoringStrategy.serde());
    guiManagedObjectSerdes.add(CallingChannel.serde());
    guiManagedObjectSerdes.add(SalesChannel.serde());
    guiManagedObjectSerdes.add(Supplier.serde());
    guiManagedObjectSerdes.add(PaymentMean.serde());
    guiManagedObjectSerdes.add(Product.serde());
    guiManagedObjectSerdes.add(CatalogCharacteristic.serde());
    guiManagedObjectSerdes.add(ContactPolicy.serde());
    guiManagedObjectSerdes.add(JourneyObjective.serde());
    guiManagedObjectSerdes.add(OfferObjective.serde());
    guiManagedObjectSerdes.add(ProductType.serde());
    guiManagedObjectSerdes.add(UCGRule.serde());
    guiManagedObjectSerdes.add(Deliverable.serde());
    guiManagedObjectSerdes.add(TokenType.serde());
    guiManagedObjectSerdes.add(IncompleteObject.serde());
    guiManagedObjectSerdes.add(Report.serde());
    guiManagedObjectSerdes.add(MailTemplate.serde());
    guiManagedObjectSerdes.add(SMSTemplate.serde());
    guiManagedObjectSerdes.add(UploadedFile.serde());
    guiManagedObjectSerdes.add(UploadedTarget.serde());
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
  private String guiManagedObjectName;
  private GUIManagedObjectType guiManagedObjectType;
  private long epoch;
  private Date effectiveStartDate;
  private Date effectiveEndDate;
  private boolean readOnly;
  private boolean active;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //
  
  public String getGUIManagedObjectID() { return guiManagedObjectID; }
  public String getGUIManagedObjectName() { return guiManagedObjectName; }
  public GUIManagedObjectType getGUIManagedObjectType() { return guiManagedObjectType; }
  public JSONObject getJSONRepresentation() { return jsonRepresentation; }
  public long getEpoch() { return epoch; }
  public Date getEffectiveStartDate() { return (effectiveStartDate != null) ? effectiveStartDate : NGLMRuntime.BEGINNING_OF_TIME; }
  public Date getEffectiveEndDate() { return (effectiveEndDate != null) ? effectiveEndDate : NGLMRuntime.END_OF_TIME; }
  
  //
  //  package protected
  //

  boolean getReadOnly() { return readOnly; }
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
    struct.put("guiManagedObjectName", guiManagedObject.getGUIManagedObjectName());
    struct.put("guiManagedObjectType", guiManagedObject.getGUIManagedObjectType().getExternalRepresentation());
    struct.put("epoch", guiManagedObject.getEpoch());
    struct.put("effectiveStartDate", guiManagedObject.getRawEffectiveStartDate());
    struct.put("effectiveEndDate", guiManagedObject.getRawEffectiveEndDate());
    struct.put("readOnly", guiManagedObject.getReadOnly());
    struct.put("active", guiManagedObject.getActive());
  }

  /*****************************************
  *
  *  constructor -- incomplete
  *
  *****************************************/

  protected GUIManagedObject(String guiManagedObjectID)
  {
    this.jsonRepresentation = new JSONObject();
    this.guiManagedObjectID = guiManagedObjectID;
    this.guiManagedObjectName = null;
    this.guiManagedObjectType = GUIManagedObjectType.Other;
    this.epoch = -1;
    this.effectiveStartDate = null;
    this.effectiveEndDate = null;
    this.readOnly = false;
    this.active = false;
  }
                             
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  protected GUIManagedObject(SchemaAndValue schemaAndValue)
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
    JSONObject jsonRepresentation = parseRepresentation(valueStruct.getString("jsonRepresentation"));
    String guiManagedObjectID = valueStruct.getString("guiManagedObjectID");
    String guiManagedObjectName = valueStruct.getString("guiManagedObjectName");
    GUIManagedObjectType guiManagedObjectType = GUIManagedObjectType.fromExternalRepresentation(valueStruct.getString("guiManagedObjectType"));
    long epoch = valueStruct.getInt64("epoch");
    Date effectiveStartDate = (Date) valueStruct.get("effectiveStartDate");
    Date effectiveEndDate = (Date) valueStruct.get("effectiveEndDate");
    boolean readOnly = valueStruct.getBoolean("readOnly");
    boolean active = valueStruct.getBoolean("active");

    //
    //  return
    //

    this.jsonRepresentation = jsonRepresentation;
    this.guiManagedObjectID = guiManagedObjectID;
    this.guiManagedObjectName = guiManagedObjectName;
    this.guiManagedObjectType = guiManagedObjectType;
    this.epoch = epoch;
    this.effectiveStartDate = effectiveStartDate;
    this.effectiveEndDate = effectiveEndDate;
    this.readOnly = readOnly;
    this.active = active;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  public GUIManagedObject(JSONObject jsonRoot, GUIManagedObjectType guiManagedObjectType, long epoch)
  {
    this.jsonRepresentation = jsonRoot;
    this.guiManagedObjectID = JSONUtilities.decodeString(jsonRoot, "id", true);
    this.guiManagedObjectName = JSONUtilities.decodeString(jsonRoot, "name", false);
    this.guiManagedObjectType = guiManagedObjectType;
    this.epoch = epoch;
    this.effectiveStartDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "effectiveStartDate", false));
    this.effectiveEndDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "effectiveEndDate", false));
    this.readOnly = JSONUtilities.decodeBoolean(jsonRoot, "readOnly", Boolean.FALSE);
    this.active = JSONUtilities.decodeBoolean(jsonRoot, "active", Boolean.TRUE);
  }

  //
  //  constructor
  //

  public GUIManagedObject(JSONObject jsonRoot, long epoch)
  {
    this(jsonRoot, GUIManagedObjectType.Other, epoch);
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

  public static Date parseDateField(String stringDate) throws JSONUtilitiesException
  {
    //
    //  parse via standard date formats
    //

    Date result = null;
    ParseException parseException = null;
    if (stringDate != null && stringDate.trim().length() > 0)
      {
        for (SimpleDateFormat standardDateFormat : standardDateFormats)
          {
            //
            //  parse
            //

            synchronized (standardDateFormat)
              {
                try
                  {
                    result = standardDateFormat.parse(stringDate.trim());
                    parseException = null;
                  }
                catch (ParseException e)
                  {
                    result = null;
                    parseException = e;
                  }
              }

            //
            //  result
            //

            if (result != null) break;
          }
      }

    //
    //  exception
    //

    if (parseException != null)
      {
        throw new JSONUtilitiesException("parseDateField", parseException);
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
    if (obj instanceof GUIManagedObject)
      {
        GUIManagedObject guiManagedObject = (GUIManagedObject) obj;
        result = true;
        result = result && Objects.equals(guiManagedObjectID, guiManagedObject.getGUIManagedObjectID());
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
    return guiManagedObjectID.hashCode();
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
    *  constructor -- standard
    *
    *****************************************/

    public IncompleteObject(String guiManagedObjectID)
    {
      super(guiManagedObjectID);
    }
    
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
      Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

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

    public IncompleteObject(JSONObject jsonRoot, GUIManagedObjectType guiManagedObjectType, long epoch)
    {
      super(jsonRoot, guiManagedObjectType, epoch);
    }

    /*****************************************
    *
    *  constructor -- external JSON
    *
    *****************************************/

    public IncompleteObject(JSONObject jsonRoot, long epoch)
    {
      super(jsonRoot, epoch);
    }
  }
}
