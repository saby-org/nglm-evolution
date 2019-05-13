/*****************************************************************************
*
*  ContactPolicyTouchPoint.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.JSONUtilities;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ContactPolicyTouchPoint
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
    schemaBuilder.name("contact_policy_touchpoint");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("touchPointID", Schema.STRING_SCHEMA);
    schemaBuilder.field("touchPointName", Schema.STRING_SCHEMA);
    schemaBuilder.field("timeWindows", TimeWindow.schema());
    schemaBuilder.field("messageLimits", MessageLimits.schema());
    schemaBuilder.field("contactPolicyBlackout", SchemaBuilder.array(ContactPolicyBlackout.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<ContactPolicyTouchPoint> serde = new ConnectSerde<ContactPolicyTouchPoint>(schema, false, ContactPolicyTouchPoint.class, ContactPolicyTouchPoint::pack, ContactPolicyTouchPoint::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<ContactPolicyTouchPoint> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String touchPointID;
  private String touchPointName;
  private TimeWindow timeWindows;
  private MessageLimits messageLimits;
  private Set<ContactPolicyBlackout> blackoutList;
  

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getTouchPointID() { return touchPointID; }
  public String getTouchPointName() { return touchPointName; }
  public TimeWindow getTimeWindows() { return timeWindows; }
  public MessageLimits getMessageLimits() { return messageLimits; }
  public Set<ContactPolicyBlackout> getContactPolicyBlackoutList(){ return blackoutList; }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public ContactPolicyTouchPoint(String touchPointID, String touchPointName, TimeWindow timeWindows, MessageLimits messageLimits, Set<ContactPolicyBlackout> blackoutList)
  {
    this.touchPointID = touchPointID;
    this.touchPointName = touchPointName;
    this.timeWindows = timeWindows;
    this.messageLimits = messageLimits;
    this.blackoutList = blackoutList;
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    ContactPolicyTouchPoint contactPolicy = (ContactPolicyTouchPoint) value;
    Struct struct = new Struct(schema);
    struct.put("touchPointID", contactPolicy.getTouchPointID());
    struct.put("touchPointName", contactPolicy.getTouchPointName());
    struct.put("timeWindows", TimeWindow.pack(contactPolicy.getTimeWindows()));
    struct.put("messageLimits", MessageLimits.pack(contactPolicy.getMessageLimits()));
    struct.put("contactPolicyBlackout", packContactPolicyBlackout(contactPolicy.getContactPolicyBlackoutList()));
    return struct;
  }
  
  /****************************************
  *
  *  packContactPolicyBlackout
  *
  ****************************************/

  private static List<Object> packContactPolicyBlackout(Set<ContactPolicyBlackout> blackoutPolicy)
  {
    List<Object> result = new ArrayList<Object>();
    for (ContactPolicyBlackout blackout : blackoutPolicy)
      {
        result.add(ContactPolicyBlackout.pack(blackout));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static ContactPolicyTouchPoint unpack(SchemaAndValue schemaAndValue)
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
    String touchPointID = valueStruct.getString("touchPointID");
    String touchPointName = valueStruct.getString("touchPointName");
    TimeWindow timeWindows = TimeWindow.unpack(new SchemaAndValue(schema.field("timeWindows").schema(), valueStruct.get("timeWindows")));
    MessageLimits mesageLimits = MessageLimits.unpack(new SchemaAndValue(schema.field("messageLimits").schema(), valueStruct.get("messageLimits")));
    Set<ContactPolicyBlackout> blackoutList = unpackContactPolicyBlackout(schema.field("contactPolicyBlackout").schema(), valueStruct.get("contactPolicyBlackout"));
    
    //
    //  return
    //

    return new ContactPolicyTouchPoint(touchPointID, touchPointName, timeWindows, mesageLimits, blackoutList);
  }
  
  /*****************************************
  *
  *  unpackContactPolicyBlackout
  *
  *****************************************/

  private static Set<ContactPolicyBlackout> unpackContactPolicyBlackout(Schema schema, Object value)
  {
    //
    //  get schema for ContactPolicyBlackout
    //

    Schema blackoutSchema = schema.valueSchema();

    //
    //  unpack
    //

    Set<ContactPolicyBlackout> result = new HashSet<ContactPolicyBlackout>();
    List<Object> valueArray = (List<Object>) value;
    for (Object blackout : valueArray)
      {
        result.add(ContactPolicyBlackout.unpack(new SchemaAndValue(blackoutSchema, blackout)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ContactPolicyTouchPoint(JSONObject jsonRoot) throws GUIManagerException
  {

    this.touchPointID = JSONUtilities.decodeString(jsonRoot, "touchPointID", true);
    this.touchPointName = JSONUtilities.decodeString(jsonRoot, "touchPointName", true);
    this.timeWindows = new TimeWindow((JSONObject)(JSONUtilities.decodeJSONObject(jsonRoot, "timeWindows", false)));
    this.messageLimits = new MessageLimits((JSONObject)(JSONUtilities.decodeJSONObject(jsonRoot, "messageLimits", false)));
    this.blackoutList = decodeContactPolicyBlackout(JSONUtilities.decodeJSONArray(jsonRoot, "contactPolicyBlackout", false));
  }
  
  /*****************************************
  *
  *  decodeContactPolicyBlackout
  *
  *****************************************/

  private Set<ContactPolicyBlackout> decodeContactPolicyBlackout(JSONArray jsonArray) throws GUIManagerException
  {
    Set<ContactPolicyBlackout> result = new HashSet<ContactPolicyBlackout>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new ContactPolicyBlackout((JSONObject) jsonArray.get(i)));
          }
      }
    return result;
  }

  /*****************************************
  *
  *  validate
  *
  *****************************************/

  public void validate(Date date, String touchPointID) throws GUIManagerException
  {
    /*****************************************
    *
    *  validate touch points exist
    *
    *****************************************/
    TouchPoint touchPoint = Deployment.getTouchPoints().get(touchPointID);
    if (touchPoint == null) throw new GUIManagerException("unknown touch point", touchPointID);
  }

  /*****************************************
  *
  *  schedule
  *
  *****************************************/

  public Date schedule(String touchPointID, Date now)
  {
    return now;
  }
  
  public static class ContactPolicyBlackout
  {
    
    //
    //  logger
    //

    private static final Logger log = LoggerFactory.getLogger(ContactPolicyBlackout.class);
    
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
      schemaBuilder.name("contact_policy_blackout");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("from", Schema.INT64_SCHEMA);
      schemaBuilder.field("until", Schema.INT64_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<ContactPolicyBlackout> serde = new ConnectSerde<ContactPolicyBlackout>(schema, false, ContactPolicyBlackout.class, ContactPolicyBlackout::pack, ContactPolicyBlackout::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<ContactPolicyBlackout> serde() { return serde; }

    /*****************************************
    *
    *  data
    *
    *****************************************/
    
    private Date from;
    private Date until;
    
    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public Date getStartTime() { return from; }
    public Date getEndTime() { return until; }
    

    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    public ContactPolicyBlackout(Date from, Date until)
    {
      this.from = from;
      this.until = until;
    }
    
    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      ContactPolicyBlackout contactPolicyBlackout = (ContactPolicyBlackout) value;
      Struct struct = new Struct(schema);
      struct.put("from", contactPolicyBlackout.getStartTime().getTime());
      struct.put("until", contactPolicyBlackout.getEndTime().getTime());
      return struct;
    }
    
    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static ContactPolicyBlackout unpack(SchemaAndValue schemaAndValue)
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
      
      long stTime = valueStruct.getInt64("from");
      long edTime = valueStruct.getInt64("until");
      
      Date from = new Date(stTime);
      Date until = new Date(edTime);

      //
      //  return
      //

      return new ContactPolicyBlackout(from, until);
    }
    
    /*****************************************
    *
    *  constructor -- JSON
    *
    *****************************************/

    public ContactPolicyBlackout(JSONObject jsonRoot) throws GUIManagerException
    {
      
      /*****************************************
      *
      *  attributes
      *
      *****************************************/
      
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      String stTime = JSONUtilities.decodeString(jsonRoot, "from", true);
      String edTime = JSONUtilities.decodeString(jsonRoot, "until", true);
      try {
        this.from = sdf.parse(stTime);
        this.until = sdf.parse(edTime);
      }catch(Exception e) {
        log.warn("ContactPolicyBlackout: parse exception ", e);
      }
    }
  }
  
}
