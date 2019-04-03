/*****************************************************************************
*
*  ContactPolicyTouchPoint.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SchemaUtilities;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Objects;

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
    schemaBuilder.name("contactpolicy_touchpoint");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("touchPointID", Schema.STRING_SCHEMA);
    schemaBuilder.field("contactPolicyTimeWindows", SchemaBuilder.array(ContactPolicyTimeWindow.schema()).name("contactpolicy_touchpoint_contactpolicy_timewindows").schema());
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
  private List<ContactPolicyTimeWindow> contactPolicyTimeWindows;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getTouchPointID() { return touchPointID; }
  public List<ContactPolicyTimeWindow> getContactPolicyTimeWindows() { return contactPolicyTimeWindows; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ContactPolicyTouchPoint(String touchPointID, List<ContactPolicyTimeWindow> contactPolicyTimeWindows)
  {
    this.touchPointID = touchPointID;
    this.contactPolicyTimeWindows = contactPolicyTimeWindows;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    ContactPolicyTouchPoint contactPolicyTouchPoint = (ContactPolicyTouchPoint) value;
    Struct struct = new Struct(schema);
    struct.put("touchPointID", contactPolicyTouchPoint.getTouchPointID());
    struct.put("contactPolicyTimeWindows", packContactPolicyTimeWindows(contactPolicyTouchPoint.getContactPolicyTimeWindows()));
    return struct;
  }

  /*****************************************
  *
  *  packContactPolicyTimeWindows
  *
  *****************************************/

  private static List<Object> packContactPolicyTimeWindows(List<ContactPolicyTimeWindow> contactPolicyTimeWindows)
  {
    List<Object> result = new ArrayList<Object>();
    for (ContactPolicyTimeWindow contactPolicyTimeWindow : contactPolicyTimeWindows)
      {
        result.add(ContactPolicyTimeWindow.pack(contactPolicyTimeWindow));
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
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String touchPointID = valueStruct.getString("touchPointID");
    List<ContactPolicyTimeWindow> contactPolicyTimeWindows = unpackContactPolicyTimeWindows(schema.field("contactPolicyTimeWindows").schema(), valueStruct.get("contactPolicyTimeWindows"));

    //
    //  return
    //

    return new ContactPolicyTouchPoint(touchPointID, contactPolicyTimeWindows);
  }

  /*****************************************
  *
  *  unpackContactPolicyTimeWindows
  *
  *****************************************/

  private static List<ContactPolicyTimeWindow> unpackContactPolicyTimeWindows(Schema schema, Object value)
  {
    //
    //  get schema for ContactPolicyTimeWindow
    //

    Schema contactPolicyTimeWindowSchema = schema.valueSchema();

    //
    //  unpack
    //

    List<ContactPolicyTimeWindow> result = new ArrayList<ContactPolicyTimeWindow>();
    List<Object> valueArray = (List<Object>) value;
    for (Object contactPolicyTimeWindow : valueArray)
      {
        result.add(ContactPolicyTimeWindow.unpack(new SchemaAndValue(contactPolicyTimeWindowSchema, contactPolicyTimeWindow)));
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
    this.contactPolicyTimeWindows = decodeContactPolicyTimeWindows(JSONUtilities.decodeJSONArray(jsonRoot, "contactPolicyTimeWindows", true));
  }

  /*****************************************
  *
  *  decodeContactPolicyTimeWindows
  *
  *****************************************/

  private List<ContactPolicyTimeWindow> decodeContactPolicyTimeWindows(JSONArray jsonArray) throws GUIManagerException
  {
    List<ContactPolicyTimeWindow> contactPolicyTimeWindows = new ArrayList<ContactPolicyTimeWindow>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        contactPolicyTimeWindows.add(new ContactPolicyTimeWindow((JSONObject) jsonArray.get(i)));
      }
    return contactPolicyTimeWindows;
  }

  /*****************************************
  *
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof ContactPolicyTouchPoint)
      {
        ContactPolicyTouchPoint contactPolicyTouchPoint = (ContactPolicyTouchPoint) obj;
        result = true;
        result = result && Objects.equals(touchPointID, contactPolicyTouchPoint.getTouchPointID());
        result = result && Objects.equals(contactPolicyTimeWindows, contactPolicyTouchPoint.getContactPolicyTimeWindows());
      }
    return result;
  }
}
