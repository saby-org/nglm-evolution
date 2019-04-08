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
  *  enum
  *
  *****************************************/

  public enum ContactType
  {
    CallToAction("callToAction", "Call To Action"),
    Reminder("reminder", "Reminder"),
    Response("response", "Response"),
    Unknown("(unknown)", "(unknown)");
    private String externalRepresentation;
    private String display;
    private ContactType(String externalRepresentation, String display) { this.externalRepresentation = externalRepresentation; this.display = display; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public String getDisplay() { return display; }
    public static ContactType fromExternalRepresentation(String externalRepresentation) { for (ContactType enumeratedValue : ContactType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
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
    schemaBuilder.name("contactpolicy_touchpoint");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("touchPointID", Schema.STRING_SCHEMA);
    schemaBuilder.field("contactTypes", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
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
  private Set<ContactType> contactTypes;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getTouchPointID() { return touchPointID; }
  public Set<ContactType> getContactTypes() { return contactTypes; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ContactPolicyTouchPoint(String touchPointID, Set<ContactType> contactTypes)
  {
    this.touchPointID = touchPointID;
    this.contactTypes = contactTypes;
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
    struct.put("contactTypes", packContactTypes(contactPolicyTouchPoint.getContactTypes()));
    return struct;
  }

  /*****************************************
  *
  *  packContactTypes
  *
  *****************************************/

  private static List<Object> packContactTypes(Set<ContactType> contactTypes)
  {
    List<Object> result = new ArrayList<Object>();
    for (ContactType contactType : contactTypes)
      {
        result.add(contactType.getExternalRepresentation());
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
    Set<ContactType> contactTypes = unpackContactTypes(schema.field("contactTypes").schema(), valueStruct.get("contactTypes"));

    //
    //  return
    //

    return new ContactPolicyTouchPoint(touchPointID, contactTypes);
  }

  /*****************************************
  *
  *  unpackContactTypes
  *
  *****************************************/

  private static Set<ContactType> unpackContactTypes(Schema schema, Object value)
  {
    Set<ContactType> result = new HashSet<ContactType>();
    List<String> valueArray = (List<String>) value;
    for (String contactType : valueArray)
      {
        result.add(ContactType.fromExternalRepresentation(contactType));
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
    this.contactTypes = decodeContactTypes(JSONUtilities.decodeJSONArray(jsonRoot, "contactTypes", true));
  }

  /*****************************************
  *
  *  decodeContactTypes
  *
  *****************************************/

  private Set<ContactType> decodeContactTypes(JSONArray jsonArray) throws GUIManagerException
  {
    Set<ContactType> contactTypes = new HashSet<ContactType>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        contactTypes.add(ContactType.fromExternalRepresentation((String) jsonArray.get(i)));
      }
    return contactTypes;
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
        result = result && Objects.equals(contactTypes, contactPolicyTouchPoint.getContactTypes());
      }
    return result;
  }
}
