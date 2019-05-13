/*****************************************************************************
*
*  ContactPolicy.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import com.evolving.nglm.core.JSONUtilities;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.Objects;
import java.util.List;

public class ContactPolicy extends GUIManagedObject
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
    schemaBuilder.name("contact_policy");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("contactPolicyTouchPoints", SchemaBuilder.array(ContactPolicyTouchPoint.schema()).name("contact_policy_touchpoints").schema());
    schemaBuilder.field("isDefault", Schema.BOOLEAN_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<ContactPolicy> serde = new ConnectSerde<ContactPolicy>(schema, false, ContactPolicy.class, ContactPolicy::pack, ContactPolicy::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<ContactPolicy> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private List<ContactPolicyTouchPoint> contactPolicyTouchPoints;
  private boolean isDefault;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getContactPolicyID() { return getGUIManagedObjectID(); }
  public String getContactPolicyName() { return getGUIManagedObjectName(); }
  public List<ContactPolicyTouchPoint> getContactPolicyTouchPoints() { return contactPolicyTouchPoints; }
  public boolean isDefault() { return isDefault; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public ContactPolicy(SchemaAndValue schemaAndValue, List<ContactPolicyTouchPoint> contactPolicyTouchPoints, boolean isDefault)
  {
    super(schemaAndValue);
    this.contactPolicyTouchPoints = contactPolicyTouchPoints;
    this.isDefault = isDefault;
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    ContactPolicy contactPolicy = (ContactPolicy) value;
    Struct struct = new Struct(schema);
    packCommon(struct, contactPolicy);
    struct.put("contactPolicyTouchPoints", packContactPolicyTouchPoints(contactPolicy.getContactPolicyTouchPoints()));
    struct.put("isDefault", contactPolicy.isDefault());
    return struct;
  }

  /*****************************************
  *
  *  packContactPolicyTouchPoints
  *
  *****************************************/

  private static List<Object> packContactPolicyTouchPoints(List<ContactPolicyTouchPoint> contactPolicyTouchPoints)
  {
    List<Object> result = new ArrayList<Object>();
    for (ContactPolicyTouchPoint contactPolicyTouchPoint : contactPolicyTouchPoints)
      {
        result.add(ContactPolicyTouchPoint.pack(contactPolicyTouchPoint));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static ContactPolicy unpack(SchemaAndValue schemaAndValue)
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
    List<ContactPolicyTouchPoint> contactPolicyTouchPoints = unpackContactPolicyTouchPoints(schema.field("contactPolicyTouchPoints").schema(), valueStruct.get("contactPolicyTouchPoints"));
    boolean isDefault = valueStruct.getBoolean("isDefault");
    
    //
    //  return
    //

    return new ContactPolicy(schemaAndValue, contactPolicyTouchPoints, isDefault);
  }

  /*****************************************
  *
  *  unpackContactPolicyTouchPoints
  *
  *****************************************/

  private static List<ContactPolicyTouchPoint> unpackContactPolicyTouchPoints(Schema schema, Object value)
  {
    //
    //  get schema for ContactPolicyTouchPoint
    //

    Schema contactPolicyTouchPointSchema = schema.valueSchema();

    //
    //  unpack
    //

    List<ContactPolicyTouchPoint> result = new ArrayList<ContactPolicyTouchPoint>();
    List<Object> valueArray = (List<Object>) value;
    for (Object contactPolicyTouchPoint : valueArray)
      {
        result.add(ContactPolicyTouchPoint.unpack(new SchemaAndValue(contactPolicyTouchPointSchema, contactPolicyTouchPoint)));
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

  public ContactPolicy(JSONObject jsonRoot, long epoch, GUIManagedObject existingContactPolicyUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingContactPolicyUnchecked != null) ? existingContactPolicyUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingContactPolicy
    *
    *****************************************/

    ContactPolicy existingContactPolicy = (existingContactPolicyUnchecked != null && existingContactPolicyUnchecked instanceof ContactPolicy) ? (ContactPolicy) existingContactPolicyUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.contactPolicyTouchPoints = decodeContactPolicyTouchPoints(JSONUtilities.decodeJSONArray(jsonRoot, "contactPolicyTouchPoints", true));
    this.isDefault = JSONUtilities.decodeBoolean(jsonRoot, "isDefault", Boolean.TRUE);

    /*****************************************
    *
    *  validate
    *
    *****************************************/

    if (getRawEffectiveStartDate() != null) throw new GUIManagerException("unsupported start date", JSONUtilities.decodeString(jsonRoot, "effectiveStartDate", false));
    if (getRawEffectiveEndDate() != null) throw new GUIManagerException("unsupported end date", JSONUtilities.decodeString(jsonRoot, "effectiveEndDate", false));

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingContactPolicy))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  decodeContactPolicyTouchPoints
  *
  *****************************************/

  private List<ContactPolicyTouchPoint> decodeContactPolicyTouchPoints(JSONArray jsonArray) throws GUIManagerException
  {
    List<ContactPolicyTouchPoint> contactPolicyTouchPoints = new ArrayList<ContactPolicyTouchPoint>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        contactPolicyTouchPoints.add(new ContactPolicyTouchPoint((JSONObject) jsonArray.get(i)));
      }
    return contactPolicyTouchPoints;
  }

  /*****************************************
  *
  *  validate
  *
  *****************************************/

  public void validate(Date date) throws GUIManagerException
  {
    /*****************************************
    *
    *  validate touch points exist
    *
    *****************************************/

    for (ContactPolicyTouchPoint contactPolicyTouchPoint : contactPolicyTouchPoints)
      {
        TouchPoint touchPoint = Deployment.getTouchPoints().get(contactPolicyTouchPoint.getTouchPointID());
        if (touchPoint == null) throw new GUIManagerException("unknown touch point", contactPolicyTouchPoint.getTouchPointID());
      }
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

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(ContactPolicy existingContactPolicy)
  {
    if (existingContactPolicy != null && existingContactPolicy.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingContactPolicy.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(contactPolicyTouchPoints, existingContactPolicy.getContactPolicyTouchPoints());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
}