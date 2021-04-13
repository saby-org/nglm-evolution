/*****************************************************************************
*
*  ContactPolicy.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
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
import java.util.HashMap;
import java.util.Objects;
import java.util.List;
import java.util.Map;

@GUIDependencyDef(objectType = "contactPolicy", serviceClass = ContactPolicyService.class, dependencies = { })
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
    schemaBuilder.field("contactPolicyCommunicationChannels", SchemaBuilder.array(ContactPolicyCommunicationChannels.schema()).name("contactPolicyCommunicationChannels").schema());
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

  private List<ContactPolicyCommunicationChannels> contactPolicyCommunicationChannels;
  private boolean isDefault;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getContactPolicyID() { return getGUIManagedObjectID(); }
  public String getContactPolicyName() { return getGUIManagedObjectName(); }
  public List<ContactPolicyCommunicationChannels> getContactPolicyCommunicationChannels() { return contactPolicyCommunicationChannels; }
  public boolean isDefault() { return isDefault; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public ContactPolicy(SchemaAndValue schemaAndValue, List<ContactPolicyCommunicationChannels> contactPolicyCommunicationChannels, boolean isDefault)
  {
    super(schemaAndValue);
    this.contactPolicyCommunicationChannels = contactPolicyCommunicationChannels;
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
    struct.put("contactPolicyCommunicationChannels", packContactPolicyCommunicationChannels(contactPolicy.getContactPolicyCommunicationChannels()));
    struct.put("isDefault", contactPolicy.isDefault());
    return struct;
  }

  /*****************************************
  *
  *  packContactPolicyCommunicationChannels
  *
  *****************************************/

  private static List<Object> packContactPolicyCommunicationChannels(List<ContactPolicyCommunicationChannels> contactPolicyCommunicationChannels)
  {
    List<Object> result = new ArrayList<Object>();
    for (ContactPolicyCommunicationChannels contactPolicyTouchPoint : contactPolicyCommunicationChannels)
      {
        result.add(ContactPolicyCommunicationChannels.pack(contactPolicyTouchPoint));
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
    List<ContactPolicyCommunicationChannels> contactPolicyCommunicationChannels = unpackContactPolicyCommunicationChannels(schema.field("contactPolicyCommunicationChannels").schema(), valueStruct.get("contactPolicyCommunicationChannels"));
    boolean isDefault = valueStruct.getBoolean("isDefault");
    
    //
    //  return
    //

    return new ContactPolicy(schemaAndValue, contactPolicyCommunicationChannels, isDefault);
  }

  /*****************************************
  *
  *  unpackContactPolicyCommunicationChannels
  *
  *****************************************/

  private static List<ContactPolicyCommunicationChannels> unpackContactPolicyCommunicationChannels(Schema schema, Object value)
  {
    //
    //  get schema for ContactPolicyCommunicationChannels
    //

    Schema contactPolicyTouchPointSchema = schema.valueSchema();

    //
    //  unpack
    //

    List<ContactPolicyCommunicationChannels> result = new ArrayList<ContactPolicyCommunicationChannels>();
    List<Object> valueArray = (List<Object>) value;
    for (Object contactPolicyTouchPoint : valueArray)
      {
        result.add(ContactPolicyCommunicationChannels.unpack(new SchemaAndValue(contactPolicyTouchPointSchema, contactPolicyTouchPoint)));
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

  public ContactPolicy(JSONObject jsonRoot, long epoch, GUIManagedObject existingContactPolicyUnchecked, int tenantID) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingContactPolicyUnchecked != null) ? existingContactPolicyUnchecked.getEpoch() : epoch, tenantID);

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

    this.contactPolicyCommunicationChannels = decodeContactPolicyCommunicationChannels(JSONUtilities.decodeJSONArray(jsonRoot, "contactPolicyCommunicationChannels", true));
    this.isDefault = JSONUtilities.decodeBoolean(jsonRoot, "isDefault", Boolean.TRUE);

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
  *  decodeContactPolicyCommunicationChannels
  *
  *****************************************/

  private List<ContactPolicyCommunicationChannels> decodeContactPolicyCommunicationChannels(JSONArray jsonArray) throws GUIManagerException
  {
    List<ContactPolicyCommunicationChannels> contactPolicyCommunicationChannels = new ArrayList<ContactPolicyCommunicationChannels>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        contactPolicyCommunicationChannels.add(new ContactPolicyCommunicationChannels((JSONObject) jsonArray.get(i)));
      }
    return contactPolicyCommunicationChannels;
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
    *  validate communication channel exist
    *
    *****************************************/

    for (ContactPolicyCommunicationChannels communicationChannel : contactPolicyCommunicationChannels)
      {
        CommunicationChannel channel = Deployment.getCommunicationChannels().get(communicationChannel.getCommunicationChannelID());
        if (channel == null) throw new GUIManagerException("unknown communication channel ", communicationChannel.getCommunicationChannelID());
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
        epochChanged = epochChanged || ! Objects.equals(contactPolicyCommunicationChannels, existingContactPolicy.getContactPolicyCommunicationChannels());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
  
 
}