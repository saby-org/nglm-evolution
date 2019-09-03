/*****************************************************************************
*
*  SegmentContactPolicy.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.LinkedHashMap;
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
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class SegmentContactPolicy extends GUIManagedObject
{
  /*****************************************
  *
  *  singletonID
  *
  *****************************************/

  public static final String singletonID = "base";

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
    schemaBuilder.name("segment_contact_policy");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("dimensionID", Schema.STRING_SCHEMA);
    schemaBuilder.field("segments", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<SegmentContactPolicy> serde = new ConnectSerde<SegmentContactPolicy>(schema, false, SegmentContactPolicy.class, SegmentContactPolicy::pack, SegmentContactPolicy::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SegmentContactPolicy> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String dimensionID;
  private Map<String, String> segments;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getDimensionID() { return dimensionID; }
  public Map<String, String> getSegments() { return segments; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public SegmentContactPolicy(SchemaAndValue schemaAndValue, String dimensionID, Map<String, String> segments)
  {
    super(schemaAndValue);
    this.dimensionID = dimensionID;
    this.segments = segments;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    SegmentContactPolicy segmentContactPolicy = (SegmentContactPolicy) value;
    Struct struct = new Struct(schema);
    packCommon(struct, segmentContactPolicy);
    struct.put("dimensionID", segmentContactPolicy.getDimensionID());
    struct.put("segments", segmentContactPolicy.getSegments());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static SegmentContactPolicy unpack(SchemaAndValue schemaAndValue)
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
    String dimensionID = valueStruct.getString("dimensionID");
    Map<String, String> segments = (Map<String, String>) valueStruct.get("segments");

    //
    //  return
    //

    return new SegmentContactPolicy(schemaAndValue, dimensionID, segments);
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SegmentContactPolicy(JSONObject jsonRoot, long epoch, GUIManagedObject existingSegmentContactPolicyUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingSegmentContactPolicyUnchecked != null) ? existingSegmentContactPolicyUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingSegmentContactPolicy
    *
    *****************************************/

    SegmentContactPolicy existingContactPolicy = (existingSegmentContactPolicyUnchecked != null && existingSegmentContactPolicyUnchecked instanceof SegmentContactPolicy) ? (SegmentContactPolicy) existingSegmentContactPolicyUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.dimensionID = JSONUtilities.decodeString(jsonRoot, "dimensionID", true);
    this.segments = decodeContactPolicies(JSONUtilities.decodeJSONArray(jsonRoot, "contactPolicies", new JSONArray()));

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
  *  decodeContactPolicies
  *
  *****************************************/

  private Map<String,String> decodeContactPolicies(JSONArray jsonArray)
  {
    Map<String,String> contactPolicies = new LinkedHashMap<String,String>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            JSONObject contactPolicyJSON = (JSONObject) jsonArray.get(i);
            String segment = JSONUtilities.decodeString(contactPolicyJSON, "segmentID", true);
            String contactPolicyID = JSONUtilities.decodeString(contactPolicyJSON, "contactPolicyID", true);
            contactPolicies.put(segment,contactPolicyID);
          }
      }
    return contactPolicies;
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(SegmentContactPolicy existingSegmentContactPolicy)
  {
    if (existingSegmentContactPolicy != null && existingSegmentContactPolicy.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingSegmentContactPolicy.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(getDimensionID(), existingSegmentContactPolicy.getDimensionID());
        epochChanged = epochChanged || ! Objects.equals(getSegments(), existingSegmentContactPolicy.getSegments());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }

  /*****************************************
  *
  *  validate
  *
  *****************************************/

  public void validate(ContactPolicyService contactPolicyService, SegmentationDimensionService dimensionService, Date date) throws GUIManagerException
  {
    /*****************************************
    *
    *  validate contact policy exists and is active
    *
    *****************************************/

    //
    //  dimension
    //

    if (dimensionID != null)
      {
        SegmentationDimension segmentationDimension = dimensionService.getActiveSegmentationDimension(dimensionID, date);
        if (segmentationDimension == null) throw new GUIManagerException("unknown segmentation dimension", dimensionID);
      }

    //
    //  contact policies
    //

    for (String contactPolicyID : segments.values())
      {
        ContactPolicy contactPolicy = contactPolicyService.getActiveContactPolicy(contactPolicyID, date);
        if (contactPolicy == null) throw new GUIManagerException("unknown contact policy", contactPolicyID);
      }
  }
}
