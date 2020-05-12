/*****************************************************************************
*
*  SubscriberRelatives.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.SubscriberProfileService.SubscriberProfileServiceException;

public class SubscriberRelatives
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
    schemaBuilder.name("subscriber_hierarchy");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(0));
    schemaBuilder.field("parentSubscriberID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("childrenSubscriberIDs", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schema = schemaBuilder.build();
  };

  //
  // serde
  //

  private static ConnectSerde<SubscriberRelatives> serde = new ConnectSerde<SubscriberRelatives>(schema, false, SubscriberRelatives.class, SubscriberRelatives::pack, SubscriberRelatives::unpack);

  //
  // accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SubscriberRelatives> serde() { return serde; }

  /*****************************************
   *
   * data
   *
   *****************************************/

  private String parentSubscriberID;
  private Set<String> childrenSubscriberIDs; 

  /*****************************************
   *
   * accessors
   *
   *****************************************/

  public String getParentSubscriberID() { return parentSubscriberID; }
  public Set<String> getChildrenSubscriberIDs() { return childrenSubscriberIDs; }

  /*****************************************
   *
   * setters
   *
   *****************************************/

  public void setParentSubscriberID(String parentSubscriberID) { this.parentSubscriberID = parentSubscriberID; }
  public void addChildSubscriberID(String childSubscriberID) 
  {
    if(!this.childrenSubscriberIDs.contains(childSubscriberID)) {
      this.childrenSubscriberIDs.add(childSubscriberID);
    }
  }
  public void removeChildSubscriberID(String childSubscriberID)
  {
    this.childrenSubscriberIDs.remove(childSubscriberID);
  }
  
  /*****************************************
   *
   * constructor
   *
   *****************************************/

  public SubscriberRelatives(String parentSubscriberID, Set<String> childrenSubscriberIDs)
    {
      this.parentSubscriberID = parentSubscriberID;
      this.childrenSubscriberIDs = childrenSubscriberIDs;
    }

  /*****************************************
   *
   * constructor -- empty
   *
   *****************************************/

  public SubscriberRelatives()
    {
      this.parentSubscriberID = null;
      this.childrenSubscriberIDs = new LinkedHashSet<String>();
    }
  
  /*****************************************
   *
   * getJSONRepresentation
   *
   *****************************************/

  public JSONObject getJSONRepresentation(String relationshipID, SubscriberProfileService subscriberProfileService, ReferenceDataReader<String, SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    HashMap<String, Object> json = new HashMap<String, Object>();
    
    //
    //  obj
    //
    
    json.put("relationshipID", relationshipID);
    json.put("relationshipName", Deployment.getSupportedRelationships().get(relationshipID) != null ? Deployment.getSupportedRelationships().get(relationshipID).getName() : null);
    
    //
    //  parent
    //
    
    HashMap<String, Object> parentJson = new HashMap<String, Object>();
    try
      {
        SubscriberProfile parentProfile = subscriberProfileService.getSubscriberProfile(getParentSubscriberID());
        if (parentProfile != null)
          {
            SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(parentProfile, subscriberGroupEpochReader, SystemTime.getCurrentTime());
            parentJson.put("subscriberID", getParentSubscriberID());
            for (String id : Deployment.getAlternateIDs().keySet())
              {
                AlternateID alternateID = Deployment.getAlternateIDs().get(id);
                CriterionField criterionField = Deployment.getProfileCriterionFields().get(alternateID.getProfileCriterionField());
                if (criterionField != null)
                  {
                    String alternateIDValue = (String) criterionField.retrieve(evaluationRequest);
                    parentJson.put(alternateID.getID(), alternateIDValue);
                  }
              }
          }
        json.put("parentDetails", JSONUtilities.encodeObject(parentJson));
      } 
    catch (SubscriberProfileServiceException e)
      {
        e.printStackTrace();
      }
    
    //
    //  children
    //
    
    json.put("numberOfChildren", getChildrenSubscriberIDs().size());
    json.put("childrenSubscriberIDs", JSONUtilities.encodeArray(new ArrayList<String>(getChildrenSubscriberIDs())));
    
    //
    //  result
    //
    
    return JSONUtilities.encodeObject(json);
  }
  
  /*****************************************
   *
   * pack
   *
   *****************************************/

  public static Object pack(Object value)
  {
    SubscriberRelatives hierarchy = (SubscriberRelatives) value;
    Struct struct = new Struct(schema);
    struct.put("parentSubscriberID", hierarchy.getParentSubscriberID());
    struct.put("childrenSubscriberIDs", packChildrenSubscriberIDs(hierarchy.getChildrenSubscriberIDs()));
    return struct;
  }

  /****************************************
   *
   * packChildrenSubscriberIDs
   *
   ****************************************/

  private static List<Object> packChildrenSubscriberIDs(Set<String> childrenSubscriberIDs)
  {
    List<Object> result = new ArrayList<Object>();
    for (String childID : childrenSubscriberIDs)
      {
        result.add(childID);
      }
    return result;
  }

  /*****************************************
   *
   * unpack
   *
   *****************************************/

  public static SubscriberRelatives unpack(SchemaAndValue schemaAndValue)
  {
    //
    // data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    // unpack
    //

    Struct valueStruct = (Struct) value;
    String parentSubscriberID = valueStruct.getString("parentSubscriberID");
    Set<String> childrenSubscriberIDs = unpackChildrenSubscriberIDs((List<String>) valueStruct.get("childrenSubscriberIDs"));

    //
    // validate
    //

    if (childrenSubscriberIDs == null)
      {
        childrenSubscriberIDs = new LinkedHashSet<String>();
      }

    //
    // return
    //

    return new SubscriberRelatives(parentSubscriberID, childrenSubscriberIDs);
  }
  
  /*****************************************
   *
   * unpackChildrenSubscriberIDs
   *
   *****************************************/

  private static Set<String> unpackChildrenSubscriberIDs(List<String> childrenSubscriberIDs)
  {
    Set<String> result = new LinkedHashSet<String>();
    for (String childID : childrenSubscriberIDs)
      {
        result.add(childID);
      }
    return result;
  }

}