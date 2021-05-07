/*****************************************************************************
*
*  SubscriberRelatives.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;

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
  private List<String> childrenSubscriberIDs; 

  /*****************************************
   *
   * accessors
   *
   *****************************************/

  public String getParentSubscriberID() { return parentSubscriberID; }
  public List<String> getChildrenSubscriberIDs() { return childrenSubscriberIDs; }

  /*****************************************
   *
   * setters
   *
   *****************************************/

  public void setParentSubscriberID(String parentSubscriberID) { this.parentSubscriberID = parentSubscriberID; }
  public void addChildSubscriberID(String childSubscriberID) 
  {
    if(!this.childrenSubscriberIDs.contains(childSubscriberID)) 
      {
        this.childrenSubscriberIDs.add(childSubscriberID);
      }
    
    while(this.childrenSubscriberIDs.size() > 10)
      {
        this.childrenSubscriberIDs.remove(this.childrenSubscriberIDs.remove(0));
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

  public SubscriberRelatives(String parentSubscriberID, List<String> childrenSubscriberIDs)
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
      this.childrenSubscriberIDs = new ArrayList<String>();
    }
  
  /*****************************************
  *
  *  getJSONRepresentation
  *
  *****************************************/
    
  public JSONObject getJSONRepresentation(String relationshipID)
    {
      HashMap<String,Object> json = new HashMap<String,Object>();
      json.put("relationshipID", relationshipID);
      json.put("parentSubscriberID", getParentSubscriberID());
      json.put("childrenSubscriberIDs", JSONUtilities.encodeArray(new ArrayList<String>(getChildrenSubscriberIDs())));
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

  private static List<Object> packChildrenSubscriberIDs(List<String> childrenSubscriberIDs)
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
      List<String> childrenSubscriberIDs = unpackChildrenSubscriberIDs((List<String>) valueStruct.get("childrenSubscriberIDs"));
      
      //
      // validate
      //

      if (childrenSubscriberIDs == null) 
        {
          childrenSubscriberIDs = new ArrayList<String>();
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

  private static List<String> unpackChildrenSubscriberIDs(List<String> childrenSubscriberIDs)
    {
      List<String> result = new ArrayList<String>();
      for (String childID : childrenSubscriberIDs)
        {
          result.add(childID);
        }
      return result;
    }

}