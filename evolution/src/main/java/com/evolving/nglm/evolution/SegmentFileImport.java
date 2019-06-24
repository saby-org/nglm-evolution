/*****************************************************************************
*
*  SegmentFileImport.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class SegmentFileImport implements Segment
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SegmentFileImport.class);

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
    //
    //  schema
    //
    
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("segment_file_import");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("id", Schema.STRING_SCHEMA);
    schemaBuilder.field("name", Schema.STRING_SCHEMA);
    schemaBuilder.field("contactPolicyID", Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  accessor
  //

  public static Schema schema() { return schema; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String id;
  private String name;
  private String contactPolicyID;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  public SegmentFileImport(String id, String name, String contactPolicyID)
  {
    this.id = id;
    this.name = name;
    this.contactPolicyID = contactPolicyID;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  SegmentFileImport(JSONObject jsonRoot) throws GUIManagerException
  {
    this.id = JSONUtilities.decodeString(jsonRoot, "id", true);
    this.name = JSONUtilities.decodeString(jsonRoot, "name", true);
    this.contactPolicyID = JSONUtilities.decodeString(jsonRoot, "contactPolicyID", false);
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/
  
  public String getID() { return id; }
  public String getName() { return name; }
  public String getContactPolicyID() { return contactPolicyID; }
  public boolean getDependentOnExtendedSubscriberProfile() { return false; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<SegmentFileImport> serde()
  {
    return new ConnectSerde<SegmentFileImport>(schema, false, SegmentFileImport.class, SegmentFileImport::pack, SegmentFileImport::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    SegmentFileImport segment = (SegmentFileImport) value;
    Struct struct = new Struct(schema);
    struct.put("id", segment.getID());
    struct.put("name", segment.getName());
    struct.put("contactPolicyID", segment.getContactPolicyID());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static SegmentFileImport unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack all but argument
    //

    if(value == null){
      return null;
    }
    Struct valueStruct = (Struct) value;
    String id = valueStruct.getString("id");
    String name = valueStruct.getString("name");
    String contactPolicyID = valueStruct.getString("contactPolicyID");

    //
    //  construct
    //

    SegmentFileImport result = new SegmentFileImport(id, name, contactPolicyID);

    //
    //  return
    //

    return result;
  }
}
