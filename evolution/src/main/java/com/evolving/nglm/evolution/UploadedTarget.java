package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class UploadedTarget extends GUIManagedObject
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
    schemaBuilder.name("uploadedtarget");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("targetName", Schema.STRING_SCHEMA);
    schemaBuilder.field("targetFileID", Schema.STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<UploadedTarget> serde = new ConnectSerde<UploadedTarget>(schema, false, UploadedTarget.class, UploadedTarget::pack, UploadedTarget::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<UploadedTarget> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/
  
  private String targetName;
  private String targetFileID;
  
  
  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getTargetID() { return getGUIManagedObjectID(); }
  public String getTargetName() { return targetName; }
  public String getUploadedTargetFileID () { return targetFileID; }
  
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public UploadedTarget(SchemaAndValue schemaAndValue, String targetName, String uploadedTargetFileID)
  {
    super(schemaAndValue);
    this.targetName = targetName;
    this.targetFileID = uploadedTargetFileID;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    UploadedTarget target = (UploadedTarget) value;
    Struct struct = new Struct(schema);
    packCommon(struct, target);
    struct.put("targetName", target.getTargetName());
    struct.put("targetFileID", target.getUploadedTargetFileID());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static UploadedTarget unpack(SchemaAndValue schemaAndValue)
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
    String targetName = valueStruct.getString("targetName");
    String targetFileID = valueStruct.getString("targetFileID");
    
    //
    //  return
    //

    return new UploadedTarget(schemaAndValue, targetName, targetFileID);
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public UploadedTarget(JSONObject jsonRoot, long epoch, GUIManagedObject existingTargetUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingTargetUnchecked != null) ? existingTargetUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingSupplier
    *
    *****************************************/

    UploadedTarget existingTarget = (existingTargetUnchecked != null && existingTargetUnchecked instanceof UploadedTarget) ? (UploadedTarget) existingTargetUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.targetName = JSONUtilities.decodeString(jsonRoot, "targetName", true);
    this.targetFileID = JSONUtilities.decodeString(jsonRoot, "targetFileID", true);

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingTarget))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(UploadedTarget existingTarget)
  {
    if (existingTarget != null && existingTarget.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingTarget.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(targetName, existingTarget.getTargetName());
        epochChanged = epochChanged || ! Objects.equals(targetFileID, existingTarget.getUploadedTargetFileID());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
  
  public void validate(UploadedFileService uploadedFileService, Date now) throws GUIManagerException 
  {
    UploadedFile uploadedFile = (UploadedFile) uploadedFileService.getStoredUploadedFile(targetFileID);
    if (uploadedFile == null) throw new GUIManagerException("unknown uploaded file with id ", targetFileID);
  }
}
