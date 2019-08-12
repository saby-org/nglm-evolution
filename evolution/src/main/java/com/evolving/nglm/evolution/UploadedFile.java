package com.evolving.nglm.evolution;

import java.util.Date;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class UploadedFile extends GUIManagedObject
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
    schemaBuilder.name("uploadedFile");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("applicationID", Schema.STRING_SCHEMA);
    schemaBuilder.field("customerAlternateID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("sourceFilename", Schema.STRING_SCHEMA);
    schemaBuilder.field("destinationFilename", Schema.STRING_SCHEMA);
    schemaBuilder.field("fileType", Schema.STRING_SCHEMA);
    schemaBuilder.field("fileEncoding", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("fileSize", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("numberOfLines", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("uploadDate", Timestamp.SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<UploadedFile> serde = new ConnectSerde<UploadedFile>(schema, false, UploadedFile.class, UploadedFile::pack, UploadedFile::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<UploadedFile> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/
  
  private String applicationID;
  private String customerAlternateID;
  private String sourceFilename;
  private String destinationFilename;
  private String fileType;
  private String fileEncoding;
  private int fileSize;
  private int numberOfLines;
  private Date uploadDate;
  
  public static final String OUTPUT_FOLDER = "/app/uploaded/";
  
  /****************************************
  *
  *  accessors
  *
  ****************************************/
  
  public String getApplicationID() { return applicationID; }
  public String getCustomerAlternateID() { return customerAlternateID; }
  public String getSourceFilename() { return sourceFilename; }
  public String getDestinationFilename() { return destinationFilename; }
  public String getFileType() { return fileType; }
  public String getFileEncoding() { return fileEncoding; }
  public int getFileSize() { return fileSize; }
  public int getNumberOfLines() { return numberOfLines; }
  public Date getUploadDate() { return uploadDate; }
  
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public UploadedFile(JSONObject jsonRoot, long epoch, GUIManagedObject existingUploadedFileUnchecked) throws GUIManagerException
  {
    //
    //  standard
    //

    super(jsonRoot, (existingUploadedFileUnchecked != null) ? existingUploadedFileUnchecked.getEpoch() : epoch);
    this.applicationID = JSONUtilities.decodeString(jsonRoot, "applicationID", true);
    this.customerAlternateID = JSONUtilities.decodeString(jsonRoot, "customerAlternateID", false);
    this.sourceFilename = JSONUtilities.decodeString(jsonRoot, "sourceFilename", true);
    this.fileType = JSONUtilities.decodeString(jsonRoot, "fileType", true);
    this.fileEncoding = JSONUtilities.decodeString(jsonRoot, "fileEncoding", false);
    this.fileSize = JSONUtilities.decodeInteger(jsonRoot, "fileSize", false);
    this.numberOfLines = JSONUtilities.decodeInteger(jsonRoot, "numberOfLines", false);
    this.uploadDate = SystemTime.getCurrentTime();

    //
    //  destinationFilename
    //

    this.destinationFilename = this.getGUIManagedObjectID() + "-" + sourceFilename;
  }
  
  /*****************************************
  *
  *  constructor unpack
  *
  *****************************************/

  public UploadedFile(SchemaAndValue schemaAndValue, String applicationID, String customerAlternateID, String sourceFilename, String destinationFilename, String fileType, String fileEncoding, int fileSize, int numberOfLines, Date uploadDate)
  {
    super(schemaAndValue);
    this.applicationID = applicationID;
    this.customerAlternateID = customerAlternateID;
    this.sourceFilename = sourceFilename;
    this.destinationFilename = destinationFilename;
    this.fileType = fileType;
    this.fileEncoding = fileEncoding;
    this.fileSize = fileSize;
    this.numberOfLines = numberOfLines;
    this.uploadDate = uploadDate;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    UploadedFile uploadFile = (UploadedFile) value;
    Struct struct = new Struct(schema);
    packCommon(struct, uploadFile);
    struct.put("applicationID", uploadFile.getApplicationID());
    struct.put("customerAlternateID", uploadFile.getCustomerAlternateID());
    struct.put("sourceFilename", uploadFile.getSourceFilename());
    struct.put("destinationFilename", uploadFile.getDestinationFilename());
    struct.put("fileType", uploadFile.getFileType());
    struct.put("fileEncoding", uploadFile.getFileEncoding());
    struct.put("fileSize", uploadFile.getFileSize());
    struct.put("numberOfLines", uploadFile.getNumberOfLines());
    struct.put("uploadDate", uploadFile.getUploadDate());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static UploadedFile unpack(SchemaAndValue schemaAndValue)
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
    String applicationID = valueStruct.getString("applicationID");
    String customerAlternateID = valueStruct.getString("customerAlternateID");
    String sourceFilename = valueStruct.getString("sourceFilename");
    String destinationFilename = valueStruct.getString("destinationFilename");
    String fileType = valueStruct.getString("fileType");
    String fileEncoding = valueStruct.getString("fileEncoding");
    int fileSize = valueStruct.getInt32("fileSize");
    int numberOfLines = valueStruct.getInt32("numberOfLines");
    Date uploadDate = (Date) valueStruct.get("uploadDate");
    
    //
    //  return
    //

    return new UploadedFile(schemaAndValue, applicationID, customerAlternateID, sourceFilename, destinationFilename, fileType, fileEncoding, fileSize, numberOfLines, uploadDate);
  }

  /****************************************
  *
  *  validate
  *
  ****************************************/
  
  public void validate() throws GUIManagerException 
  {
    //
    //  ensure alternateID (if specified)
    //
    
    if (customerAlternateID != null)
      {
        AlternateID alternateID = Deployment.getAlternateIDs().get(customerAlternateID);
        if (alternateID == null) throw new GUIManagerException("unknown alternateid ", customerAlternateID);
      }
  }
  
}
