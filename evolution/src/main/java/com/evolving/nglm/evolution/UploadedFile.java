package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),2));
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
    schemaBuilder.field("metaData", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().schema());
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
  private Integer fileSize;
  private Integer numberOfLines;
  private Date uploadDate;
  private Map<String, JSONObject> metaData;
  
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
  public Integer getFileSize() { return fileSize; }
  public Integer getNumberOfLines() { return numberOfLines; }
  public Date getUploadDate() { return uploadDate; }
  public Map<String, JSONObject> getMetaData() { return metaData; }
  
  //
  //  setters
  //

  public void setMetaData(Map<String, JSONObject> metaData) { this.metaData = (metaData != null) ? metaData : new HashMap<String,JSONObject>(); }
  public void setApplicationID(String applicationID){ this.applicationID = applicationID; }
  public void setCustomerAlternateID(String customerAlternateID){ this.customerAlternateID = customerAlternateID; }
  public void setNumberOfLines(Integer numberOfLines){ this.numberOfLines = numberOfLines; }
  
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
    this.metaData = new HashMap<String, JSONObject>();

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

  public UploadedFile(SchemaAndValue schemaAndValue, String applicationID, String customerAlternateID, String sourceFilename, String destinationFilename, String fileType, String fileEncoding, Integer fileSize, Integer numberOfLines, Date uploadDate, Map<String, JSONObject> metaData)
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
    setMetaData(metaData);
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
    struct.put("metaData", packMetaData(uploadFile.getMetaData()));
    return struct;
  }
  
  /****************************************
  *
  *  packMetaData
  *
  ****************************************/

  private static Map<String, String> packMetaData(Map<String, JSONObject> metaData)
  {
    Map<String, String> result = new HashMap<String, String>();
    if(metaData != null)
      {
        for (String parameterName : metaData.keySet())
          {
            JSONObject jsonObject = metaData.get(parameterName);
            result.put(parameterName, jsonObject.toString());
          }
      }
      return result;
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
    Integer fileSize = valueStruct.getInt32("fileSize");
    Integer numberOfLines = valueStruct.getInt32("numberOfLines");
    Date uploadDate = (Date) valueStruct.get("uploadDate");  
    Map<String, JSONObject> metaData = (schemaVersion >= 2) ? unpackMetaData(schema.field("metaData").schema(), (Map<String,String>) valueStruct.get("metaData")) : new HashMap<String, JSONObject>();
    
    //
    //  return
    //

    return new UploadedFile(schemaAndValue, applicationID, customerAlternateID, sourceFilename, destinationFilename, fileType, fileEncoding, fileSize, numberOfLines, uploadDate, metaData);
  }
  
  /*****************************************
  *
  *  unpackMetaData
  *
  *****************************************/

  private static Map<String,JSONObject> unpackMetaData(Schema schema, Map<String,String> metaData)
  {
    Map<String,JSONObject> result = new HashMap<String,JSONObject>();
    JSONParser parser = new JSONParser();
    try
    {
      for (String metaDataKey : metaData.keySet())
        {
          JSONObject jsonObject = (JSONObject) parser.parse(metaData.get(metaDataKey));
          result.put(metaDataKey, jsonObject);
        }        
    } catch (ParseException e)
    {
      log.warn("UploadedFile.unpackMetaData(Issue unpacking metaData from string to JSONObject. Ignoring key"+metaData+")", e);
    }
    return result;
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
  
  /****************************************
  *
  *  addMetaData
  *
  ****************************************/
  
  public void addMetaData(String key, JSONObject metaDataValue)
  {
    if(metaDataValue != null && !metaDataValue.isEmpty())
      {
        this.metaData.put(key, metaDataValue);
      }
  }
  
}
