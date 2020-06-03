/*****************************************************************************
*
*  ErrorRecord.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.Date;

public class ErrorRecord 
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("inputRecord", Schema.STRING_SCHEMA);
    schemaBuilder.field("processingDate", Timestamp.SCHEMA);
    schemaBuilder.field("processingSource", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("description", Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  }

  //
  //  accessor
  //

  public static Schema schema() { return schema; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  String inputRecord;
  Date processingDate;
  String processingSource;
  String description;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getInputRecord() { return inputRecord; }
  public Date getProcessingDate() { return processingDate; }
  public String getProcessingSource() { return processingSource; }
  public String getDescription() { return description; }

  /*****************************************
  *
  *  constructor - basic
  *
  *****************************************/

  public ErrorRecord(String inputRecord, Date processingDate, String processingSource, String description)
  {
    this.inputRecord = inputRecord;
    this.processingDate = processingDate;
    this.processingSource = processingSource;
    this.description = description;
  }

  /****************************************
  *
  *  pack
  *
  ****************************************/
  
  public static Object pack(Object value)
  {
    ErrorRecord errorRecord = (ErrorRecord) value;
    Struct struct = new Struct(schema);
    struct.put("inputRecord", errorRecord.getInputRecord());
    struct.put("processingDate", errorRecord.getProcessingDate());
    struct.put("processingSource", errorRecord.getProcessingSource());
    struct.put("description", errorRecord.getDescription());
    return struct;
  }

  /****************************************
  *
  *  constructor -- unpack
  *
  ****************************************/

  public static ErrorRecord unpack(SchemaAndValue schemaAndValue)
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
    String inputRecord = valueStruct.getString("inputRecord");
    Date processingDate = (Date) valueStruct.get("processingDate");
    String processingSource = valueStruct.getString("processingSource");
    String description = valueStruct.getString("description");
    
    //
    //  return
    //

    return new ErrorRecord(inputRecord, processingDate, processingSource, description);
  }

}
