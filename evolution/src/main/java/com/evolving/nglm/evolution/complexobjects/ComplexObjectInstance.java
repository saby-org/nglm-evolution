/****************************************************************************
*
*  ComplexObjectInstances.java
*
****************************************************************************/

package com.evolving.nglm.evolution.complexobjects;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.OfferService;

public class ComplexObjectInstance
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
  
  private static ComplexObjectTypeService complexObjectTypeService;
  static
  {
    complexObjectTypeService = new ComplexObjectTypeService(System.getProperty("broker.servers"), "complexobjectinstance-complexobjecttypeservice", Deployment.getComplexObjectTypeTopic(), false);
    complexObjectTypeService.start();
        
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("complexObjectInstance");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("complexObjectTypeID", Schema.STRING_SCHEMA);
    schemaBuilder.field("fieldValues", Schema.BYTES_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  // serde
  //

  private static ConnectSerde<ComplexObjectInstance> serde = new ConnectSerde<ComplexObjectInstance>(schema, false, ComplexObjectInstance.class, ComplexObjectInstance::pack, ComplexObjectInstance::unpack);
  
  //
  // accessor
  //

  public static Schema commonSchema() { return schema; }
  public static ConnectSerde<ComplexObjectInstance> commonSerde() { return serde; }

  /*****************************************
  *
  * data
  *
  *****************************************/

  private String complexObjectTypeID;
  private Map<String, ComplexObjectinstanceFieldValue> fieldValues;

  /*****************************************
  *
  * accessors
  *
  *****************************************/

  
  public String getComplexObjectTypeID() { return complexObjectTypeID; }
  public Map<String, ComplexObjectinstanceFieldValue> getFieldValues() { return fieldValues; }

  //
  //  setters
  //

  public void setComplexObjectTypeID(String complexObjectTypeID) { this.complexObjectTypeID = complexObjectTypeID; }
  public void setFieldValues(Map<String, ComplexObjectinstanceFieldValue> fieldValues) { this.fieldValues = fieldValues; }
  
  /*****************************************
  *
  * constructor unpack
  *
  *****************************************/

  public ComplexObjectInstance(String complexObjectTypeID, byte[] fieldValues)
  {
    this.complexObjectTypeID = complexObjectTypeID;
    this.fieldValues = unserializeFields(fieldValues);
  }

  /*****************************************
  *
  * pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    ComplexObjectInstance complexObjectInstance = (ComplexObjectInstance) value;
    Struct struct = new Struct(schema);

    struct.put("complexObjectTypeID", complexObjectInstance.getComplexObjectTypeID());
    struct.put("fieldValues", complexObjectInstance.serializeFields());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static ComplexObjectInstance unpack(SchemaAndValue schemaAndValue)
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
    String complexObjectTypeID = valueStruct.getString("complexObjectTypeID");
    byte[] fieldValues = valueStruct.getBytes("fieldValues");
    
    //
    //  return
    //

    return new ComplexObjectInstance(complexObjectTypeID, fieldValues);
  }

  /*****************************************
  *
  *  serializeFields
  *
  *****************************************/

  private byte[] serializeFields()
  {
    // for each field of this complex object, let switch on each supported datatype and encode a tag/length/value
    // <field_private_ID 2 bytes><length 2 bytes><value>, so overhead of 4 bytes per value
    ComplexObjectType complexObjectType = complexObjectTypeService.getActiveComplexObjectType(complexObjectTypeID, SystemTime.getCurrentTime());
    if(complexObjectType == null) { /*Should not happen as the detection is done before calling Pack */ return new byte[] {}; };
    for(ComplexObjectinstanceFieldValue fieldValue : this.getFieldValues().values())
      {
        ComplexObjectTypeField fieldType = complexObjectType.getFields().get(fieldValue.getPrivateFieldID());
        if(fieldType != null)
          {
            if(fieldValue.getValue() == null) {/*no need to serialize*/ continue;}
            switch (fieldType.getCriterionDataType())
            {
              case BooleanCriterion:
                Boolean value = (Boolean)fieldValue.getValue();
                byte[] result = new byte[5];
                result[0] = (byte) (fieldType.getPrivateID() & 0xFF00 >> 8);
                result[1] = (byte) (fieldType.getPrivateID() & 0xFF);
                result[2] = 0;
                result[3] = (byte) (value != null ? 1 : 0);
                result[4] = (byte) (value.booleanValue() ? 1 : 0); 
                break;
              case IntegerCriterion :
                int size 
                break;
              case DoubleCriterion :
                break;
              case StringCriterion :
                break;
              case StringSetCriterion :
                break;
              case DateCriterion :
                break;
              default:
                break;
              }
          }
        else 
          {
            // the field have disappeared from the config
            // nothing to do
          }
      }
    
  }
  /*****************************************
  *
  *  ComplexObjectinstanceFieldValue
  *
  *****************************************/

  private Map<String, ComplexObjectinstanceFieldValue> unserializeFields(byte[] fieldValues2)
  {
    // TODO Auto-generated method stub
    return null;
  }

  /*****************************************
  *
  *  toString
  *
  *****************************************/

  @Override
  public String toString()
  {
    return "ComplexObjectInstance [complexObjectTypeID=" + complexObjectTypeID + ", fieldValues=" + fieldValues + "]";
  }
  
}
