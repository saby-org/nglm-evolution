/*****************************************************************************
*
*  ConnectSerde.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.FileWithVariableEvent;

import io.confluent.connect.avro.AvroConverter;
import kafka.log.Log;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ConnectSerde<T> implements Serde<T>
{
  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  //
  // logger
  //

  private static final Logger log = LoggerFactory.getLogger(ConnectSerde.class);

  private boolean isKey;
  private boolean useWrapper;
  private Map<Class, Schema> schemas = new HashMap<Class, Schema>();
  private Map<String, Schema> schemasByName = new HashMap<String, Schema>();
  private Map<String, Schema> optionalSchemasByName = new HashMap<String, Schema>();
  private Map<Schema, PackSchema> packSchemas = new HashMap<Schema, PackSchema>();
  private Map<Schema, UnpackSchema> unpackSchemas = new HashMap<Schema, UnpackSchema>();
  private UnpackSchema unpackSchemaCommon = null;
  private Schema schema;
  private Schema optionalSchema;
  private Converter converter;
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ConnectSerde(Schema schema, boolean isKey, Class<T> objectClass, PackSchema packSchema, UnpackSchema unpackSchema)
  {
    /****************************************
    *
    *  optionalSchema
    *
    ****************************************/

    Schema optionalSchema = null;
    switch (schema.type())
      {
        case STRUCT:
          SchemaBuilder structSchemaBuilder = SchemaBuilder.struct();
          structSchemaBuilder.name(schemaName(schema));
          structSchemaBuilder.version(schema.version());
          for (Field field : schema.fields())
            {
              structSchemaBuilder.field(field.name(), field.schema());
            }
          optionalSchema = structSchemaBuilder.optional().defaultValue(null).build();
          break;

        case STRING:
          optionalSchema = Schema.OPTIONAL_STRING_SCHEMA;
          break;

        case INT32:
          optionalSchema = Schema.OPTIONAL_INT32_SCHEMA;
          break;

        case INT64:
          optionalSchema = Schema.OPTIONAL_INT64_SCHEMA;
          break;

        default:
          throw new RuntimeException("unsupported type in ConnectSerde: " + schema.type());
      }

    /****************************************
    *
    *  basic
    *
    ****************************************/

    this.isKey = isKey;
    this.useWrapper = false;
    this.schemas.put(objectClass, schema);
    this.schemasByName.put(schemaName(schema), schema);
    this.optionalSchemasByName.put(schemaName(schema), optionalSchema);
    this.packSchemas.put(schema, packSchema);
    this.unpackSchemas.put(schema, unpackSchema);

    /****************************************
    *
    *  schema/optionalSchema
    *
    ****************************************/

    this.schema = schema;
    this.optionalSchema = optionalSchema;

    /****************************************
    *
    *  line format
    *
    ****************************************/

    if (System.getProperty("nglm.converter") != null && System.getProperty("nglm.converter").equals("Avro"))
      {
        Map<String, String> avroConverterConfigs = new HashMap<String,String>();
        avroConverterConfigs.put("schema.registry.url", System.getProperty("nglm.schemaRegistryURL"));
        this.converter = new AvroConverter();
        this.converter.configure(avroConverterConfigs, isKey);
      }
    else
      {
        this.converter = new JsonConverter();
        this.converter.configure(Collections.<String, Object>emptyMap(), isKey);
      }
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ConnectSerde(String schemaName, boolean isKey, UnpackSchema unpackSchemaCommon, ConnectSerde<? extends T>... serdes)
  {
    /****************************************
    *
    *  basic
    *
    ****************************************/
    
    this.isKey = isKey;
    this.useWrapper = true;
    this.unpackSchemaCommon = unpackSchemaCommon;
    for (ConnectSerde<? extends T> serde : serdes)
      {
        this.schemas.putAll(serde.schemas);
        this.schemasByName.putAll(serde.schemasByName);
        this.optionalSchemasByName.putAll(serde.optionalSchemasByName);
        this.packSchemas.putAll(serde.packSchemas);
        this.unpackSchemas.putAll(serde.unpackSchemas);
      }
    
    /****************************************
    *
    *  schema / optionalSchema
    *
    ****************************************/

    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name(schemaName);
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    for (String fieldName : optionalSchemasByName.keySet())
      {
        schemaBuilder.field(fieldName, optionalSchemasByName.get(fieldName));
      }
    this.schema = schemaBuilder.build();
    this.optionalSchema = schemaBuilder.optional().defaultValue(null).build();

    /****************************************
    *
    *  line format
    *
    ****************************************/

    if (System.getProperty("nglm.converter") != null && System.getProperty("nglm.converter").equals("Avro"))
      {
        Map<String, String> avroConverterConfigs = new HashMap<String,String>();
        avroConverterConfigs.put("schema.registry.url", System.getProperty("nglm.schemaRegistryURL"));
        this.converter = new AvroConverter();
        this.converter.configure(avroConverterConfigs, isKey);
      }
    else
      {
        this.converter = new JsonConverter();
        this.converter.configure(Collections.<String, Object>emptyMap(), isKey);
      }
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ConnectSerde(String schemaName, boolean isKey, ConnectSerde<? extends T>... serdes)
  {
    this(schemaName, isKey, (UnpackSchema) null, serdes);
  }

  /*****************************************
  *
  *  schema
  *
  *****************************************/

  public Schema schema() { return schema; }
  public Schema optionalSchema() { return optionalSchema; }

  /*****************************************
  *
  *  configure
  *
  *****************************************/

  @Override public void configure(Map<String, ?> configs, boolean isKey) { }

  /*****************************************
  *
  *  close
  *
  *****************************************/

  @Override public void close() { }

  /*****************************************
  *
  *  serializer
  *
  *****************************************/

  public Serializer<T> serializer()
  {
    return new Serializer<T>()
    {
      @Override public void configure(Map<String, ?> configs, boolean isKey) { }
      @Override public void close() { }
      @Override public byte[] serialize(String topic, T data)
      {
        if ("filewithvariableevent".equalsIgnoreCase(topic))
          {
            log.info("RAJ K schema " + schema.fields());
            log.info("RAJ K data " + data.toString());
          }
        return converter.fromConnectData(topic, schema, pack(data));
      }
    };
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public Object pack(T data)
  {
    Object result = null;
    if (useWrapper)
      {
        //
        //  data
        //

        Schema concreteSchema = schemas.get(data.getClass());
        Schema optionalConcreteSchema = optionalSchemasByName.get(schemaName(concreteSchema));
        PackSchema packSchema = packSchemas.get(concreteSchema);
        Object concretePackedData = packSchema.pack(data);

        //
        //  convert to optional
        //


        Object packedData = null;
        switch (concreteSchema.type())
          {
            case STRUCT:
              Struct optionalPackedData = new Struct(optionalConcreteSchema);
              for (Field field : concreteSchema.fields())
                {
                  optionalPackedData.put(field, ((Struct) concretePackedData).get(field));
                }
              packedData = optionalPackedData;
              break;

            case STRING:
              packedData = concretePackedData;
              break;

            case INT32:
              packedData = concretePackedData;
              break;

            case INT64:
              packedData = concretePackedData;
              break;

            default:
              throw new RuntimeException("unsupported type in ConnectSerde: " + schema.type());
          }


        //
        //  wrap
        //

        Struct wrapper = new Struct(schema);
        wrapper.put(schemaName(concreteSchema), packedData);

        //
        //  result
        //
        
        result = wrapper;
      }
    else
      {
        //
        //  data
        //

        PackSchema packSchema = packSchemas.get(schema);
        Object packedData = packSchema.pack(data);

        //
        //  result
        //
        
        result = packedData;
      }

    //
    //  return
    //
    
    return result;
  }

  /*****************************************
  *
  *  optionalSerializer
  *
  *****************************************/

  public Serializer<T> optionalSerializer()
  {
    return new Serializer<T>()
    {
      @Override public void configure(Map<String, ?> configs, boolean isKey) { }
      @Override public void close() { }
      @Override public byte[] serialize(String topic, T data)
      {
        Object packData = packOptional(data);
        try
        {
          return converter.fromConnectData(topic, optionalSchema, packData);
        }
        catch (DataException e)
        {
          // wrap exception to add more info
          String moreData = "null fields : [ ";
          if (packData instanceof Struct)
            {
              Struct struct = (Struct) packData;
              Schema schema = struct.schema();
              if (schema != null)
                {
                  for (Field field : schema.fields())
                    {
                      if (struct.get(field.name()) == null)
                        {
                          moreData += field.name() + " ";
                        }
                    }
                }
            }
          moreData += "]";
          throw new DataException(packData.toString() + " " + optionalSchema.toString() + " " + moreData, e);
        }
      }
    };
  }

  /*****************************************
  *
  *  packOptional
  *
  *****************************************/

  public Object packOptional(T data)
  {
    Object result = null;
    if (data != null && useWrapper)
      {
        //
        //  data
        //

        Schema concreteSchema = schemas.get(data.getClass());
        Schema optionalConcreteSchema = optionalSchemasByName.get(schemaName(concreteSchema));
        PackSchema packSchema = packSchemas.get(concreteSchema);
        Object concretePackedData = packSchema.pack(data);

        //
        //  convert to optional
        //

        Object packedData = null;
        switch (concreteSchema.type())
          {
            case STRUCT:
              Struct optionalPackedData = new Struct(optionalConcreteSchema);
              for (Field field : concreteSchema.fields())
                {
                  optionalPackedData.put(field, ((Struct) concretePackedData).get(field));
                }
              packedData = optionalPackedData;
              break;

            case STRING:
              packedData = concretePackedData;
              break;

            case INT32:
              packedData = concretePackedData;
              break;

            case INT64:
              packedData = concretePackedData;
              break;

            default:
              throw new RuntimeException("unsupported type in ConnectSerde: " + schema.type());
          }


        //
        //  wrap
        //

        Struct wrapper = new Struct(optionalSchema);
        wrapper.put(schemaName(concreteSchema), packedData);

        //
        //  result
        //
        
        result = wrapper;
      }
    else if (data != null)
      {
        //
        //  data
        //

        PackSchema packSchema = packSchemas.get(schema);
        Object concretePackedData = packSchema.pack(data);

        //
        //  convert to optional
        //

        Object packedData = null;
        switch (schema.type())
          {
            case STRUCT:
              Struct optionalPackedData = new Struct(optionalSchema);
              for (Field field : schema.fields())
                {
                  optionalPackedData.put(field, ((Struct) concretePackedData).get(field));
                }
              packedData = optionalPackedData;
              break;

            case STRING:
              packedData = concretePackedData;
              break;

            case INT32:
              packedData = concretePackedData;
              break;

            case INT64:
              packedData = concretePackedData;
              break;

            default:
              throw new RuntimeException("unsupported type in ConnectSerde: " + schema.type());
          }

        //
        //  result
        //
        
        result = packedData;
      }
    else
      {
        result = null;
      }

    //
    //  return
    //
    
    return result;
  }

  /*****************************************
  *
  *  deserializer
  *
  *****************************************/

  public Deserializer<T> deserializer()
  {
    return new Deserializer<T>()
    {
      @Override public void configure(Map<String, ?> configs, boolean isKey) { }
      @Override public void close() { }
      @Override public T deserialize(String topic, byte[] data)
      {
        if (data == null) throw new SerializationException("error deserializing null value");
        return unpack(converter.toConnectData(topic, data));
      }
    };
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public T unpack(SchemaAndValue topLevelSchemaAndValue)
  {
    T result = null;
    Schema topLevelPersistedSchema = topLevelSchemaAndValue.schema();
    if (!schemasByName.keySet().contains(schemaName(topLevelPersistedSchema)))
      {
        //
        //  get wrapper
        //

        SchemaAndValue wrapperSchemaAndValue = topLevelSchemaAndValue;
        Schema persistedSchema = wrapperSchemaAndValue.schema();
        Struct wrapper = (Struct) wrapperSchemaAndValue.value();

        //
        //  get (single) schema and value
        //

        Schema persistedValueSchema = null;
        Object value = null;
        for (Field field : persistedSchema.fields())
          {
            if (wrapper.get(field) != null)
              {
                persistedValueSchema = field.schema();
                value = wrapper.get(field);
              }
          }

        //
        //  validate
        //

        if (value == null) throw new SerializationException("error deserializing null value");

        //
        //  unpack
        //

        Schema schema = schemasByName.get(schemaName(persistedValueSchema));
        UnpackSchema unpackSchema = (unpackSchemaCommon != null) ? unpackSchemaCommon : unpackSchemas.get(schema);
        result = (T) unpackSchema.unpack(new SchemaAndValue(persistedValueSchema, value));
      }
    else
      {
        //
        //  get unpack
        //

        SchemaAndValue schemaAndValue = topLevelSchemaAndValue;
        Schema persistedSchema = schemaAndValue.schema();
        Schema schema = schemasByName.get(schemaName(persistedSchema));
        UnpackSchema unpackSchema = (unpackSchemaCommon != null) ? unpackSchemaCommon : unpackSchemas.get(schema);

        //
        //  unpack
        //

        result = (T) unpackSchema.unpack(schemaAndValue);
      }
    return result;
  }

  /*****************************************
  *
  *  optionalDeserializer
  *
  *****************************************/

  public Deserializer<T> optionalDeserializer()
  {
    return new Deserializer<T>()
    {
      @Override public void configure(Map<String, ?> configs, boolean isKey) { }
      @Override public void close() { }
      @Override public T deserialize(String topic, byte[] data)
      {
        return unpackOptional((data != null) ? converter.toConnectData(topic, data) : new SchemaAndValue(optionalSchema, null));
      }
    };
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public T unpackOptional(SchemaAndValue topLevelSchemaAndValue)
  {
    return (topLevelSchemaAndValue.value() != null) ? unpack(topLevelSchemaAndValue) : null;
  }


  /*****************************************
  *
  *  optionalSerde
  *
  *****************************************/

  public Serde<T> optionalSerde()
  {
    return new OptionalConnectSerde();
  }

  /*****************************************
  *
  *  class OptionalConnectSerde
  *
  *****************************************/

  private class OptionalConnectSerde implements Serde<T>
  {
    //
    //  configure
    //

    @Override public void configure(Map<String, ?> configs, boolean isKey) { }

    //
    //  close
    //

    @Override public void close() { }

    //
    //  serializer
    //

    @Override public Serializer<T> serializer() { return optionalSerializer(); }

    //
    //  deserializera
    //
    
    @Override public Deserializer<T> deserializer() { return optionalDeserializer(); }
  }

  /*****************************************
  *
  *  schemaName
  *
  *****************************************/

  private String schemaName(Schema schema)
  {
    String result;
    switch (schema.type())
      {
        case STRUCT:
          result = schema.name();
          break;

        case STRING:
        case INT32:
        case INT64:
          result = "raw" + schema.type().toString();
          break;

        default:
          throw new RuntimeException("unsupported type in ConnectSerde: " + schema.type());
      }
    return result;
  }

  /*****************************************
  *
  *  interface PackSchema
  *
  *****************************************/

  public interface PackSchema
  {
    public Object pack(Object value);
  }

  /*****************************************
  *
  *  interface UnpackSchema
  *
  *****************************************/

  public interface UnpackSchema
  {
    public Object unpack(SchemaAndValue schemaAndValue);
  }
}
