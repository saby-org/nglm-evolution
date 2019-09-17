/*****************************************************************************
*
*  StateStoreSerde.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.ServerRuntimeException;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import io.confluent.connect.avro.AvroConverter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StateStoreSerde<T extends StateStore> implements Serde<T>
{
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  private Serde<T> connectSerde;
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public StateStoreSerde(ConnectSerde<T> connectSerde)
  {
    this.connectSerde = connectSerde.optionalSerde();
  }

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
        if (data.getKafkaRepresentation() == null) throw new ServerRuntimeException("stateStoreSerde found null representation");
        return data.getKafkaRepresentation();
      }
    };
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
        T result = connectSerde.deserializer().deserialize(topic, data);
        result.setKafkaRepresentation(data);
        return result;
      }
    };
  }

  /*****************************************
  *
  *  setKafkaRepresentation
  *
  *****************************************/

  public void setKafkaRepresentation(String topic, T data)
  {
    data.setKafkaRepresentation(connectSerde.serializer().serialize(topic, data));
  }
}
