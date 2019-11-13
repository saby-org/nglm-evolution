/****************************************************************************
*
*  PointFulfillmentRequestTransformer.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SimpleTransformStreamProcessor;
import com.evolving.nglm.core.StringKey;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.streams.KeyValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class PointFulfillmentRequestTransformer extends SimpleTransformStreamProcessor
{
  /*****************************************
  *
  *  config
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(PointFulfillmentRequestTransformer.class);

  /****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args) throws Exception
  {
    //
    //  instance  
    //

    PointFulfillmentRequestTransformer transformer = new PointFulfillmentRequestTransformer();

    //
    //  run
    //

    transformer.run(args, "streams-transform-pointfulfillmentrequest", StringKey.serde(), PointFulfillmentRequest.serde(), StringKey.serde(), PointFulfillmentRequest.serde());
  }

  /*****************************************
  *
  *  transform
  *
  *****************************************/

  @Override protected KeyValue<Object,Object> transform(Object key, Object value)
  {
    /*****************************************
    *
    *  fields
    *
    *****************************************/

    StringKey objectKey = (StringKey) key;
    PointFulfillmentRequest pointFulfillmentRequest = (PointFulfillmentRequest) value;

    /*****************************************
    *
    *  rekey
    *
    *****************************************/

    return new KeyValue(new StringKey(pointFulfillmentRequest.getSubscriberID()), pointFulfillmentRequest);
  }
}
