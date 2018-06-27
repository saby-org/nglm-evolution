/****************************************************************************
*
*  InteractionLogTransformer.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SimpleTransformStreamProcessor;
import com.evolving.nglm.core.StringKey;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.streams.KeyValue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class InteractionLogTransformer extends SimpleTransformStreamProcessor
{
  /*****************************************
  *
  *  config
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(InteractionLogTransformer.class);

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

    InteractionLogTransformer transformer = new InteractionLogTransformer();

    //
    //  run
    //

    transformer.run(args, "streams-transform-interactionlog", Serdes.String(), Serdes.String(), StringKey.serde(), InteractionLog.serde());
  }

  /*****************************************
  *
  *  transform
  *
  *****************************************/

  @Override protected KeyValue<Object,Object> transform(Object key, Object value)
  {
    String record = (String) value;
    try
      {
        JSONObject jsonRoot = (JSONObject) (new JSONParser()).parse(record);
        InteractionLog interactionLog = new InteractionLog(jsonRoot);
        StringKey subscriberID = new StringKey(interactionLog.getSubscriberID());
        return new KeyValue<Object,Object>(subscriberID, interactionLog);
      }
    catch (org.json.simple.parser.ParseException|JSONUtilitiesException e)
      {
        log.info("processRecord error parsing: {}", record);
        log.info("processRecord unknown unparsable json: {}", e.getMessage());
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.info(stackTraceWriter.toString());
        return null;
      }
  }
}
