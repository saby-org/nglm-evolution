/****************************************************************************
*
*  ExternalDeliveryRequestFileSourceConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.FileSourceConnector;
import com.evolving.nglm.core.FileSourceTask;
import com.evolving.nglm.core.FileSourceTask.KeyValue;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.core.UniqueKeyServer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ExternalDeliveryRequestFileSourceConnector extends FileSourceConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/

  @Override public Class<? extends Task> taskClass()
  {
    return ExternalDeliveryRequestFileSourceTask.class;
  }

  /*****************************************
  *
  *  class ExternalDeliveryRequestFileSourceTask
  *
  *****************************************/

  public static class ExternalDeliveryRequestFileSourceTask extends FileSourceTask
  {
    /*****************************************
    *
    *  config
    *
    *****************************************/

    //
    //  logger
    //

    private static final Logger log = LoggerFactory.getLogger(ExternalDeliveryRequestFileSourceConnector.class);

    //
    //  uniqueKeyServer
    //

    private static UniqueKeyServer uniqueKeyServer = new UniqueKeyServer();

    //
    //  constructors/serdes
    //

    private Map<String,Constructor> deliveryRequestConstructors = new HashMap<String,Constructor>();
    private Map<String,ConnectSerde<DeliveryRequest>> deliveryRequestSerdes = new HashMap<String,ConnectSerde<DeliveryRequest>>();

    /*****************************************
    *
    *  start
    *
    *****************************************/

    @Override public void start(Map<String, String> properties)
    {
      /*****************************************
      *
      *  generate "topic" declaration
      *
      ****************************************/

      //
      //  generate
      //

      int deliveryManagerCount = 0;
      StringBuilder topic = new StringBuilder();
      for (DeliveryManagerDeclaration deliveryManager : Deployment.getDeliveryManagers().values())
        {
          if (deliveryManagerCount > 0) topic.append(",");
          topic.append(deliveryManager.getDeliveryType());
          topic.append(":");
          topic.append(deliveryManager.getRequestTopic());
          deliveryManagerCount += 1;
        }

      //
      //  populate
      //

      properties.put("topic", topic.toString());

      /*****************************************
      *
      *  super
      *
      *****************************************/

      super.start(properties);

      /*****************************************
      *
      *  deliveryRequestConstructors
      *
      *****************************************/

      for (DeliveryManagerDeclaration deliveryManager : Deployment.getDeliveryManagers().values())
        {
          try
            {
              Class<DeliveryRequest> deliveryRequestClass = (Class<DeliveryRequest>) Class.forName(deliveryManager.getRequestClassName());
              Constructor deliveryRequestConstructor = deliveryRequestClass.getDeclaredConstructor(JSONObject.class, DeliveryManagerDeclaration.class);
              Method serdeMethod = deliveryRequestClass.getMethod("serde");
              ConnectSerde<DeliveryRequest> deliveryRequestSerde = (ConnectSerde<DeliveryRequest>) serdeMethod.invoke(null);
              deliveryRequestConstructors.put(deliveryManager.getDeliveryType(), deliveryRequestConstructor);
              deliveryRequestSerdes.put(deliveryManager.getDeliveryType(), deliveryRequestSerde);
            }
          catch (InvocationTargetException e)
            {
              throw new RuntimeException(e.getCause());
            }
          catch (NoSuchMethodException|IllegalAccessException|ClassNotFoundException e)
            {
              throw new RuntimeException(e);
            }
        }
    }
    
    /*****************************************
    *
    *  processRecord
    *
    *****************************************/

    @Override protected List<KeyValue> processRecord(String record) throws FileSourceTaskException
    {
      List<KeyValue> result = null;
      try
        {
          /*****************************************
          *
          *  parse
          *
          *****************************************/

          JSONObject jsonRoot = (JSONObject) (new JSONParser()).parse(record);          

          /****************************************
          *
          *  deliveryManager
          *
          ****************************************/
          
          String deliveryType = JSONUtilities.decodeString(jsonRoot, "deliveryType", true);
          DeliveryManagerDeclaration deliveryManager = Deployment.getDeliveryManagers().get(deliveryType);
          Constructor deliveryRequestConstructor = deliveryRequestConstructors.get(deliveryType);
          ConnectSerde<DeliveryRequest> deliveryRequestSerde = deliveryRequestSerdes.get(deliveryType);
          if (deliveryManager == null) throw new FileSourceTaskException("unknown deliveryType: " + deliveryType);

          /*****************************************
          *
          *  deliveryRequestID (if necessary)
          *
          *****************************************/

          String deliveryRequestID = JSONUtilities.decodeString(jsonRoot, "deliveryRequestID", false);
          if (deliveryRequestID == null)
            {
              deliveryRequestID = String.format("%d", uniqueKeyServer.getKey());
              jsonRoot.put("deliveryRequestID", deliveryRequestID);
            }

          /****************************************
          *
          *  constructor
          *
          ****************************************/
          
          DeliveryRequest deliveryRequest = null;
          try
            {
              deliveryRequest = (DeliveryRequest) deliveryRequestConstructor.newInstance(jsonRoot, deliveryManager);
            }
          catch (InvocationTargetException e)
            {
              if (e.getCause() instanceof JSONUtilitiesException) throw (JSONUtilitiesException) e.getCause();
              throw new RuntimeException(e.getCause());
            }
          catch (InstantiationException|IllegalAccessException e)
            {
              throw new RuntimeException(e);
            }

          /*****************************************
          *
          *  result
          *
          *****************************************/
              
          result = Collections.<KeyValue>singletonList(new KeyValue(deliveryRequest.getDeliveryType(), Schema.STRING_SCHEMA, deliveryRequest.getDeliveryRequestID(), deliveryRequestSerde.schema(), deliveryRequestSerde.pack(deliveryRequest)));;
        }
      catch (org.json.simple.parser.ParseException | RuntimeException e)
        {
          log.info("processRecord error parsing: {}", record);
          log.info("processRecord unknown unparsable json: {}", e.getMessage());
          StringWriter stackTraceWriter = new StringWriter();
          e.printStackTrace(new PrintWriter(stackTraceWriter, true));
          log.info(stackTraceWriter.toString());
          throw new FileSourceTaskException(e);
        }
      return result;
    }
  }
}
