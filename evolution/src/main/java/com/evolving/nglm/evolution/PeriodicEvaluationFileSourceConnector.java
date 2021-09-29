/****************************************************************************
*
*  PeriodicEvaluationFileSourceConnector.java
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

public class PeriodicEvaluationFileSourceConnector extends FileSourceConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/

  @Override public Class<? extends Task> taskClass()
  {
    return PeriodicEvaluationFileSourceTask.class;
  }

  /*****************************************
  *
  *  class PeriodicEvaluationFileSourceTask
  *
  *****************************************/

  public static class PeriodicEvaluationFileSourceTask extends FileSourceTask
  {
    /*****************************************
    *
    *  config
    *
    *****************************************/

    //
    //  logger
    //

    private static final Logger log = LoggerFactory.getLogger(PeriodicEvaluationFileSourceConnector.class);

    /*****************************************
    *
    *  start
    *
    *****************************************/

    @Override public void start(Map<String, String> properties)
    {
      super.start(properties);
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
          JSONObject jsonRoot = (JSONObject) (new JSONParser()).parse(record);          
          String subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
          TimedEvaluation periodicEvaluation = new TimedEvaluation(subscriberID, SystemTime.getCurrentTime(), true, false, "CDR");
          result = Collections.<KeyValue>singletonList(new KeyValue(Schema.STRING_SCHEMA, periodicEvaluation.getSubscriberID(), TimedEvaluation.schema(), TimedEvaluation.pack(periodicEvaluation)));
        }
      catch (org.json.simple.parser.ParseException|JSONUtilitiesException e)
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
