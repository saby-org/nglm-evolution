/****************************************************************************
*
*  SubscriberTraceControlFileSourceConnector.java
*
****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SubscriberTraceControlFileSourceConnector extends FileSourceConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/

  @Override public Class<? extends Task> taskClass()
  {
    return SubscriberTraceControlFileSourceTask.class;
  }

  /*****************************************
  *
  *  class UninterpretedFileSourceTask
  *
  *****************************************/

  public static class SubscriberTraceControlFileSourceTask extends com.evolving.nglm.core.FileSourceTask
  {
    /*****************************************
    *
    *  config
    *
    *****************************************/

    //
    //  logger
    //

    private static final Logger log = LoggerFactory.getLogger(SubscriberTraceControlFileSourceConnector.class);

    /*****************************************
    *
    *  static
    *
    *****************************************/

    private static boolean useAlternateID;
    private static boolean useAutoProvision;
    private static SubscriberIDService subscriberIDService;
    static
    {
      useAlternateID = Deployment.getSubscriberTraceControlAlternateID() != null;
      useAutoProvision = useAlternateID && Objects.equals(Deployment.getSubscriberTraceControlAlternateID(), Deployment.getExternalSubscriberID());
      subscriberIDService = useAlternateID ? new SubscriberIDService(Deployment.getRedisSentinels(), "SubscriberTraceControlFileSourceConnector") : null;
    }
    
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

    @Override protected List<KeyValue> processRecord(String record)
    {
      /*****************************************
      *
      *  parseRecord
      *
      *****************************************/

      //
      //  parse
      //

      String[] tokens = record.split("[,]", 3);

      //
      //  verify number of fields (version 1)
      //

      if (tokens.length >= 3)
        {
          log.error("processRecord unknown record format: {}", record);
          return Collections.<KeyValue>emptyList();
        }

      //
      //  version
      //

      Integer version = readInteger(tokens[0], record);
      if (version == null || version != 1)
        {
          log.error("processRecord unknown record version: {}", record);
          return Collections.<KeyValue>emptyList();
        }

      //
      //  subscriberID
      //

      String subscriberID = readString(tokens[1], record);
      if (subscriberID == null)
        {
          log.error("processRecord empty subscriberID: {}", record);
          return Collections.<KeyValue>emptyList();
        }

      //
      //  subscriberTraceEnabled
      //

      Boolean subscriberTraceEnabled = readBoolean(tokens[2], record);
      if (subscriberTraceEnabled == null)
        {
          log.error("processRecord empty subscriberTraceEnabled: {}", record);
          return Collections.<KeyValue>emptyList();
        }
      
      int tenant = 1; // by default
      if(tokens.length == 4)
        {
          tenant = Integer.parseInt(tokens[3]);
        }

      /*****************************************
      *
      *  effectiveSubscriberID (to handle alternateID and autoProvision)
      *
      *****************************************/

      //
      //  resolve subscriberID (if necessary)
      //
      
      String resolvedSubscriberID = null;
      if (useAlternateID)
        {
          try
            {
              resolvedSubscriberID = subscriberIDService.getSubscriberID(Deployment.getSubscriberTraceControlAlternateID(), subscriberID);
            }
          catch (SubscriberIDService.SubscriberIDServiceException e)
            {
              log.error("unable to resolve subscriberID: {}", subscriberID);
              StringWriter stackTraceWriter = new StringWriter();
              e.printStackTrace(new PrintWriter(stackTraceWriter, true));
              log.error(stackTraceWriter.toString());
              return Collections.<KeyValue>emptyList();
            }
        }

      //
      //  cases
      //

      String effectiveSubscriberID;
      boolean autoProvision;
      if (useAlternateID && resolvedSubscriberID != null)
        {
          effectiveSubscriberID = resolvedSubscriberID;
          autoProvision = false;
        }
      else if (useAlternateID && resolvedSubscriberID == null && useAutoProvision)
        {
          effectiveSubscriberID = subscriberID;
          autoProvision = true;
        }
      else if (useAlternateID && resolvedSubscriberID == null && ! useAutoProvision)
        {
          effectiveSubscriberID = null;
          autoProvision = false;
        }
      else
        {
          effectiveSubscriberID = subscriberID;
          autoProvision = false;
        }
      
      /*****************************************
      *
      *  create sourceRecord with subscriberTraceControl message (if appropriate)
      *
      *****************************************/

      List<KeyValue> result = null;
      if (effectiveSubscriberID != null)
        {
          SubscriberTraceControl subscriberTraceControl = new SubscriberTraceControl(effectiveSubscriberID, subscriberTraceEnabled.booleanValue(), tenant);
          result = Collections.<KeyValue>singletonList(new KeyValue((autoProvision ? "subscribertracecontrol-assignsubscriberid" : "subscribertracecontrol"), Schema.STRING_SCHEMA, effectiveSubscriberID, SubscriberTraceControl.schema(), SubscriberTraceControl.pack(subscriberTraceControl)));
        }
      else
        {
          log.error("could not process subscriber identified by {}", subscriberID);
          return Collections.<KeyValue>emptyList();
        }

      /*****************************************
      *
      *  sourceRecord
      *
      *****************************************/

      return result;
    }
  }
}
