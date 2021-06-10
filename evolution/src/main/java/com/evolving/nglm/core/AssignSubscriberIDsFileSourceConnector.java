/****************************************************************************
*
*  AssignSubscriberIDsFileSourceConnector.java
*
****************************************************************************/

package com.evolving.nglm.core;

import com.evolving.nglm.core.SubscriberStreamEvent.SubscriberAction;

import com.rii.utilities.JSONUtilities.JSONUtilitiesException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AssignSubscriberIDsFileSourceConnector extends FileSourceConnector
{
  /****************************************
  *
  *  attributes
  *
  ****************************************/

  private static SubscriberIDService subscriberIDService = null;
  static
  {
    subscriberIDService = new SubscriberIDService(Deployment.getRedisSentinels(), "AssignSubscriberIDsFileSourceConnector-Connector");
  }
  
  /****************************************
  *
  *  taskClass
  *
  ****************************************/

  @Override public Class<? extends Task> taskClass()
  {
    return AssignSubscriberIDsFileSourceTask.class;
  }

  /*****************************************
  *
  *  class AssignSubscriberIDsFileSourceTask
  *
  *****************************************/

  public static class AssignSubscriberIDsFileSourceTask extends com.evolving.nglm.core.FileSourceTask
  {
    /*****************************************
    *
    *  config
    *
    *****************************************/

    //
    //  logger
    //

    private static final Logger log = LoggerFactory.getLogger(AssignSubscriberIDsFileSourceConnector.class);

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
          /****************************************
          *
          *  decode alternateID/subscriberID
          *
          ****************************************/
          
          JSONObject jsonRoot = (JSONObject) (new JSONParser()).parse(record);
          AlternateID alternateID = Deployment.getAlternateIDs().get(JSONUtilities.decodeString(jsonRoot, "alternateID", true));
          boolean useInternalSubscriberID = Objects.equals(JSONUtilities.decodeString(jsonRoot, "alternateID", true), "internal");
          boolean useExternalSubscriberID = (alternateID != null) && alternateID.getExternalSubscriberID();
          String alternateSubscriberID = JSONUtilities.decodeString(jsonRoot, "alternateSubscriberID", true);
          int tenantID = JSONUtilities.decodeInteger(jsonRoot, "tenantID", 1);
          SubscriberAction subscriberAction = SubscriberAction.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "subscriberAction", "standard"));

          /****************************************
          *
          *  decode alternateIDs
          *
          ****************************************/
          
          JSONArray assignAlternateIDsJSON = JSONUtilities.decodeJSONArray(jsonRoot, "assignAlternateIDs", new JSONArray());
          Map<String,String> assignAlternateIDs = new HashMap<String,String>();
          for (int i=0; i<assignAlternateIDsJSON.size(); i++)
            {
              JSONObject assignAlternateIDJSON = (JSONObject) assignAlternateIDsJSON.get(i);
              String assignAlternateID = JSONUtilities.decodeString(assignAlternateIDJSON, "alternateID", true);
              String assignAlternateSubscriberID = JSONUtilities.decodeString(assignAlternateIDJSON, "alternateSubscriberID", false);
              assignAlternateIDs.put(assignAlternateID, assignAlternateSubscriberID);
            }

          /**n***************************************
          *
          *  validate alternateID
          *
          *****************************************/

          if (! useInternalSubscriberID && alternateID == null)
            {
              log.error("unknown alternateID: {}", JSONUtilities.decodeString(jsonRoot, "alternateID", "(not found)"));
              return Collections.<KeyValue>emptyList();
            }

          /*****************************************
          *
          *  validate/process subscriberAction
          *
          *****************************************/

          switch (subscriberAction)
            {
              case Standard:
                break;

              case Delete:
                if (assignAlternateIDs.size() > 0)
                  {
                    log.error("cannot specify alternateIDs when deleting subscriber");
                    return Collections.<KeyValue>emptyList();
                  }
                break;

              default:
                log.error("cannot request subscriber action {}", subscriberAction.getExternalRepresentation());
                return Collections.<KeyValue>emptyList();
            }

          /*****************************************
          *
          *  resolve
          *
          *****************************************/

          String effectiveSubscriberID;
          boolean autoProvision;
          if (useInternalSubscriberID)
            {
              effectiveSubscriberID = alternateSubscriberID;
              autoProvision = false;
            }
          else if (useExternalSubscriberID)
            {
              effectiveSubscriberID = alternateSubscriberID;
              autoProvision = true;
            }
          else
            {
              effectiveSubscriberID = resolveSubscriberID(alternateID, alternateSubscriberID);
              autoProvision = false;
            }
          
          /*****************************************
          *
          *  validate effectiveSubscriberID
          *
          *****************************************/

          if (effectiveSubscriberID == null)
            {
              log.error("unresolvable alternateSubscriberID: {} ({})", JSONUtilities.decodeString(jsonRoot, "alternateSubscriberID", "(not found)"), JSONUtilities.decodeString(jsonRoot, "alternateID", "(not found)"));
              return Collections.<KeyValue>emptyList();
            }
          
          /****************************************
          *
          *  send AssignSubscriberIDsEvent
          *
          ****************************************/
          
          AssignSubscriberIDs assignSubscriberIDs = new AssignSubscriberIDs(effectiveSubscriberID, SystemTime.getCurrentTime(), subscriberAction, assignAlternateIDs, tenantID);
          result = Collections.<KeyValue>singletonList(new KeyValue((autoProvision ? "assignexternalsubscriberids" : "assignsubscriberids"), Schema.STRING_SCHEMA, effectiveSubscriberID, AssignSubscriberIDs.schema(), AssignSubscriberIDs.pack(assignSubscriberIDs)));
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

    /****************************************
    *
    *  resolveSubscriberID
    *
    ****************************************/

    private String resolveSubscriberID(AlternateID alternateID, String alternateSubscriberID)
    {
      String subscriberID = null;
      while (!getStopRequested())
        {
          try
            {
              subscriberID = subscriberIDService.getSubscriberID(alternateID.getID(), alternateSubscriberID);
              break;
            }
          catch (SubscriberIDService.SubscriberIDServiceException e)
            {
              //
              // sleep before retry
              //

              synchronized (this)
                {
                  if (! getStopRequested())
                    {
                      try
                        {
                          this.wait(10*1000L);
                        }
                      catch (InterruptedException e1)
                        {
                          // ignore
                        }
                    }
                }
            }
        }
      return subscriberID;
    }
  }
}
