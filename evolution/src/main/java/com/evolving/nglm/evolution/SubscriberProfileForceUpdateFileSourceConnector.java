package com.evolving.nglm.evolution;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.FileSourceConnector;
import com.evolving.nglm.core.FileSourceTask;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;


public class SubscriberProfileForceUpdateFileSourceConnector extends FileSourceConnector
{

  @Override public Class<? extends Task> taskClass()
  {
    return SubscriberProfileForceUpdateFileSourceConnectorTask.class;
  }

  public static class SubscriberProfileForceUpdateFileSourceConnectorTask extends FileSourceTask
  {

    private static final Logger log = LoggerFactory.getLogger(SubscriberProfileForceUpdateFileSourceConnector.class);

    @Override protected List<KeyValue> processRecord(String jsonRecord) throws FileSourceTaskException, InterruptedException {
      List<KeyValue> result = new ArrayList<KeyValue>();
      try
        {
          JSONObject jsonRoot = (JSONObject) (new JSONParser()).parse(jsonRecord);

          //
          //  customerID
          //

          String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);

          //
          //  resolve subscriberID
          //

          String subscriberID = resolveSubscriberID(Deployment.getGetCustomerAlternateID(),customerID);
          if (subscriberID != null)
            {
              jsonRoot.put("subscriberID", subscriberID);
              SubscriberProfileForceUpdate subscriberProfileForceUpdate = new SubscriberProfileForceUpdate(jsonRoot);
              result.add(new KeyValue(Schema.STRING_SCHEMA, subscriberProfileForceUpdate.getSubscriberID(), SubscriberProfileForceUpdate.schema(), SubscriberProfileForceUpdate.pack(subscriberProfileForceUpdate)));
            }
        }
      catch (org.json.simple.parser.ParseException|JSONUtilitiesException e)
        {
          log.error("processRecord error parsing: {}", jsonRecord);
          log.error("processRecord unknown unparsable json: {}", e.getMessage());
          StringWriter stackTraceWriter = new StringWriter();
          e.printStackTrace(new PrintWriter(stackTraceWriter, true));
          log.info(stackTraceWriter.toString());
          throw new FileSourceTaskException(e);
        }
      catch (GUIManagerException ge)
        {
          throw new FileSourceTaskException(ge);
        }
      return result;
    }
  }
}
