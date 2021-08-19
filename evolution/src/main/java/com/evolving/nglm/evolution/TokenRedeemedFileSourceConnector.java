package com.evolving.nglm.evolution;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
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


public class TokenRedeemedFileSourceConnector extends FileSourceConnector
{

  @Override public Class<? extends Task> taskClass()
  {
    return TokenRedeemedFileSourceConnectorTask.class;
  }

  public static class TokenRedeemedFileSourceConnectorTask extends FileSourceTask
  {

    private static final Logger log = LoggerFactory.getLogger(TokenRedeemedFileSourceConnector.class);

    @Override protected List<KeyValue> processRecord(String jsonRecord) throws FileSourceTaskException, InterruptedException {
      List<KeyValue> result = null;
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
            TokenRedeemed tokenRedeemed = new TokenRedeemed(jsonRoot);
            result = Collections.<KeyValue>singletonList(new KeyValue(Schema.STRING_SCHEMA, tokenRedeemed.getSubscriberID(), TokenRedeemed.schema(), TokenRedeemed.pack(tokenRedeemed)));
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
