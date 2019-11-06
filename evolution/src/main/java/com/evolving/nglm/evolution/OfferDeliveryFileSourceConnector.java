package com.evolving.nglm.evolution;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.FileSourceConnector;
import com.evolving.nglm.core.FileSourceTask;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SubscriberIDService;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;


public class OfferDeliveryFileSourceConnector extends FileSourceConnector
{
  /****************************************
  *
  * attributes
  *
  ****************************************/

  private static SubscriberIDService subscriberIDService = null;

  @Override public Class<? extends Task> taskClass()
  {
    return OfferDeliveryFileSourceConnectorTask.class;
  }
  
  
  /*****************************************
  *
  *  class OfferDeliveryFileSourceConnectorTask
  *
  *****************************************/

  public static class OfferDeliveryFileSourceConnectorTask extends FileSourceTask
  {
    
    /*****************************************
    *
    *  config
    *
    *****************************************/

    //
    //  logger
    //

    private static final Logger log = LoggerFactory.getLogger(OfferDeliveryFileSourceConnector.class);

    /*****************************************
    *
    *  start
    *
    *****************************************/

    @Override public void start(Map<String, String> properties)
    {
      super.start(properties);
      
      //
      //  subscriberIDService
      //

      subscriberIDService = new SubscriberIDService(Deployment.getRedisSentinels(), "OfferDeliveryFileSourceConnector-" + Integer.toString(getTaskNumber()));
    }
    
    /*****************************************
    *
    *  stop
    *
    *****************************************/

    @Override public void stop()
    {
      //
      //  subscriberIDService
      //

      if (subscriberIDService != null) subscriberIDService.close();

      //
      //  super
      //

      super.stop();
    }

    /*****************************************
    *
    *  processRecord
    *
    *****************************************/
    
    @Override protected List<KeyValue> processRecord(String jsonRecord) throws FileSourceTaskException
    {
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
        
        String subscriberID = resolveSubscriberID(customerID);
        if (subscriberID != null)
          {
            jsonRoot.put("subscriberID", subscriberID);
            OfferDelivery offerDelivery = new OfferDelivery(jsonRoot);
            result = Collections.<KeyValue>singletonList(new KeyValue(Schema.STRING_SCHEMA, offerDelivery.getSubscriberID(), OfferDelivery.schema(), OfferDelivery.pack(offerDelivery)));
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
      return result;
    }
    
    /****************************************
    *
    *  resolveSubscriberID
    *
    ****************************************/

    private String resolveSubscriberID(String msisdn)
    {
      String result = null;
      while (!getStopRequested())
        {
          try
            {
              result = subscriberIDService.getSubscriberID(Deployment.getGetCustomerAlternateID(), msisdn);
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
      return result;
    }
  }
}
