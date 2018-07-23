/****************************************************************************
*
*  RegressionCriteriaSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;

import com.rii.utilities.SystemTime;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RegressionCriteriaSinkConnector extends SimpleESSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return RegressionCriteriaSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class RegressionCriteriaSinkTask extends StreamESSinkTask
  {
    /****************************************
    *
    *  attributes
    *
    ****************************************/

    OfferService offerService = null;
    ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;

    /*****************************************
    *
    *  start
    *
    *****************************************/

    @Override public void start(Map<String, String> taskConfig)
    {
      //
      //  super
      //

      super.start(taskConfig);

      //
      //  services
      //

      offerService = new OfferService(Deployment.getBrokerServers(), "regression-criteria-sink-connector-" + Integer.toString(getTaskNumber()), Deployment.getOfferTopic(), false);
      offerService.start();
      subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("regression", "regression=criteria-subscriberGroupReader-" + Integer.toString(getTaskNumber()), Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);
    }

    /*****************************************
    *
    *  getDocumentMap
    *
    *****************************************/
    
    @Override public Map<String,Object> getDocumentMap(SinkRecord sinkRecord)
    {
      /****************************************
      *
      *  extract SubscriberProfile
      *
      ****************************************/

      Object subscriberProfileValue = sinkRecord.value();
      Schema subscriberProfileValueSchema = sinkRecord.valueSchema();
      SubscriberProfile subscriberProfile = SubscriberProfile.unpack(new SchemaAndValue(subscriberProfileValueSchema, subscriberProfileValue));

      /****************************************
      *
      *  retrieve active offers
      *
      ****************************************/

      Date now = SystemTime.getCurrentTime();
      Collection<Offer> activeOffers = offerService.getActiveOffers(now);

      /****************************************
      *
      *  evaluate subscriber against Criteria Offer (if available)
      *
      ****************************************/

      //
      //  evaluate
      //
      
      Map<String,Object> documentMap = null;
      for (Offer offer : activeOffers)
        {
          if (offer.getOfferID().equals("CriteriaOffer"))
            {
              //
              //  evaluate
              //
          
              SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, now);
              boolean eligible = offer.evaluateProfileCriteria(evaluationRequest);

              //
              //  result
              //

              documentMap = new HashMap<String,Object>();
              documentMap.put("subscriberID", subscriberProfile.getSubscriberID());
              documentMap.put("offerID", offer.getOfferID());
              documentMap.put("eligible", eligible ? "true" : "false");
              documentMap.put("evaluationDate", now);
            }
        }
      
      //
      //  defend against no CriteriaOffer -- result
      //
      
      if (documentMap == null)
        {
          documentMap = new HashMap<String,Object>();
          documentMap.put("subscriberID", subscriberProfile.getSubscriberID());
          documentMap.put("offerID", "(no CriteriaOffer)");
          documentMap.put("eligible", "false");
          documentMap.put("evaluationDate", now);
        }
      
      /****************************************
      *
      *  return
      *
      ****************************************/

      return documentMap;
    }    
  }
}
