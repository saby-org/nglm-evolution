/****************************************************************************
*
*  JourneyMetricESSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.DeploymentCommon;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;
import java.util.Map;

public class TokenChangeESSinkConnector extends SimpleESSinkConnector
{
  
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return TokenChangeESSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class TokenChangeESSinkTask extends StreamESSinkTask<TokenChange>
  {
    public static final String ES_FIELD_SUBSCRIBER_ID = "subscriberID";
    public static final String ES_FIELD_TOKEN_CODE = "tokenCode";
    
    @Override public TokenChange unpackRecord(SinkRecord sinkRecord) 
    {
      Object tokenChangeValue = sinkRecord.value();
      Schema tokenChangeValueSchema = sinkRecord.valueSchema();
      return TokenChange.unpack(new SchemaAndValue(tokenChangeValueSchema, tokenChangeValue));
    }
    
    @Override
    protected String getDocumentIndexName(TokenChange tokenChange)
    {
      String timeZone = DeploymentCommon.getDeployment(tokenChange.getTenantID()).getTimeZone();
      return this.getDefaultIndexName() + RLMDateUtils.formatDateISOWeek(tokenChange.getEventDate(), timeZone);
    }

    @Override public Map<String,Object> getDocumentMap(TokenChange tokenChange)
    {
      Map<String,Object> documentMap = new HashMap<String,Object>();
      
      documentMap.put(ES_FIELD_TOKEN_CODE, tokenChange.getTokenCode());
      documentMap.put(ES_FIELD_SUBSCRIBER_ID, tokenChange.getSubscriberID());
      documentMap.put("tenantID", tokenChange.getTenantID());
      documentMap.put("action", tokenChange.getAction());
      documentMap.put("eventDatetime", tokenChange.getEventDate()!=null?RLMDateUtils.formatDateForElasticsearchDefault(tokenChange.getEventDate()):"");
      documentMap.put("eventID", tokenChange.getEventID());
      documentMap.put("returnCode", tokenChange.getReturnStatus());
      documentMap.put("origin", tokenChange.getOrigin());
      documentMap.put("moduleID", tokenChange.getModuleID());
      documentMap.put("featureID", tokenChange.getFeatureID());
      documentMap.put("acceptedOfferID", tokenChange.getAcceptedOfferID());
      documentMap.put("presentedOffersIDs", tokenChange.getPresentedOffersIDs());
      return documentMap;
    }
  }
}
