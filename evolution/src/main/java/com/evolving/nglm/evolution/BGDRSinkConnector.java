package com.evolving.nglm.evolution;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import com.evolving.nglm.core.DeploymentCommon;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;

public class BGDRSinkConnector extends SimpleESSinkConnector
{

  
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return BGDRSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class BGDRSinkTask extends StreamESSinkTask<BadgeChange>
  {
    public static final String ES_FIELD_SUBSCRIBER_ID = "subscriberID";
    
    @Override public BadgeChange unpackRecord(SinkRecord sinkRecord) 
    {
      Object badgeChangeValue = sinkRecord.value();
      Schema badgeChangeValueSchema = sinkRecord.valueSchema();
      return BadgeChange.unpack(new SchemaAndValue(badgeChangeValueSchema, badgeChangeValue));
    }
    
    @Override
    protected String getDocumentIndexName(BadgeChange badgeChange)
    {
      String timeZone = DeploymentCommon.getDeployment(badgeChange.getTenantID()).getTimeZone();
      return this.getDefaultIndexName() + RLMDateUtils.formatDateISOWeek(badgeChange.getEventDate(), timeZone);
    }

    @Override public Map<String,Object> getDocumentMap(BadgeChange badgeChange)
    {
      Map<String,Object> documentMap = new HashMap<String,Object>();
      documentMap.put(ES_FIELD_SUBSCRIBER_ID, badgeChange.getSubscriberID());
      SinkConnectorUtils.putAlternateIDs(badgeChange.getAlternateIDs(), documentMap);
      documentMap.put("tenantID", badgeChange.getTenantID());
      documentMap.put("eventDatetime", badgeChange.getEventDate()!=null?RLMDateUtils.formatDateForElasticsearchDefault(badgeChange.getEventDate()):"");
      documentMap.put("eventID", badgeChange.getEventID());
      documentMap.put("moduleID", badgeChange.getModuleID());
      documentMap.put("featureID", badgeChange.getFeatureID());
      documentMap.put("origin", badgeChange.getOrigin());
      documentMap.put("returnCode", badgeChange.getReturnStatus().getGenericResponseCode());
      documentMap.put("returnStatus", badgeChange.getReturnStatus().getGenericResponseMessage());
      documentMap.put("badgeID", badgeChange.getBadgeID());
      return documentMap;
    }
  }


}
