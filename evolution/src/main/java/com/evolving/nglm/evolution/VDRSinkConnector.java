/****************************************************************************
*
*  VDRSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class VDRSinkConnector extends SimpleESSinkConnector
{
  
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return VDRSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class VDRSinkTask extends StreamESSinkTask<VoucherChange>
  {

    private static String elasticSearchDateFormat = com.evolving.nglm.core.Deployment.getElasticsearchDateFormat();
    private DateFormat dateFormat = new SimpleDateFormat(elasticSearchDateFormat);

    public static final String ES_FIELD_SUBSCRIBER_ID = "subscriberID";
    public static final String ES_FIELD_VOUCHER_CODE = "voucherCode";
    
    @Override public VoucherChange unpackRecord(SinkRecord sinkRecord) 
    {
      Object voucherChangeValue = sinkRecord.value();
      Schema voucherChangeValueSchema = sinkRecord.valueSchema();
      return VoucherChange.unpack(new SchemaAndValue(voucherChangeValueSchema, voucherChangeValue));
    }
    
    @Override
    protected String getDocumentIndexName(VoucherChange voucherChange)
    {
      return this.getDefaultIndexName() + RLMDateUtils.printISOWeek(voucherChange.getEventDate());
    }

    @Override public Map<String,Object> getDocumentMap(VoucherChange voucherChange)
    {
      Map<String,Object> documentMap = new HashMap<String,Object>();
      
      documentMap.put(ES_FIELD_VOUCHER_CODE, voucherChange.getVoucherCode());
      documentMap.put(ES_FIELD_SUBSCRIBER_ID, voucherChange.getSubscriberID());
      SinkConnectorUtils.putAlternateIDs(voucherChange.getAlternateIDs(), documentMap);
      documentMap.put("voucherID", voucherChange.getVoucherID());
      documentMap.put("action", voucherChange.getAction());
      documentMap.put("eventDatetime", voucherChange.getEventDate()!=null?dateFormat.format(voucherChange.getEventDate()):"");
      documentMap.put("eventID", voucherChange.getEventID());
      documentMap.put("returnCode", voucherChange.getReturnStatus().getGenericResponseCode());
      documentMap.put("returnCodeDetails", voucherChange.getReturnStatus().getGenericResponseMessage());
      documentMap.put("origin", voucherChange.getOrigin());
      documentMap.put("moduleID", voucherChange.getModuleID());
      documentMap.put("featureID", voucherChange.getFeatureID()); 
      documentMap.put("expiryDate", voucherChange.getNewVoucherExpiryDate()); 
      
      return documentMap;
    }
  }
}
