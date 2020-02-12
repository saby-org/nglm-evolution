/****************************************************************************
*
*  CampaignESSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.ChangeLogESSinkTask;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;

import com.evolving.nglm.evolution.DeliveryRequest;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
   
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import org.json.simple.JSONObject;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class CampaignESSinkConnector extends SimpleESSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return CampaignESSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class CampaignESSinkTask extends StreamESSinkTask
  {
    @Override public Map<String,Object> getDocumentMap(SinkRecord sinkRecord)
    {

      /****************************************
      *
      *  extract object
      *
      ****************************************/

      Object GUIManagedObjectValue = sinkRecord.value();
      Schema GUIManagedObjectValueSchema = sinkRecord.valueSchema();
      GUIManagedObject guiManagedObject = GUIManagedObject.commonSerde().unpack(new SchemaAndValue(GUIManagedObjectValueSchema, GUIManagedObjectValue));

      //
      //  data
      //

      GUIManagedObjectType guiManagedObjectType = guiManagedObject.getGUIManagedObjectType();
      
      /****************************************
      *
      *  documentMap
      *
      ****************************************/

      Map<String,Object> documentMap = new HashMap<String,Object>();

      //
      //  return if not a campaign
      //

      if(guiManagedObjectType != GUIManagedObjectType.Campaign || !(guiManagedObject.getAccepted()))
        {
          return null;
        }
      else
        {
          Journey journey = (Journey) guiManagedObject;
          documentMap.put("campaignName", journey.getJourneyName());
          documentMap.put("campaignID", journey.getJourneyID());
          documentMap.put("startDate", guiManagedObject.getEffectiveStartDate());
          documentMap.put("endDate", guiManagedObject.getEffectiveEndDate());

          /****************************************
          *
          *  return
          *
          ****************************************/

          return documentMap;
        }
    }    
  }
}
