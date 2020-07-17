/****************************************************************************
*
*  ExtendedSubscriberProfileESSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ChangeLogESSinkTask;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.ReferenceDataReader;

import com.evolving.nglm.core.SystemTime;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public abstract class ExtendedSubscriberProfileESSinkConnector extends SimpleESSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static abstract class ExtendedSubscriberProfileESSinkTask extends ChangeLogESSinkTask<ExtendedSubscriberProfile>
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    protected ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;

    /*****************************************
    *
    *  start
    *
    *****************************************/

    @Override public void start(Map<String, String> taskConfig)
    {
      super.start(taskConfig);
      this.subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("extendedProfileSinkConnector-subscriberGroupEpoch", Integer.toHexString(getTaskNumber()), Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);
      SubscriberState.forceClassLoad();
    }

    /*****************************************
    *
    *  stop
    *
    *****************************************/

    @Override public void stop()
    {
      //
      //  reference reader
      //

      if (subscriberGroupEpochReader != null) subscriberGroupEpochReader.close();
      
      //
      //  super
      //

      super.stop();
    }

    /*****************************************
    *
    *  unpackRecord
    *
    *****************************************/
    
    @Override public ExtendedSubscriberProfile unpackRecord(SinkRecord sinkRecord) 
    {
      Object extendedSubscriberProfileValue = sinkRecord.value();
      Schema extendedSubscriberProfileValueSchema = sinkRecord.valueSchema();
      return ExtendedSubscriberProfile.getExtendedSubscriberProfileSerde().unpack(new SchemaAndValue(extendedSubscriberProfileValueSchema, extendedSubscriberProfileValue));
    }
    
    /*****************************************
    *
    *  getDocumentID
    *
    *****************************************/

    @Override public String getDocumentID(ExtendedSubscriberProfile extendedSubscriberProfile)
    {
      return extendedSubscriberProfile.getSubscriberID();
    }

    /*****************************************
    *
    *  abstract
    *
    *****************************************/

    protected abstract void addToDocumentMap(Map<String,Object> documentMap, ExtendedSubscriberProfile extendedSubscriberProfile, Date now); 

    /*****************************************
    *
    *  getDocumentMap
    *
    *****************************************/

    @Override public Map<String,Object> getDocumentMap(ExtendedSubscriberProfile extendedSubscriberProfile)
    {
      Date now = SystemTime.getCurrentTime();
      
      Map<String,Object> documentMap = new HashMap<String,Object>();
      documentMap.put("subscriberID", extendedSubscriberProfile.getSubscriberID());
      addToDocumentMap(documentMap, extendedSubscriberProfile, now);
      
      return documentMap;
    }    
  }
}
