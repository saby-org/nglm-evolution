/****************************************************************************
*
*  SubscriberProfileESSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ChangeLogESSinkTask;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.ReferenceDataReader;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public abstract class SubscriberProfileESSinkConnector extends SimpleESSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static abstract class SubscriberProfileESSinkTask extends ChangeLogESSinkTask<SubscriberState>
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    protected ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
    private LoyaltyProgramService loyaltyProgramService;
    private PointService pointService;

    /*****************************************
    *
    *  start
    *
    *****************************************/

    @Override public void start(Map<String, String> taskConfig)
    {
      super.start(taskConfig);
      this.subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("profileSinkConnector-subscriberGroupEpoch", Integer.toHexString(getTaskNumber()), Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);
      SubscriberState.forceClassLoad();
      
      loyaltyProgramService = new LoyaltyProgramService(Deployment.getBrokerServers(), "sinkconnector-loyaltyprogramservice" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getLoyaltyProgramTopic(), false);
      loyaltyProgramService.start();
      
      pointService = new PointService(Deployment.getBrokerServers(), "sinkconnector-pointservice" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getPointTopic(), false);
      pointService.start();
      
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
    
    @Override public SubscriberState unpackRecord(SinkRecord sinkRecord) 
    {
      Object subscriberStateValue = sinkRecord.value();
      Schema subscriberStateValueSchema = sinkRecord.valueSchema();
      return SubscriberState.unpack(new SchemaAndValue(subscriberStateValueSchema, subscriberStateValue));
    }
    
    /*****************************************
    *
    *  getDocumentID
    *
    *****************************************/

    @Override public String getDocumentID(SubscriberState subscriberState)
    {
      return subscriberState.getSubscriberID();
    }

    /*****************************************
    *
    *  abstract
    *
    *****************************************/

    protected abstract void addToDocumentMap(Map<String,Object> documentMap, SubscriberProfile subscriberProfile, Date now); 

    /*****************************************
    *
    *  getDocumentMap
    *
    *****************************************/

    @Override public Map<String,Object> getDocumentMap(SubscriberState subscriberState)
    {
      /****************************************
      *
      *  extract SubscriberProfile
      *
      ****************************************/
      
      SubscriberProfile subscriberProfile = subscriberState.getSubscriberProfile();

      /*****************************************
      *
      *  context
      *
      *****************************************/

      Date now = SystemTime.getCurrentTime();

      /****************************************
      *
      *  documentMap
      *
      ****************************************/

      Map<String,Object> documentMap = new HashMap<String,Object>();
      documentMap.put("subscriberID", subscriberProfile.getSubscriberID());
      documentMap.put("evaluationDate", now); // @rl TODO: has the exact same content as lastUpdateDate, wrong date format (no timezone), is it used somewhere ? Purpose seems to be the date of evaluation of every metricHistory. Keep only one, maybe remove this one, if not used ?
      documentMap.put("evolutionSubscriberStatus", (subscriberProfile.getEvolutionSubscriberStatus() != null) ? subscriberProfile.getEvolutionSubscriberStatus().getExternalRepresentation() : null);
      documentMap.put("previousEvolutionSubscriberStatus", (subscriberProfile.getPreviousEvolutionSubscriberStatus() != null) ? subscriberProfile.getPreviousEvolutionSubscriberStatus().getExternalRepresentation() : null);
      documentMap.put("evolutionSubscriberStatusChangeDate", subscriberProfile.getEvolutionSubscriberStatusChangeDate());
      documentMap.put("universalControlGroup", subscriberProfile.getUniversalControlGroup());
      documentMap.put("language", subscriberProfile.getLanguage());
      documentMap.put("segments", subscriberProfile.getSegments(subscriberGroupEpochReader));
      documentMap.put("stratum", subscriberProfile.getSegmentsMap(subscriberGroupEpochReader));
      documentMap.put("targets", subscriberProfile.getTargets(subscriberGroupEpochReader));
      documentMap.put("loyaltyPrograms", subscriberProfile.getLoyaltyProgramsJSON(loyaltyProgramService, pointService));
      documentMap.put("pointFluctuations", subscriberProfile.getPointFluctuationsJSON());
      documentMap.put("pointBalances", subscriberProfile.getPointsBalanceJSON());
      documentMap.put("vouchers", subscriberProfile.getVouchersJSON());
      documentMap.put("subscriberJourneys", subscriberProfile.getSubscriberJourneysJSON());
      documentMap.put("lastUpdateDate", DatacubeGenerator.TIMESTAMP_FORMAT.format(now));
      documentMap.put("relationships", subscriberProfile.getSubscriberRelationsJSON());
      addToDocumentMap(documentMap, subscriberProfile, now);
      
      //
      //  return
      //
      
      return documentMap;
    }    
  }
}
