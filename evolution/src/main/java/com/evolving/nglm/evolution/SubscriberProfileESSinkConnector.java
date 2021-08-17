/****************************************************************************
*
*  SubscriberProfileESSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ChangeLogESSinkTask;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.RLMDateUtils;
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
    private DynamicCriterionFieldService dynamicCriterionFieldService; // For criterion init purpose only
    private SegmentationDimensionService segmentationDimensionService;

    /*****************************************
    *
    *  start
    *
    *****************************************/

    @Override public void start(Map<String, String> taskConfig)
    {
      super.start(taskConfig);
      this.subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("profileSinkConnector-subscriberGroupEpoch", Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);
      SubscriberState.forceClassLoad();

      dynamicCriterionFieldService = new DynamicCriterionFieldService(Deployment.getBrokerServers(), "NOT_USED", Deployment.getDynamicCriterionFieldTopic(), false);
      dynamicCriterionFieldService.start();
      CriterionContext.initialize(dynamicCriterionFieldService); // Workaround: CriterionContext must be initialized before creating the JourneyService. (explain ?)
      
      loyaltyProgramService = new LoyaltyProgramService(Deployment.getBrokerServers(), "sinkconnector-loyaltyprogramservice" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getLoyaltyProgramTopic(), false);
      loyaltyProgramService.start();
      
      pointService = new PointService(Deployment.getBrokerServers(), "sinkconnector-pointservice" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getPointTopic(), false);
      pointService.start();
      
      segmentationDimensionService = new SegmentationDimensionService(Deployment.getBrokerServers(), "sinkconnector-segmentationDimensionService" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getSegmentationDimensionTopic(), false);
      segmentationDimensionService.start();
      
    }

    /*****************************************
    *
    *  stop
    *
    *****************************************/

    @Override public void stop()
    {

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
      documentMap.put("tenantID", subscriberProfile.getTenantID());
      documentMap.put("evaluationDate", RLMDateUtils.formatDateForElasticsearchDefault(now)); // @rl TODO: has the exact same content as lastUpdateDate, wrong date format (no timezone), is it used somewhere ? Purpose seems to be the date of evaluation of every metricHistory. Keep only one, maybe remove this one, if not used ?
      documentMap.put("evolutionSubscriberStatus", (subscriberProfile.getEvolutionSubscriberStatus() != null) ? subscriberProfile.getEvolutionSubscriberStatus().getExternalRepresentation() : null);
      documentMap.put("previousEvolutionSubscriberStatus", (subscriberProfile.getPreviousEvolutionSubscriberStatus() != null) ? subscriberProfile.getPreviousEvolutionSubscriberStatus().getExternalRepresentation() : null);
      documentMap.put("evolutionSubscriberStatusChangeDate", RLMDateUtils.formatDateForElasticsearchDefault(subscriberProfile.getEvolutionSubscriberStatusChangeDate()));
      documentMap.put("universalControlGroup", subscriberProfile.getUniversalControlGroup());
      documentMap.put("language", subscriberProfile.getLanguage());
      documentMap.put("segments", subscriberProfile.getSegments(subscriberGroupEpochReader));
      documentMap.put("stratum", subscriberProfile.getSegmentsMap(subscriberGroupEpochReader));
      documentMap.put("statisticsStratum", subscriberProfile.getStatisticsSegmentsMap(subscriberGroupEpochReader, segmentationDimensionService));
      documentMap.put("targets", subscriberProfile.getTargets(subscriberGroupEpochReader));
      documentMap.put("loyaltyPrograms", subscriberProfile.getLoyaltyProgramsJSON(loyaltyProgramService, pointService));
      documentMap.put("pointFluctuations", subscriberProfile.getPointFluctuationsJSON());
      documentMap.put("pointBalances", subscriberProfile.getPointsBalanceJSON());
      documentMap.put("vouchers", subscriberProfile.getVouchersJSON());
      documentMap.put("tokens", subscriberProfile.getTokensJSON());
      documentMap.put("subscriberJourneys", subscriberProfile.getSubscriberJourneysJSON());
      documentMap.put("lastUpdateDate", RLMDateUtils.formatDateForElasticsearchDefault(now));
      documentMap.put("relationships", subscriberProfile.getSubscriberRelationsJSON());
      documentMap.put("exclusionInclusionList", subscriberProfile.getExclusionInclusionTargets(subscriberGroupEpochReader));
      documentMap.put("universalControlGroupPrevious",subscriberProfile.getUniversalControlGroupPrevious());
      log.info("before");
      documentMap.put("universalControlGroupChangeDate",RLMDateUtils.formatDateForElasticsearchDefault(subscriberProfile.getUniversalControlGroupChangeDate()));
      log.info("after");
      log.info("after2");
      addToDocumentMap(documentMap, subscriberProfile, now);
      
      //
      
      //  return
      //
      
      return documentMap;
    }    
  }
}
