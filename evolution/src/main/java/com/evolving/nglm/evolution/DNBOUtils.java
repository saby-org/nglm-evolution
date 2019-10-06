/*****************************************************************************
*
*  DNBOUtils.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class DNBOUtils
{

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(DNBOUtils.class);
  
  //
  //  ChoiceField
  //

  public enum ChoiceField
  {
    ManualAllocation("manualAllocation", "journey.status.manual"),
    AutomaticAllocation("automaticAllocation", "journey.status.automaticallocation"),
    AutomaticRedeem("automaticRedeem", "journey.status.automaticredeem"),
    Unknown("(unknown)", "(unknown)");
    private String externalRepresentation;
    private String choiceParameterName;
    private ChoiceField(String externalRepresentation, String choiceParameterName) { this.externalRepresentation = externalRepresentation; this.choiceParameterName = choiceParameterName; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public String getChoiceParameterName() { return choiceParameterName; }
    public static ChoiceField fromExternalRepresentation(String externalRepresentation) { for (ChoiceField enumeratedValue : ChoiceField.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }
  
  
  /*****************************************
  *
  *  class AllocationAction
  *
  *****************************************/

  public static class ActionManager extends com.evolving.nglm.evolution.ActionManager
  {
    private static KafkaProducer<byte[], byte[]> kafkaProducer;
    /*
    private static OfferService offerService;
    private static SegmentationDimensionService segmentationDimensionService;
    private static ProductService productService;
    private static ProductTypeService productTypeService;
    private static CatalogCharacteristicService catalogCharacteristicService;
    private static ReferenceDataReader<PropensityKey, PropensityState> propensityDataReader;
    private static ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
    private static ScoringStrategyService scoringStrategyService;
    private static DNBOMatrixService dnboMatrixService;
    */

    //private static SubscriberProfileService subscriberProfileService;
    //private static JourneyService journeyService;
    //private static JourneyObjectiveService journeyObjectiveService;
    //private static LoyaltyProgramService loyaltyProgramService;
    //private static OfferObjectiveService offerObjectiveService;
    //private static SubscriberMessageTemplateService subscriberMessageTemplateService;
    //private static SalesChannelService salesChannelService;
    //private static SubscriberIDService subscriberIDService;
    //private static TokenTypeService tokenTypeService;

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ActionManager(JSONObject configuration) throws GUIManagerException
    {
      super(configuration);
      String bootstrapServers = Deployment.getBrokerServers();
      Properties kafkaProducerProperties = new Properties();
      kafkaProducerProperties.put("bootstrap.servers", bootstrapServers);
      kafkaProducerProperties.put("acks", "all");
      kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
      kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
      kafkaProducer = new KafkaProducer<byte[], byte[]>(kafkaProducerProperties);
      /*
      String offerTopic = Deployment.getOfferTopic();
      String subscriberGroupEpochTopic = Deployment.getSubscriberGroupEpochTopic();
      String segmentationDimensionTopic = Deployment.getSegmentationDimensionTopic();
      //String journeyTopic = Deployment.getJourneyTopic();
      //String journeyObjectiveTopic = Deployment.getJourneyObjectiveTopic();
      //String loyaltyProgramTopic = Deployment.getLoyaltyProgramTopic();
      //String offerObjectiveTopic = Deployment.getOfferObjectiveTopic();
      //String redisServer = Deployment.getRedisSentinels();
      //String subscriberProfileEndpoints = Deployment.getSubscriberProfileEndpoints();

      String apiProcessKey = "0";
      String prefix = "journeyManager";
      offerService = new OfferService(bootstrapServers, prefix+"-offerservice-" + apiProcessKey , offerTopic, false);
      segmentationDimensionService = new SegmentationDimensionService(bootstrapServers, prefix+"-segmentationDimensionservice-001", segmentationDimensionTopic, false);
      //subscriberProfileService = new EngineSubscriberProfileService(subscriberProfileEndpoints);
      //journeyService = new JourneyService(bootstrapServers, prefix+"-journeyservice-" + apiProcessKey, journeyTopic, false);
      //journeyObjectiveService = new JourneyObjectiveService(bootstrapServers, prefix+"-journeyObjectiveService-" + apiProcessKey, journeyObjectiveTopic, false);
      //loyaltyProgramService = new LoyaltyProgramService(bootstrapServers, prefix+"-loyaltyprogramservice-" + apiProcessKey, loyaltyProgramTopic, false);
      //offerObjectiveService = new OfferObjectiveService(bootstrapServers, prefix+"-offerObjectiveService-" + apiProcessKey, offerObjectiveTopic, false);
      //subscriberMessageTemplateService = new SubscriberMessageTemplateService(bootstrapServers, prefix+"-subscribermessagetemplateservice-" + apiProcessKey, Deployment.getSubscriberMessageTemplateTopic(), false);
      //salesChannelService = new SalesChannelService(bootstrapServers, prefix+"-salesChannelService-" + apiProcessKey, Deployment.getSalesChannelTopic(), false);
      //tokenTypeService = new TokenTypeService(bootstrapServers, prefix+"-tokentypeservice-" + apiProcessKey, Deployment.getTokenTypeTopic(), false);
      //subscriberIDService = new SubscriberIDService(redisServer, prefix+"-" + apiProcessKey);
      subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader(prefix+"-subscribergroupepoch", apiProcessKey, bootstrapServers, subscriberGroupEpochTopic, SubscriberGroupEpoch::unpack);
      scoringStrategyService = new ScoringStrategyService(Deployment.getBrokerServers(), prefix+"-scoringstrategyservice-" + apiProcessKey, Deployment.getScoringStrategyTopic(), false);
      productService = new ProductService(Deployment.getBrokerServers(), prefix+"-productservice-" + apiProcessKey, Deployment.getProductTopic(), false);
      productTypeService = new ProductTypeService(Deployment.getBrokerServers(), prefix+"-producttypeservice-" + apiProcessKey, Deployment.getProductTypeTopic(), false);
      catalogCharacteristicService = new CatalogCharacteristicService(Deployment.getBrokerServers(), prefix+"-catalogcharacteristicservice-" + apiProcessKey, Deployment.getCatalogCharacteristicTopic(), false);
      dnboMatrixService = new DNBOMatrixService(Deployment.getBrokerServers(),prefix+"-matrixservice"+apiProcessKey,Deployment.getDNBOMatrixTopic(),false);

      propensityDataReader = ReferenceDataReader.<PropensityKey, PropensityState>startReader(prefix+"-propensitystate", prefix+"-propensityreader-"+apiProcessKey,
          Deployment.getBrokerServers(), Deployment.getPropensityLogTopic(), PropensityState::unpack);

      //
      //  start
      //

      offerService.start();
      segmentationDimensionService.start();
      //subscriberProfileService.start();
      //journeyService.start();
      //journeyObjectiveService.start();
      //loyaltyProgramService.start();
      //offerObjectiveService.start();
      //subscriberMessageTemplateService.start();
      //salesChannelService.start();
      //tokenTypeService.start();
      dnboMatrixService.start();
      */
      Properties producerProperties = new Properties();
      producerProperties.put("bootstrap.servers", bootstrapServers);
      producerProperties.put("acks", "all");
      producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
      producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
      kafkaProducer = new KafkaProducer<byte[], byte[]>(producerProperties);

    }
        
    /*****************************************
    *
    *  execute
    *
    *****************************************/

    @Override public JourneyRequest executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
    {
      ParameterMap map = subscriberEvaluationRequest.getJourneyNode().getNodeParameters();

      String field = "node.parameter.strategy";
      String strategyID = map.containsKey(field) ? (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,field) : null;
      if (strategyID == null) throw new ServerRuntimeException("No Strategy");
      if (log.isTraceEnabled()) log.trace("presentationStrategy : "+strategyID);

      field = "node.parameter.tokenType";
      String tokenTypeId = map.containsKey(field) ? (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,field) : null;
      if (tokenTypeId == null) throw new ServerRuntimeException("No Token Type Id");
      
      // the ID passed by the gui is the regexp, see GUIManager.evaluateEnumeratedValues()
      // TODO : replace with ID, and call TokenTypeService here to get the regexp (and pass ID to evolution engine)
      String codeFormat = tokenTypeId;
      if (log.isTraceEnabled()) log.trace("codeFormat : "+codeFormat);

      field = "node.parameter.choice";
      String choiceFieldStr = (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,field);
      ChoiceField choiceField = map.containsKey(field) ? ChoiceField.fromExternalRepresentation(choiceFieldStr) : null;
      if (choiceField == null) throw new ServerRuntimeException("No choice field");
      if (choiceField == ChoiceField.Unknown) throw new ServerRuntimeException("Unknown choice field: " + choiceFieldStr);
      if (log.isTraceEnabled()) log.trace("choiceField : "+choiceField.getExternalRepresentation());
      
      Boolean automaticAllocation = (choiceField == ChoiceField.AutomaticAllocation);
      Boolean automaticRedeem = (choiceField == ChoiceField.AutomaticRedeem);

      String tokenCode = TokenUtils.generateFromRegex(codeFormat);
      if (log.isTraceEnabled()) log.trace("We generated "+tokenCode);
      
      String deliveryRequestSource = subscriberEvaluationRequest.getJourneyState().getJourneyID();
      String journeyID = deliveryRequestSource; // (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.journey");
      JourneyRequest journeyRequest = new JourneyRequest(evolutionEventContext, deliveryRequestSource, journeyID);

      ParameterMap parameterMap = new ParameterMap();
      parameterMap.put("tokenCode", tokenCode);
      parameterMap.put("tokenStrategy", strategyID);
      parameterMap.put("tokenAutoBounded", automaticAllocation);
      parameterMap.put("tokenAutoRedeemed", automaticRedeem);

      switch (choiceField)
      {
        case ManualAllocation :
          {
            // Nothing more to do
            break;
          }
        case AutomaticAllocation :
        case AutomaticRedeem :
          {
            Date now = evolutionEventContext.now();
            String subscriberID = evolutionEventContext.getSubscriberState().getSubscriberID();
            
            // TODO following commented code is temporary
            // Right now, the NextBestOff journey box only allocates the token, and stores it in customer profile. 
            // The following code should be moved to some kind of queueing mechanism, like Deliverymanagers
            
            /*
            StringBuffer returnedLog = new StringBuffer();
            SubscriberProfile subscriberProfile = evolutionEventContext.getSubscriberState().getSubscriberProfile();
            ScoringStrategy strategy = scoringStrategyService.getActiveScoringStrategy(strategyID, now);
            double rangeValue = 0; // Not used
            DNBOMatrixAlgorithmParameters dnboMatrixAlgorithmParameters = new DNBOMatrixAlgorithmParameters(dnboMatrixService,rangeValue);

            // Allocate offers for this subscriber
            Collection<ProposedOfferDetails> presentedOffers;
            try
              {
                presentedOffers = TokenUtils.getOffers(
                    now , null,
                    subscriberProfile, strategy,
                    productService, productTypeService, catalogCharacteristicService, propensityDataReader, subscriberGroupEpochReader, segmentationDimensionService, dnboMatrixAlgorithmParameters, offerService,
                    returnedLog, subscriberID
                    );
              }
            catch (GetOfferException e)
              {
                throw new ServerRuntimeException("unknown offer while scoring : " + e.getLocalizedMessage()); 
              }
            //   store list of offerIds in parameterMap
            ArrayList<String> presentedOfferIDs = new ArrayList<>();
            for (ProposedOfferDetails presentedOffer : presentedOffers)
              {
                presentedOfferIDs.add(presentedOffer.getOfferId());
              }
            parameterMap.put("presentedOfferIDs", presentedOfferIDs);

            if (choiceField == ChoiceField.AutomaticRedeem)
              {
                if (presentedOfferIDs.isEmpty())
                  {
                    throw new ServerRuntimeException("cannot select first offer because list is empty");                           
                  }
                //   select 1st offer of the list
                String acceptedOfferID = presentedOfferIDs.get(0);
                parameterMap.put("acceptedOffersID", acceptedOfferID);

                // TODO
                String featureID = "featureID";
                String salesChannelID = "salesChannelID";
                try
                  {
                    ThirdPartyManager.purchaseOffer(subscriberID, acceptedOfferID, salesChannelID, 1, "deliveryRequestID", "eventID", "moduleID", featureID, "deliveryType", kafkaProducer);
                  }
                catch (ThirdPartyManagerException e)
                  {
                    throw new ServerRuntimeException(e.getLocalizedMessage());        
                  }
              }
              */
            break;
          }
          default :
            {
              throw new ServerRuntimeException("unknown choice field: " + choiceField.getExternalRepresentation());        
            }
      }
      
      // set tokenCode in subscriber profile
      
      Date now = new Date();
      String subscriberID = evolutionEventContext.getSubscriberState().getSubscriberID();
      SubscriberProfileForceUpdate subscriberProfileForceUpdate = new SubscriberProfileForceUpdate(subscriberID, now , parameterMap);

      //
      //  submit to kafka
      //

      kafkaProducer.send(new ProducerRecord<byte[], byte[]>(Deployment.getSubscriberProfileForceUpdateTopic(), StringKey.serde().serializer().serialize(Deployment.getSubscriberProfileForceUpdateTopic(), new StringKey(subscriberProfileForceUpdate.getSubscriberID())), SubscriberProfileForceUpdate.serde().serializer().serialize(Deployment.getSubscriberProfileForceUpdateTopic(), subscriberProfileForceUpdate)));
      return journeyRequest;
    }
  }

}