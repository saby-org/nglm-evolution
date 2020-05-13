/*****************************************************************************
*
*  GUIManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SubscriberIDService;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionException;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;
import com.evolving.nglm.evolution.SegmentationDimension.SegmentationDimensionTargetingType;

public class GUIManagerGeneral extends GUIManager
{

  private static final Logger log = LoggerFactory.getLogger(GUIManagerGeneral.class);

  public GUIManagerGeneral(JourneyService journeyService, SegmentationDimensionService segmentationDimensionService, PointService pointService, OfferService offerService, ReportService reportService, PaymentMeanService paymentMeanService, ScoringStrategyService scoringStrategyService, PresentationStrategyService presentationStrategyService, CallingChannelService callingChannelService, SalesChannelService salesChannelService, SourceAddressService sourceAddressService, SupplierService supplierService, ProductService productService, CatalogCharacteristicService catalogCharacteristicService, ContactPolicyService contactPolicyService, JourneyObjectiveService journeyObjectiveService, OfferObjectiveService offerObjectiveService, ProductTypeService productTypeService, UCGRuleService ucgRuleService, DeliverableService deliverableService, TokenTypeService tokenTypeService, VoucherTypeService voucherTypeService, VoucherService voucherService, SubscriberMessageTemplateService subscriberTemplateService, SubscriberProfileService subscriberProfileService, SubscriberIDService subscriberIDService, DeliverableSourceService deliverableSourceService, UploadedFileService uploadedFileService, TargetService targetService, CommunicationChannelService communicationChannelService, CommunicationChannelBlackoutService communicationChannelBlackoutService, LoyaltyProgramService loyaltyProgramService, ResellerService resellerService, ExclusionInclusionTargetService exclusionInclusionTargetService, SegmentContactPolicyService segmentContactPolicyService, CriterionFieldAvailableValuesService criterionFieldAvailableValuesService, DNBOMatrixService dnboMatrixService, DynamicCriterionFieldService dynamicCriterionFieldService, DynamicEventDeclarationsService dynamicEventDeclarationsService, JourneyTemplateService journeyTemplateService, KafkaResponseListenerService<StringKey,PurchaseFulfillmentRequest> purchaseResponseListenerService, SharedIDService subscriberGroupSharedIDService, ZookeeperUniqueKeyServer zuks, int httpTimeout, KafkaProducer<byte[], byte[]> kafkaProducer, RestHighLevelClient elasticsearch, SubscriberMessageTemplateService subscriberMessageTemplateService, String getCustomerAlternateID, GUIManagerContext guiManagerContext, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, ReferenceDataReader<String,JourneyTrafficHistory> journeyTrafficReader, ReferenceDataReader<String,RenamedProfileCriterionField> renamedProfileCriterionFieldReader, ReferenceDataReader<PropensityKey, PropensityState> propensityDataReader)
  {
    super.callingChannelService = callingChannelService;
    super.catalogCharacteristicService = catalogCharacteristicService;
    super.communicationChannelBlackoutService = communicationChannelBlackoutService;
    super.communicationChannelService = communicationChannelService;
    super.contactPolicyService = contactPolicyService;
    super.criterionFieldAvailableValuesService = criterionFieldAvailableValuesService;
    super.deliverableService = deliverableService;
    super.deliverableSourceService = deliverableSourceService;
    super.exclusionInclusionTargetService = exclusionInclusionTargetService;
    super.journeyObjectiveService = journeyObjectiveService;
    super.journeyService = journeyService;
    super.loyaltyProgramService = loyaltyProgramService;
    super.offerObjectiveService = offerObjectiveService;
    super.offerService = offerService;
    super.paymentMeanService = paymentMeanService;
    super.pointService = pointService;
    super.presentationStrategyService = presentationStrategyService;
    super.productService = productService;
    super.productTypeService = productTypeService;
    super.reportService = reportService;
    super.resellerService = resellerService;
    super.salesChannelService = salesChannelService;
    super.scoringStrategyService = scoringStrategyService;
    super.segmentationDimensionService = segmentationDimensionService;
    super.segmentContactPolicyService = segmentContactPolicyService;
    super.sourceAddressService = sourceAddressService;
    super.subscriberIDService = subscriberIDService;
    super.subscriberMessageTemplateService = subscriberMessageTemplateService;
    super.subscriberProfileService = subscriberProfileService;
    super.supplierService = supplierService;
    super.targetService = targetService;
    super.tokenTypeService = tokenTypeService;
    super.ucgRuleService = ucgRuleService;
    super.uploadedFileService = uploadedFileService;
    super.voucherService = voucherService;
    super.voucherTypeService = voucherTypeService;
    super.dnboMatrixService = dnboMatrixService;
    super.dynamicCriterionFieldService = dynamicCriterionFieldService;
    super.dynamicEventDeclarationsService = dynamicEventDeclarationsService;
    super.journeyTemplateService = journeyTemplateService;
    super.purchaseResponseListenerService = purchaseResponseListenerService;
    super.subscriberGroupSharedIDService = subscriberGroupSharedIDService;
    super.purchaseResponseListenerService = purchaseResponseListenerService;

    super.zuks = zuks;
    super.httpTimeout = httpTimeout;
    super.kafkaProducer = kafkaProducer;
    super.elasticsearch = elasticsearch;
    
    super.getCustomerAlternateID = getCustomerAlternateID;
    super.guiManagerContext = guiManagerContext;
    super.subscriberGroupEpochReader = subscriberGroupEpochReader;
    super.journeyTrafficReader = journeyTrafficReader;
    super.renamedProfileCriterionFieldReader = renamedProfileCriterionFieldReader;
    super.propensityDataReader = propensityDataReader;
  }


  /*****************************************
  *
  *  getStaticConfiguration
  *
  *****************************************/

  JSONObject processGetStaticConfiguration(String userID, JSONObject jsonRoot, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve supportedLanguages
    *
    *****************************************/

    List<JSONObject> supportedLanguages = new ArrayList<JSONObject>();
    for (SupportedLanguage supportedLanguage : Deployment.getSupportedLanguages().values())
      {
        JSONObject supportedLanguageJSON = supportedLanguage.getJSONRepresentation();
        supportedLanguages.add(supportedLanguageJSON);
      }

    /*****************************************
    *
    *  retrieve supportedCurrencies
    *
    *****************************************/

    List<JSONObject> supportedCurrencies = new ArrayList<JSONObject>();
    for (SupportedCurrency supportedCurrency : Deployment.getSupportedCurrencies().values())
      {
        JSONObject supportedCurrencyJSON = supportedCurrency.getJSONRepresentation();
        supportedCurrencies.add(supportedCurrencyJSON);
      }

    /*****************************************
    *
    *  retrieve supportedTimeUnits
    *
    *****************************************/

    List<JSONObject> supportedTimeUnits = new ArrayList<JSONObject>();
    for (SupportedTimeUnit supportedTimeUnit : Deployment.getSupportedTimeUnits().values())
      {
        JSONObject supportedTimeUnitJSON = supportedTimeUnit.getJSONRepresentation();
        supportedTimeUnits.add(supportedTimeUnitJSON);
      }

    /*****************************************
    *
    *  retrieve supportedRelationships
    *
    *****************************************/

    List<JSONObject> supportedRelationships = new ArrayList<JSONObject>();
    for (SupportedRelationship supportedRelationship : Deployment.getSupportedRelationships().values())
      {
        JSONObject supportedRelationshipJSON = supportedRelationship.getJSONRepresentation();
        supportedRelationships.add(supportedRelationshipJSON);
      }

    /*****************************************
    *
    *  retrieve communicationChannels
    *
    *****************************************/

    List<JSONObject> communicationChannels = new ArrayList<JSONObject>();
    for (GUIManagedObject communicationChannel : communicationChannelService.getStoredCommunicationChannels(includeArchived))
      {
        JSONObject communicationChannelJSON = communicationChannel.getJSONRepresentation();
        communicationChannels.add(communicationChannelJSON);
      }

    /*****************************************
    *
    *  retrieve callingChannelProperties
    *
    *****************************************/

    List<JSONObject> callingChannelProperties = new ArrayList<JSONObject>();
    for (CallingChannelProperty callingChannelProperty : Deployment.getCallingChannelProperties().values())
      {
        JSONObject callingChannelPropertyJSON = callingChannelProperty.getJSONRepresentation();
        callingChannelProperties.add(callingChannelPropertyJSON);
      }

    /*****************************************
    *
    *  retrieve catalogCharacteristicUnits
    *
    *****************************************/

    List<JSONObject> catalogCharacteristicUnits = new ArrayList<JSONObject>();
    for (CatalogCharacteristicUnit catalogCharacteristicUnit : Deployment.getCatalogCharacteristicUnits().values())
      {
        JSONObject catalogCharacteristicUnitJSON = catalogCharacteristicUnit.getJSONRepresentation();
        catalogCharacteristicUnits.add(catalogCharacteristicUnitJSON);
      }

    /*****************************************
    *
    *  retrieve supported data types
    *
    *****************************************/

    List<JSONObject> supportedDataTypes = new ArrayList<JSONObject>();
    for (SupportedDataType supportedDataType : Deployment.getSupportedDataTypes().values())
      {
        JSONObject supportedDataTypeJSON = supportedDataType.getJSONRepresentation();
        supportedDataTypes.add(supportedDataTypeJSON);
      }

    /*****************************************
    *
    *  retrieve profile criterion fields
    *
    *****************************************/

    List<JSONObject> profileCriterionFields = processCriterionFields(CriterionContext.Profile.getCriterionFields(), false);

    /*****************************************
    *
    *  retrieve presentation criterion fields
    *
    *****************************************/

    List<JSONObject> presentationCriterionFields = processCriterionFields(CriterionContext.Presentation.getCriterionFields(), false);


    /*****************************************
    *
    *  retrieve offerProperties
    *
    *****************************************/

    List<JSONObject> offerProperties = new ArrayList<JSONObject>();
    for (OfferProperty offerProperty : Deployment.getOfferProperties().values())
      {
        JSONObject offerPropertyJSON = offerProperty.getJSONRepresentation();
        offerProperties.add(offerPropertyJSON);
      }

    /*****************************************
    *
    *  retrieve scoringEngines
    *
    *****************************************/

    List<JSONObject> scoringEngines = new ArrayList<JSONObject>();
    for (ScoringEngine scoringEngine : Deployment.getScoringEngines().values())
      {
        JSONObject scoringEngineJSON = scoringEngine.getJSONRepresentation();
        scoringEngines.add(scoringEngineJSON);
      }

    /*****************************************
    *
    *  retrieve offerOptimizationAlgorithms
    *
    *****************************************/

    List<JSONObject> offerOptimizationAlgorithms = new ArrayList<JSONObject>();
    for (OfferOptimizationAlgorithm offerOptimizationAlgorithm : Deployment.getOfferOptimizationAlgorithms().values())
      {
        JSONObject offerOptimizationAlgorithmJSON = offerOptimizationAlgorithm.getJSONRepresentation();
        offerOptimizationAlgorithms.add(offerOptimizationAlgorithmJSON);
      }

    /*****************************************
    *
    *  retrieve nodeTypes
    *
    *****************************************/

    List<JSONObject> nodeTypes;
    try
      {
        nodeTypes = processNodeTypes(Deployment.getNodeTypes(), Collections.<String,CriterionField>emptyMap(), Collections.<String,CriterionField>emptyMap());
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }

    /*****************************************
    *
    *  retrieve journeyToolboxSections
    *
    *****************************************/

    List<JSONObject> journeyToolboxSections = new ArrayList<JSONObject>();
    for (ToolboxSection journeyToolboxSection : Deployment.getJourneyToolbox().values())
      {
        JSONObject journeyToolboxSectionJSON = journeyToolboxSection.getJSONRepresentation();
        journeyToolboxSections.add(journeyToolboxSectionJSON);
      }

    /*****************************************
    *
    *  retrieve campaignToolboxSections
    *
    *****************************************/

    List<JSONObject> campaignToolboxSections = new ArrayList<JSONObject>();
    for (ToolboxSection campaignToolboxSection : Deployment.getCampaignToolbox().values())
      {
        JSONObject campaignToolboxSectionJSON = campaignToolboxSection.getJSONRepresentation();
        campaignToolboxSections.add(campaignToolboxSectionJSON);
      }

    /*****************************************
    *
    *  retrieve workflowToolboxSections
    *
    *****************************************/

    List<JSONObject> workflowToolboxSections = new ArrayList<JSONObject>();
    for (ToolboxSection workflowToolboxSection : Deployment.getWorkflowToolbox().values())
      {
        JSONObject workflowToolboxSectionJSON = workflowToolboxSection.getJSONRepresentation();
        workflowToolboxSections.add(workflowToolboxSectionJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("supportedLanguages", JSONUtilities.encodeArray(supportedLanguages));
    response.put("supportedCurrencies", JSONUtilities.encodeArray(supportedCurrencies));
    response.put("supportedRelationships", JSONUtilities.encodeArray(supportedRelationships));
    response.put("callingChannelProperties", JSONUtilities.encodeArray(callingChannelProperties));
    response.put("supportedDataTypes", JSONUtilities.encodeArray(supportedDataTypes));
    response.put("profileCriterionFields", JSONUtilities.encodeArray(profileCriterionFields));
    response.put("presentationCriterionFields", JSONUtilities.encodeArray(presentationCriterionFields));
    response.put("offerProperties", JSONUtilities.encodeArray(offerProperties));
    response.put("scoringEngines", JSONUtilities.encodeArray(scoringEngines));
    response.put("offerOptimizationAlgorithms", JSONUtilities.encodeArray(offerOptimizationAlgorithms));
    response.put("nodeTypes", JSONUtilities.encodeArray(nodeTypes));
    response.put("journeyToolbox", JSONUtilities.encodeArray(journeyToolboxSections));
    response.put("campaignToolbox", JSONUtilities.encodeArray(campaignToolboxSections));
    response.put("workflowToolbox", JSONUtilities.encodeArray(workflowToolboxSections));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getSupportedLanguages
  *
  *****************************************/

  JSONObject processGetSupportedLanguages(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve supportedLanguages
    *
    *****************************************/

    List<JSONObject> supportedLanguages = new ArrayList<JSONObject>();
    for (SupportedLanguage supportedLanguage : Deployment.getSupportedLanguages().values())
      {
        JSONObject supportedLanguageJSON = supportedLanguage.getJSONRepresentation();
        supportedLanguages.add(supportedLanguageJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("supportedLanguages", JSONUtilities.encodeArray(supportedLanguages));
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  getSupportedCurrencies
  *
  *****************************************/

  JSONObject processGetSupportedCurrencies(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve supportedCurrencies
    *
    *****************************************/

    List<JSONObject> supportedCurrencies = new ArrayList<JSONObject>();
    for (SupportedCurrency supportedCurrency : Deployment.getSupportedCurrencies().values())
      {
        JSONObject supportedCurrencyJSON = supportedCurrency.getJSONRepresentation();
        supportedCurrencies.add(supportedCurrencyJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("supportedCurrencies", JSONUtilities.encodeArray(supportedCurrencies));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getSupportedTimeUnits
  *
  *****************************************/

  JSONObject processGetSupportedTimeUnits(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve supportedTimeUnits
    *
    *****************************************/

    List<JSONObject> supportedTimeUnits = new ArrayList<JSONObject>();
    for (SupportedTimeUnit supportedTimeUnit : Deployment.getSupportedTimeUnits().values())
      {
        JSONObject supportedTimeUnitJSON = supportedTimeUnit.getJSONRepresentation();
        supportedTimeUnits.add(supportedTimeUnitJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("supportedTimeUnits", JSONUtilities.encodeArray(supportedTimeUnits));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getCatalogCharacteristicUnits
  *
  *****************************************/

  JSONObject processGetCatalogCharacteristicUnits(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve catalogCharacteristicUnits
    *
    *****************************************/

    List<JSONObject> catalogCharacteristicUnits = new ArrayList<JSONObject>();
    for (CatalogCharacteristicUnit catalogCharacteristicUnit : Deployment.getCatalogCharacteristicUnits().values())
      {
        JSONObject catalogCharacteristicUnitJSON = catalogCharacteristicUnit.getJSONRepresentation();
        catalogCharacteristicUnits.add(catalogCharacteristicUnitJSON);
      }

    /*****************************************et
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("catalogCharacteristicUnits", JSONUtilities.encodeArray(catalogCharacteristicUnits));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getSupportedDataTypes
  *
  *****************************************/

  JSONObject processGetSupportedDataTypes(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve supported data types
    *
    *****************************************/

    List<JSONObject> supportedDataTypes = new ArrayList<JSONObject>();
    for (SupportedDataType supportedDataType : Deployment.getSupportedDataTypes().values())
      {
        JSONObject supportedDataTypeJSON = supportedDataType.getJSONRepresentation();
        supportedDataTypes.add(supportedDataTypeJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("supportedDataTypes", JSONUtilities.encodeArray(supportedDataTypes));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getSupportedEvents
  *
  *****************************************/

  JSONObject processGetSupportedEvents(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve events
    *
    *****************************************/

    List<JSONObject> events = evaluateEnumeratedValues("eventNames", SystemTime.getCurrentTime(), true);

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("events", JSONUtilities.encodeArray(events));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processConfigAdaptorSupportedLanguages
  *
  *****************************************/

  JSONObject processConfigAdaptorSupportedLanguages(JSONObject jsonRoot)
  {
    return processGetSupportedLanguages(null, jsonRoot);
  }

  /*****************************************
  *
  *  processGetCountBySegmentationEligibility
  *
  *****************************************/

  JSONObject processGetCountBySegmentationEligibility(String userID,JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  populate segmentationDimensionID with "nothing"
    *
    *****************************************/

    jsonRoot.put("id", "(not used)"); 

    /*****************************************
    *
    *  validate targetingType
    *
    *****************************************/

    SegmentationDimensionTargetingType targetingType = SegmentationDimensionTargetingType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "targetingType", true));
    if(targetingType != SegmentationDimensionTargetingType.ELIGIBILITY)
      {
        //
        //  log
        //

        log.warn("Invalid dimension targeting type for processGetCountBySegmentationEligibility. Targeting type: {}",targetingType.getExternalRepresentation());

        //
        //  response
        //

        response.put("responseCode", "segmentationDimensionNotValid");
        response.put("responseMessage", "Segmentation dimension not ELIGIBILITY");
        response.put("responseParameter", null);
        return JSONUtilities.encodeObject(response);
      }

    /****************************************
    *
    *  response
    *
    ****************************************/

    List<JSONObject> aggregationResult = new ArrayList<>();
    List<QueryBuilder> processedQueries = new ArrayList<>();
    try
      {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().sort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).query(QueryBuilders.matchAllQuery()).size(0);
        List<FiltersAggregator.KeyedFilter> aggFilters = new ArrayList<>();
        SegmentationDimensionEligibility segmentationDimensionEligibility = new SegmentationDimensionEligibility(segmentationDimensionService, jsonRoot, epochServer.getKey(), null, false);
        for(SegmentEligibility segmentEligibility :segmentationDimensionEligibility.getSegments())
          {
            BoolQueryBuilder query = QueryBuilders.boolQuery();
            for(QueryBuilder processedQuery : processedQueries)
              {
                query = query.mustNot(processedQuery);
              }
            BoolQueryBuilder segmentQuery = QueryBuilders.boolQuery();
            for(EvaluationCriterion evaluationCriterion:segmentEligibility.getProfileCriteria())
              {
                segmentQuery = segmentQuery.filter(evaluationCriterion.esQuery());
                processedQueries.add(segmentQuery);
              }
            query = query.filter(segmentQuery);
            //use name as key, even if normally should use id, to make simpler to use this count in chart
            aggFilters.add(new FiltersAggregator.KeyedFilter(segmentEligibility.getName(),query));
          }
        AggregationBuilder aggregation = null;
        FiltersAggregator.KeyedFilter [] filterArray = new FiltersAggregator.KeyedFilter [aggFilters.size()];
        filterArray = aggFilters.toArray(filterArray);
        aggregation = AggregationBuilders.filters("SegmentEligibility",filterArray);
        ((FiltersAggregationBuilder) aggregation).otherBucket(true);
        ((FiltersAggregationBuilder) aggregation).otherBucketKey("other_key");
        searchSourceBuilder.aggregation(aggregation);

        //
        //  search in ES
        //

        SearchRequest searchRequest = new SearchRequest("subscriberprofile").source(searchSourceBuilder);
        SearchResponse searchResponse = elasticsearch.search(searchRequest, RequestOptions.DEFAULT);
        Filters aggResultFilters = searchResponse.getAggregations().get("SegmentEligibility");
        for (Filters.Bucket entry : aggResultFilters.getBuckets())
          {
            HashMap<String,Object> aggItem = new HashMap<String,Object>();
            String key = entry.getKeyAsString();            // bucket key
            long docCount = entry.getDocCount();            // Doc count
            aggItem.put("name",key);
            aggItem.put("count",docCount);
            aggregationResult.add(JSONUtilities.encodeObject(aggItem));
          }
      }
    catch(Exception ex)
      {
        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        ex.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("responseCode", "systemError");
        response.put("responseMessage", ex.getMessage());
        response.put("responseParameter", (ex instanceof GUIManagerException) ? ((GUIManagerException) ex).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
    response.put("responseCode", "ok");
    response.put("result",aggregationResult);
    return JSONUtilities.encodeObject(response);
  }


  /*****************************************
  *
  *  processEvaluateProfileCriteria
  *
  *****************************************/

  JSONObject processEvaluateProfileCriteria(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  parse
    *
    *****************************************/

    List<EvaluationCriterion> criteriaList = new ArrayList<EvaluationCriterion>();
    try
      {
        JSONArray jsonCriteriaList = JSONUtilities.decodeJSONArray(jsonRoot, "profileCriteria", true);
        for (int i=0; i<jsonCriteriaList.size(); i++)
          {
            criteriaList.add(new EvaluationCriterion((JSONObject) jsonCriteriaList.get(i), CriterionContext.FullDynamicProfile));
          }
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("responseCode", "argumentError");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }

    Boolean returnQuery = JSONUtilities.decodeBoolean(jsonRoot, "returnQuery", Boolean.FALSE);

    /*****************************************
    *
    *  construct query
    *
    *****************************************/

    BoolQueryBuilder query = null;
    try
      {
        query = EvaluationCriterion.esCountMatchCriteriaGetQuery(criteriaList);
      }
    catch (CriterionException e)
      {
        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("responseCode", "argumentError");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  execute query
    *
    *****************************************/

    long result;
    try
      {
        result = EvaluationCriterion.esCountMatchCriteriaExecuteQuery(query, elasticsearch);
      }
    catch (IOException|ElasticsearchStatusException e)
      {
        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("responseCode", "systemError");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", null);
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", "ok");
    response.put("result", result);
    if (returnQuery && (query != null))
      {
        try
          {
            JSONObject queryJSON = (JSONObject) (new JSONParser()).parse(query.toString());
            response.put("query", JSONUtilities.encodeObject(queryJSON));
          }
        catch (ParseException e)
          {
            log.debug("Cannot parse query string {} : {}", query.toString(), e.getLocalizedMessage());
          }
      }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetPointList
  *
  *****************************************/

  JSONObject processGetPointList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert Points
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> points = new ArrayList<JSONObject>();
    for (GUIManagedObject point : pointService.getStoredPoints(includeArchived))
      {
        points.add(pointService.generateResponseJSON(point, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("points", JSONUtilities.encodeArray(points));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetPoint
  *
  *****************************************/

  JSONObject processGetPoint(String userID, JSONObject jsonRoot, boolean includeArchived)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String pointID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate point
    *
    *****************************************/

    GUIManagedObject point = pointService.getStoredPoint(pointID, includeArchived);
    JSONObject pointJSON = pointService.generateResponseJSON(point, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (point != null) ? "ok" : "pointNotFound");
    if (point != null) response.put("point", pointJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutPoint
  *
  *****************************************/

  JSONObject processPutPoint(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  pointID
    *
    *****************************************/

    String pointID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (pointID == null)
      {
        //little hack here :
        //   since pointID = deliverableID (if creditable) = paymentMeanID (if debitable), we need to be sure that 
        //   deliverableID and paymentMeanID are unique, so point IDs start at position 10001
        //   NOTE : we will be in trouble when we will have more than 10000 deliverables/paymentMeans ...
        String idString = pointService.generatePointID();
        try
          {
            int id = Integer.parseInt(idString);
            pointID = String.valueOf(id > 10000 ? id : (10000 + id));
          } catch (NumberFormatException e)
          {
            throw new ServerRuntimeException("ProcessPutPoint : could not generate new ID");
          }
        jsonRoot.put("id", pointID);
      }

    /*****************************************
    *
    *  existing point
    *
    *****************************************/

    GUIManagedObject existingPoint = pointService.getStoredPoint(pointID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingPoint != null && existingPoint.getReadOnly())
      {
        response.put("id", existingPoint.getGUIManagedObjectID());
        response.put("accepted", existingPoint.getAccepted());
        response.put("valid", existingPoint.getAccepted());
        response.put("processing", pointService.isActivePoint(existingPoint, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process point
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate Point
        *
        ****************************************/

        Point point = new Point(jsonRoot, epoch, existingPoint);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        pointService.putPoint(point, (existingPoint == null), userID);
        
        /*****************************************
        *
        *  add dynamic criterion fields)
        *
        *****************************************/

        dynamicCriterionFieldService.addPointCriterionFields(point, (existingPoint == null));

        /*****************************************
        *
        *  create related deliverable and related paymentMean
        *
        *****************************************/

        DeliveryManagerDeclaration deliveryManager = Deployment.getDeliveryManagers().get("pointFulfillment");
        JSONObject deliveryManagerJSON = (deliveryManager != null) ? deliveryManager.getJSONRepresentation() : null;
        String providerID = (deliveryManagerJSON != null) ? (String) deliveryManagerJSON.get("providerID") : null;

        //
        // deliverable
        //

        if (providerID != null && point.getCreditable())
          {
            Map<String, Object> deliverableMap = new HashMap<String, Object>();
            deliverableMap.put("id", "point-" + point.getPointID());
            deliverableMap.put("fulfillmentProviderID", providerID);
            deliverableMap.put("externalAccountID", point.getPointID());
            deliverableMap.put("name", point.getPointName());
            deliverableMap.put("display", point.getDisplay());
            deliverableMap.put("active", true);
            deliverableMap.put("unitaryCost", 0);
            Deliverable deliverable = new Deliverable(JSONUtilities.encodeObject(deliverableMap), epoch, null);
            deliverableService.putDeliverable(deliverable, true, userID);
          }

        //
        // paymentMean
        //

        if (providerID != null && point.getDebitable())
          {
            Map<String, Object> paymentMeanMap = new HashMap<String, Object>();
            paymentMeanMap.put("id", "point-" + point.getPointID());
            paymentMeanMap.put("fulfillmentProviderID", providerID);
            paymentMeanMap.put("externalAccountID", point.getPointID());
            paymentMeanMap.put("name", point.getPointName());
            paymentMeanMap.put("display", point.getDisplay());
            paymentMeanMap.put("active", true);
            PaymentMean paymentMean = new PaymentMean(JSONUtilities.encodeObject(paymentMeanMap), epoch, null);
            paymentMeanService.putPaymentMean(paymentMean, true, userID);
          }

        /*****************************************
        *
        *  revalidate
        *
        *****************************************/

        revalidateSubscriberMessageTemplates(now);
        revalidateTargets(now);
        revalidateJourneys(now);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", point.getPointID());
        response.put("accepted", point.getAccepted());
        response.put("valid", point.getAccepted());
        response.put("processing", pointService.isActivePoint(point, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        pointService.putIncompletePoint(incompleteObject, (existingPoint == null), userID);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("pointID", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "pointNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemovePoint
  *
  *****************************************/

  JSONObject processRemovePoint(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String pointID = JSONUtilities.decodeString(jsonRoot, "id", true);
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove related deliverable and related paymentMean
    *
    *****************************************/

    DeliveryManagerDeclaration deliveryManager = Deployment.getDeliveryManagers().get("pointFulfillment");
    JSONObject deliveryManagerJSON = (deliveryManager != null) ? deliveryManager.getJSONRepresentation() : null;
    String providerID = (deliveryManagerJSON != null) ? (String) deliveryManagerJSON.get("providerID") : null;
    if (providerID != null)
      {
        //
        //  deliverable
        //

        Collection<GUIManagedObject> deliverableObjects = deliverableService.getStoredDeliverables();
        for (GUIManagedObject deliverableObject : deliverableObjects)
          {
            if(deliverableObject instanceof Deliverable)
              {
                Deliverable deliverable = (Deliverable) deliverableObject;
                if (deliverable.getFulfillmentProviderID().equals(providerID) && deliverable.getExternalAccountID().equals(pointID))
                  {
                    deliverableService.removeDeliverable(deliverable.getDeliverableID(), "0");
                  }
              }
          }

        //
        //  paymentMean
        //

        Collection<GUIManagedObject> paymentMeanObjects = paymentMeanService.getStoredPaymentMeans();
        for(GUIManagedObject paymentMeanObject : paymentMeanObjects)
          {
            if(paymentMeanObject instanceof PaymentMean)
              {
                PaymentMean paymentMean = (PaymentMean) paymentMeanObject;
                if(paymentMean.getFulfillmentProviderID().equals(providerID) && paymentMean.getExternalAccountID().equals(pointID))
                  {
                    paymentMeanService.removePaymentMean(paymentMean.getPaymentMeanID(), "0");
                  }
              }
          }
      }

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject point = pointService.getStoredPoint(pointID);
    if (point != null && (force || !point.getReadOnly()))
      {
        //
        //  remove point
        //

        pointService.removePoint(pointID, userID);

        //
        //  remove dynamic criterion fields
        //

        dynamicCriterionFieldService.removePointCriterionFields(point);

        //
        //  revalidate
        //

        revalidateSubscriberMessageTemplates(now);
        revalidateTargets(now);
        revalidateJourneys(now);
      }

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (point != null && (force || !point.getReadOnly()))
      responseCode = "ok";
    else if (point != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "pointNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  
}

