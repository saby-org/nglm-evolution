/*****************************************************************************
*
*  GUIManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.FileUpload;
import org.apache.commons.fileupload.RequestContext;
import org.apache.commons.fileupload.util.Streams;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedNested;
import org.elasticsearch.search.aggregations.bucket.range.ParsedRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.reflections.Reflections;
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
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;
import com.evolving.nglm.evolution.SegmentationDimension.SegmentationDimensionTargetingType;
import com.evolving.nglm.evolution.complexobjects.ComplexObjectType;
import com.evolving.nglm.evolution.complexobjects.ComplexObjectTypeService;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;
import com.sun.net.httpserver.HttpExchange;

public class GUIManagerGeneral extends GUIManager
{

  //
  // log
  //
  
  private static final Logger log = LoggerFactory.getLogger(GUIManagerGeneral.class);

  private static final int HOW_MANY_TIMES_TO_TRY_TO_GENERATE_A_VOUCHER_CODE = 100;
  
  //
  //  data
  //
  
  private Map<String, GUIDependencyModelTree> guiDependencyModelTreeMap = new LinkedHashMap<String, GUIDependencyModelTree>();
  private final List<GUIService> guiServiceList = new ArrayList<GUIService>();


  /***************************
   * 
   * GUIManagerGeneral
   * 
   ***************************/
  
  public GUIManagerGeneral(JourneyService journeyService, SegmentationDimensionService segmentationDimensionService, PointService pointService, ComplexObjectTypeService complexObjectTypeService, OfferService offerService, ReportService reportService, PaymentMeanService paymentMeanService, ScoringStrategyService scoringStrategyService, PresentationStrategyService presentationStrategyService, CallingChannelService callingChannelService, SalesChannelService salesChannelService, SourceAddressService sourceAddressService, SupplierService supplierService, ProductService productService, CatalogCharacteristicService catalogCharacteristicService, ContactPolicyService contactPolicyService, JourneyObjectiveService journeyObjectiveService, OfferObjectiveService offerObjectiveService, ProductTypeService productTypeService, UCGRuleService ucgRuleService, DeliverableService deliverableService, TokenTypeService tokenTypeService, VoucherTypeService voucherTypeService, VoucherService voucherService, SubscriberMessageTemplateService subscriberTemplateService, SubscriberProfileService subscriberProfileService, SubscriberIDService subscriberIDService, DeliverableSourceService deliverableSourceService, UploadedFileService uploadedFileService, TargetService targetService, CommunicationChannelBlackoutService communicationChannelBlackoutService, LoyaltyProgramService loyaltyProgramService, ResellerService resellerService, ExclusionInclusionTargetService exclusionInclusionTargetService, SegmentContactPolicyService segmentContactPolicyService, CriterionFieldAvailableValuesService criterionFieldAvailableValuesService, DNBOMatrixService dnboMatrixService, DynamicCriterionFieldService dynamicCriterionFieldService, DynamicEventDeclarationsService dynamicEventDeclarationsService, JourneyTemplateService journeyTemplateService, KafkaResponseListenerService<StringKey,PurchaseFulfillmentRequest> purchaseResponseListenerService, SharedIDService subscriberGroupSharedIDService, ZookeeperUniqueKeyServer zuks, int httpTimeout, KafkaProducer<byte[], byte[]> kafkaProducer, ElasticsearchClientAPI elasticsearch, SubscriberMessageTemplateService subscriberMessageTemplateService, String getCustomerAlternateID, GUIManagerContext guiManagerContext, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, ReferenceDataReader<String,RenamedProfileCriterionField> renamedProfileCriterionFieldReader)
  {
    super.callingChannelService = callingChannelService;
    super.catalogCharacteristicService = catalogCharacteristicService;
    super.communicationChannelBlackoutService = communicationChannelBlackoutService;
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
    super.complexObjectTypeService = complexObjectTypeService;
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
    super.renamedProfileCriterionFieldReader = renamedProfileCriterionFieldReader;
    
    guiServiceList.add(callingChannelService);
    guiServiceList.add(catalogCharacteristicService);
    guiServiceList.add(communicationChannelBlackoutService);
    guiServiceList.add(contactPolicyService);
    guiServiceList.add(criterionFieldAvailableValuesService);
    guiServiceList.add(deliverableService);
    guiServiceList.add(exclusionInclusionTargetService);
    guiServiceList.add(journeyObjectiveService);
    guiServiceList.add(journeyService);
    guiServiceList.add(loyaltyProgramService);
    guiServiceList.add(offerObjectiveService);
    guiServiceList.add(offerService);
    guiServiceList.add(paymentMeanService);
    guiServiceList.add(pointService);
    guiServiceList.add(presentationStrategyService);
    guiServiceList.add(productService);
    guiServiceList.add(productTypeService);
    guiServiceList.add(reportService);
    guiServiceList.add(resellerService);
    guiServiceList.add(salesChannelService);
    guiServiceList.add(scoringStrategyService);
    guiServiceList.add(segmentationDimensionService);
    guiServiceList.add(segmentContactPolicyService);
    guiServiceList.add(sourceAddressService);
    guiServiceList.add(subscriberMessageTemplateService);
    guiServiceList.add(supplierService);
    guiServiceList.add(targetService);
    guiServiceList.add(tokenTypeService);
    guiServiceList.add(ucgRuleService);
    guiServiceList.add(uploadedFileService);
    guiServiceList.add(voucherService);
    guiServiceList.add(voucherTypeService);
    guiServiceList.add(dnboMatrixService);
    guiServiceList.add(dynamicCriterionFieldService);
    guiServiceList.add(dynamicEventDeclarationsService);
    guiServiceList.add(journeyTemplateService);
    
    //
    //  buildGUIDependencyModelTreeMap
    //
    
    buildGUIDependencyModelTreeMap();
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
    for (CommunicationChannel communicationChannel : Deployment.getCommunicationChannels().values())
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

    boolean evaluateBySegmentId = false;

    if(JSONUtilities.decodeString(jsonRoot,"id") == null)
    {
      evaluateBySegmentId = false;
      jsonRoot.put("id","new_dimension");
    }
    else
    {
      evaluateBySegmentId = true;
    }

    /****************************************
    *
    *  response
    *
    ****************************************/

    List<JSONObject> aggregationResult = new ArrayList<>();
    List<QueryBuilder> processedQueries = new ArrayList<>();
    //String stratumESFieldName = Deployment.getProfileCriterionFields().get("subscriber.stratum").getESField();
    String stratumESFieldName = "stratum";
    try
      {
        SegmentationDimensionEligibility segmentationDimensionEligibility = new SegmentationDimensionEligibility(segmentationDimensionService, jsonRoot, epochServer.getKey(), null, false);
        if(evaluateBySegmentId)
        {
          SegmentationDimension storedSegmentationDimensionEligibility = segmentationDimensionService.getActiveSegmentationDimension(segmentationDimensionEligibility.getGUIManagedObjectID() , SystemTime.getCurrentTime());
          evaluateBySegmentId = evaluateBySegmentId && segmentationDimensionEligibility.getSegmentsConditionEqual(storedSegmentationDimensionEligibility);
        }
        if(evaluateBySegmentId) return processGetCountBySegmentationEligibilityBySegmentId(segmentationDimensionEligibility);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().sort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).query(QueryBuilders.matchAllQuery()).size(0);
        List<FiltersAggregator.KeyedFilter> aggFilters = new ArrayList<>();
        for (SegmentEligibility segmentEligibility : segmentationDimensionEligibility.getSegments())
        {
          BoolQueryBuilder query = QueryBuilders.boolQuery();
          for (QueryBuilder processedQuery : processedQueries)
          {
            query = query.mustNot(processedQuery);
          }
          BoolQueryBuilder segmentQuery = QueryBuilders.boolQuery();
          for (EvaluationCriterion evaluationCriterion : segmentEligibility.getProfileCriteria())
          {
            segmentQuery = segmentQuery.filter(evaluationCriterion.esQuery());
            processedQueries.add(segmentQuery);
          }
          query = query.filter(segmentQuery);
          //use name as key, even if normally should use id, to make simpler to use this count in chart
          aggFilters.add(new FiltersAggregator.KeyedFilter(segmentEligibility.getName(), query));
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

  private JSONObject processGetCountBySegmentationEligibilityBySegmentId(SegmentationDimensionEligibility segmentationDimensionEligibility)
  {

    /****************************************
     *
     *  response
     *
     ****************************************/
    HashMap<String,Object> response = new HashMap<String,Object>();

    List<JSONObject> aggregationResult = new ArrayList<>();
    List<QueryBuilder> processedQueries = new ArrayList<>();
    //String stratumESFieldName = Deployment.getProfileCriterionFields().get("subscriber.stratum").getESField();
    String stratumESFieldName = "stratum";
    try
    {
      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().sort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).query(QueryBuilders.matchAllQuery()).size(0);
      List<FiltersAggregator.KeyedFilter> aggFilters = new ArrayList<>();
      for(SegmentEligibility segmentEligibility :segmentationDimensionEligibility.getSegments())
      {
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(stratumESFieldName+"."+segmentationDimensionEligibility.getGUIManagedObjectID(), segmentEligibility.getID());
        //use name as key, even if normally should use id, to make simpler to use this count in chart
        aggFilters.add(new FiltersAggregator.KeyedFilter(segmentEligibility.getName(),termQueryBuilder));
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
    Collection <GUIManagedObject> pointObjects = new ArrayList<GUIManagedObject>();
    if (jsonRoot.containsKey("ids"))
      {
        JSONArray pointIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
        for (int i = 0; i < pointIDs.size(); i++)
          {
            String pointID = pointIDs.get(i).toString();
            GUIManagedObject point = pointService.getStoredPoint(pointID, includeArchived);
            if (point != null)
              {
                pointObjects.add(point);
              }
          }
                
      }
          
    else
      {
        pointObjects = pointService.getStoredPoints(includeArchived);
      }
          
    for (GUIManagedObject point : pointObjects)
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
    Boolean dryRun = false;
    

    /*****************************************
    *
    *  dryRun
    *
    *****************************************/
    if (jsonRoot.containsKey("dryRun")) {
      dryRun = JSONUtilities.decodeBoolean(jsonRoot, "dryRun", false);
    }

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
         * store
         *
         *****************************************/
        if (!dryRun)
          {

            pointService.putPoint(point, (existingPoint == null), userID);

            /*****************************************
             *
             * add dynamic criterion fields)
             *
             *****************************************/

            dynamicCriterionFieldService.addPointCriterionFields(point, (existingPoint == null));

            /*****************************************
             *
             * create related deliverable and related paymentMean
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
                deliverableMap.put("id", CommodityDeliveryManager.POINT_PREFIX + point.getPointID());
                deliverableMap.put("fulfillmentProviderID", providerID);
                deliverableMap.put("externalAccountID", point.getPointID());
                deliverableMap.put("name", point.getPointName());
                deliverableMap.put("display", point.getDisplay());
                deliverableMap.put("active", true);
                deliverableMap.put("unitaryCost", 0);
                deliverableMap.put("label", "points");
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
                paymentMeanMap.put("label", point.getLabel());
                PaymentMean paymentMean = new PaymentMean(JSONUtilities.encodeObject(paymentMeanMap), epoch, null);
                paymentMeanService.putPaymentMean(paymentMean, true, userID);
              }

            /*****************************************
             *
             * revalidate
             *
             *****************************************/

            revalidateSubscriberMessageTemplates(now);
            revalidateTargets(now);
            revalidateJourneys(now);
          }

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
        if (!dryRun)
          {
            pointService.putIncompletePoint(incompleteObject, (existingPoint == null), userID);
          }

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
    String responseCode = "";
    String singleIDresponseCode = "";
    List<GUIManagedObject> points = new ArrayList<GUIManagedObject>();
    List<String> validIDs = new ArrayList<>();
    JSONArray pointIDs = new JSONArray();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);
    //
    //remove single point
    //
    if (jsonRoot.containsKey("id"))
      {
        String pointID = JSONUtilities.decodeString(jsonRoot, "id", false);
        pointIDs.add(pointID);
        GUIManagedObject point = pointService.getStoredPoint(pointID);
        if (point != null && (force || !point.getReadOnly()))
          singleIDresponseCode = "ok";
        else if (point != null)
          singleIDresponseCode = "failedReadOnly";
        else
          {
            singleIDresponseCode = "pointNotFound";
          }


      }
    //
    // multiple deletion
    //
    
    if (jsonRoot.containsKey("ids"))
      {
        pointIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids", false);
      }  

    for (int i = 0; i < pointIDs.size(); i++)
      {
        String pointID = pointIDs.get(i).toString();
        GUIManagedObject point = pointService.getStoredPoint(pointID);

        if (point != null && (force || !point.getReadOnly()))
          {
            points.add(point);
            validIDs.add(pointID);
          }
      }

    /*****************************************
     *
     * remove related deliverable and related paymentMean
     *
     *****************************************/
    for (int i = 0; i < points.size(); i++)
      {
        GUIManagedObject point = points.get(i);
        DeliveryManagerDeclaration deliveryManager = Deployment.getDeliveryManagers().get("pointFulfillment");
        JSONObject deliveryManagerJSON = (deliveryManager != null) ? deliveryManager.getJSONRepresentation() : null;
        String providerID = (deliveryManagerJSON != null) ? (String) deliveryManagerJSON.get("providerID") : null;
        if (providerID != null)
          {
            //
            // deliverable
            //

            Collection<GUIManagedObject> deliverableObjects = deliverableService.getStoredDeliverables();
            for (GUIManagedObject deliverableObject : deliverableObjects)
              {
                if (deliverableObject instanceof Deliverable)
                  {
                    Deliverable deliverable = (Deliverable) deliverableObject;
                    if (deliverable.getFulfillmentProviderID().equals(providerID)
                        && deliverable.getExternalAccountID().equals(point.getGUIManagedObjectID()))
                      {
                        deliverableService.removeDeliverable(deliverable.getDeliverableID(), "0");
                      }
                  }
              }

            //
            // paymentMean
            //

            Collection<GUIManagedObject> paymentMeanObjects = paymentMeanService.getStoredPaymentMeans();
            for (GUIManagedObject paymentMeanObject : paymentMeanObjects)
              {
                if (paymentMeanObject instanceof PaymentMean)
                  {
                    PaymentMean paymentMean = (PaymentMean) paymentMeanObject;
                    if (paymentMean.getFulfillmentProviderID().equals(providerID)
                        && paymentMean.getExternalAccountID().equals(point.getGUIManagedObjectID()))
                      {
                        paymentMeanService.removePaymentMean(paymentMean.getPaymentMeanID(), "0");
                      }
                  }
              }
          }

        /*****************************************
         *
         * remove
         *
         *****************************************/

        //
        // remove point
        //

        pointService.removePoint(point.getGUIManagedObjectID(), userID);

        //
        // remove dynamic criterion fields
        //

        dynamicCriterionFieldService.removePointCriterionFields(point);

        //
        // revalidate
        //

        revalidateSubscriberMessageTemplates(now);
        revalidateTargets(now);
        revalidateJourneys(now);
          
      }
        /*****************************************
         *
         * responseCode
         *
         *****************************************/
    if (jsonRoot.containsKey("id"))
      {
        response.put("responseCode", singleIDresponseCode);
        return JSONUtilities.encodeObject(response);
      }

    else
      {
        response.put("responseCode", "ok");
      }

    /*****************************************
     *
     * response
     *
     *****************************************/
    response.put("removedPointIDs", JSONUtilities.encodeArray(validIDs));

    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processRemoveScore
  *
  *****************************************/

  JSONObject processRemoveScore(String userID, JSONObject jsonRoot)
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
    String responseCode = "";
    String singleIDresponseCode = "";
    List<GUIManagedObject> scores = new ArrayList<GUIManagedObject>();
    List<String> validIDs = new ArrayList<>();
    JSONArray pointIDs = new JSONArray();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);
    //
    //remove single point
    //
    if (jsonRoot.containsKey("id"))
      {
        String pointID = JSONUtilities.decodeString(jsonRoot, "id", false);
        pointIDs.add(pointID);
        GUIManagedObject point = pointService.getStoredScore(pointID);
        if (point != null && (force || !point.getReadOnly()))
          singleIDresponseCode = "ok";
        else if (point != null)
          singleIDresponseCode = "failedReadOnly";
        else
          {
            singleIDresponseCode = "pointNotFound";
          }


      }
    //
    // multiple deletion
    //
    
    if (jsonRoot.containsKey("ids"))
      {
        pointIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids", false);
      }  

    for (int i = 0; i < pointIDs.size(); i++)
      {
        String pointID = pointIDs.get(i).toString();
        GUIManagedObject score = pointService.getStoredScore(pointID);

        if (score != null && (force || !score.getReadOnly()))
          {
            scores.add(score);
            validIDs.add(pointID);
          }
      }

    /*****************************************
     *
     * remove related deliverable and related paymentMean
     *
     *****************************************/
    for (int i = 0; i < scores.size(); i++)
      {
        GUIManagedObject score = scores.get(i);
        DeliveryManagerDeclaration deliveryManager = Deployment.getDeliveryManagers().get("pointFulfillment");
        JSONObject deliveryManagerJSON = (deliveryManager != null) ? deliveryManager.getJSONRepresentation() : null;
        String providerID = (deliveryManagerJSON != null) ? (String) deliveryManagerJSON.get("providerID") : null;
        if (providerID != null)
          {
            //
            // deliverable
            //

            Collection<GUIManagedObject> deliverableObjects = deliverableService.getStoredDeliverables();
            for (GUIManagedObject deliverableObject : deliverableObjects)
              {
                if (deliverableObject instanceof Deliverable)
                  {
                    Deliverable deliverable = (Deliverable) deliverableObject;
                    if (deliverable.getFulfillmentProviderID().equals(providerID)
                        && deliverable.getExternalAccountID().equals(score.getGUIManagedObjectID()))
                      {
                        deliverableService.removeDeliverable(deliverable.getDeliverableID(), "0");
                      }
                  }
              }

            //
            // paymentMean
            //

            Collection<GUIManagedObject> paymentMeanObjects = paymentMeanService.getStoredPaymentMeans();
            for (GUIManagedObject paymentMeanObject : paymentMeanObjects)
              {
                if (paymentMeanObject instanceof PaymentMean)
                  {
                    PaymentMean paymentMean = (PaymentMean) paymentMeanObject;
                    if (paymentMean.getFulfillmentProviderID().equals(providerID)
                        && paymentMean.getExternalAccountID().equals(score.getGUIManagedObjectID()))
                      {
                        paymentMeanService.removePaymentMean(paymentMean.getPaymentMeanID(), "0");
                      }
                  }
              }
          }

        /*****************************************
         *
         * remove
         *
         *****************************************/

        //
        // remove point
        //

        pointService.removePoint(score.getGUIManagedObjectID(), userID);

        //
        // remove dynamic criterion fields
        //

        //dynamicCriterionFieldService.removePointCriterionFields(score);

        //
        // revalidate
        //

        revalidateSubscriberMessageTemplates(now);
        revalidateTargets(now);
        revalidateJourneys(now);
          
      }
        /*****************************************
         *
         * responseCode
         *
         *****************************************/
    if (jsonRoot.containsKey("id"))
      {
        response.put("responseCode", singleIDresponseCode);
        return JSONUtilities.encodeObject(response);
      }

    else
      {
        response.put("responseCode", "ok");
      }

    /*****************************************
     *
     * response
     *
     *****************************************/
    response.put("removedPointIDs", JSONUtilities.encodeArray(validIDs));

    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
   *
   * processSetStatusPoint
   *
   *****************************************/

  JSONObject processSetStatusPoint(String userID, JSONObject jsonRoot)
  {
    /****************************************
     *
     * response
     *
     ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String, Object> response = new HashMap<String, Object>();
    JSONArray pointIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
    List<String> statusSetIDs = new ArrayList<>();
    Boolean status = JSONUtilities.decodeBoolean(jsonRoot, "active");
    long epoch = epochServer.getKey();

    for (int i = 0; i < pointIDs.size(); i++)
      {

        String pointID = pointIDs.get(i).toString();
        GUIManagedObject existingElement = pointService.getStoredPoint(pointID);
        if (existingElement != null && !(existingElement.getReadOnly()))
          {
            statusSetIDs.add(pointID);
            JSONObject elementRoot = (JSONObject) existingElement.getJSONRepresentation().clone();
            if (existingElement != null && !(existingElement.getReadOnly()))
            try
              {
                /****************************************
                 *
                 * instantiate Point
                 *
                 ****************************************/

                Point point = new Point(elementRoot, epoch, existingElement);

                /*****************************************
                 *
                 * store
                 *
                 *****************************************/

                pointService.putPoint(point, (existingElement == null), userID);

                /*****************************************
                 *
                 * revalidate
                 *
                 *****************************************/

                revalidateSubscriberMessageTemplates(now);
                revalidateTargets(now);
                revalidateJourneys(now);

              }
            catch (JSONUtilitiesException | GUIManagerException e)
              {
                //
                // incompleteObject
                //

                IncompleteObject incompleteObject = new IncompleteObject(elementRoot, epoch);

                //
                // store
                //

                pointService.putIncompletePoint(incompleteObject, (existingElement == null), userID);

                //
                // log
                //

                StringWriter stackTraceWriter = new StringWriter();
                e.printStackTrace(new PrintWriter(stackTraceWriter, true));
                if (log.isWarnEnabled())
                  {
                    log.warn("Exception processing REST api: {}", stackTraceWriter.toString());
                  }

              }
          }
      }
    response.put("responseCode", "ok");
    response.put("statusSetIds", statusSetIDs);
    return JSONUtilities.encodeObject(response);
  }
  
  
  /*****************************************
  *
  *  processGetComplexObjectTypeList
  *
  *****************************************/

  JSONObject processGetComplexObjectTypeList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert ComplexObjectTypes
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> complexObjectTypes = new ArrayList<JSONObject>();
    Collection <GUIManagedObject> complexObjectTypeObjects = new ArrayList<GUIManagedObject>();
    if (jsonRoot.containsKey("ids"))
      {
        JSONArray complexObjectTypeIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
        for (int i = 0; i < complexObjectTypeIDs.size(); i++)
          {
            String complexObjectTypeID = complexObjectTypeIDs.get(i).toString();
            GUIManagedObject complexObjectType = complexObjectTypeService.getStoredComplexObjectType(complexObjectTypeID, includeArchived);
            if (complexObjectType != null)
              {
                complexObjectTypeObjects.add(complexObjectType);
              }
          }                
      }          
    else
      {
        complexObjectTypeObjects = complexObjectTypeService.getStoredComplexObjectTypes(includeArchived);
      }
          
    for (GUIManagedObject complexObjectType : complexObjectTypeObjects)
      {
        complexObjectTypes.add(complexObjectTypeService.generateResponseJSON(complexObjectType, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("complexObjectTypes", JSONUtilities.encodeArray(complexObjectTypes));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetComplexObjectType
  *
  *****************************************/

  JSONObject processGetComplexObjectType(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String complexObjectTypeID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate complexObjectType
    *
    *****************************************/

    GUIManagedObject complexObjectType = complexObjectTypeService.getStoredComplexObjectType(complexObjectTypeID, includeArchived);
    JSONObject complexObjectTypeJSON = complexObjectTypeService.generateResponseJSON(complexObjectType, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (complexObjectType != null) ? "ok" : "complexObjectTypeNotFound");
    if (complexObjectType != null) response.put("complexObjectType", complexObjectTypeJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutComplexObjectType
  *
  *****************************************/

  JSONObject processPutComplexObjectType(String userID, JSONObject jsonRoot)
  {
    
    //{ "id" : "45", "name" : "ComplexTypeA", "display" : "ComplexTypeA", "active" : true, "availableNames" : ["name1", "name2"], "fields" : { "field1" :  "string", "field2" : "integer"} , "apiVersion" : 1 }

    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();
    Boolean dryRun = false;
    

    /*****************************************
    *
    *  dryRun
    *
    *****************************************/
    if (jsonRoot.containsKey("dryRun")) {
      dryRun = JSONUtilities.decodeBoolean(jsonRoot, "dryRun", false);
    }

    /*****************************************
    *
    *  complexObjectTypeID
    *
    *****************************************/

    String complexObjectTypeID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (complexObjectTypeID == null)
      {
        String idString = complexObjectTypeService.generateComplexObjectTypeID();
        jsonRoot.put("id", idString);
      }

    /*****************************************
    *
    *  existing complexObjectType
    *
    *****************************************/

    GUIManagedObject existingComplexObjectType = complexObjectTypeService.getStoredComplexObjectType(complexObjectTypeID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingComplexObjectType != null && existingComplexObjectType.getReadOnly())
      {
        response.put("id", existingComplexObjectType.getGUIManagedObjectID());
        response.put("accepted", existingComplexObjectType.getAccepted());
        response.put("valid", existingComplexObjectType.getAccepted());
        response.put("processing", complexObjectTypeService.isActiveComplexObjectType(existingComplexObjectType, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process complexObjectType
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate ComplexObjectType
        *
        ****************************************/

        ComplexObjectType complexObjectType = new ComplexObjectType(jsonRoot, epoch, existingComplexObjectType);

        /*****************************************
         *
         * store
         *
         *****************************************/
        if (!dryRun)
          {

            complexObjectTypeService.putComplexObjectType(complexObjectType, (existingComplexObjectType == null), userID);

            /*****************************************
             *
             * add dynamic criterion fields)
             *
             *****************************************/

            dynamicCriterionFieldService.addComplexObjectTypeCriterionFields(complexObjectType, (existingComplexObjectType == null));

          }

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", complexObjectType.getComplexObjectTypeID());
        response.put("accepted", complexObjectType.getAccepted());
        response.put("valid", complexObjectType.getAccepted());
        response.put("processing", complexObjectTypeService.isActiveComplexObjectType(complexObjectType, now));
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
        if (!dryRun)
          {
            complexObjectTypeService.putIncompleteComplexObjectType(incompleteObject, (existingComplexObjectType == null), userID);
          }

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("complexObjectTypeID", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "complexObjectTypeNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveComplexObjectType
  *
  *****************************************/

  JSONObject processRemoveComplexObjectType(String userID, JSONObject jsonRoot)
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
    String responseCode = "";
    String singleIDresponseCode = "";
    List<GUIManagedObject> complexObjectTypes = new ArrayList<GUIManagedObject>();
    List<String> validIDs = new ArrayList<>();
    JSONArray complexObjectTypeIDs = new JSONArray();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);
    //
    //remove single complexObjectType
    //
    if (jsonRoot.containsKey("id"))
      {
        String complexObjectTypeID = JSONUtilities.decodeString(jsonRoot, "id", false);
        complexObjectTypeIDs.add(complexObjectTypeID);
        GUIManagedObject complexObjectType = complexObjectTypeService.getStoredComplexObjectType(complexObjectTypeID);
        if (complexObjectType != null && (force || !complexObjectType.getReadOnly()))
          singleIDresponseCode = "ok";
        else if (complexObjectType != null)
          singleIDresponseCode = "failedReadOnly";
        else
          {
            singleIDresponseCode = "complexObjectTypeNotFound";
          }


      }
    //
    // multiple deletion
    //
    
    if (jsonRoot.containsKey("ids"))
      {
        complexObjectTypeIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids", false);
      }  

    for (int i = 0; i < complexObjectTypeIDs.size(); i++)
      {
        String complexObjectTypeID = complexObjectTypeIDs.get(i).toString();
        GUIManagedObject complexObjectType = complexObjectTypeService.getStoredComplexObjectType(complexObjectTypeID);

        if (complexObjectType != null && (force || !complexObjectType.getReadOnly()))
          {
            complexObjectTypes.add(complexObjectType);
            validIDs.add(complexObjectTypeID);
          }
      }

    /*****************************************
     *
     * remove related deliverable and related paymentMean
     *
     *****************************************/
    for (int i = 0; i < complexObjectTypes.size(); i++)
      {
        GUIManagedObject complexObjectType = complexObjectTypes.get(i);
        DeliveryManagerDeclaration deliveryManager = Deployment.getDeliveryManagers().get("complexObjectTypeFulfillment");
        JSONObject deliveryManagerJSON = (deliveryManager != null) ? deliveryManager.getJSONRepresentation() : null;
        String providerID = (deliveryManagerJSON != null) ? (String) deliveryManagerJSON.get("providerID") : null;
        if (providerID != null)
          {
            //
            // deliverable
            //

            Collection<GUIManagedObject> deliverableObjects = deliverableService.getStoredDeliverables();
            for (GUIManagedObject deliverableObject : deliverableObjects)
              {
                if (deliverableObject instanceof Deliverable)
                  {
                    Deliverable deliverable = (Deliverable) deliverableObject;
                    if (deliverable.getFulfillmentProviderID().equals(providerID)
                        && deliverable.getExternalAccountID().equals(complexObjectType.getGUIManagedObjectID()))
                      {
                        deliverableService.removeDeliverable(deliverable.getDeliverableID(), "0");
                      }
                  }
              }

            //
            // paymentMean
            //

            Collection<GUIManagedObject> paymentMeanObjects = paymentMeanService.getStoredPaymentMeans();
            for (GUIManagedObject paymentMeanObject : paymentMeanObjects)
              {
                if (paymentMeanObject instanceof PaymentMean)
                  {
                    PaymentMean paymentMean = (PaymentMean) paymentMeanObject;
                    if (paymentMean.getFulfillmentProviderID().equals(providerID)
                        && paymentMean.getExternalAccountID().equals(complexObjectType.getGUIManagedObjectID()))
                      {
                        paymentMeanService.removePaymentMean(paymentMean.getPaymentMeanID(), "0");
                      }
                  }
              }
          }

        /*****************************************
         *
         * remove
         *
         *****************************************/

        //
        // remove complexObjectType
        //

        complexObjectTypeService.removeComplexObjectType(complexObjectType.getGUIManagedObjectID(), userID);

        //
        // remove dynamic criterion fields
        //
        if(!(complexObjectType instanceof IncompleteObject))
          {
            dynamicCriterionFieldService.removeComplexObjectTypeCriterionFields(complexObjectType);
          }

        //
        // revalidate
        //

        revalidateSubscriberMessageTemplates(now);
        revalidateTargets(now);
        revalidateJourneys(now);
          
      }
        /*****************************************
         *
         * responseCode
         *
         *****************************************/
    if (jsonRoot.containsKey("id"))
      {
        response.put("responseCode", singleIDresponseCode);
        return JSONUtilities.encodeObject(response);
      }

    else
      {
        response.put("responseCode", "ok");
      }

    /*****************************************
     *
     * response
     *
     *****************************************/
    response.put("removedComplexObjectTypeIDs", JSONUtilities.encodeArray(validIDs));

    return JSONUtilities.encodeObject(response);
  }


  /*****************************************
  *
  *  processGetCountBySegmentationRanges
  *
  *****************************************/

  JSONObject processGetCountBySegmentationRanges(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  parse input (segmentationDimension)
    *
    *****************************************/

    boolean evaluateBySegmentId = false;

    if(JSONUtilities.decodeString(jsonRoot,"id")== null)
    {
      evaluateBySegmentId = false;
      jsonRoot.put("id","new_dimension");
    }
    else
    {
      evaluateBySegmentId = true;
    }

    SegmentationDimensionRanges segmentationDimensionRanges = null;
    try
      {
        switch (SegmentationDimensionTargetingType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "targetingType", true)))
          {
            case RANGES:
              segmentationDimensionRanges = new SegmentationDimensionRanges(segmentationDimensionService, jsonRoot, epochServer.getKey(), null, false);
              break;

            case Unknown:
              throw new GUIManagerException("unsupported dimension type", JSONUtilities.decodeString(jsonRoot, "targetingType", false));
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

        response.put("responseCode", "segmentationDimensionNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }

    if(evaluateBySegmentId)
    {
      SegmentationDimension storedSegmentationDimensionRanges = segmentationDimensionService.getActiveSegmentationDimension(segmentationDimensionRanges.getGUIManagedObjectID() , SystemTime.getCurrentTime());
      evaluateBySegmentId = evaluateBySegmentId &&  segmentationDimensionRanges.getSegmentsConditionEqual(storedSegmentationDimensionRanges);
      log.info("evaluate by segment_id = "+evaluateBySegmentId);
    }
    //tamporary call only for banglaink hot fix because in 1.4.26 sbm is called
    if(evaluateBySegmentId) return processGetCountBySegmentationRangesBySegmentId(userID,jsonRoot);
    /*****************************************
    *
    *  extract BaseSplits
    *
    *****************************************/

    List<BaseSplit> baseSplits = segmentationDimensionRanges.getBaseSplit();
    int nbBaseSplits = baseSplits.size();

    /*****************************************
    *
    *  construct query
    *
    *****************************************/

    final String MAIN_AGG_NAME = "MAIN";
    final String RANGE_AGG_PREFIX = "RANGE-";

    //
    //  Main aggregation query: BaseSplit
    //

    List<BoolQueryBuilder> baseSplitQueries = new ArrayList<BoolQueryBuilder>();
    try
      {
        //
        // BaseSplit query creation
        //

        for(int i = 0; i < nbBaseSplits; i++)
          {
            baseSplitQueries.add(QueryBuilders.boolQuery());
          }

        for(int i = 0; i < nbBaseSplits; i++)
          {
            BoolQueryBuilder query = baseSplitQueries.get(i);
            BaseSplit baseSplit = baseSplits.get(i);

            //
            // Filter this bucket with this BaseSplit criteria
            //

            if(baseSplit.getProfileCriteria().isEmpty())
              {
                //
                // If there is not any profile criteria, just filter with a match_all query.
                //

                query = query.filter(QueryBuilders.matchAllQuery());
              }
            else
              {
                for (EvaluationCriterion evaluationCriterion : baseSplit.getProfileCriteria())
                  {
                    query = query.filter(evaluationCriterion.esQuery());
                  }
              }

            //
            //  Must_not for all following buckets (reminder : bucket must be disjointed, if not, some customer could be counted in several buckets)
            //

            for(int j = i+1; j < nbBaseSplits; j++)
              {
                BoolQueryBuilder nextQuery = baseSplitQueries.get(j);
                if(baseSplit.getProfileCriteria().isEmpty())
                  {
                    //
                    // If there is not any profile criteria, just filter with a match_all query.
                    //
                    nextQuery = nextQuery.mustNot(QueryBuilders.matchAllQuery());
                  }
                else
                  {
                    for (EvaluationCriterion evaluationCriterion : baseSplit.getProfileCriteria())
                      {
                        nextQuery = nextQuery.mustNot(evaluationCriterion.esQuery());
                      }
                  }
              }
          }
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
    *  the main aggregation is a filter aggregation.Each filter query will constitute a bucket representing a BaseSplit.
    *
    *****************************************/

    List<KeyedFilter> queries = new ArrayList<KeyedFilter>();
    for(int i = 0; i < nbBaseSplits; i++)
      {
        BoolQueryBuilder query = baseSplitQueries.get(i);
        String bucketName = baseSplits.get(i).getSplitName(); // Warning: input must ensure that all BaseSplit names are different. ( TODO )
        queries.add(new FiltersAggregator.KeyedFilter(bucketName, query));
      }

    //
    // @DEBUG: *otherBucket* can be activated for debug purpose: .otherBucket(true).otherBucketKey("OTH_BUCK")
    //

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    AggregationBuilder aggregation = AggregationBuilders.filters(MAIN_AGG_NAME, queries.toArray(new KeyedFilter[queries.size()]));

    //
    //  Sub-aggregation query: Segments
    //    sub-aggregations corresponding to all ranges-aggregation are added to the query
    //

    for(int i = 0; i < nbBaseSplits; i++)
      {
        BaseSplit baseSplit = baseSplits.get(i);
        if(baseSplit.getVariableName() == null)
          {
            //
            // This means that it should be the default segment, ranges-aggregation does not make sense here.
            //

            QueryBuilder match_all = QueryBuilders.matchAllQuery();
            AggregationBuilder other = AggregationBuilders.filter(RANGE_AGG_PREFIX+baseSplit.getSplitName(), match_all);
            aggregation.subAggregation(other);
          }
        else
          {
            //
            // Warning: input must ensure that all BaseSplit names are different. ( TODO )
            //

            RangeAggregationBuilder range = AggregationBuilders.range(RANGE_AGG_PREFIX+baseSplit.getSplitName());

            //aici de scris codul cand nu exista range
            //TermsAggregationBuilder term = AggregationBuilders.terms(RANGE_AGG_PREFIX+baseSplit.getSplitName());

            //term = term.field(Deployment.getSu)

            //
            // Retrieving the ElasticSearch field from the Criterion field.
            //

            CriterionField criterionField = CriterionContext.FullProfile.getCriterionFields().get(baseSplit.getVariableName());
            if(criterionField.getESField() == null)
              {
                //
                // If this Criterion field does not correspond to any field from Deployment.json, raise an error
                //

                log.warn("Unknown criterion field {}", baseSplit.getVariableName());

                //
                //  response
                //

                response.put("responseCode", "systemError");
                response.put("responseMessage", "Unknown criterion field "+baseSplit.getVariableName()); // TODO security issue ?
                response.put("responseParameter", null);
                return JSONUtilities.encodeObject(response);
              }

            //
            //
            //

            range = range.field(criterionField.getESField());
            for(SegmentRanges segment : baseSplit.getSegments())
              {
                //
                // Warning: input must ensure that all segment names are different. ( TODO )
                //

                range = range.addRange(new Range(segment.getName(), (segment.getRangeMin() != null)? new Double (segment.getRangeMin()) : null, (segment.getRangeMax() != null)? new Double (segment.getRangeMax()) : null));
              }
            aggregation.subAggregation(range);
            //add no value aggrebation for null values for range field
            BoolQueryBuilder builder = QueryBuilders.boolQuery();
            ExistsQueryBuilder existsQueryBuilder = QueryBuilders.existsQuery(criterionField.getESField());
            builder.mustNot().add(existsQueryBuilder);
            AggregationBuilder noValueAgg = AggregationBuilders.filter(RANGE_AGG_PREFIX+baseSplit.getSplitName()+"NOVALUE",builder);
            aggregation.subAggregation(noValueAgg);
          }
      }

    searchSourceBuilder.aggregation(aggregation);

    /*****************************************
    *
    *  construct response (JSON object)
    *
    *****************************************/

    JSONObject responseJSON = new JSONObject();
    List<JSONObject> responseBaseSplits = new ArrayList<JSONObject>();
    for(int i = 0; i < nbBaseSplits; i++)
      {
        BaseSplit baseSplit = baseSplits.get(i);
        JSONObject responseBaseSplit = new JSONObject();
        List<JSONObject> responseSegments = new ArrayList<JSONObject>();
        responseBaseSplit.put("splitName", baseSplit.getSplitName());

        //
        //  ranges
        //   the "count" field will be filled with the result of the ElasticSearch query
        //

        for(SegmentRanges segment : baseSplit.getSegments())
          {
            JSONObject responseSegment = new JSONObject();
            responseSegment.put("name", segment.getName());
            responseSegments.add(responseSegment);
          }
        responseBaseSplit.put("segments", responseSegments);
        responseBaseSplits.add(responseBaseSplit);
      }

    /*****************************************
    *
    *  execute query
    *
    *****************************************/

    SearchRequest searchRequest = new SearchRequest("subscriberprofile").source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).aggregation(aggregation).size(0));
    SearchResponse searchResponse = null;
    try
      {
        searchResponse = elasticsearch.search(searchRequest, RequestOptions.DEFAULT);
      }
    catch (IOException e)
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
    *  retrieve result and fill the response JSON object
    *
    *****************************************/

    Filters mainAggregationResult = searchResponse.getAggregations().get(MAIN_AGG_NAME);

    //
    //  fill response JSON object with counts for each segments from ElasticSearch result
    //

    for(JSONObject responseBaseSplit : responseBaseSplits)
      {
        Filters.Bucket bucket = mainAggregationResult.getBucketByKey((String) responseBaseSplit.get("splitName"));
        ParsedAggregation segmentAggregationResult = bucket.getAggregations().get(RANGE_AGG_PREFIX+bucket.getKeyAsString());
        ParsedAggregation noValueAggregationResult = bucket.getAggregations().get(RANGE_AGG_PREFIX+bucket.getKeyAsString()+"NOVALUE");
        if (segmentAggregationResult instanceof ParsedFilter)
          {
            //
            // This specific segment aggregation is corresponding to the "default" BaseSplit (without any variableName)
            //

            ParsedFilter other = (ParsedFilter) segmentAggregationResult;

            //
            //  fill the "count" field of the response JSON object (for each segments)
            List<JSONObject> responseSegments = (List<JSONObject>)responseBaseSplit.get("segments");
            for(JSONObject responseSegment : responseSegments)
              {
                responseSegment.put("count", other.getDocCount());
              }
            JSONObject noValueSegment = new JSONObject();
            noValueSegment.put("name", "no value");
            noValueSegment.put("count",((ParsedFilter)noValueAggregationResult).getDocCount());
            responseSegments.add(noValueSegment);
          }
        else
          {
            //
            // Segment aggregation is a range-aggregation.
            //

            ParsedRange ranges = (ParsedRange) segmentAggregationResult;
            List<ParsedRange.ParsedBucket> segmentBuckets = (List<ParsedRange.ParsedBucket>) ranges.getBuckets();

            //
            // bucketMap is an hash map for caching purpose
            //

            Map<String, ParsedRange.ParsedBucket> bucketMap = new HashMap<>(segmentBuckets.size());
            for (ParsedRange.ParsedBucket segmentBucket : segmentBuckets)
              {
                bucketMap.put(segmentBucket.getKey(), segmentBucket);
              }

            //
            //  fill the "count" field of the response JSON object (for each segments)
            //
            List<JSONObject> responseSegments = (List<JSONObject>)responseBaseSplit.get("segments");
            for(JSONObject responseSegment : responseSegments)
              {
                responseSegment.put("count", bucketMap.get(responseSegment.get("name")).getDocCount());
              }
            //read no value aggregation values and add for each segment
            JSONObject noValueSegment = new JSONObject();
            noValueSegment.put("name", "no available values");
            noValueSegment.put("count",((ParsedFilter)noValueAggregationResult).getDocCount());
            responseSegments.add(noValueSegment);
          }
      }
    responseJSON.put("baseSplit", responseBaseSplits);

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("result", responseJSON);
    response.put("responseCode", "ok");
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   *  processGetCountBySegmentationRanges
   *
   *****************************************/

  JSONObject processGetCountBySegmentationRangesBySegmentId(String userID, JSONObject jsonRoot)
  {
    /****************************************
     *
     *  response
     *
     ****************************************/

    HashMap<String, Object> response = new HashMap<String, Object>();

    /*****************************************
     *
     *  parse input (segmentationDimension)
     *
     *****************************************/

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().sort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).query(QueryBuilders.matchAllQuery()).size(0);
    JSONObject responseJSON = new JSONObject();
    //String segmentsESFieldName = Deployment.getProfileCriterionFields().get("subscriber.stratum").getESField();
    String stratumESFieldName = "stratum";

    SegmentationDimensionRanges segmentationDimensionRanges = null;
    try
    {
      switch (SegmentationDimensionTargetingType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "targetingType", true)))
      {
      case RANGES:
        segmentationDimensionRanges = new SegmentationDimensionRanges(segmentationDimensionService, jsonRoot, epochServer.getKey(), null, false);
        break;

      case Unknown:
        throw new GUIManagerException("unsupported dimension type", JSONUtilities.decodeString(jsonRoot, "targetingType", false));
      }
    }
    catch (JSONUtilitiesException | GUIManagerException e)
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

      response.put("responseCode", "segmentationDimensionNotValid");
      response.put("responseMessage", e.getMessage());
      response.put("responseParameter", (e instanceof GUIManagerException) ?
          ((GUIManagerException) e).getResponseParameter() :
          null);
      return JSONUtilities.encodeObject(response);
    }

    /*****************************************
     *
     *  extract BaseSplits
     *
     *****************************************/

    List<BaseSplit> baseSplits = segmentationDimensionRanges.getBaseSplit();
    int nbBaseSplits = baseSplits.size();

    /*****************************************
     *
     *  construct query
     *
     *****************************************/
    try
    {
      for (int i = 0; i < nbBaseSplits; i++)
      {
        BaseSplit baseSplit = baseSplits.get(i);
        List<SegmentRanges> ranges = baseSplit.getSegments();

        //create aggregations for all ids from a base split
        //TermsQueryBuilder splitTerms = QueryBuilders.termsQuery(stratumESFieldName,ranges.stream().map(SegmentRanges::getID).collect(Collectors.toList()));
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        AggregationBuilder baseSpitAgg = AggregationBuilders.filter(baseSplit.getSplitName(),matchAllQueryBuilder);

        //add subaggregations for each segment
        for (SegmentRanges range : ranges)
        {
          TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(stratumESFieldName+"."+segmentationDimensionRanges.getGUIManagedObjectID(), range.getID());
          baseSpitAgg.subAggregation(AggregationBuilders.filter(range.getName(),termQueryBuilder));
        }

        searchSourceBuilder.aggregation(baseSpitAgg);
      }

      /*****************************************
       *
       *  construct response (JSON object)
       *
       *****************************************/


      List<JSONObject> responseBaseSplits = new ArrayList<JSONObject>();
      for(int i = 0; i < nbBaseSplits; i++)
      {
        BaseSplit baseSplit = baseSplits.get(i);
        JSONObject responseBaseSplit = new JSONObject();
        List<JSONObject> responseSegments = new ArrayList<JSONObject>();
        responseBaseSplit.put("splitName", baseSplit.getSplitName());

        //
        //  ranges
        //   the "count" field will be filled with the result of the ElasticSearch query
        //

        for(SegmentRanges segment : baseSplit.getSegments())
        {
          JSONObject responseSegment = new JSONObject();
          responseSegment.put("name", segment.getName());
          responseSegments.add(responseSegment);
        }
        responseBaseSplit.put("segments", responseSegments);
        responseBaseSplits.add(responseBaseSplit);
      }

      //
      //  search in ES
      //

      SearchRequest searchRequest = new SearchRequest("subscriberprofile").source(searchSourceBuilder);
      SearchResponse searchResponse = elasticsearch.search(searchRequest, RequestOptions.DEFAULT);

      Aggregations resultAggs = searchResponse.getAggregations();

      for(JSONObject responseBaseSplit : responseBaseSplits)
      {
        ParsedAggregation splitAgg = resultAggs.get((String) responseBaseSplit.get("splitName"));
       // responseBaseSplit.put("count",((ParsedFilter)splitAgg).getDocCount());
        for(JSONObject responseSegment : (List<JSONObject>)responseBaseSplit.get("segments"))
        {
          ParsedFilter segmentFilter = ((ParsedFilter)splitAgg).getAggregations().get((String)responseSegment.get("name"));
          responseSegment.put("count",segmentFilter.getDocCount());
        }

      }
      responseJSON.put("baseSplit", responseBaseSplits);

    }
    catch (Exception e)
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
      response.put("responseParameter", (e instanceof GUIManagerException) ?
          ((GUIManagerException) e).getResponseParameter() :
          null);
      return JSONUtilities.encodeObject(response);
    }

    response.put("responseCode", "ok");
    response.put("result",responseJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetCatalogCharacteristic
  *
  *****************************************/

  JSONObject processGetCatalogCharacteristic(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String catalogCharacteristicID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate scoring strategy
    *
    *****************************************/

    GUIManagedObject catalogCharacteristic = catalogCharacteristicService.getStoredCatalogCharacteristic(catalogCharacteristicID, includeArchived);
    JSONObject catalogCharacteristicJSON = catalogCharacteristicService.generateResponseJSON(catalogCharacteristic, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (catalogCharacteristic != null) ? "ok" : "catalogCharacteristicNotFound");
    if (catalogCharacteristic != null) response.put("catalogCharacteristic", catalogCharacteristicJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutCatalogCharacteristic
  *
  *****************************************/

  JSONObject processPutCatalogCharacteristic(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();
    Boolean dryRun = false;
    

    /*****************************************
    *
    *  dryRun
    *
    *****************************************/
    if (jsonRoot.containsKey("dryRun")) {
      dryRun = JSONUtilities.decodeBoolean(jsonRoot, "dryRun", false);
    }

    /*****************************************
    *
    *  catalogCharacteristicID
    *
    *****************************************/

    String catalogCharacteristicID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (catalogCharacteristicID == null)
      {
        catalogCharacteristicID = catalogCharacteristicService.generateCatalogCharacteristicID();
        jsonRoot.put("id", catalogCharacteristicID);
      }

    /*****************************************
    *
    *  existing catalogCharacteristic
    *
    *****************************************/

    GUIManagedObject existingCatalogCharacteristic = catalogCharacteristicService.getStoredCatalogCharacteristic(catalogCharacteristicID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingCatalogCharacteristic != null && existingCatalogCharacteristic.getReadOnly())
      {
        response.put("id", existingCatalogCharacteristic.getGUIManagedObjectID());
        response.put("accepted", existingCatalogCharacteristic.getAccepted());
        response.put("valid", existingCatalogCharacteristic.getAccepted());
        response.put("processing", catalogCharacteristicService.isActiveCatalogCharacteristic(existingCatalogCharacteristic, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process catalogCharacteristic
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate catalogCharacteristic
        *
        ****************************************/

        CatalogCharacteristic catalogCharacteristic = new CatalogCharacteristic(jsonRoot, epoch, existingCatalogCharacteristic);

        /*****************************************
        *
        *  store
        *
        *****************************************/
        if (!dryRun)
          {

            catalogCharacteristicService.putCatalogCharacteristic(catalogCharacteristic,
                (existingCatalogCharacteristic == null), userID);

            /*****************************************
             *
             * revalidate dependent objects
             *
             *****************************************/

            revalidateOffers(now);
            revalidateJourneyObjectives(now);
            revalidateOfferObjectives(now);
            revalidateProductTypes(now);
            revalidateProducts(now);
            // right now voucher has no characteristics, the way I'm
            // implementing however I think should logically have
            revalidateVoucherTypes(now);
            revalidateVouchers(now);
          }

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", catalogCharacteristic.getCatalogCharacteristicID());
        response.put("accepted", catalogCharacteristic.getAccepted());
        response.put("valid", catalogCharacteristic.getAccepted());
        response.put("processing", catalogCharacteristicService.isActiveCatalogCharacteristic(catalogCharacteristic, now));
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
        if (!dryRun)
          {
            catalogCharacteristicService.putCatalogCharacteristic(incompleteObject,
                (existingCatalogCharacteristic == null), userID);

            //
            // revalidate dependent objects
            //

            revalidateOffers(now);
            revalidateJourneyObjectives(now);
            revalidateOfferObjectives(now);
            revalidateProductTypes(now);
            revalidateProducts(now);
            revalidateVoucherTypes(now);
            revalidateVouchers(now);
          }

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "catalogCharacteristicNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveCatalogCharacteristic
  *
  *****************************************/

  JSONObject processRemoveCatalogCharacteristic(String userID, JSONObject jsonRoot)
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

    String responseCode = "";
    String singleIDresponseCode = "";    
    List<GUIManagedObject> catalogCharacteristics = new ArrayList<>();
    List<String> validIDs = new ArrayList<>();
    JSONArray catalogCharacteristicIDs = new JSONArray();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);
    //
    //remove single journey
    //
    if (jsonRoot.containsKey("id"))
      {
        String catalogCharacteristicID = JSONUtilities.decodeString(jsonRoot, "id", false);
        catalogCharacteristicIDs.add(catalogCharacteristicID);
        GUIManagedObject catalogCharacteristic = catalogCharacteristicService
            .getStoredCatalogCharacteristic(catalogCharacteristicID);
        if (catalogCharacteristic != null && (force || !catalogCharacteristic.getReadOnly()))
          singleIDresponseCode = "ok";
        else if (catalogCharacteristic != null)
          singleIDresponseCode = "failedReadOnly";
        else singleIDresponseCode = "catalogCharacteristicNotFound";
      }
    //
    // multiple deletion
    //
    
    if (jsonRoot.containsKey("ids"))
      {
        catalogCharacteristicIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids", false);
      }    

    for (int i = 0; i < catalogCharacteristicIDs.size(); i++)
      {
        String catalogCharacteristicID = catalogCharacteristicIDs.get(i).toString();
        GUIManagedObject catalogCharacteristic = catalogCharacteristicService
            .getStoredCatalogCharacteristic(catalogCharacteristicID);

        if (catalogCharacteristic != null && (force || !catalogCharacteristic.getReadOnly()))
          {
            catalogCharacteristics.add(catalogCharacteristic);
            validIDs.add(catalogCharacteristicID);
          }
      }

    /*****************************************
     *
     * remove
     *
     *****************************************/
    for (int i = 0; i < catalogCharacteristics.size(); i++)
      {

        GUIManagedObject catalogCharacteristic = catalogCharacteristics.get(i);
        catalogCharacteristicService.removeCatalogCharacteristic(catalogCharacteristic.getGUIManagedObjectID(), userID);

        /*****************************************
         *
         * revalidate dependent objects
         *
         *****************************************/

        revalidateOffers(now);
        revalidateJourneyObjectives(now);
        revalidateOfferObjectives(now);
        revalidateProductTypes(now);
        revalidateProducts(now);
        revalidateVoucherTypes(now);
        revalidateVouchers(now);
      }

    /*****************************************
     *
     * responseCode
     *
     *****************************************/

    if (jsonRoot.containsKey("id"))
      {
        response.put("responseCode", singleIDresponseCode);
        return JSONUtilities.encodeObject(response);
      }

    else
      {
        response.put("responseCode", "ok");
      }
    /*****************************************
     *
     * response
     *
     *****************************************/
    response.put("removedCatalogCharacteristicIDS", JSONUtilities.encodeArray(validIDs));

    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
   *
   * processSetStatusCatalogCharacteristic
   *
   *****************************************/

  JSONObject processSetStatusCatalogCharacteristic(String userID, JSONObject jsonRoot)
  {
    /****************************************
     *
     * response
     *
     ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String, Object> response = new HashMap<String, Object>();
    JSONArray catalogCharacteristicIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
    List<String> statusSetIDs = new ArrayList<>();
    Boolean status = JSONUtilities.decodeBoolean(jsonRoot, "active");
    long epoch = epochServer.getKey();

    for (int i = 0; i < catalogCharacteristicIDs.size(); i++)
      {

        String catalogCharacteristicID = catalogCharacteristicIDs.get(i).toString();
        GUIManagedObject existingElement = catalogCharacteristicService
            .getStoredCatalogCharacteristic(catalogCharacteristicID);
        if (existingElement != null && !(existingElement.getReadOnly()))
          {
            statusSetIDs.add(catalogCharacteristicID);
            JSONObject elementRoot = (JSONObject) existingElement.getJSONRepresentation().clone();
            elementRoot.put("active", status);
            try
              {
                /****************************************
                 *
                 * instantiate catalogCharacteristic
                 *
                 ****************************************/

                CatalogCharacteristic catalogCharacteristic = new CatalogCharacteristic(elementRoot, epoch,
                    existingElement);

                /*****************************************
                 *
                 * store
                 *
                 *****************************************/
                catalogCharacteristicService.putCatalogCharacteristic(catalogCharacteristic, (existingElement == null),
                    userID);

                /*****************************************
                 *
                 * revalidate dependent objects
                 *
                 *****************************************/

                revalidateOffers(now);
                revalidateJourneyObjectives(now);
                revalidateOfferObjectives(now);
                revalidateProductTypes(now);
                revalidateProducts(now);
                // right now voucher has no characteristics, the way I'm
                // implementing however I think should logically have
                revalidateVoucherTypes(now);
                revalidateVouchers(now);

              }
            catch (JSONUtilitiesException | GUIManagerException e)
              {
                //
                // incompleteObject
                //

                IncompleteObject incompleteObject = new IncompleteObject(elementRoot, epoch);

                //
                // store
                //

                catalogCharacteristicService.putCatalogCharacteristic(incompleteObject, (existingElement == null),
                    userID);

                //
                // revalidate dependent objects
                //

                revalidateOffers(now);
                revalidateJourneyObjectives(now);
                revalidateOfferObjectives(now);
                revalidateProductTypes(now);
                revalidateProducts(now);
                revalidateVoucherTypes(now);
                revalidateVouchers(now);

                //
                // log
                //

                StringWriter stackTraceWriter = new StringWriter();
                e.printStackTrace(new PrintWriter(stackTraceWriter, true));
                if (log.isWarnEnabled())
                  {
                    log.warn("Exception processing REST api: {}", stackTraceWriter.toString());
                  }

              }
          }
      }
    response.put("responseCode", "ok");
    response.put("statusSetIds", statusSetIDs);
    return JSONUtilities.encodeObject(response);

  }


  /*****************************************
  *
  *  processGetCatalogCharacteristicList
  *
  *****************************************/

  JSONObject processGetCatalogCharacteristicList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert catalogCharacteristics
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> catalogCharacteristics = new ArrayList<JSONObject>();
    Collection <GUIManagedObject> catalogCharacteristicObjects = new ArrayList<GUIManagedObject>();
    
    if (jsonRoot.containsKey("ids"))
      {
  JSONArray catalogCharacteristicIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
  for (int i = 0; i < catalogCharacteristicIDs.size(); i++)
    {
      String catalogCharacteristicID = catalogCharacteristicIDs.get(i).toString();
      GUIManagedObject catalogCharacteristic = catalogCharacteristicService.getStoredCatalogCharacteristic(catalogCharacteristicID, includeArchived);
      if (catalogCharacteristic != null)
        {
          catalogCharacteristicObjects.add(catalogCharacteristic);
        }
    }
      }
    else
      {
        catalogCharacteristicObjects = catalogCharacteristicService.getStoredCatalogCharacteristics(includeArchived);
      }
    for (GUIManagedObject catalogCharacteristic : catalogCharacteristicObjects)
      {
        catalogCharacteristics.add(catalogCharacteristicService.generateResponseJSON(catalogCharacteristic, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("catalogCharacteristics", JSONUtilities.encodeArray(catalogCharacteristics));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetDeliverable
  *
  *****************************************/

  JSONObject processGetDeliverable(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String deliverableID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate scoring strategy
    *
    *****************************************/

    GUIManagedObject deliverable = deliverableService.getStoredDeliverable(deliverableID, includeArchived);
    JSONObject deliverableJSON = deliverableService.generateResponseJSON(deliverable, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (deliverable != null) ? "ok" : "deliverableNotFound");
    if (deliverable != null) response.put("deliverable", deliverableJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetDeliverableByName
  *
  *****************************************/

  JSONObject processGetDeliverableByName(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String deliverableName = JSONUtilities.decodeString(jsonRoot, "name", true);

    /*****************************************
    *
    *  retrieve and decorate scoring strategy
    *
    *****************************************/

    GUIManagedObject deliverable = deliverableService.getStoredDeliverableByName(deliverableName, includeArchived);
    JSONObject deliverableJSON = deliverableService.generateResponseJSON(deliverable, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (deliverable != null) ? "ok" : "deliverableNotFound");
    if (deliverable != null) response.put("deliverable", deliverableJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutDeliverable
  *
  *****************************************/

  JSONObject processPutDeliverable(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();
    Boolean dryRun = false;
    

    /*****************************************
    *
    *  dryRun
    *
    *****************************************/
    if (jsonRoot.containsKey("dryRun")) {
      dryRun = JSONUtilities.decodeBoolean(jsonRoot, "dryRun", false);
    }

    /*****************************************
    *
    *  deliverableID
    *
    *****************************************/

    String deliverableID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (deliverableID == null)
      {
        deliverableID = deliverableService.generateDeliverableID();
        jsonRoot.put("id", deliverableID);
      }

    /*****************************************
    *
    *  existing deliverable
    *
    *****************************************/

    GUIManagedObject existingDeliverable = deliverableService.getStoredDeliverable(deliverableID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingDeliverable != null && existingDeliverable.getReadOnly())
      {
        response.put("id", existingDeliverable.getGUIManagedObjectID());
        response.put("accepted", existingDeliverable.getAccepted());
        response.put("valid", existingDeliverable.getAccepted());
        response.put("processing", deliverableService.isActiveDeliverable(existingDeliverable, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process deliverable
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate deliverable
        *
        ****************************************/

        Deliverable deliverable = new Deliverable(jsonRoot, epoch, existingDeliverable);

        /*****************************************
        *
        *  store
        *
        *****************************************/
        if (!dryRun)
          {

            deliverableService.putDeliverable(deliverable, (existingDeliverable == null), userID);

            /*****************************************
             *
             * revalidateProducts
             *
             *****************************************/

            revalidateProducts(now);
          }

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", deliverable.getDeliverableID());
        response.put("accepted", deliverable.getAccepted());
        response.put("valid", deliverable.getAccepted());
        response.put("processing", deliverableService.isActiveDeliverable(deliverable, now));
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
        if (!dryRun)
          {

            deliverableService.putDeliverable(incompleteObject, (existingDeliverable == null), userID);

            //
            // revalidateProducts
            //

            revalidateProducts(now);
          }

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "deliverableNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveDeliverable
  *
  *****************************************/

  JSONObject processRemoveDeliverable(String userID, JSONObject jsonRoot)
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

    String responseCode = "";
    String singleIDresponseCode = "";
    List<GUIManagedObject> deliverables = new ArrayList<>();
    JSONArray deliverableIDs = new JSONArray();
    List<String> validIDs = new ArrayList<>();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);
    //
    //remove single deliverable
    //
    if (jsonRoot.containsKey("id"))
      {
        String deliverableID = JSONUtilities.decodeString(jsonRoot, "id", false);
        deliverableIDs.add(deliverableID);
        GUIManagedObject deliverable = deliverableService.getStoredDeliverable(deliverableID);
        if (deliverable != null && (force || !deliverable.getReadOnly()))
          singleIDresponseCode = "ok";
        else if (deliverable != null)
          singleIDresponseCode = "failedReadOnly";
        else singleIDresponseCode = "deliverableNotFound";
      }
    //
    // multiple deletion
    //
    
    if (jsonRoot.containsKey("ids"))
      {
        deliverableIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids", false);
      }
   

    for (int i = 0; i < deliverableIDs.size(); i++)
      {
        String deliverableID = deliverableIDs.get(i).toString();
        GUIManagedObject deliverable = deliverableService.getStoredDeliverable(deliverableID);
        if (deliverable != null && (force || !deliverable.getReadOnly())) 
          {
            deliverables.add(deliverable);
            validIDs.add(deliverableID);
          }

      }
        
  

    /*****************************************
    *
    *  remove
    *
    *****************************************/
    for (int i = 0; i < deliverables.size(); i++)
      {

        GUIManagedObject deliverable = deliverables.get(i);
        deliverableService.removeDeliverable(deliverable.getGUIManagedObjectID(), userID);

        /*****************************************
         *
         * revalidateProducts
         *
         *****************************************/

        revalidateProducts(now);
        
      }

    /*****************************************
    *
    * responseCode
    *
    *****************************************/

   if (jsonRoot.containsKey("id"))
     {
       response.put("responseCode", singleIDresponseCode);
       return JSONUtilities.encodeObject(response);
     }

   else
     {
       response.put("responseCode", "ok");
     }
   /*****************************************
   *
   * response
   *
   *****************************************/
  response.put("removedDeliverableIDS", JSONUtilities.encodeArray(validIDs));
  return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   * processSetStatusDeliverable
   *
   *****************************************/

  JSONObject processSetStatusDeliverable(String userID, JSONObject jsonRoot)
  {
    /****************************************
     *
     * response
     *
     ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String, Object> response = new HashMap<String, Object>();
    JSONArray deliverableIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
    List<String> statusSetIDs = new ArrayList<>();
    Boolean status = JSONUtilities.decodeBoolean(jsonRoot, "active");
    long epoch = epochServer.getKey();

    for (int i = 0; i < deliverableIDs.size(); i++)
      {

        String deliverableID = deliverableIDs.get(i).toString();
        GUIManagedObject existingElement = deliverableService.getStoredDeliverable(deliverableID);
        if (existingElement != null && !(existingElement.getReadOnly()))
          {
            statusSetIDs.add(deliverableID);
            JSONObject elementRoot = (JSONObject) existingElement.getJSONRepresentation().clone();
            elementRoot.put("active", status);

            try
              {
                /****************************************
                 *
                 * instantiate deliverable
                 *
                 ****************************************/

                Deliverable deliverable = new Deliverable(elementRoot, epoch, existingElement);

                /*****************************************
                 *
                 * store
                 *
                 *****************************************/
                deliverableService.putDeliverable(deliverable, (existingElement == null), userID);

                /*****************************************
                 *
                 * revalidateProducts
                 *
                 *****************************************/

                revalidateProducts(now);

              }
            catch (JSONUtilitiesException | GUIManagerException e)
              {
                //
                // incompleteObject
                //

                IncompleteObject incompleteObject = new IncompleteObject(elementRoot, epoch);

                //
                // store
                //
                deliverableService.putDeliverable(incompleteObject, (existingElement == null), userID);

                //
                // revalidateProducts
                //

                revalidateProducts(now);

                //
                // log
                //

                StringWriter stackTraceWriter = new StringWriter();
                e.printStackTrace(new PrintWriter(stackTraceWriter, true));
                if (log.isWarnEnabled())
                  {
                    log.warn("Exception processing REST api: {}", stackTraceWriter.toString());
                  }

                //
                // response
                //

              }
          }
      }
    response.put("responseCode", "ok");
    response.put("statusSetIds", statusSetIDs);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetTokenTypeList
  *
  *****************************************/

  JSONObject processGetTokenTypeList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert tokenTypes
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> tokenTypes = new ArrayList<JSONObject>();
    Collection <GUIManagedObject> tokenTypeObjects = new ArrayList<GUIManagedObject>();
    
    if (jsonRoot.containsKey("ids"))
      {
        JSONArray tokenTypeIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
        for (int i = 0; i < tokenTypeIDs.size(); i++)
          {
            String tokenTypeID = tokenTypeIDs.get(i).toString();
            GUIManagedObject tokenType = tokenTypeService.getStoredTokenType(tokenTypeID, includeArchived);
            if (tokenType != null)
              {
                tokenTypeObjects.add(tokenType);
              }
          }
      }
    else
      {
        tokenTypeObjects = tokenTypeService.getStoredTokenTypes(includeArchived);
      }
    for (GUIManagedObject tokenType : tokenTypeObjects)
      {
        tokenTypes.add(tokenTypeService.generateResponseJSON(tokenType, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("tokenTypes", JSONUtilities.encodeArray(tokenTypes));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetTokenType
  *
  *****************************************/

  JSONObject processGetTokenType(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String tokenTypeID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate tokenType
    *
    *****************************************/

    GUIManagedObject tokenType = tokenTypeService.getStoredTokenType(tokenTypeID, includeArchived);
    JSONObject tokenTypeJSON = tokenTypeService.generateResponseJSON(tokenType, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (tokenType != null) ? "ok" : "tokenTypeNotFound");
    if (tokenType != null) response.put("tokenType", tokenTypeJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutTokenType
  *
  *****************************************/

  JSONObject processPutTokenType(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();
    Boolean dryRun = false;
    

    /*****************************************
    *
    *  dryRun
    *
    *****************************************/
    if (jsonRoot.containsKey("dryRun")) {
      dryRun = JSONUtilities.decodeBoolean(jsonRoot, "dryRun", false);
    }

    /*****************************************
    *
    *  tokenTypeID
    *
    *****************************************/

    String tokenTypeID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (tokenTypeID == null)
      {
        tokenTypeID = tokenTypeService.generateTokenTypeID();
        jsonRoot.put("id", tokenTypeID);
      }

    /*****************************************
    *
    *  existing tokenType
    *
    *****************************************/

    GUIManagedObject existingTokenType = tokenTypeService.getStoredTokenType(tokenTypeID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingTokenType != null && existingTokenType.getReadOnly())
      {
        response.put("id", existingTokenType.getGUIManagedObjectID());
        response.put("accepted", existingTokenType.getAccepted());
        response.put("valid", existingTokenType.getAccepted());
        response.put("processing", tokenTypeService.isActiveTokenType(existingTokenType, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process tokenType
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate tokenType
        *
        ****************************************/

        TokenType tokenType = new TokenType(jsonRoot, epoch, existingTokenType);

        /*****************************************
        *
        *  store
        *
        *****************************************/
        if (!dryRun)
          {

            tokenTypeService.putTokenType(tokenType, (existingTokenType == null), userID);

            /*****************************************
             *
             * revalidateProducts
             *
             *****************************************/

            revalidateProducts(now);
          }

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", tokenType.getTokenTypeID());
        response.put("accepted", tokenType.getAccepted());
        response.put("valid", tokenType.getAccepted());
        response.put("processing", tokenTypeService.isActiveTokenType(tokenType, now));
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
        if (!dryRun)
          {
            tokenTypeService.putTokenType(incompleteObject, (existingTokenType == null), userID);

            //
            // revalidateProducts
            //

            revalidateProducts(now);
          }

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "tokenTypeNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveTokenType
  *
  *****************************************/

  JSONObject processRemoveTokenType(String userID, JSONObject jsonRoot)
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

    String responseCode = "";
    String singleIDresponseCode = "";
    List<GUIManagedObject> tokenTypes = new ArrayList<>();
    List<String> validIDs = new ArrayList<>();
    JSONArray tokenTypeIDs = new JSONArray();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);
    //
    //remove single tokenType
    //
    if (jsonRoot.containsKey("id"))
      {
        String tokenTypeID = JSONUtilities.decodeString(jsonRoot, "id", false);
        tokenTypeIDs.add(tokenTypeID);
        GUIManagedObject tokenType = tokenTypeService.getStoredTokenType(tokenTypeID);
        if (tokenType != null && (force || !tokenType.getReadOnly()))
          singleIDresponseCode = "ok";
        else if (tokenType != null)
          singleIDresponseCode = "failedReadOnly";
        else singleIDresponseCode = "tokenTypeNotFound";

      }
    //
    // multiple deletion
    //
    
    if (jsonRoot.containsKey("ids"))
      {
        tokenTypeIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids", false);
      }
    
    for (int i = 0; i < tokenTypeIDs.size(); i++)
      {
        String tokenTypeID = tokenTypeIDs.get(i).toString();
        GUIManagedObject tokenType = tokenTypeService.getStoredTokenType(tokenTypeID);
        
        if (tokenType != null && (force || !tokenType.getReadOnly()))
          {
            tokenTypes.add(tokenType);
            validIDs.add(tokenTypeID);
          }
      }
        
  

    /*****************************************
    *
    *  remove
    *
    *****************************************/
    for (int i = 0; i < tokenTypes.size(); i++)
      {

        GUIManagedObject tokenType = tokenTypes.get(i);

        tokenTypeService.removeTokenType(tokenType.getGUIManagedObjectID(), userID);

        /*****************************************
         *
         * revalidateProducts
         *
         *****************************************/

        revalidateProducts(now);
      }

    /*****************************************
     *
     * responseCode
     *
     *****************************************/

    if (jsonRoot.containsKey("id"))
      {
        response.put("responseCode", singleIDresponseCode);
        return JSONUtilities.encodeObject(response);
      }

    else
      {
        response.put("responseCode", "ok");
      }

    /*****************************************
     *
     * response
     *
     *****************************************/
    response.put("removedTokenTypeIDS", JSONUtilities.encodeArray(validIDs));

    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   * processSetStatusTokenType
   *
   *****************************************/

  JSONObject processSetStatusTokenType(String userID, JSONObject jsonRoot)
  {
    /****************************************
     *
     * response
     *
     ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String, Object> response = new HashMap<String, Object>();
    JSONArray tokenTypeIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
    List<String> statusSetIDs = new ArrayList<>();
    Boolean status = JSONUtilities.decodeBoolean(jsonRoot, "active");
    long epoch = epochServer.getKey();

    for (int i = 0; i < tokenTypeIDs.size(); i++)
      {

        String tokenTypeID = tokenTypeIDs.get(i).toString();
        GUIManagedObject existingElement = tokenTypeService.getStoredTokenType(tokenTypeID);
        if (existingElement != null && !(existingElement.getReadOnly()))
          {
            statusSetIDs.add(tokenTypeID);
            JSONObject elementRoot = (JSONObject) existingElement.getJSONRepresentation().clone();
            elementRoot.put("active", status);
            try
              {
                /****************************************
                 *
                 * instantiate tokenType
                 *
                 ****************************************/

                TokenType tokenType = new TokenType(elementRoot, epoch, existingElement);

                /*****************************************
                 *
                 * store
                 *
                 *****************************************/

                tokenTypeService.putTokenType(tokenType, (existingElement == null), userID);

                /*****************************************
                 *
                 * revalidateProducts
                 *
                 *****************************************/

                revalidateProducts(now);

              }
            catch (JSONUtilitiesException | GUIManagerException e)
              {
                //
                // incompleteObject
                //

                IncompleteObject incompleteObject = new IncompleteObject(elementRoot, epoch);

                //
                // store
                //
                tokenTypeService.putTokenType(incompleteObject, (existingElement == null), userID);

                //
                // revalidateProducts
                //

                revalidateProducts(now);

                //
                // log
                //

                StringWriter stackTraceWriter = new StringWriter();
                e.printStackTrace(new PrintWriter(stackTraceWriter, true));
                if (log.isWarnEnabled())
                  {
                    log.warn("Exception processing REST api: {}", stackTraceWriter.toString());
                  }

              }
          }
      }
    response.put("responseCode", "ok");
    response.put("statusSetIds", statusSetIDs);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetTokenCodesFormats
  *
  *****************************************/
  JSONObject processGetTokenCodesFormats(String userID, JSONObject jsonRoot)
  {

    /*****************************************
    *
    *  retrieve tokenCodesFormats
    *
    *****************************************/

    List<JSONObject> supportedTokenCodesFormats = new ArrayList<JSONObject>();
    for (SupportedTokenCodesFormat supportedTokenCodesFormat : Deployment.getSupportedTokenCodesFormats().values())
      {
        JSONObject supportedTokenCodesFormatJSON = supportedTokenCodesFormat.getJSONRepresentation();
        supportedTokenCodesFormats.add(supportedTokenCodesFormatJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("supportedTokenCodesFormats", JSONUtilities.encodeArray(supportedTokenCodesFormats));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetVoucherCodePatternList
  *
  *****************************************/
  JSONObject processGetVoucherCodePatternList(String userID, JSONObject jsonRoot)
  {

    /*****************************************
    *
    *  retrieve voucherCodePatternList
    *
    *****************************************/

    List<JSONObject> supportedVoucherCodePatternList = new ArrayList<JSONObject>();
    for (SupportedVoucherCodePattern supportedVoucherCodePattern : Deployment.getSupportedVoucherCodePatternList().values())
      {
        JSONObject supportedVoucherCodePatternJSON = supportedVoucherCodePattern.getJSONRepresentation();
        supportedVoucherCodePatternList.add(supportedVoucherCodePatternJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("supportedVoucherCodePatternList", JSONUtilities.encodeArray(supportedVoucherCodePatternList));
    return JSONUtilities.encodeObject(response);
  }
  

  /*****************************************
  *
  *  processGenerateVouchers
  *
  *****************************************/

  JSONObject processGenerateVouchers(String userID, JSONObject jsonRoot)
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
    String pattern = JSONUtilities.decodeString(jsonRoot, "pattern", true);
    int quantity = JSONUtilities.decodeInteger(jsonRoot, "quantity", true);
    Date expirationDate = GUIManagedObject.parseDateField(JSONUtilities.decodeString(jsonRoot, "expirationDate", true));
    
    // find existing vouchers
    
    List<String> existingVoucherCodes = new ArrayList<>();
    Collection<GUIManagedObject> uploadedFileObjects = uploadedFileService.getStoredGUIManagedObjects(true);

    String supplierID = JSONUtilities.decodeString(jsonRoot, "supplierID", true);

    String applicationID = "vouchers_" + supplierID; // TODO CHECK THIS MK
    
    for (GUIManagedObject uploaded : uploadedFileObjects)
      {
        String fileApplicationID = JSONUtilities.decodeString(uploaded.getJSONRepresentation(), "applicationID", false);
        if (Objects.equals(applicationID, fileApplicationID))
          {
            if (uploaded instanceof UploadedFile)
              {
                UploadedFile uploadedFile = (UploadedFile) uploaded;
                BufferedReader reader = null;
                String filename = UploadedFile.OUTPUT_FOLDER + uploadedFile.getDestinationFilename();
                try
                {
                  reader = new BufferedReader(new FileReader(filename));
                  for (String line; (line = reader.readLine()) != null;)
                    {
                      if (line.trim().isEmpty()) continue;
                      existingVoucherCodes.add(line.trim());
                    }
                }
                catch (IOException e)
                {
                  log.info("Unable to read voucher file " + filename);
                } finally {
                  try {
                    if (reader != null) reader.close();
                  }
                  catch (IOException e)
                  {
                    log.info("Unable to read voucher file " + filename);
                  }
                }
              }
          }
      }
        
    List<String> currentVoucherCodes = new ArrayList<>();
    for (int q=0; q<quantity; q++)
      {
        String voucherCode = null; 
        boolean newVoucherGenerated = false;
        for (int i=0; i<HOW_MANY_TIMES_TO_TRY_TO_GENERATE_A_VOUCHER_CODE; i++)
          {
            voucherCode = TokenUtils.generateFromRegex(pattern);
            if (!currentVoucherCodes.contains(voucherCode) && !existingVoucherCodes.contains(voucherCode))
              {
                newVoucherGenerated = true;
                break;
              }
          }
        if (!newVoucherGenerated)
          {
            log.info("After " + HOW_MANY_TIMES_TO_TRY_TO_GENERATE_A_VOUCHER_CODE + " tries, unable to generate a new voucher code with pattern " + pattern);
            break;
          }
        log.debug("voucherCode  generated : " + voucherCode);
        currentVoucherCodes.add(voucherCode);
      }

    // convert list to InputStream
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try
    {
      for (String voucherCode : currentVoucherCodes)
        {
          baos.write(voucherCode.getBytes());
          baos.write("\n".getBytes());
        }
    }
    catch (IOException e) // will never happen as we write to memory
    {
      log.info("Issue when converting voucher list to file : " + e.getLocalizedMessage());
      log.debug("Voucher list : " + currentVoucherCodes);
    }
    byte[] bytes = baos.toByteArray();
    InputStream vouchersStream = new ByteArrayInputStream(bytes);

    // write list to UploadedFile

    String fileID = uploadedFileService.generateFileID();
    String sourceFilename = "Generated_internally_" + fileID + ".txt";
    
    JSONObject fileJSON = new JSONObject();
    fileJSON.put("id", fileID);
    fileJSON.put("applicationID", applicationID);
    fileJSON.put("sourceFilename", sourceFilename);
    fileJSON.put("fileType", ".txt");

    GUIManagedObject existingFileUpload = uploadedFileService.getStoredUploadedFile(fileID);
    long epoch = epochServer.getKey();
    
    try
      {
        UploadedFile uploadedFile = new UploadedFile(fileJSON, epoch, existingFileUpload);
        uploadedFileService.putUploadedFile(uploadedFile, vouchersStream, uploadedFile.getDestinationFilename(), (uploadedFile == null), userID);
      }
    catch (GUIManagerException|IOException e)
      {
        log.info("Issue when creating uploaded voucher file : " + e.getLocalizedMessage());
      }
    
    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("id", fileID);
    response.put("responseCode", "ok");
    return JSONUtilities.encodeObject(response);
  }

  
  /*****************************************
  *
  *  processGetPaymentMean
  *
  *****************************************/

  JSONObject processGetPaymentMean(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String paymentMeanID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate payment mean
    *
    *****************************************/

    GUIManagedObject paymentMean = paymentMeanService.getStoredPaymentMean(paymentMeanID, includeArchived);
    JSONObject paymentMeanJSON = paymentMeanService.generateResponseJSON(paymentMean, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (paymentMean != null) ? "ok" : "paymentMeanNotFound");
    if (paymentMean != null) response.put("paymentMean", paymentMeanJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutPaymentMean
  *
  *****************************************/

  JSONObject processPutPaymentMean(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();
    Boolean dryRun = false;
    

    /*****************************************
    *
    *  dryRun
    *
    *****************************************/
    if (jsonRoot.containsKey("dryRun")) {
      dryRun = JSONUtilities.decodeBoolean(jsonRoot, "dryRun", false);
    }

    /*****************************************
    *
    *  paymentMeanID
    *
    *****************************************/

    String paymentMeanID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (paymentMeanID == null)
      {
        paymentMeanID = paymentMeanService.generatePaymentMeanID();
        jsonRoot.put("id", paymentMeanID);
      }

    /*****************************************
    *
    *  existing paymentMean
    *
    *****************************************/

    GUIManagedObject existingPaymentMean = paymentMeanService.getStoredPaymentMean(paymentMeanID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingPaymentMean != null && existingPaymentMean.getReadOnly())
      {
        response.put("id", existingPaymentMean.getGUIManagedObjectID());
        response.put("accepted", existingPaymentMean.getAccepted());
        response.put("processing", paymentMeanService.isActivePaymentMean(existingPaymentMean, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process paymentMean
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate paymentMean
        *
        ****************************************/

        PaymentMean paymentMean = new PaymentMean(jsonRoot, epoch, existingPaymentMean);

        /*****************************************
        *
        *  store
        *
        *****************************************/
        if (!dryRun)
          {

            paymentMeanService.putPaymentMean(paymentMean, (existingPaymentMean == null), userID);

            /*****************************************
             *
             * revalidateProducts
             *
             *****************************************/

            revalidateProducts(now);
          }

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", paymentMean.getPaymentMeanID());
        response.put("accepted", paymentMean.getAccepted());
        response.put("processing", paymentMeanService.isActivePaymentMean(paymentMean, now));
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
        if (!dryRun)
          {
            paymentMeanService.putIncompletePaymentMean(incompleteObject, (existingPaymentMean == null), userID);

            //
            // revalidateProducts
            //

            revalidateProducts(now);
          }

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "paymentMeanNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemovePaymentMean
  *
  *****************************************/

  JSONObject processRemovePaymentMean(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    String responseCode = "";
    String singleIDresponseCode = "";
    List<GUIManagedObject> paymentMeans = new ArrayList<>();
    List<String> validIDs = new ArrayList<>();
    JSONArray paymentMeanIDs = new JSONArray();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);
    //
    //remove single journey
    //
    if (jsonRoot.containsKey("id"))
      {
        String paymentMeanID = JSONUtilities.decodeString(jsonRoot, "id", false);
        paymentMeanIDs.add(paymentMeanID);
        GUIManagedObject paymentMean = paymentMeanService.getStoredPaymentMean(paymentMeanID);

        if (paymentMean != null && (force || !paymentMean.getReadOnly()))
          singleIDresponseCode = "ok";
        else if (paymentMean != null)
          singleIDresponseCode = "failedReadOnly";
        else singleIDresponseCode = "paymentMeanNotFound";

      }
    //
    // multiple deletion
    //
    
    if (jsonRoot.containsKey("ids"))
      {
        paymentMeanIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids", false);
      }
  
    for (int i = 0; i < paymentMeanIDs.size(); i++)
      {
        String paymentMeanID = paymentMeanIDs.get(i).toString();
        GUIManagedObject paymentMean = paymentMeanService.getStoredPaymentMean(paymentMeanID);
        
        if (paymentMean != null && (force || !paymentMean.getReadOnly()))
          {
            paymentMeans.add(paymentMean);
            validIDs.add(paymentMeanID);
          }
      }
        
  

    /*****************************************
    *
    *  remove
    *
    *****************************************/
    for (int i = 0; i < paymentMeans.size(); i++)
      {

        GUIManagedObject paymentMean = paymentMeans.get(i);
          paymentMeanService.removePaymentMean(paymentMean.getGUIManagedObjectID(), userID);
      }
    /*****************************************
     *
     * responseCode
     *
     *****************************************/
    if (jsonRoot.containsKey("id"))
      {
        response.put("responseCode", singleIDresponseCode);
        return JSONUtilities.encodeObject(response);
      }

    else
      {
        response.put("responseCode", "ok");
      }
    /*****************************************
     *
     * response
     *
     *****************************************/

    response.put("removedPaymentMeanIDS", JSONUtilities.encodeArray(validIDs));
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
   *
   * processSetStatusPaymentMean
   *
   *****************************************/

  JSONObject processSetStatusPaymentMean(String userID, JSONObject jsonRoot)
  {
    /****************************************
     *
     * response
     *
     ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String, Object> response = new HashMap<String, Object>();
    JSONArray paymentMeanIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
    List<String> statusSetIDs = new ArrayList<>();
    Boolean status = JSONUtilities.decodeBoolean(jsonRoot, "active");
    long epoch = epochServer.getKey();

    for (int i = 0; i < paymentMeanIDs.size(); i++)
      {

        String paymentMeanID = paymentMeanIDs.get(i).toString();
        GUIManagedObject existingElement = paymentMeanService.getStoredPaymentMean(paymentMeanID);
        if (existingElement != null && !(existingElement.getReadOnly()))
          {
            statusSetIDs.add(paymentMeanID);
            JSONObject elementRoot = (JSONObject) existingElement.getJSONRepresentation().clone();
            elementRoot.put("active", status);
            try
              {
                /****************************************
                 *
                 * instantiate paymentMean
                 *
                 ****************************************/

                PaymentMean paymentMean = new PaymentMean(elementRoot, epoch, existingElement);

                /*****************************************
                 *
                 * store
                 *
                 *****************************************/
                paymentMeanService.putPaymentMean(paymentMean, (existingElement == null), userID);

                /*****************************************
                 *
                 * revalidateProducts
                 *
                 *****************************************/

                revalidateProducts(now);

              }
            catch (JSONUtilitiesException | GUIManagerException e)
              {
                //
                // incompleteObject
                //

                IncompleteObject incompleteObject = new IncompleteObject(elementRoot, epoch);

                //
                // store
                //

                paymentMeanService.putIncompletePaymentMean(incompleteObject, (existingElement == null), userID);

                //
                // revalidateProducts
                //

                revalidateProducts(now);

                //
                // log
                //

                StringWriter stackTraceWriter = new StringWriter();
                e.printStackTrace(new PrintWriter(stackTraceWriter, true));
                if (log.isWarnEnabled())
                  {
                    log.warn("Exception processing REST api: {}", stackTraceWriter.toString());
                  }
              }
          }
      }
    response.put("responseCode", "ok");
    response.put("statusSetIds", statusSetIDs);
    return JSONUtilities.encodeObject(response);

  }


  /*****************************************
  *
  *  processPutFile
  *
  *****************************************/

  void processPutFile(JSONObject jsonResponse, HttpExchange exchange) throws IOException
  {
    /****************************************
    *
    *  response map and object
    *
    ****************************************/

    JSONObject jsonRoot = null;
    String fileID = null;
    String userID = null;
    String responseText = null;

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  check incoming request
    *
    ****************************************/

    //
    //  contentType
    //

    String contentType = exchange.getRequestHeaders().getFirst("Content-Type"); 
    if(contentType == null)
      { 
        responseText = "Content-Type is null";    
      }
    else if (!contentType.startsWith(MULTIPART_FORM_DATA))
      { 
        responseText = "Message is not multipart/form-data";
      } 

    //
    //  contentLength
    //

    String contentLengthString = exchange.getRequestHeaders().getFirst("Content-Length"); 
    if(contentLengthString == null)
      { 
        responseText = "Content of message is null";  
      } 

    /****************************************
    *
    *  apache FileUpload API
    *
    ****************************************/

    final InputStream requestBodyStream = exchange.getRequestBody(); 
    final String contentEncoding = exchange.getRequestHeaders().getFirst("Content-Encoding");
    FileUpload upload = new FileUpload(); 
    FileItemIterator fileItemIterator; 
    try
      {
        fileItemIterator = upload.getItemIterator(new RequestContext()
        { 
          public String getCharacterEncoding() { return contentEncoding; } 
          public String getContentType() { return contentType; } 
          public int getContentLength() { return 0; } 
          public InputStream getInputStream() throws IOException { return requestBodyStream; }
        }); 

        if (!fileItemIterator.hasNext())
          { 
            responseText = "Body is empty";
          }

        //
        // here we will extract the meta data of the request and the file
        //

        if (responseText == null)
          {
            boolean uploadFile = false;
            while (fileItemIterator.hasNext())
              {
                FileItemStream fis = fileItemIterator.next();
                if (fis.getFieldName().equals(FILE_UPLOAD_META_DATA))
                  {
                    InputStream streams = fis.openStream();
                    String jsonAsString = Streams.asString(streams, "UTF-8");
                    jsonRoot = (JSONObject) (new JSONParser()).parse(jsonAsString);
                    userID = JSONUtilities.decodeString(jsonRoot, "userID", true);
                    if(!jsonRoot.containsKey("id"))
                      {
                        fileID = uploadedFileService.generateFileID();
                      }
                    else
                      {
                        fileID = JSONUtilities.decodeString(jsonRoot, "id", true);
                      }
                    if (streams != null) streams.close();
                    jsonRoot.put("id", fileID);
                    uploadFile = true;
                  }
                if (fis.getFieldName().equals(FILE_REQUEST) && uploadFile)
                  {
                    // converted the meta data and now attempting to save the file locally
                    //

                    long epoch = epochServer.getKey();

                    /*****************************************
                    *
                    *  existing UploadedFile
                    *
                    *****************************************/

                    GUIManagedObject existingFileUpload = uploadedFileService.getStoredUploadedFile(fileID);
                    try
                      {
                        /****************************************
                        *
                        *  instantiate new UploadedFile
                        *
                        ****************************************/

                        UploadedFile uploadedFile = new UploadedFile(jsonRoot, epoch, existingFileUpload);

                        /*****************************************
                        *
                        *  store UploadedFile
                        *
                        *****************************************/

                        uploadedFileService.putUploadedFile(uploadedFile, fis.openStream(), uploadedFile.getDestinationFilename(), (uploadedFile == null), userID);

                        /*****************************************
                        *
                        *  revalidate dependent objects
                        *
                        *****************************************/

                        revalidateTargets(now);

                        /*****************************************
                        *
                        *  response
                        *
                        *****************************************/

                        jsonResponse.put("id", fileID);
                        jsonResponse.put("accepted", true);
                        jsonResponse.put("valid", true);
                        jsonResponse.put("processing", true);
                        if(uploadedFile.getMetaData() != null && uploadedFile.getMetaData().get("segmentCounts") != null) 
                          {
                            jsonResponse.put("segmentCounts", uploadedFile.getMetaData().get("segmentCounts"));
                          }
                        jsonResponse.put("responseCode", "ok");
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

                        uploadedFileService.putIncompleteUploadedFile(incompleteObject, (existingFileUpload == null), userID);

                        //
                        //  revalidate dependent objects
                        //

                        revalidateTargets(now);

                        //
                        //  log
                        //

                        StringWriter stackTraceWriter = new StringWriter();
                        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
                        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

                        //
                        //  response
                        //

                        jsonResponse.put("id", incompleteObject.getGUIManagedObjectID());
                        jsonResponse.put("responseCode", "fileNotValid");
                        jsonResponse.put("responseMessage", e.getMessage());
                        jsonResponse.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
                      }
                  }
              }
          }
        else
          {
            jsonResponse.put("responseCode", "systemError");
            jsonResponse.put("responseMessage", responseText);
          }

        //
        //  log
        //

        log.debug("API (raw response): {}", jsonResponse.toString());

        //
        //  send
        //

        exchange.sendResponseHeaders(200, 0);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
        writer.write(jsonResponse.toString());
        writer.close();
        exchange.close();
      }
    catch (Exception e)
      { 
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("Failed to write file REST api: {}", stackTraceWriter.toString());   
        jsonResponse.put("responseCode", "systemError");
        jsonResponse.put("responseMessage", e.getMessage());
        exchange.sendResponseHeaders(200, 0);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
        writer.write(jsonResponse.toString());
        writer.close();
        exchange.close();
      } 
  }
  
  /*****************************************
  *
  *  processPutUploadedFileWithVariables
  *
  *****************************************/

  void processPutUploadedFileWithVariables(JSONObject jsonResponse, HttpExchange exchange) throws IOException
  {
    
    /****************************************
    *
    *  response map and object
    *
    ****************************************/

    JSONObject jsonRoot = null;
    String fileID = null;
    String userID = null;
    String responseText = null;

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  check incoming request
    *
    ****************************************/

    //
    //  contentType
    //

    String contentType = exchange.getRequestHeaders().getFirst("Content-Type"); 
    if(contentType == null)
      { 
        responseText = "Content-Type is null";    
      }
    else if (!contentType.startsWith(MULTIPART_FORM_DATA))
      { 
        responseText = "Message is not multipart/form-data";
      } 

    //
    //  contentLength
    //

    String contentLengthString = exchange.getRequestHeaders().getFirst("Content-Length"); 
    if(contentLengthString == null)
      { 
        responseText = "Content of message is null";  
      } 

    /****************************************
    *
    *  apache FileUpload API
    *
    ****************************************/

    final InputStream requestBodyStream = exchange.getRequestBody(); 
    final String contentEncoding = exchange.getRequestHeaders().getFirst("Content-Encoding");
    FileUpload upload = new FileUpload(); 
    FileItemIterator fileItemIterator; 
    
    try
      {
        fileItemIterator = upload.getItemIterator(new RequestContext()
        { 
          public String getCharacterEncoding() { return contentEncoding; } 
          public String getContentType() { return contentType; } 
          public int getContentLength() { return 0; } 
          public InputStream getInputStream() throws IOException { return requestBodyStream; }
        }); 

        if (!fileItemIterator.hasNext())
          { 
            responseText = "Body is empty";
          }

        //
        // here we will extract the meta data of the request and the file
        //
        
        if (responseText == null)
          {
            boolean uploadFile = false;
            while (fileItemIterator.hasNext())
              {
                FileItemStream fis = fileItemIterator.next();
                String fieldName = fis.getFieldName();
                switch (fieldName)
                {
                  case FILE_UPLOAD_META_DATA:
                    InputStream streams = fis.openStream();
                    String jsonAsString = Streams.asString(streams, "UTF-8");
                    jsonRoot = (JSONObject) (new JSONParser()).parse(jsonAsString);
                    String applicationID = JSONUtilities.decodeString(jsonRoot, "applicationID", true);
                    if (!UploadedFileService.FILE_WITH_VARIABLES_APPLICATION_ID.equals(applicationID)) throw new GUIManagerException("invalid applicationID", applicationID);
                    userID = JSONUtilities.decodeString(jsonRoot, "userID", true);
                    if(!jsonRoot.containsKey("id"))
                      {
                        fileID = uploadedFileService.generateFileID();
                      }
                    else
                      {
                        fileID = JSONUtilities.decodeString(jsonRoot, "id", true);
                      }
                    jsonRoot.put("id", fileID);
                    uploadFile = true;
                    break;
                    
                  case FILE_REQUEST:
                    if (uploadFile)
                      {
                        // converted the meta data and now attempting to save the file locally
                        //

                        long epoch = epochServer.getKey();

                        /*****************************************
                         *
                         * existing UploadedFile
                         *
                         *****************************************/
                        
                        GUIManagedObject existingFileUpload = uploadedFileService.getStoredUploadedFile(fileID);
                        try
                          {
                            /****************************************
                            *
                            *  instantiate new UploadedFile
                            *
                            ****************************************/

                            UploadedFile uploadedFile = new UploadedFile(jsonRoot, epoch, existingFileUpload);
                            
                            /*****************************************
                            *
                            *  store UploadedFile
                            *
                            *****************************************/
                            
                            uploadedFileService.putUploadedFileWithVariables(uploadedFile, fis.openStream(), uploadedFile.getDestinationFilename(), (uploadedFile == null), userID);
                            
                            /*****************************************
                            *
                            *  revalidate dependent objects
                            *
                            *****************************************/

                            revalidateJourneys(now);

                            /*****************************************
                            *
                            *  response
                            *
                            *****************************************/
                            
                            jsonResponse.put("id", fileID);
                            jsonResponse.put("accepted", true);
                            jsonResponse.put("valid", true);
                            jsonResponse.put("processing", true);
                            if(uploadedFile.getMetaData() != null && uploadedFile.getMetaData().get("variables") != null) 
                              {
                                jsonResponse.put("variables", JSONUtilities.decodeJSONArray(uploadedFile.getMetaData().get("variables"), "fileVariables", new JSONArray()));
                              }
                            jsonResponse.put("responseCode", "ok");
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

                          uploadedFileService.putIncompleteUploadedFile(incompleteObject, (existingFileUpload == null), userID);

                          //
                          //  revalidate dependent objects
                          //

                          revalidateTargets(now);

                          //
                          //  log
                          //

                          StringWriter stackTraceWriter = new StringWriter();
                          e.printStackTrace(new PrintWriter(stackTraceWriter, true));
                          log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

                          //
                          //  response
                          //

                          jsonResponse.put("id", incompleteObject.getGUIManagedObjectID());
                          jsonResponse.put("responseCode", "fileNotValid");
                          jsonResponse.put("responseMessage", e.getMessage());
                          jsonResponse.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
                        }
                      }

                  default:
                    break;
                }
              }
          }
        else
          {
            jsonResponse.put("responseCode", "systemError");
            jsonResponse.put("responseMessage", responseText);
          }
        
        //
        //  log
        //

        log.debug("API (raw response): {}", jsonResponse.toString());
        
        //
        //  send
        //

        exchange.sendResponseHeaders(200, 0);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
        writer.write(jsonResponse.toString());
        writer.close();
        exchange.close();
      }
    catch (Exception e)
      {
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("Failed to write file REST api: {}", stackTraceWriter.toString());
        jsonResponse.put("responseCode", "systemError");
        jsonResponse.put("responseMessage", e.getMessage());
        exchange.sendResponseHeaders(200, 0);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
        writer.write(jsonResponse.toString());
        writer.close();
        exchange.close();
      } 
  }
 
  /*****************************************
  *
  *  processGetFilesList
  *
  *****************************************/

  JSONObject processGetFilesList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert UploadedFiles
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> uploadedFiles = new ArrayList<JSONObject>();
    Collection <GUIManagedObject> uploadedFileObjects = new ArrayList<GUIManagedObject>();
    
    if (jsonRoot.containsKey("ids"))
      {
        JSONArray uploadedFileIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
        for (int i = 0; i < uploadedFileIDs.size(); i++)
          {
            String uploadedFileID = uploadedFileIDs.get(i).toString();
            GUIManagedObject uploadedFile = uploadedFileService.getStoredUploadedFile(uploadedFileID, includeArchived);
            if (uploadedFile != null)
              {
                uploadedFileObjects.add(uploadedFile);
              }
          }
      }
    else
      {
        uploadedFileObjects = uploadedFileService.getStoredGUIManagedObjects(includeArchived);
      }
    String applicationID = JSONUtilities.decodeString(jsonRoot, "applicationID", true);
    for (GUIManagedObject uploaded : uploadedFileObjects)
      {
        String fileApplicationID = JSONUtilities.decodeString(uploaded.getJSONRepresentation(), "applicationID", false);
        if (Objects.equals(applicationID, fileApplicationID))
          {
            JSONObject jsonObject = uploadedFileService.generateResponseJSON(uploaded, fullDetails, now);
            if(fullDetails && applicationID.equals(UploadedFileService.basemanagementApplicationID))
              {
                jsonObject.put("segmentCounts", ((UploadedFile)uploaded).getMetaData().get("segmentCounts"));
              }
            else if (fullDetails && applicationID.equals(UploadedFileService.FILE_WITH_VARIABLES_APPLICATION_ID))
              {
                jsonObject.put("variables", JSONUtilities.decodeJSONArray(((UploadedFile)uploaded).getMetaData().get("variables"), "fileVariables", new JSONArray()));
              }
            uploadedFiles.add(jsonObject);
          }
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    //
    //  uploadedFiles
    //

    HashMap<String,Object> responseResult = new HashMap<String,Object>();
    responseResult.put("applicationID", applicationID);
    responseResult.put("uploadedFiles", JSONUtilities.encodeArray(uploadedFiles));

    //
    //  response
    //

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("uploadedFiles", JSONUtilities.encodeObject(responseResult));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processRemoveUploadedFile
  *
  *****************************************/

  JSONObject processRemoveUploadedFile(String userID, JSONObject jsonRoot)
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

    String responseCode = "";
    String singleIDresponseCode = "";
    List<GUIManagedObject> uploadFiles = new ArrayList<>();
    List<String> validIDs = new ArrayList<>();
    JSONArray uploadFilesIDs = new JSONArray();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);
    //
    //remove single uploadFile
    //
    if (jsonRoot.containsKey("id"))
      {
        String uploadFilesID = JSONUtilities.decodeString(jsonRoot, "id", false);
        uploadFilesIDs.add(uploadFilesID);
        GUIManagedObject uploadedFileID = uploadedFileService.getStoredUploadedFile(uploadFilesID);

        if (uploadedFileID != null && (force || !uploadedFileID.getReadOnly()))
          singleIDresponseCode = "ok";
        else if (uploadedFileID != null)
          singleIDresponseCode = "failedReadOnly";
        else singleIDresponseCode = "uploadedFileNotFound";
      }
    //
    // multiple deletion
    //
    
    if (jsonRoot.containsKey("ids"))
      {
        uploadFilesIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids", false);
      }
   
    for (int i = 0; i < uploadFilesIDs.size(); i++)
      {
        String uploadFilesID = uploadFilesIDs.get(i).toString();
        GUIManagedObject uploadedFileID = uploadedFileService.getStoredUploadedFile(uploadFilesID);
        
        if (uploadedFileID != null && (force || !uploadedFileID.getReadOnly()))
          {
            uploadFiles.add(uploadedFileID);
            validIDs.add(uploadFilesID);
          }
      }
        
  

    /*****************************************
    *
    *  remove
    *
    *****************************************/
    for (int i = 0; i < uploadFiles.size(); i++)
      {

        GUIManagedObject existingFileUpload = uploadFiles.get(i);

        uploadedFileService.deleteUploadedFile(existingFileUpload.getGUIManagedObjectID(), userID,
            (UploadedFile) existingFileUpload);

        /*****************************************
         *
         * revalidate dependent objects
         *
         *****************************************/

        revalidateTargets(now);
      }

    /*****************************************
     *
     * responseCode
     *
     *****************************************/

    if (jsonRoot.containsKey("id"))
      {
        response.put("responseCode", singleIDresponseCode);
        return JSONUtilities.encodeObject(response);
      }

    else
      {
        response.put("responseCode", "ok");
      }
    /*****************************************
     *
     * response
     *
     *****************************************/
    response.put("removedFileIDS", JSONUtilities.encodeArray(validIDs));

    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getEffectiveSystemTime
  *
  *****************************************/

  JSONObject processGetEffectiveSystemTime(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("effectiveSystemTime", SystemTime.getCurrentTime());
    return JSONUtilities.encodeObject(response);
  }
  
  /*********************************************
  *
  *  processGetTenantList
  *
  *********************************************/

  JSONObject processGetTenantList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert Tenants
    *
    *****************************************/
    Date now = SystemTime.getCurrentTime();
    List<JSONObject> tenantList = new ArrayList<JSONObject>();

    // TODO move this to be a regular GUIManagedObject
    {
      JSONObject result = new JSONObject();
      result.put("id", "0");
      result.put("name", "global");
      result.put("description", "Global");
      result.put("display", "Global");
      result.put("isDefault", false);
      result.put("language", "1");
      result.put("active", true);
      result.put("readOnly", true);
      tenantList.add(result);
    }
    {
      JSONObject result = new JSONObject();
      result.put("id", "1");
      result.put("name", "default");
      result.put("description", "Default");
      result.put("display", "Default");
      result.put("isDefault", true);
      result.put("language", "1");
      result.put("active", true);
      result.put("readOnly", true);
      tenantList.add(result);
    }
    
    /*****************************************
    *
    *  response
    *
    *****************************************/
    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("tenants", JSONUtilities.encodeArray(tenantList));
    return JSONUtilities.encodeObject(response);
  }
  
  /****************************************
  *
  *  processGetDependencies
  *
  ****************************************/
  
  public JSONObject processGetDependencies(String userID, JSONObject jsonRoot)
  {
    Map<String, Object> response = new LinkedHashMap<String, Object>();
    
    //
    // request data
    //
    
    String objetType = JSONUtilities.decodeString(jsonRoot, "objectType", true).toLowerCase();
    String objectID = JSONUtilities.decodeString(jsonRoot, "id", true);
    boolean fiddleTest = JSONUtilities.decodeBoolean(jsonRoot, "fiddleTest", Boolean.FALSE);
    
    //
    //  guiDependencyModelTree
    //
    
    GUIDependencyModelTree guiDependencyModelTree =  guiDependencyModelTreeMap.get(objetType);
    if (guiDependencyModelTree != null )
      {
        try
          {
            List<JSONObject> dependentList = new LinkedList<JSONObject>();
            
            //
            //  createDependencyTreeMAP
            //
            
            GUIManagedObjectDependencyHelper.createDependencyTreeMAP(guiDependencyModelTreeMap, guiDependencyModelTree, guiDependencyModelTree.getDependencyList(), objectID, dependentList, fiddleTest, guiServiceList);
            
            //
            //  response
            //
            
            if (fiddleTest)
              {
                response.put("children", JSONUtilities.encodeArray(dependentList));
              }
            else
              {
                response.put("dependencies", JSONUtilities.encodeArray(dependentList));
              }
            response.put("responseCode", "ok");
          } 
        catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e)
          {
            response.put("responseCode", "systemError");
            response.put("responseMessage", e.getMessage());
            
            StringWriter stackTraceWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stackTraceWriter, true));
            log.error(stackTraceWriter.toString());
          }
      }
    else
      {
        response.put("responseCode", "objetTypeNotValid");
        response.put("responseMessage", "objectType " + objetType + " not found in configuration");
      }
    
    //
    //  return
    //
    
    return JSONUtilities.encodeObject(response);
  }
  
  /****************************************
  *
  *  buildGUIDependencyModelTreeMap
  *
  ****************************************/
  
  private void buildGUIDependencyModelTreeMap()
  {
    //
    //  scan com.evolving.nglm.evolution
    //
    
    Reflections reflections = new Reflections("com.evolving.nglm.evolution");
    
    //
    //  get the annoted classes
    //
    
    Set<Class<?>> guiDependencyModelClassList = reflections.getTypesAnnotatedWith(GUIDependencyDef.class);
    for (Class guiDependencyModelClass : guiDependencyModelClassList)
      {
        GUIDependencyModelTree guiDependencyModelTree = new GUIDependencyModelTree(guiDependencyModelClass, guiDependencyModelClassList);
        guiDependencyModelTreeMap.put(guiDependencyModelTree.getGuiManagedObjectType(), guiDependencyModelTree);
      }
  }
}

