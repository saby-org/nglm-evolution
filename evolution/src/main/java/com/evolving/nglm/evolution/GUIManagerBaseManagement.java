/*****************************************************************************
*
*  GUIManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.zookeeper.ZooKeeper;
import org.elasticsearch.client.RestHighLevelClient;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SubscriberIDService;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.DeliverableSourceService;
import com.evolving.nglm.evolution.GUIManager.GUIManagerContext;
import com.evolving.nglm.evolution.GUIManager.RenamedProfileCriterionField;
import com.evolving.nglm.evolution.GUIManager.SharedIDService;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;
import com.evolving.nglm.evolution.SegmentationDimension.SegmentationDimensionTargetingType;

public class GUIManagerBaseManagement extends GUIManager
{

  private static final Logger log = LoggerFactory.getLogger(GUIManagerBaseManagement.class);
  
  public GUIManagerBaseManagement(JourneyService journeyService, SegmentationDimensionService segmentationDimensionService, PointService pointService, OfferService offerService, ReportService reportService, PaymentMeanService paymentMeanService, ScoringStrategyService scoringStrategyService, PresentationStrategyService presentationStrategyService, CallingChannelService callingChannelService, SalesChannelService salesChannelService, SourceAddressService sourceAddressService, SupplierService supplierService, ProductService productService, CatalogCharacteristicService catalogCharacteristicService, ContactPolicyService contactPolicyService, JourneyObjectiveService journeyObjectiveService, OfferObjectiveService offerObjectiveService, ProductTypeService productTypeService, UCGRuleService ucgRuleService, DeliverableService deliverableService, TokenTypeService tokenTypeService, VoucherTypeService voucherTypeService, VoucherService voucherService, SubscriberMessageTemplateService subscriberTemplateService, SubscriberProfileService subscriberProfileService, SubscriberIDService subscriberIDService, DeliverableSourceService deliverableSourceService, UploadedFileService uploadedFileService, TargetService targetService, CommunicationChannelService communicationChannelService, CommunicationChannelBlackoutService communicationChannelBlackoutService, LoyaltyProgramService loyaltyProgramService, ResellerService resellerService, ExclusionInclusionTargetService exclusionInclusionTargetService, SegmentContactPolicyService segmentContactPolicyService, CriterionFieldAvailableValuesService criterionFieldAvailableValuesService, DNBOMatrixService dnboMatrixService, DynamicCriterionFieldService dynamicCriterionFieldService, DynamicEventDeclarationsService dynamicEventDeclarationsService, JourneyTemplateService journeyTemplateService, KafkaResponseListenerService<StringKey,PurchaseFulfillmentRequest> purchaseResponseListenerService, SharedIDService subscriberGroupSharedIDService, ZookeeperUniqueKeyServer zuks, int httpTimeout, KafkaProducer<byte[], byte[]> kafkaProducer, RestHighLevelClient elasticsearch, SubscriberMessageTemplateService subscriberMessageTemplateService, String getCustomerAlternateID, GUIManagerContext guiManagerContext, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, ReferenceDataReader<String,JourneyTrafficHistory> journeyTrafficReader, ReferenceDataReader<String,RenamedProfileCriterionField> renamedProfileCriterionFieldReader, ReferenceDataReader<PropensityKey, PropensityState> propensityDataReader)
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
  *  processConfigAdaptorSegmentationDimension
  *
  *****************************************/

  JSONObject processConfigAdaptorSegmentationDimension(JSONObject jsonRoot)
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

    String segmentationDimensionID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate segmentationDimension
    *
    *****************************************/

    GUIManagedObject segmentationDimension = segmentationDimensionService.getStoredSegmentationDimension(segmentationDimensionID);
    JSONObject segmentationDimensionJSON = segmentationDimensionService.generateResponseJSON(segmentationDimension, true, SystemTime.getCurrentTime());

    //
    //  remove gui specific fields
    //
    
    segmentationDimensionJSON.remove("readOnly");
    segmentationDimensionJSON.remove("accepted");
    segmentationDimensionJSON.remove("valid");
    segmentationDimensionJSON.remove("active");
    
    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (segmentationDimension != null) ? "ok" : "segmentationDimensionNotFound");
    if (segmentationDimension != null) response.put("segmentationDimension", segmentationDimensionJSON);
    return JSONUtilities.encodeObject(response);
  }


  /*****************************************
  *
  *  processGetExclusionInclusionTarget
  *
  *****************************************/

  JSONObject processGetExclusionInclusionTarget(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String exclusionInclusionTargetID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate exclusionInclusionTarget
    *
    *****************************************/

    GUIManagedObject exclusionInclusionTarget = exclusionInclusionTargetService.getStoredExclusionInclusionTarget(exclusionInclusionTargetID, includeArchived);
    JSONObject exclusionInclusionTargetJSON = exclusionInclusionTargetService.generateResponseJSON(exclusionInclusionTarget, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (exclusionInclusionTarget != null) ? "ok" : "exclusionInclusionTargetNotFound");
    if (exclusionInclusionTarget != null) response.put("exclusionInclusionTarget", exclusionInclusionTargetJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutExclusionInclusionTarget
  *
  *****************************************/

  JSONObject processPutExclusionInclusionTarget(String userID, JSONObject jsonRoot)
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
    *  exclusionInclusionTargetID
    *
    *****************************************/

    String exclusionInclusionTargetID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (exclusionInclusionTargetID == null)
      {
        exclusionInclusionTargetID = subscriberGroupSharedIDService.generateID();
        jsonRoot.put("id", exclusionInclusionTargetID);
      }

    /*****************************************
    *
    *  existing exclusionInclusionTarget
    *
    *****************************************/

    GUIManagedObject existingExclusionInclusionTarget = exclusionInclusionTargetService.getStoredExclusionInclusionTarget(exclusionInclusionTargetID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingExclusionInclusionTarget != null && existingExclusionInclusionTarget.getReadOnly())
      {
        response.put("id", existingExclusionInclusionTarget.getGUIManagedObjectID());
        response.put("accepted", existingExclusionInclusionTarget.getAccepted());
        response.put("valid", existingExclusionInclusionTarget.getAccepted());
        response.put("processing", exclusionInclusionTargetService.isActiveExclusionInclusionTarget(existingExclusionInclusionTarget, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process exclusionInclusionTarget
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate exclusionInclusionTarget
        *
        ****************************************/

        ExclusionInclusionTarget exclusionInclusionTarget = new ExclusionInclusionTarget(jsonRoot, epoch, existingExclusionInclusionTarget);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        exclusionInclusionTargetService.putExclusionInclusionTarget(exclusionInclusionTarget, uploadedFileService, subscriberIDService, (existingExclusionInclusionTarget == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", exclusionInclusionTarget.getExclusionInclusionTargetID());
        response.put("accepted", exclusionInclusionTarget.getAccepted());
        response.put("valid", exclusionInclusionTarget.getAccepted());
        response.put("processing", exclusionInclusionTargetService.isActiveExclusionInclusionTarget(exclusionInclusionTarget, now));
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

        exclusionInclusionTargetService.putExclusionInclusionTarget(incompleteObject, uploadedFileService, subscriberIDService, (existingExclusionInclusionTarget == null), userID);

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
        response.put("responseCode", "exclusionInclusionTargetNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }
  
  /*****************************************
  *
  *  processGetExclusionInclusionTargetList
  *
  *****************************************/

  JSONObject processGetExclusionInclusionTargetList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert exclusionInclusionTargets
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> exclusionInclusionTargets = new ArrayList<JSONObject>();
    for (GUIManagedObject exclusionInclusionTarget : exclusionInclusionTargetService.getStoredExclusionInclusionTargets(includeArchived))
      {
        exclusionInclusionTargets.add(exclusionInclusionTargetService.generateResponseJSON(exclusionInclusionTarget, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("exclusionInclusionTargets", JSONUtilities.encodeArray(exclusionInclusionTargets));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetSegmentationDimension
  *
  *****************************************/

  JSONObject processGetSegmentationDimension(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String segmentationDimensionID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate segmentationDimension
    *
    *****************************************/

    GUIManagedObject segmentationDimension = segmentationDimensionService.getStoredSegmentationDimension(segmentationDimensionID, includeArchived);
    JSONObject segmentationDimensionJSON = segmentationDimensionService.generateResponseJSON(segmentationDimension, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (segmentationDimension != null) ? "ok" : "segmentationDimensionNotFound");
    if (segmentationDimension != null) response.put("segmentationDimension", segmentationDimensionJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutSegmentationDimension
  *
  *****************************************/

  JSONObject processPutSegmentationDimension(String userID, JSONObject jsonRoot)
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
    *  segmentationDimensionID
    *
    *****************************************/

    String segmentationDimensionID = JSONUtilities.decodeString(jsonRoot, "id", false);
    boolean resetSegmentIDs = false;
    if (segmentationDimensionID == null)
      {
        segmentationDimensionID = subscriberGroupSharedIDService.generateID();
        jsonRoot.put("id", segmentationDimensionID);
        resetSegmentIDs = true;
      }

    /*****************************************
    *
    *  existing segmentationDimension
    *
    *****************************************/

    GUIManagedObject existingSegmentationDimension = segmentationDimensionService.getStoredSegmentationDimension(segmentationDimensionID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingSegmentationDimension != null && existingSegmentationDimension.getReadOnly())
      {
        response.put("id", existingSegmentationDimension.getGUIManagedObjectID());
        response.put("accepted", existingSegmentationDimension.getAccepted());
        response.put("valid", existingSegmentationDimension.getAccepted());
        response.put("processing", segmentationDimensionService.isActiveSegmentationDimension(existingSegmentationDimension, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process segmentationDimension
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate segmentationDimension
        *
        ****************************************/

        SegmentationDimension segmentationDimension = null;
        switch (SegmentationDimensionTargetingType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "targetingType", true)))
          {
            case ELIGIBILITY:
              segmentationDimension = new SegmentationDimensionEligibility(segmentationDimensionService, jsonRoot, epoch, existingSegmentationDimension, resetSegmentIDs);
              break;

            case RANGES:
              segmentationDimension = new SegmentationDimensionRanges(segmentationDimensionService, jsonRoot, epoch, existingSegmentationDimension, resetSegmentIDs);
              break;

            case FILE:
              segmentationDimension = new SegmentationDimensionFileImport(segmentationDimensionService, jsonRoot, epoch, existingSegmentationDimension, resetSegmentIDs);
              break;

            case Unknown:
              throw new GUIManagerException("unsupported dimension type", JSONUtilities.decodeString(jsonRoot, "targetingType", false));
          }

        /*****************************************
        *
        *  initialize/update subscriber group
        *
        *****************************************/

        //
        //  open zookeeper and lock dimension
        //

        ZooKeeper zookeeper = SubscriberGroupEpochService.openZooKeeperAndLockGroup(segmentationDimension.getSegmentationDimensionID());

        //
        //  create or ensure subscriberGroupEpoch exists
        //

        SubscriberGroupEpoch existingSubscriberGroupEpoch = SubscriberGroupEpochService.retrieveSubscriberGroupEpoch(zookeeper, segmentationDimension.getSegmentationDimensionID());

        //
        //  submit new subscriberGroupEpoch
        //

        SubscriberGroupEpoch subscriberGroupEpoch = SubscriberGroupEpochService.updateSubscriberGroupEpoch(zookeeper, segmentationDimension.getSegmentationDimensionID(), existingSubscriberGroupEpoch, kafkaProducer, Deployment.getSubscriberGroupEpochTopic());

        //
        //  update segmentationDimension
        //

        segmentationDimension.setSubscriberGroupEpoch(subscriberGroupEpoch);

        //
        //  close zookeeper and release dimension
        //

        SubscriberGroupEpochService.closeZooKeeperAndReleaseGroup(zookeeper, segmentationDimension.getSegmentationDimensionID());

        /*****************************************
        *
        *  store
        *
        *****************************************/

        segmentationDimensionService.putSegmentationDimension(segmentationDimension, uploadedFileService, subscriberIDService, (existingSegmentationDimension == null), userID);

        /*****************************************
        *
        *  revalidate
        *
        *****************************************/

        revalidateUCGRules(now);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", segmentationDimension.getSegmentationDimensionID());
        response.put("accepted", segmentationDimension.getAccepted());
        response.put("valid", segmentationDimension.getAccepted());
        response.put("processing", segmentationDimensionService.isActiveSegmentationDimension(segmentationDimension, now));
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

        segmentationDimensionService.putIncompleteSegmentationDimension(incompleteObject, (existingSegmentationDimension == null), userID);

        //
        //  revalidate
        //

        revalidateUCGRules(now);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("segmentationDimensionID", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "segmentationDimensionNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveSegmentationDimension
  *
  *****************************************/

  JSONObject processRemoveSegmentationDimension(String userID, JSONObject jsonRoot)
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

    String segmentationDimensionID = JSONUtilities.decodeString(jsonRoot, "id", true);
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject segmentationDimensionUnchecked = segmentationDimensionService.getStoredSegmentationDimension(segmentationDimensionID);
    if (segmentationDimensionUnchecked != null && (force || !segmentationDimensionUnchecked.getReadOnly()))
      {
        /*****************************************
        *
        *  initialize/update subscriber group
        *
        *****************************************/

        if (segmentationDimensionUnchecked.getAccepted())
          {
            //
            //  segmentationDimension
            //

            SegmentationDimension segmentationDimension = (SegmentationDimension) segmentationDimensionUnchecked;

            //
            //  open zookeeper and lock dimension
            //

            ZooKeeper zookeeper = SubscriberGroupEpochService.openZooKeeperAndLockGroup(segmentationDimension.getSegmentationDimensionID());

            //
            //  create or ensure subscriberGroupEpoch exists
            //

            SubscriberGroupEpoch existingSubscriberGroupEpoch = SubscriberGroupEpochService.retrieveSubscriberGroupEpoch(zookeeper, segmentationDimension.getSegmentationDimensionID());

            //
            //  submit new subscriberGroupEpoch
            //

            SubscriberGroupEpoch subscriberGroupEpoch = SubscriberGroupEpochService.updateSubscriberGroupEpoch(zookeeper, segmentationDimension.getSegmentationDimensionID(), existingSubscriberGroupEpoch, kafkaProducer, Deployment.getSubscriberGroupEpochTopic());

            //
            //  update segmentationDimension
            //

            segmentationDimension.setSubscriberGroupEpoch(subscriberGroupEpoch);

            //
            //  close zookeeper and release dimension
            //

            SubscriberGroupEpochService.closeZooKeeperAndReleaseGroup(zookeeper, segmentationDimension.getSegmentationDimensionID());

          }

        /*****************************************
        *
        *  remove
        *
        *****************************************/

        segmentationDimensionService.removeSegmentationDimension(segmentationDimensionID, userID);
      }

    /*****************************************
    *
    *  revalidate
    *
    *****************************************/

    revalidateUCGRules(now);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (segmentationDimensionUnchecked != null && (force || !segmentationDimensionUnchecked.getReadOnly()))
      responseCode = "ok";
    else if (segmentationDimensionUnchecked != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "segmentationDimensionNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  revalidateUCGRules
  *
  *****************************************/

  private void revalidateUCGRules(Date date)
  {
    /****************************************
    *
    *  identify
    *
    ****************************************/

    Set<GUIManagedObject> modifiedUCGRules = new HashSet<GUIManagedObject>();
    for (GUIManagedObject existingUCGRule : ucgRuleService.getStoredUCGRules())
      {
        //
        //  modifiedUCGRule
        //

        long epoch = epochServer.getKey();
        GUIManagedObject modifiedUCGRule;
        try
          {
            UCGRule ucgRule = new UCGRule(existingUCGRule.getJSONRepresentation(), epoch, existingUCGRule);
            ucgRule.validate(ucgRuleService, segmentationDimensionService, date);
            modifiedUCGRule = ucgRule;
          }
        catch (JSONUtilitiesException|GUIManagerException e)
          {
            modifiedUCGRule = new IncompleteObject(existingUCGRule.getJSONRepresentation(), epoch);
          }

        //
        //  changed?
        //

        if (existingUCGRule.getAccepted() != modifiedUCGRule.getAccepted())
          {
            modifiedUCGRules.add(modifiedUCGRule);
          }
      }

    /****************************************
    *
    *  update
    *
    ****************************************/

    for (GUIManagedObject modifiedUCGRule : modifiedUCGRules)
      {
        ucgRuleService.putGUIManagedObject(modifiedUCGRule, date, false, null);
      }
  }


  /*****************************************
  *
  *  processGetTarget
  *
  *****************************************/

  JSONObject processGetTarget(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String targetID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate target
    *
    *****************************************/

    GUIManagedObject target = targetService.getStoredTarget(targetID, includeArchived);
    JSONObject targetJSON = targetService.generateResponseJSON(target, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (target != null) ? "ok" : "targetNotFound");
    if (target != null) response.put("target", targetJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutTarget
  *
  *****************************************/

  JSONObject processPutTarget(String userID, JSONObject jsonRoot)
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
    *  targetID
    *
    *****************************************/

    String targetID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (targetID == null)
      {
        targetID = subscriberGroupSharedIDService.generateID();
        jsonRoot.put("id", targetID);
      }

    /*****************************************
    *
    *  existing target
    *
    *****************************************/

    GUIManagedObject existingTarget = targetService.getStoredTarget(targetID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingTarget != null && existingTarget.getReadOnly())
      {
        response.put("id", existingTarget.getGUIManagedObjectID());
        response.put("accepted", existingTarget.getAccepted());
        response.put("valid", existingTarget.getAccepted());
        response.put("processing", targetService.isActiveTarget(existingTarget, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process target
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate Target
        *
        ****************************************/

        Target target = new Target(jsonRoot, epoch, existingTarget);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        targetService.putTarget(target, uploadedFileService, subscriberIDService, (existingTarget == null), userID);

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

        response.put("id", target.getGUIManagedObjectID());
        response.put("accepted", target.getAccepted());
        response.put("valid", target.getAccepted());
        response.put("processing", targetService.isActiveTarget(target, now));
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

        targetService.putTarget(incompleteObject, uploadedFileService, subscriberIDService, (existingTarget == null), userID);

        //
        //  revalidate dependent objects
        //

        revalidateJourneys(now);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("targetID", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "targetNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }
  /*****************************************
  *
  *  processRemoveTarget
  *
  *****************************************/

  JSONObject processRemoveTarget(String userID, JSONObject jsonRoot){

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

    String targetID = JSONUtilities.decodeString(jsonRoot, "id", true);
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject existingTarget = targetService.getStoredTarget(targetID);
    if (existingTarget != null && (force || !existingTarget.getReadOnly()) ){
      targetService.removeTarget(targetID, userID);
    }

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (existingTarget != null && (force || !existingTarget.getReadOnly())) {
      responseCode = "ok";
    }
    else if (existingTarget != null) {
      responseCode = "failedReadOnly";
    }
    else {
      responseCode = "targetNotFound";
    }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetUCGDimensionList
  *
  *****************************************/

  JSONObject processGetUCGDimensionList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert segmentationDimensions
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> segmentationDimensions = new ArrayList<JSONObject>();
    for (GUIManagedObject segmentationDimension : segmentationDimensionService.getStoredSegmentationDimensions(includeArchived))
      {
        SegmentationDimension dimension = (SegmentationDimension) segmentationDimension;
        if (dimension.getDefaultSegmentID() != null)
          {
            segmentationDimensions.add(segmentationDimensionService.generateResponseJSON(segmentationDimension, fullDetails, now));
          }
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("segmentationDimensions", JSONUtilities.encodeArray(segmentationDimensions));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetUCGRule
  *
  *****************************************/

  JSONObject processGetUCGRule(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String ucgRuleID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate ucg rule
    *
    *****************************************/

    GUIManagedObject ucgRule = ucgRuleService.getStoredUCGRule(ucgRuleID, includeArchived);
    JSONObject productJSON = ucgRuleService.generateResponseJSON(ucgRule, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (ucgRule != null) ? "ok" : "ucgRuleNotFound");
    if (ucgRule != null) response.put("ucgRule", productJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutUCGRule
  *
  *****************************************/

  JSONObject processPutUCGRule(String userID, JSONObject jsonRoot)
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
    *  productID
    *
    *****************************************/

    String ucgRuleID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (ucgRuleID == null)
      {
        ucgRuleID = ucgRuleService.generateUCGRuleID();
        jsonRoot.put("id", ucgRuleID);
      }

    /*****************************************
    *
    *  existing product
    *
    *****************************************/

    GUIManagedObject existingUCGRule = ucgRuleService.getStoredUCGRule(ucgRuleID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingUCGRule != null && existingUCGRule.getReadOnly())
      {
        response.put("id", existingUCGRule.getGUIManagedObjectID());
        response.put("accepted", existingUCGRule.getAccepted());
        response.put("valid", existingUCGRule.getAccepted());
        response.put("processing", ucgRuleService.isActiveUCGRule(existingUCGRule, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process product
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate product
        *
        ****************************************/

        UCGRule ucgRule = new UCGRule(jsonRoot, epoch, existingUCGRule);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        ucgRuleService.putUCGRule(ucgRule,segmentationDimensionService,(existingUCGRule == null), userID);

        /*****************************************
        *
        *  deactivate any active rules except current one active
        *  this replace one UCGRule active sequence from UCGRule.validate()
        *
        *****************************************/

        if(ucgRule.getActive()) {
          deactivateOtherUCGRules(ucgRule, now);
        }

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", ucgRule.getUCGRuleID());
        response.put("accepted", ucgRule.getAccepted());
        response.put("valid", ucgRule.getAccepted());
        response.put("processing", ucgRuleService.isActiveUCGRule(ucgRule, now));
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

        ucgRuleService.putUCGRule(incompleteObject, segmentationDimensionService, (existingUCGRule == null), userID);

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
        response.put("responseCode", "ucgRuleNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveUCGRule
  *
  *****************************************/

  JSONObject processRemoveUCGRule(String userID, JSONObject jsonRoot)
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

    String ucgRuleID = JSONUtilities.decodeString(jsonRoot, "id", true);
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject ucgRule = ucgRuleService.getStoredUCGRule(ucgRuleID);
    if (ucgRule != null && (force || !ucgRule.getReadOnly())) ucgRuleService.removeUCGRule(ucgRuleID, userID);


    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (ucgRule != null && (force || !ucgRule.getReadOnly()))
      responseCode = "ok";
    else if (ucgRule != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "ucgRuleNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }  
  
  /*****************************************
  *
  * refreshUCG
  *
  *****************************************/

  JSONObject processRefreshUCG(String userID, JSONObject jsonRoot) throws GUIManagerException
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
    *  identify active UCGRule (if any)
    *
    *****************************************/

    UCGRule activeUCGRule = null;
    for (UCGRule ucgRule : ucgRuleService.getActiveUCGRules(now))
      {
        activeUCGRule = ucgRule;
      }

    /*****************************************
    *
    *  refresh
    *
    *****************************************/

    if (activeUCGRule != null)
      {
        activeUCGRule.setRefreshEpoch(activeUCGRule.getRefreshEpoch() != null ? activeUCGRule.getRefreshEpoch() + 1 : 1);
        ucgRuleService.putUCGRule(activeUCGRule,segmentationDimensionService,false, userID);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (activeUCGRule != null) ? "ok" : "noActiveUCGRule");
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  deactivateOtherUCGRules
  *
  *****************************************/

  private void deactivateOtherUCGRules(UCGRule currentUCGRule,Date date)
  {
    /****************************************
    *
    *  identify
    *
    ****************************************/

    Set<GUIManagedObject> modifiedUCGRules = new HashSet<GUIManagedObject>();
    for (GUIManagedObject existingUCGRule : ucgRuleService.getStoredUCGRules())
      {
        //
        //  modifiedUCGRule
        //
        if(currentUCGRule.getGUIManagedObjectID() != existingUCGRule.getGUIManagedObjectID() && existingUCGRule.getActive() == true)
          {
            long epoch = epochServer.getKey();
            GUIManagedObject modifiedUCGRule;
            try
              {
                JSONObject existingRuleJSON = existingUCGRule.getJSONRepresentation();
                existingRuleJSON.replace("active", Boolean.FALSE);
                UCGRule ucgRule = new UCGRule(existingRuleJSON, epoch, existingUCGRule);
                //ucgRule.validate(ucgRuleService, segmentationDimensionService, date);
                modifiedUCGRule = ucgRule;
              }
            catch (JSONUtilitiesException | GUIManagerException e)
              {
                modifiedUCGRule = new IncompleteObject(existingUCGRule.getJSONRepresentation(), epoch);
              }

            //
            //  changed?
            //

            if (existingUCGRule.getAccepted() != modifiedUCGRule.getAccepted())
              {
                modifiedUCGRules.add(modifiedUCGRule);
              }
          }
      }

    /****************************************
    *
    *  update
    *
    ****************************************/

    for (GUIManagedObject modifiedUCGRule : modifiedUCGRules)
      {
        ucgRuleService.putGUIManagedObject(modifiedUCGRule, date, false, null);
      }
  }

  /*****************************************
  *
  *  processGetUCGRuleList
  *
  *****************************************/

  JSONObject processGetUCGRuleList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert ucg rules
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> ucgRules = new ArrayList<JSONObject>();
    for (GUIManagedObject ucgRule : ucgRuleService.getStoredUCGRules(includeArchived))
      {
        ucgRules.add(ucgRuleService.generateResponseJSON(ucgRule, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("ucgRules", JSONUtilities.encodeArray(ucgRules));
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processGetSegmentationDimensionList
  *
  *****************************************/

  JSONObject processGetSegmentationDimensionList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert segmentationDimensions
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> segmentationDimensions = new ArrayList<JSONObject>();
    for (GUIManagedObject segmentationDimension : segmentationDimensionService.getStoredSegmentationDimensions(includeArchived))
      {

        //        SegmentationDimensionTargetingType targetingType = ((SegmentationDimension)segmentationDimension).getTargetingType();
        //        
        //        switch(targetingType) {
        //          case ELIGIBILITY:
        //
        //            break;
        //          case FILE_IMPORT:
        //            
        //            break;
        //          case RANGES:
        //            if(((SegmentationDimensionRanges)segmentationDimension).get != null) {
        //              ContactPolicy contactPolicy = contactPolicyService.getActiveContactPolicy(((SegmentationDimensionRanges)segmentationDimension).getContactPolicyID(), now);
        //              segmentationDimensions.add(contactPolicy.getJSONRepresentation());
        //            }
        //            break;
        //        }

        segmentationDimensions.add(segmentationDimensionService.generateResponseJSON(segmentationDimension, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("segmentationDimensions", JSONUtilities.encodeArray(segmentationDimensions));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetTargetList
  *
  *****************************************/

  JSONObject processGetTargetList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {

    /*****************************************
    *
    *  retrieve target list
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> targetLists = new ArrayList<JSONObject>();
    for (GUIManagedObject targetList : targetService.getStoredTargets(includeArchived))
      {
        JSONObject targetResponse = targetService.generateResponseJSON(targetList, fullDetails, now);
        targetResponse.put("isRunning", targetService.isActiveTarget(targetList, now) ? targetService.isTargetFileBeingProcessed((Target) targetList) : false);
        targetLists.add(targetResponse);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("targets", JSONUtilities.encodeArray(targetLists));
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processRemoveExclusionInclusionTarget
  *
  *****************************************/

  JSONObject processRemoveExclusionInclusionTarget(String userID, JSONObject jsonRoot)
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

    String exclusionInclusionTargetID = JSONUtilities.decodeString(jsonRoot, "id", true);
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject exclusionInclusionTarget = exclusionInclusionTargetService.getStoredExclusionInclusionTarget(exclusionInclusionTargetID);
    if (exclusionInclusionTarget != null && (force || !exclusionInclusionTarget.getReadOnly())) exclusionInclusionTargetService.removeExclusionInclusionTarget(exclusionInclusionTargetID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (exclusionInclusionTarget != null && (force || !exclusionInclusionTarget.getReadOnly()))
      responseCode = "ok";
    else if (exclusionInclusionTarget != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "exclusionInclusionTargetNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }


}

