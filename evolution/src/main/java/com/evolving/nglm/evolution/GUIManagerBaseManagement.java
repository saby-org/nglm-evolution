/*****************************************************************************
*
*  GUIManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.zookeeper.ZooKeeper;
import org.elasticsearch.client.RestHighLevelClient;
import org.json.simple.JSONArray;
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
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;
import com.evolving.nglm.evolution.SegmentationDimension.SegmentationDimensionTargetingType;

public class GUIManagerBaseManagement extends GUIManager
{

  private static final Logger log = LoggerFactory.getLogger(GUIManagerBaseManagement.class);
  
  public GUIManagerBaseManagement(JourneyService journeyService, SegmentationDimensionService segmentationDimensionService, PointService pointService, OfferService offerService, ReportService reportService, PaymentMeanService paymentMeanService, ScoringStrategyService scoringStrategyService, PresentationStrategyService presentationStrategyService, CallingChannelService callingChannelService, SalesChannelService salesChannelService, SourceAddressService sourceAddressService, SupplierService supplierService, ProductService productService, CatalogCharacteristicService catalogCharacteristicService, ContactPolicyService contactPolicyService, JourneyObjectiveService journeyObjectiveService, OfferObjectiveService offerObjectiveService, ProductTypeService productTypeService, UCGRuleService ucgRuleService, DeliverableService deliverableService, TokenTypeService tokenTypeService, VoucherTypeService voucherTypeService, VoucherService voucherService, SubscriberMessageTemplateService subscriberTemplateService, SubscriberProfileService subscriberProfileService, SubscriberIDService subscriberIDService, DeliverableSourceService deliverableSourceService, UploadedFileService uploadedFileService, TargetService targetService, CommunicationChannelBlackoutService communicationChannelBlackoutService, LoyaltyProgramService loyaltyProgramService, ResellerService resellerService, ExclusionInclusionTargetService exclusionInclusionTargetService, SegmentContactPolicyService segmentContactPolicyService, CriterionFieldAvailableValuesService criterionFieldAvailableValuesService, DNBOMatrixService dnboMatrixService, DynamicCriterionFieldService dynamicCriterionFieldService, DynamicEventDeclarationsService dynamicEventDeclarationsService, JourneyTemplateService journeyTemplateService, KafkaResponseListenerService<StringKey,PurchaseFulfillmentRequest> purchaseResponseListenerService, SharedIDService subscriberGroupSharedIDService, ZookeeperUniqueKeyServer zuks, int httpTimeout, KafkaProducer<byte[], byte[]> kafkaProducer, RestHighLevelClient elasticsearch, SubscriberMessageTemplateService subscriberMessageTemplateService, String getCustomerAlternateID, GUIManagerContext guiManagerContext, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, ReferenceDataReader<String,JourneyTrafficHistory> journeyTrafficReader, ReferenceDataReader<String,RenamedProfileCriterionField> renamedProfileCriterionFieldReader)
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
        if (!dryRun)
          {
            exclusionInclusionTargetService.putExclusionInclusionTarget(exclusionInclusionTarget, uploadedFileService,
                subscriberIDService, (existingExclusionInclusionTarget == null), userID);
          }
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
    Collection <GUIManagedObject> exclusionInclusionTargetObjects = new ArrayList<GUIManagedObject>();
    
    if (jsonRoot.containsKey("ids"))
      {
        JSONArray exclusionInclusionTargetIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
        for (int i = 0; i < exclusionInclusionTargetIDs.size(); i++)
          {
            String exclusionInclusionTargetID = exclusionInclusionTargetIDs.get(i).toString();
            GUIManagedObject exclusionInclusionTarget = exclusionInclusionTargetService.getStoredExclusionInclusionTarget(exclusionInclusionTargetID, includeArchived);
            if (exclusionInclusionTarget != null)
              {
                exclusionInclusionTargetObjects.add(exclusionInclusionTarget);
              }
          }
      }
    else
      {
        exclusionInclusionTargetObjects = exclusionInclusionTargetService.getStoredExclusionInclusionTargets(includeArchived);
      }
    for (GUIManagedObject exclusionInclusionTarget : exclusionInclusionTargetObjects)
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
   * processSetStatusExclusionInclusionTarget
   *
   *****************************************/

  JSONObject processSetStatusExclusionInclusionTarget(String userID, JSONObject jsonRoot)
  {
    /****************************************
     *
     * response
     *
     ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String, Object> response = new HashMap<String, Object>();
    JSONArray exclusionInclusionIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
    List<String> statusSetIDs = new ArrayList<>();
    Boolean status = JSONUtilities.decodeBoolean(jsonRoot, "active");
    long epoch = epochServer.getKey();

    for (int i = 0; i < exclusionInclusionIDs.size(); i++)
      {

        String exclusionInclusionID = exclusionInclusionIDs.get(i).toString();
        GUIManagedObject existingElement = exclusionInclusionTargetService
            .getStoredExclusionInclusionTarget(exclusionInclusionID);
        if (existingElement != null && !(existingElement.getReadOnly()))
          {
            statusSetIDs.add(exclusionInclusionID);
            JSONObject elementRoot = (JSONObject) existingElement.getJSONRepresentation().clone();
            elementRoot.put("active", status);
            try
              {
                /****************************************
                 *
                 * instantiate exclusionInclusionTarget
                 *
                 ****************************************/

                ExclusionInclusionTarget exclusionInclusionTarget = new ExclusionInclusionTarget(elementRoot, epoch,
                    existingElement);

                /*****************************************
                 *
                 * store
                 *
                 *****************************************/
                exclusionInclusionTargetService.putExclusionInclusionTarget(exclusionInclusionTarget,
                    uploadedFileService, subscriberIDService, (existingElement == null), userID);

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
                exclusionInclusionTargetService.putExclusionInclusionTarget(incompleteObject,
                    uploadedFileService, subscriberIDService, (existingElement == null), userID);
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
    Boolean dryRun = false;

    /*****************************************
     *
     * dryRun
     *
     *****************************************/
    if (jsonRoot.containsKey("dryRun"))
      {
        dryRun = JSONUtilities.decodeBoolean(jsonRoot, "dryRun", false);
      }

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
        if (!dryRun)
          {

            segmentationDimensionService.putSegmentationDimension(segmentationDimension, uploadedFileService, subscriberIDService, (existingSegmentationDimension == null), userID);

            /*****************************************
             *
             * revalidate
             *
             *****************************************/

            revalidateUCGRules(now);
          }

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
        if (!dryRun)
          {
            segmentationDimensionService.putIncompleteSegmentationDimension(incompleteObject,
                (existingSegmentationDimension == null), userID);

            //
            // revalidate
            //

            revalidateUCGRules(now);
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
    String responseCode = "";
    String singleIDresponseCode = "";
    List<GUIManagedObject> segmentationDimensions = new ArrayList<>();
    List<String> validIDs = new ArrayList<>();
    JSONArray segmentationDimensionIDs = new JSONArray();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);
    //
    //remove single 
    //
    if (jsonRoot.containsKey("id"))
      {
        String segmentationDimensionID = JSONUtilities.decodeString(jsonRoot, "id", false);
        segmentationDimensionIDs.add(segmentationDimensionID);
        GUIManagedObject segmentationDimension = segmentationDimensionService
            .getStoredSegmentationDimension(segmentationDimensionID);

        if (segmentationDimension != null && (force || !segmentationDimension.getReadOnly()))
          singleIDresponseCode = "ok";
        else if (segmentationDimension != null)
          singleIDresponseCode = "failedReadOnly";
        else singleIDresponseCode = "segmentationDimensionNotFound";

      }
    //
    // multiple deletion
    //
    
    if (jsonRoot.containsKey("ids"))
      {
        segmentationDimensionIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids", false);
      }
    
    //
    // If one of the segmentationDimensionID is wrong it should reject the request
    //

    for (int i = 0; i < segmentationDimensionIDs.size(); i++)
      {
        String segmentationDimensionID = segmentationDimensionIDs.get(i).toString();
        GUIManagedObject segmentationDimension = segmentationDimensionService.getStoredSegmentationDimension(segmentationDimensionID);
        
        if (segmentationDimension != null && (force || !segmentationDimension.getReadOnly()))
          {
            validIDs.add(segmentationDimensionID);
            segmentationDimensions.add(segmentationDimension);
          }
      }      
  

    /*****************************************
     *
     * remove
     *
     *****************************************/
    for (int i = 0; i < segmentationDimensions.size(); i++)
      {
        GUIManagedObject segmentationDimensionUnchecked = segmentationDimensions.get(i);
        /*****************************************
         *
         * initialize/update subscriber group
         *
         *****************************************/

        if (segmentationDimensionUnchecked.getAccepted())
          {
            //
            // segmentationDimension
            //

            SegmentationDimension segmentationDimension = (SegmentationDimension) segmentationDimensionUnchecked;

            //
            // open zookeeper and lock dimension
            //

            ZooKeeper zookeeper = SubscriberGroupEpochService.openZooKeeperAndLockGroup(segmentationDimension.getSegmentationDimensionID());

            //
            // create or ensure subscriberGroupEpoch exists
            //

            SubscriberGroupEpoch existingSubscriberGroupEpoch = SubscriberGroupEpochService.retrieveSubscriberGroupEpoch(zookeeper, segmentationDimension.getSegmentationDimensionID());

            //
            // submit new subscriberGroupEpoch
            //

            SubscriberGroupEpoch subscriberGroupEpoch = SubscriberGroupEpochService.updateSubscriberGroupEpoch( zookeeper, segmentationDimension.getSegmentationDimensionID(), existingSubscriberGroupEpoch,kafkaProducer, Deployment.getSubscriberGroupEpochTopic());

            //
            // update segmentationDimension
            //

            segmentationDimension.setSubscriberGroupEpoch(subscriberGroupEpoch);

            //
            // close zookeeper and release dimension
            //

            SubscriberGroupEpochService.closeZooKeeperAndReleaseGroup(zookeeper,segmentationDimension.getSegmentationDimensionID());

          }

        /*****************************************
         *
         * remove
         *
         *****************************************/

        segmentationDimensionService.removeSegmentationDimension(segmentationDimensionUnchecked.getGUIManagedObjectID(),
            userID);

        /*****************************************
         *
         * revalidate
         *
         *****************************************/

        revalidateUCGRules(now);

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
    response.put("removedJourneyIDS", JSONUtilities.encodeArray(validIDs));

    return JSONUtilities.encodeObject(response);
  }
  /*****************************************
  *
  *  processSetStatusSegmentationDimension
  *
  *****************************************/

  JSONObject processSetStatusSegmentationDimension(String userID, JSONObject jsonRoot)
  {
    /****************************************
     *
     * response
     *
     ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String, Object> response = new HashMap<String, Object>();
    JSONArray segmentationDimensionIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
    List<String> statusSetIDs = new ArrayList<>();
    boolean resetSegmentIDs = false;
    Boolean status = JSONUtilities.decodeBoolean(jsonRoot, "active");
    long epoch = epochServer.getKey();

    for (int i = 0; i < segmentationDimensionIDs.size(); i++)
      {

        String segmentationDimensionID = segmentationDimensionIDs.get(i).toString();
        GUIManagedObject existingElement = segmentationDimensionService.getStoredSegmentationDimension(segmentationDimensionID);
        if (existingElement != null && !(existingElement.getReadOnly()))
          {
            statusSetIDs.add(segmentationDimensionID);
            JSONObject elementRoot = (JSONObject) existingElement.getJSONRepresentation().clone();
            elementRoot.put("active", status);
            try
              {
                /****************************************
                 *
                 * instantiate segmentationDimension
                 *
                 ****************************************/

                SegmentationDimension segmentationDimension = null;
                switch (SegmentationDimensionTargetingType
                    .fromExternalRepresentation(JSONUtilities.decodeString(elementRoot, "targetingType", true)))
                  {
                    case ELIGIBILITY:
                      segmentationDimension = new SegmentationDimensionEligibility(segmentationDimensionService,
                          elementRoot, epoch, existingElement, resetSegmentIDs);
                      break;

                    case RANGES:
                      segmentationDimension = new SegmentationDimensionRanges(segmentationDimensionService, elementRoot,
                          epoch, existingElement, resetSegmentIDs);
                      break;

                    case FILE:
                      segmentationDimension = new SegmentationDimensionFileImport(segmentationDimensionService,
                          elementRoot, epoch, existingElement, resetSegmentIDs);
                      break;

                    case Unknown:
                      throw new GUIManagerException("unsupported dimension type",
                          JSONUtilities.decodeString(elementRoot, "targetingType", false));
                  }

                /*****************************************
                 *
                 * initialize/update subscriber group
                 *
                 *****************************************/

                //
                // open zookeeper and lock dimension
                //

                ZooKeeper zookeeper = SubscriberGroupEpochService
                    .openZooKeeperAndLockGroup(segmentationDimension.getSegmentationDimensionID());

                //
                // create or ensure subscriberGroupEpoch exists
                //

                SubscriberGroupEpoch existingSubscriberGroupEpoch = SubscriberGroupEpochService
                    .retrieveSubscriberGroupEpoch(zookeeper, segmentationDimension.getSegmentationDimensionID());

                //
                // submit new subscriberGroupEpoch
                //

                SubscriberGroupEpoch subscriberGroupEpoch = SubscriberGroupEpochService.updateSubscriberGroupEpoch(
                    zookeeper, segmentationDimension.getSegmentationDimensionID(), existingSubscriberGroupEpoch,
                    kafkaProducer, Deployment.getSubscriberGroupEpochTopic());

                //
                // update segmentationDimension
                //

                segmentationDimension.setSubscriberGroupEpoch(subscriberGroupEpoch);

                //
                // close zookeeper and release dimension
                //

                SubscriberGroupEpochService.closeZooKeeperAndReleaseGroup(zookeeper,
                    segmentationDimension.getSegmentationDimensionID());

                /*****************************************
                 *
                 * store
                 *
                 *****************************************/

                segmentationDimensionService.putSegmentationDimension(segmentationDimension, uploadedFileService,
                    subscriberIDService, (existingElement == null), userID);

                /*****************************************
                 *
                 * revalidate
                 *
                 *****************************************/

                revalidateUCGRules(now);

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

                segmentationDimensionService.putIncompleteSegmentationDimension(incompleteObject,
                    (existingElement == null), userID);

                //
                // revalidate
                //

                revalidateUCGRules(now);

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
        if (!dryRun)
          {

            targetService.putTarget(target, uploadedFileService, subscriberIDService, (existingTarget == null), userID);

            /*****************************************
             *
             * revalidate dependent objects
             *
             *****************************************/

            revalidateJourneys(now);
          }

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
        if (!dryRun)
          {
            targetService.putTarget(incompleteObject, uploadedFileService, subscriberIDService,
                (existingTarget == null), userID);

            //
            // revalidate dependent objects
            //

            revalidateJourneys(now);
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

    String responseCode = "";
    String singleIDresponseCode = "";
    List<GUIManagedObject> targets = new ArrayList<>();
    List<String> validIDs = new ArrayList<>();
    JSONArray targetIDs = new JSONArray();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);
    //
    //remove single target
    //
    if (jsonRoot.containsKey("id"))
      {
        String targetID = JSONUtilities.decodeString(jsonRoot, "id", false);
        targetIDs.add(targetID);
        GUIManagedObject target = targetService.getStoredTarget(targetID);
        if (target != null && (force || !target.getReadOnly()))
          singleIDresponseCode = "ok";
        else if (target != null)
          singleIDresponseCode = "failedReadOnly";
        else singleIDresponseCode = "targetNotFound";
      }
    //
    // multiple deletion
    //
    
    if (jsonRoot.containsKey("ids"))
      {
        targetIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids", false);
      }
   
    for (int i = 0; i < targetIDs.size(); i++)
      {
        String targetID = targetIDs.get(i).toString();
        GUIManagedObject target = targetService.getStoredTarget(targetID);
        if (target != null && (force || !target.getReadOnly()))
          {
            targets.add(target);
            validIDs.add(targetID);
          }
      }
        
  

    /*****************************************
    *
    *  remove
    *
    *****************************************/
    for (int i = 0; i < targets.size(); i++)
      {

        GUIManagedObject existingTarget = targets.get(0);
        targetService.removeTarget(existingTarget.getGUIManagedObjectID(), userID);
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

    response.put("removedtargetIDS", JSONUtilities.encodeArray(validIDs));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   * processSetStatusTarget
   *
   *****************************************/

  JSONObject processSetStatusTarget(String userID, JSONObject jsonRoot)
  {
    /****************************************
     *
     * response
     *
     ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String, Object> response = new HashMap<String, Object>();
    JSONArray targetIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
    List<String> statusSetIDs = new ArrayList<>();
    Boolean status = JSONUtilities.decodeBoolean(jsonRoot, "active");
    long epoch = epochServer.getKey();

    for (int i = 0; i < targetIDs.size(); i++)
      {

        String targetID = targetIDs.get(i).toString();
        GUIManagedObject existingElement = targetService.getStoredTarget(targetID);
        if (existingElement != null && !(existingElement.getReadOnly()))
          {
            statusSetIDs.add(targetID);
            JSONObject elementRoot = (JSONObject) existingElement.getJSONRepresentation().clone();
            elementRoot.put("active", status);
            try
              {
                /****************************************
                 *
                 * instantiate Target
                 *
                 ****************************************/

                Target target = new Target(elementRoot, epoch, existingElement);

                /*****************************************
                 *
                 * store
                 *
                 *****************************************/
                targetService.putTarget(target, uploadedFileService, subscriberIDService, (existingElement == null),
                    userID);

                /*****************************************
                 *
                 * revalidate dependent objects
                 *
                 *****************************************/

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

                targetService.putTarget(incompleteObject, uploadedFileService, subscriberIDService,
                    (existingElement == null), userID);

                //
                // revalidate dependent objects
                //

                revalidateJourneys(now);

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
    Collection <GUIManagedObject> segmentationDimensionObjects = new ArrayList<GUIManagedObject>();
    
    if (jsonRoot.containsKey("ids"))
      {
        JSONArray segmentationDimensionIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
        for (int i = 0; i < segmentationDimensionIDs.size(); i++)
          {
            String segmentationDimensionID = segmentationDimensionIDs.get(i).toString();
            GUIManagedObject segmentationDimension = segmentationDimensionService
                .getStoredSegmentationDimension(segmentationDimensionID, includeArchived);
            if (segmentationDimension != null)
              {
                segmentationDimensionObjects.add(segmentationDimension);
              }
          }
      }
    else
      {
  segmentationDimensionObjects = segmentationDimensionService.getStoredSegmentationDimensions(includeArchived);
      }
    for (GUIManagedObject segmentationDimension : segmentationDimensionObjects)
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
         * store
         *
         *****************************************/
        if (!dryRun)
          {
            ucgRuleService.putUCGRule(ucgRule, segmentationDimensionService, (existingUCGRule == null), userID);

            /*****************************************
             *
             * deactivate any active rules except current one active this
             * replace one UCGRule active sequence from UCGRule.validate()
             *
             *****************************************/

            if (ucgRule.getActive())
              {
                deactivateOtherUCGRules(ucgRule, now);
              }
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
        if (!dryRun)
          {
            ucgRuleService.putUCGRule(incompleteObject, segmentationDimensionService, (existingUCGRule == null),
                userID);
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

    String responseCode = "";
    String singleIDresponseCode = "";
    List<GUIManagedObject> UCGRules = new ArrayList<>();
    List<String> validIDs = new ArrayList<>();
    JSONArray UCGRuleIDs = new JSONArray();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);
    //
    //remove single UCGRule
    //
    if (jsonRoot.containsKey("id"))
      {
        String UCGRuleID = JSONUtilities.decodeString(jsonRoot, "id", false);
        UCGRuleIDs.add(UCGRuleID);
        GUIManagedObject ucgRule = ucgRuleService.getStoredUCGRule(UCGRuleID);
        if (ucgRule != null && (force || !ucgRule.getReadOnly()))
          singleIDresponseCode = "ok";
        else if (ucgRule != null)
          singleIDresponseCode = "failedReadOnly";
        else singleIDresponseCode = "ucgRuleNotFound";

      }
    //
    // multiple deletion
    //
    
    if (jsonRoot.containsKey("ids"))
      {
        UCGRuleIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids", false);
      }
   
    for (int i = 0; i < UCGRuleIDs.size(); i++)
      {
        String UCGRuleID = UCGRuleIDs.get(i).toString();
        GUIManagedObject UCGRule = ucgRuleService.getStoredUCGRule(UCGRuleID);

        if (UCGRule != null && (force || !UCGRule.getReadOnly()))
          {
            UCGRules.add(UCGRule);
          }
      }
        
  

    /*****************************************
    *
    *  remove
    *
    *****************************************/
    for (int i = 0; i < UCGRules.size(); i++)
      {

        GUIManagedObject ucgRule = UCGRules.get(i);

        ucgRuleService.removeUCGRule(ucgRule.getGUIManagedObjectID(), userID);
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
    response.put("removedUcgRuleIDS", JSONUtilities.encodeArray(validIDs));

    return JSONUtilities.encodeObject(response);
  }  
  
  /*****************************************
   *
   * processSetStatusUCGRule
   *
   *****************************************/

  JSONObject processSetStatusUCGRule(String userID, JSONObject jsonRoot)
  {
    /****************************************
     *
     * response
     *
     ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String, Object> response = new HashMap<String, Object>();
    JSONArray ucgRuleIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
    List<String> statusSetIDs = new ArrayList<>();
    Boolean status = JSONUtilities.decodeBoolean(jsonRoot, "active");
    long epoch = epochServer.getKey();

    for (int i = 0; i < ucgRuleIDs.size(); i++)
      {

        String ucgRuleID = ucgRuleIDs.get(i).toString();
        GUIManagedObject existingElement = ucgRuleService.getStoredUCGRule(ucgRuleID);
        if (existingElement != null && !(existingElement.getReadOnly()))
          {
            statusSetIDs.add(ucgRuleID);
            JSONObject elementRoot = (JSONObject) existingElement.getJSONRepresentation().clone();
            elementRoot.put("active", status);
            try
              {
                /****************************************
                 *
                 * instantiate product
                 *
                 ****************************************/

                UCGRule ucgRule = new UCGRule(elementRoot, epoch, existingElement);

                /*****************************************
                 *
                 * store
                 *
                 *****************************************/
                ucgRuleService.putUCGRule(ucgRule, segmentationDimensionService, (existingElement == null), userID);

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

                ucgRuleService.putUCGRule(incompleteObject, segmentationDimensionService, (existingElement == null),
                    userID);
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
    Collection <GUIManagedObject> ucgRuleObjects = new ArrayList<GUIManagedObject>();
    
    if (jsonRoot.containsKey("ids"))
      {
  JSONArray ucgRuleIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
  for (int i = 0; i < ucgRuleIDs.size(); i++)
    {
      String ucgRuleID = ucgRuleIDs.get(i).toString();
      GUIManagedObject ucgRule = ucgRuleService.getStoredUCGRule(ucgRuleID, includeArchived);
      if (ucgRule == null)
        {
          HashMap<String, Object> response = new HashMap<String, Object>();
          String responseCode;
          responseCode = "ucgRuleNotFound";
          response.put("responseCode", responseCode);
          return JSONUtilities.encodeObject(response);
        }
      ucgRuleObjects.add(ucgRule);
    }
      }
    else
      {
        ucgRuleObjects = ucgRuleService.getStoredUCGRules(includeArchived);
      }
    for (GUIManagedObject ucgRule : ucgRuleObjects)
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
    Collection <GUIManagedObject> segmentationDimensionObjects = new ArrayList<GUIManagedObject>();
    
    if (jsonRoot.containsKey("ids"))
      {
        JSONArray segmentationDimensionIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
        for (int i = 0; i < segmentationDimensionIDs.size(); i++)
          {
            String segmentationDimensionID = segmentationDimensionIDs.get(i).toString();
            GUIManagedObject segmentationDimension = segmentationDimensionService
                .getStoredSegmentationDimension(segmentationDimensionID, includeArchived);
            if (segmentationDimension != null)
              {
                segmentationDimensionObjects.add(segmentationDimension);
              }            
          }
      }
    else
      {
        segmentationDimensionObjects = segmentationDimensionService.getStoredSegmentationDimensions(includeArchived);
      }
    for (GUIManagedObject segmentationDimension : segmentationDimensionObjects)
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
    Collection <GUIManagedObject> targetObjects = new ArrayList<GUIManagedObject>();
    
    if (jsonRoot.containsKey("ids"))
      {
        JSONArray targetIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
        for (int i = 0; i < targetIDs.size(); i++)
          {
            String targetID = targetIDs.get(i).toString();
            GUIManagedObject target = targetService.getStoredTarget(targetID, includeArchived);
            if (target != null)
              {
                targetObjects.add(target);
              }
          }
      }
    else
      {
        targetObjects = targetService.getStoredTargets(includeArchived);
      }
    for (GUIManagedObject targetList : targetObjects)
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

    String responseCode = "";
    String singleIDresponseCode = "";
    List<GUIManagedObject> exclusionInclusionTargets = new ArrayList<>();
    List<String> validIDs = new ArrayList<>();
    JSONArray exclusionInclusionTargetIDs = new JSONArray();

    /****************************************
    *
    *  argument
    *
    ****************************************/
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);
    
    //
    //remove single exclusionInclusionTarget
    //
    if (jsonRoot.containsKey("id"))
      {
        String exclusionInclusionTargetID = JSONUtilities.decodeString(jsonRoot, "id", false);
        exclusionInclusionTargetIDs.add(exclusionInclusionTargetID);
        GUIManagedObject exclusionInclusionTarget = exclusionInclusionTargetService
            .getStoredExclusionInclusionTarget(exclusionInclusionTargetID);

        if (exclusionInclusionTarget != null && (force || !exclusionInclusionTarget.getReadOnly()))
          singleIDresponseCode = "ok";
        else if (exclusionInclusionTarget != null)
          singleIDresponseCode = "failedReadOnly";
        else singleIDresponseCode = "exclusionInclusionTargetNotFound";

      }
    //
    // multiple deletion
    //
    
    if (jsonRoot.containsKey("ids"))
      {
        exclusionInclusionTargetIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids", false);
      }
    
    for (int i = 0; i < exclusionInclusionTargetIDs.size(); i++)
      {
        String exclusionInclusionTargetID = exclusionInclusionTargetIDs.get(i).toString();
        GUIManagedObject exclusionInclusionTarget = exclusionInclusionTargetService.getStoredExclusionInclusionTarget(exclusionInclusionTargetID);
        if (exclusionInclusionTarget != null && (force || !exclusionInclusionTarget.getReadOnly()))
          {
            exclusionInclusionTargets.add(exclusionInclusionTarget);
            validIDs.add(exclusionInclusionTargetID);
          }
      }
        
  

    /*****************************************
    *
    *  remove
    *
    *****************************************/
    for (int i = 0; i < exclusionInclusionTargets.size(); i++)
      {

        GUIManagedObject exclusionInclusionTarget = exclusionInclusionTargets.get(i);
        exclusionInclusionTargetService.removeExclusionInclusionTarget(exclusionInclusionTarget.getGUIManagedObjectID(),
            userID);

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
    response.put("removedJourneyIDS", JSONUtilities.encodeArray(validIDs));

    return JSONUtilities.encodeObject(response);
  }


}

