/*****************************************************************************
*
*  GUIManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SubscriberIDService;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Badge.BadgeType;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramType;
import com.evolving.nglm.evolution.LoyaltyProgramChallenge.ChallengeLevel;
import com.evolving.nglm.evolution.LoyaltyProgramMission.MissionSchedule;
import com.evolving.nglm.evolution.LoyaltyProgramMission.MissionStep;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;
import com.evolving.nglm.evolution.Report.SchedulingInterval;
import com.evolving.nglm.evolution.SubscriberProfileService.SubscriberProfileServiceException;
import com.evolving.nglm.evolution.complexobjects.ComplexObjectTypeService;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientException;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.sun.net.httpserver.HttpExchange;

public class GUIManagerLoyaltyReporting extends GUIManager
{

  private static final Logger log = LoggerFactory.getLogger(GUIManagerLoyaltyReporting.class);

  public GUIManagerLoyaltyReporting(JourneyService journeyService, SegmentationDimensionService segmentationDimensionService, PointService pointService, ComplexObjectTypeService complexObjectTypeService, OfferService offerService, ReportService reportService, PaymentMeanService paymentMeanService, ScoringStrategyService scoringStrategyService, PresentationStrategyService presentationStrategyService, CallingChannelService callingChannelService, SalesChannelService salesChannelService, SourceAddressService sourceAddressService, SupplierService supplierService, ProductService productService, CatalogCharacteristicService catalogCharacteristicService, ContactPolicyService contactPolicyService, JourneyObjectiveService journeyObjectiveService, OfferObjectiveService offerObjectiveService, ProductTypeService productTypeService, UCGRuleService ucgRuleService, DeliverableService deliverableService, TokenTypeService tokenTypeService, VoucherTypeService voucherTypeService, VoucherService voucherService, SubscriberMessageTemplateService subscriberTemplateService, SubscriberProfileService subscriberProfileService, SubscriberIDService subscriberIDService, UploadedFileService uploadedFileService, TargetService targetService, CommunicationChannelBlackoutService communicationChannelBlackoutService, LoyaltyProgramService loyaltyProgramService, BadgeService badgeService, BadgeObjectiveService badgeObjectiveService, ResellerService resellerService, ExclusionInclusionTargetService exclusionInclusionTargetService, SegmentContactPolicyService segmentContactPolicyService, CriterionFieldAvailableValuesService criterionFieldAvailableValuesService, DNBOMatrixService dnboMatrixService, DynamicCriterionFieldService dynamicCriterionFieldService, DynamicEventDeclarationsService dynamicEventDeclarationsService, JourneyTemplateService journeyTemplateService, KafkaResponseListenerService<StringKey,PurchaseFulfillmentRequest> purchaseResponseListenerService, SharedIDService subscriberGroupSharedIDService, ZookeeperUniqueKeyServer zuks, int httpTimeout, KafkaProducer<byte[], byte[]> kafkaProducer, ElasticsearchClientAPI elasticsearch, SubscriberMessageTemplateService subscriberMessageTemplateService, String getCustomerAlternateID, GUIManagerContext guiManagerContext, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, ReferenceDataReader<String,RenamedProfileCriterionField> renamedProfileCriterionFieldReader)
  {
    super.callingChannelService = callingChannelService;
    super.catalogCharacteristicService = catalogCharacteristicService;
    super.communicationChannelBlackoutService = communicationChannelBlackoutService;
    super.contactPolicyService = contactPolicyService;
    super.criterionFieldAvailableValuesService = criterionFieldAvailableValuesService;
    super.deliverableService = deliverableService;
    super.exclusionInclusionTargetService = exclusionInclusionTargetService;
    super.journeyObjectiveService = journeyObjectiveService;
    super.journeyService = journeyService;
    super.loyaltyProgramService = loyaltyProgramService;
    super.badgeService = badgeService;
    super.badgeObjectiveService = badgeObjectiveService;
    super.offerObjectiveService = offerObjectiveService;
    super.offerService = offerService;
    super.complexObjectTypeService = complexObjectTypeService;
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
    super.renamedProfileCriterionFieldReader = renamedProfileCriterionFieldReader;
  }
  
  /*****************************************
  *
  *  processGetLoyaltyProgram
  *
  *****************************************/

  JSONObject processGetLoyaltyProgram(String userID, JSONObject jsonRoot, LoyaltyProgramType loyaltyProgramType, boolean includeArchived, int tenantID)
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

    String loyaltyProgramID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /**************************************************************
    *
    *  retrieve and decorate loyalty program
    *
    ***************************************************************/

    GUIManagedObject loyaltyProgram = loyaltyProgramService.getStoredLoyaltyProgram(loyaltyProgramID, includeArchived);
    JSONObject loyaltyProgramJSON = loyaltyProgramService.generateResponseJSON(loyaltyProgram, true, SystemTime.getCurrentTime());
    if (loyaltyProgramType != LoyaltyProgramType.fromExternalRepresentation(JSONUtilities.decodeString(loyaltyProgramJSON, "loyaltyProgramType", true))) loyaltyProgram = null;

    /*****************************************
    *
    *  response
    *
    *****************************************/
    
    switch (loyaltyProgramType)
    {
      case POINTS:
        response.put("responseCode", (loyaltyProgram != null) ? "ok" : "loyaltyProgramNotFound");
        if (loyaltyProgram != null) response.put("loyaltyProgram", loyaltyProgramJSON);
        break;

      case CHALLENGE:
        response.put("responseCode", (loyaltyProgram != null) ? "ok" : "loyaltyProgramChallengeNotFound");
        if (loyaltyProgram != null) response.put("loyaltyProgram", loyaltyProgramJSON);
        break;
        
      case MISSION:
        response.put("responseCode", (loyaltyProgram != null) ? "ok" : "loyaltyProgramMissionNotFound");
        if (loyaltyProgram != null) response.put("loyaltyProgram", loyaltyProgramJSON);
        break;

      default:
        break;
    }
    
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutLoyaltyProgram
  *
  *****************************************/

  JSONObject processPutLoyaltyProgram(String userID, JSONObject jsonRoot, LoyaltyProgramType loyaltyProgramType, int tenantID)
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
    *  loyaltyProgramID
    *
    *****************************************/

    String loyaltyProgramID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (loyaltyProgramID == null)
      {
        loyaltyProgramID = loyaltyProgramService.generateLoyaltyProgramID();
        jsonRoot.put("id", loyaltyProgramID);
      }
    
    /*****************************************
    *
    *  existing LoyaltyProgram
    *
    *****************************************/

    GUIManagedObject existingLoyaltyProgram = loyaltyProgramService.getStoredGUIManagedObject(loyaltyProgramID);
    
    //
    //  clear bad values (happened in clone from gui)
    //
    
    if (existingLoyaltyProgram == null) 
      {
        jsonRoot.remove("lastCreatedOccurrenceNumber");
        jsonRoot.remove("lastOccurrenceCreateDate");
        jsonRoot.remove("previousPeriodStartDate");
        if (JSONUtilities.decodeBoolean(jsonRoot, "recurrence", Boolean.FALSE)) jsonRoot.put("occurrenceNumber", 1);
      }
    
    //
    // recurrence
    //
    
    boolean recurrence = JSONUtilities.decodeBoolean(jsonRoot, "recurrence", Boolean.FALSE);
    String recurrenceID = JSONUtilities.decodeString(jsonRoot, "recurrenceId", false);
    if (recurrence && recurrenceID == null) jsonRoot.put("recurrenceId", loyaltyProgramID);
    if (recurrence && JSONUtilities.decodeInteger(jsonRoot, "lastCreatedOccurrenceNumber", false) == null) jsonRoot.put("lastCreatedOccurrenceNumber", 1);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingLoyaltyProgram != null && existingLoyaltyProgram.getReadOnly())
      {
        response.put("id", existingLoyaltyProgram.getGUIManagedObjectID());
        response.put("accepted", existingLoyaltyProgram.getAccepted());
        response.put("valid", existingLoyaltyProgram.getAccepted());
        response.put("processing", loyaltyProgramService.isActiveLoyaltyProgram(existingLoyaltyProgram, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process LoyaltyProgram
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate LoyaltyProgram
        *
        ****************************************/

        LoyaltyProgram loyaltyProgram = null;
        switch (loyaltyProgramType)
        {
          case POINTS:
            loyaltyProgram = new LoyaltyProgramPoints(jsonRoot, epoch, existingLoyaltyProgram, catalogCharacteristicService, tenantID);
            break;
            
          case CHALLENGE:
            loyaltyProgram = new LoyaltyProgramChallenge(jsonRoot, epoch, existingLoyaltyProgram, catalogCharacteristicService, tenantID);
            break;
            
          case MISSION:
            jsonRoot.put("effectiveStartDate", JSONUtilities.decodeString(jsonRoot, "entryStartDate", true));
            String scheduleType = JSONUtilities.decodeString(jsonRoot, "scheduleType", true);
            if (MissionSchedule.FIXDURATION == MissionSchedule.fromExternalRepresentation(scheduleType))
              {
                Integer durationInDays  = JSONUtilities.decodeInteger(jsonRoot, "duration", true);
                String entryEndDateStr = JSONUtilities.decodeString(jsonRoot, "entryEndDate", true);
                Date effectiveEndDate = RLMDateUtils.addDays(GUIManagedObject.parseDateField(entryEndDateStr), durationInDays, Deployment.getDefault().getTimeZone());
                jsonRoot.put("effectiveEndDate", RLMDateUtils.formatDateForREST(effectiveEndDate, Deployment.getDefault().getTimeZone()));
              }
            else
              {
                jsonRoot.put("effectiveEndDate", JSONUtilities.decodeString(jsonRoot, "entryEndDate", true));
              }
            loyaltyProgram = new LoyaltyProgramMission(jsonRoot, epoch, existingLoyaltyProgram, catalogCharacteristicService, tenantID);
            break;

          case Unknown:
            throw new GUIManagerException("unsupported loyalty program type", JSONUtilities.decodeString(jsonRoot, "loyaltyProgramType", false));
        }

        /*****************************************
        *
        *  store
        *
        *****************************************/
        if (!dryRun)
          {

            loyaltyProgramService.putLoyaltyProgram(loyaltyProgram, (existingLoyaltyProgram == null), userID);

            /*****************************************
             *
             * add dynamic criterion fields)
             *
             *****************************************/

            dynamicCriterionFieldService.addLoyaltyProgramCriterionFields(loyaltyProgram, (existingLoyaltyProgram == null));

            /*****************************************
             *
             * revalidate
             *
             *****************************************/

            revalidateSubscriberMessageTemplates(now, tenantID);
            revalidateOffers(now, tenantID);
            revalidateTargets(now, tenantID);
            revalidateJourneys(now, tenantID);
          }

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", loyaltyProgram.getGUIManagedObjectID());
        response.put("accepted", loyaltyProgram.getAccepted());
        response.put("valid", loyaltyProgram.getAccepted());
        response.put("processing", loyaltyProgramService.isActiveLoyaltyProgram(loyaltyProgram, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch, tenantID);

        //
        //  store
        //
        if (!dryRun)
          {
            loyaltyProgramService.putLoyaltyProgram(incompleteObject, (existingLoyaltyProgram == null), userID);
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
        response.put("responseCode", "loyaltyProgramNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }
  
  /*****************************************
  *
  *  processUpdateLoyalty
  *
  *****************************************/

  JSONObject processUpdateLoyalty(String userID, JSONObject jsonRoot, LoyaltyProgramType loyaltyProgramType, int tenantID)
  {

    Date now = SystemTime.getCurrentTime();
    HashMap<String, Object> response = new HashMap<String, Object>();
    JSONArray loyaltyIDs = new JSONArray();
    List<GUIManagedObject> existingLoyalities = new ArrayList<GUIManagedObject>();
    List<String> updatedIDs = new ArrayList<String>();
    List<Object> exceptionList = new ArrayList<Object>();

    //
    // arguments
    //

    Boolean dryRun = JSONUtilities.decodeBoolean(jsonRoot, "dryRun", Boolean.FALSE);

    /*****************************************
     *
     * update loyalties
     *
     *****************************************/

    if (jsonRoot.containsKey("ids"))
      {
        loyaltyIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids", false); // update for multiple loyalties
      } 
    else
      {
        response.put("responseCode", "invalid" + loyaltyProgramType.toString() + "s");
        response.put("responseMessage", loyaltyProgramType.toString() + " ID is empty");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
     *
     * existing loyalty
     *
     *****************************************/
    
    for (int i = 0; i < loyaltyIDs.size(); i++)
      {
        String loyaltyID = (loyaltyIDs.get(i)).toString();
        GUIManagedObject existingLoyaltyObject = loyaltyProgramService.getStoredGUIManagedObject(loyaltyID);
        if (existingLoyaltyObject != null && (loyaltyProgramType == LoyaltyProgramType.fromExternalRepresentation(JSONUtilities.decodeString(existingLoyaltyObject.getJSONRepresentation(), "loyaltyProgramType", true))))
          {
            existingLoyalities.add(existingLoyaltyObject); //ignore the wrong loyalties
          }
      }
    
    if (existingLoyalities.isEmpty())
      {
        response.put("responseCode", "invalid" + loyaltyProgramType.toString() + "s");
        response.put("responseMessage", loyaltyProgramType.toString() + "s does not exist");
        return JSONUtilities.encodeObject(response);
      }
    
    for (GUIManagedObject existingLoyaltyToBeUpdated : existingLoyalities)
      {
        JSONObject existingLoyaltyJSONToUpdate = (JSONObject) existingLoyaltyToBeUpdated.getJSONRepresentation().clone();
        
        //
        //  clean and merge
        //
        
        jsonRoot.remove("ids");
        for (String keyToUpdate : (Set<String>) jsonRoot.keySet())
          {
            existingLoyaltyJSONToUpdate.put(keyToUpdate, jsonRoot.get(keyToUpdate));
          }
        
        //
        //  save
        //
        
        /*****************************************
        *
        *  process LoyaltyProgram
        *
        *****************************************/

        long epoch = epochServer.getKey();
        try
          {
            /****************************************
            *
            *  instantiate LoyaltyProgram
            *
            ****************************************/

            LoyaltyProgram loyaltyProgram = null;
            switch (loyaltyProgramType)
            {
              case POINTS:
                
                //
                //  need to be checked if ok - EVPRO-1060
                //
                
                throw new GUIManagerException("unsupported loyalty program type", JSONUtilities.decodeString(existingLoyaltyJSONToUpdate, "loyaltyProgramType", false));
                //loyaltyProgram = new LoyaltyProgramPoints(existingLoyaltyJSONToUpdate, epoch, existingLoyaltyToBeUpdated, catalogCharacteristicService, tenantID);
                
              case CHALLENGE:
                loyaltyProgram = new LoyaltyProgramChallenge(existingLoyaltyJSONToUpdate, epoch, existingLoyaltyToBeUpdated, catalogCharacteristicService, tenantID);
                break;
                
              case MISSION:
                existingLoyaltyJSONToUpdate.put("effectiveStartDate", JSONUtilities.decodeString(existingLoyaltyJSONToUpdate, "entryStartDate", true));
                String scheduleType = JSONUtilities.decodeString(existingLoyaltyJSONToUpdate, "scheduleType", true);
                if (MissionSchedule.FIXDURATION == MissionSchedule.fromExternalRepresentation(scheduleType))
                  {
                    Integer durationInDays  = JSONUtilities.decodeInteger(existingLoyaltyJSONToUpdate, "duration", true);
                    String entryEndDateStr = JSONUtilities.decodeString(existingLoyaltyJSONToUpdate, "entryEndDate", true);
                    Date effectiveEndDate = RLMDateUtils.addDays(GUIManagedObject.parseDateField(entryEndDateStr), durationInDays, Deployment.getDefault().getTimeZone());
                    existingLoyaltyJSONToUpdate.put("effectiveEndDate", RLMDateUtils.formatDateForREST(effectiveEndDate, Deployment.getDefault().getTimeZone()));
                  }
                else
                  {
                    existingLoyaltyJSONToUpdate.put("effectiveEndDate", JSONUtilities.decodeString(existingLoyaltyJSONToUpdate, "entryEndDate", true));
                    existingLoyaltyJSONToUpdate.remove("duration");
                  }
                loyaltyProgram = new LoyaltyProgramMission(existingLoyaltyJSONToUpdate, epoch, existingLoyaltyToBeUpdated, catalogCharacteristicService, tenantID);
                break;

              case Unknown:
                throw new GUIManagerException("unsupported loyalty program type", JSONUtilities.decodeString(existingLoyaltyJSONToUpdate, "loyaltyProgramType", false));
            }

            /*****************************************
            *
            *  store
            *
            *****************************************/
            if (!dryRun)
              {

                loyaltyProgramService.putLoyaltyProgram(loyaltyProgram, (existingLoyaltyToBeUpdated == null), userID);

                /*****************************************
                 *
                 * add dynamic criterion fields)
                 *
                 *****************************************/

                dynamicCriterionFieldService.addLoyaltyProgramCriterionFields(loyaltyProgram, (existingLoyaltyToBeUpdated == null));

                /*****************************************
                 *
                 * revalidate
                 *
                 *****************************************/

                revalidateSubscriberMessageTemplates(now, tenantID);
                revalidateOffers(now, tenantID);
                revalidateTargets(now, tenantID);
                revalidateJourneys(now, tenantID);
              }

            /*****************************************
            *
            *  add to response
            *
            *****************************************/
            
            updatedIDs.add(loyaltyProgram.getGUIManagedObjectID());
          }
        catch (JSONUtilitiesException|GUIManagerException e)
          {
            //
            //  incompleteObject
            //

            IncompleteObject incompleteObject = new IncompleteObject(existingLoyaltyJSONToUpdate, epoch, tenantID);

            //
            //  store
            //
            
            if (!dryRun)
              {
                loyaltyProgramService.putLoyaltyProgram(incompleteObject, (existingLoyaltyToBeUpdated == null), userID);
              }
            
            //
            //  log
            //

            StringWriter stackTraceWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stackTraceWriter, true));
            log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

            /*****************************************
            *
            *  add to response
            *
            *****************************************/
            
            HashMap<String, String> invalidLoyaltyExceptions = new HashMap<String, String>();
            invalidLoyaltyExceptions.put("id", incompleteObject.getGUIManagedObjectID());
            invalidLoyaltyExceptions.put("responseCode", loyaltyProgramType + "NotValid");
            invalidLoyaltyExceptions.put("responseMessage", e.getMessage());
            invalidLoyaltyExceptions.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
            
            updatedIDs.add(incompleteObject.getGUIManagedObjectID());
            exceptionList.add(invalidLoyaltyExceptions);
          }
      }
    
    /*****************************************
    *
    *  response
    *
    *****************************************/
    
    response.put("updatedIds", updatedIDs);
    response.put("exceptionIds", exceptionList);
    response.put("responseCode", "ok");
    
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processRemoveLoyaltyProgram
  *
  *****************************************/

  JSONObject processRemoveLoyaltyProgram(String userID, JSONObject jsonRoot, int tenantID){

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
    List<GUIManagedObject> loyaltyPrograms = new ArrayList<>();
    List<String> validIDs = new ArrayList<>();
    JSONArray loyaltyProgramIDs = new JSONArray();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);
    //
    //remove single loyaltyProgram
    //
    if (jsonRoot.containsKey("id"))
      {
        String loyaltyProgramID = JSONUtilities.decodeString(jsonRoot, "id", false);
        loyaltyProgramIDs.add(loyaltyProgramID);
        GUIManagedObject loyaltyProgram = loyaltyProgramService.getStoredGUIManagedObject(loyaltyProgramID);

        if (loyaltyProgram != null && (force || !loyaltyProgram.getReadOnly()))
          singleIDresponseCode = "ok";
        else if (loyaltyProgram != null)
          singleIDresponseCode = "failedReadOnly";
        else singleIDresponseCode = "loyaltyprogramNotFound";

      }
    //
    // multiple deletion
    //
    
    if (jsonRoot.containsKey("ids"))
      {
        loyaltyProgramIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids", false);
      }
   
    for (int i = 0; i < loyaltyProgramIDs.size(); i++)
      {
        String loyaltyProgramID = loyaltyProgramIDs.get(i).toString();
        GUIManagedObject loyaltyProgram = loyaltyProgramService.getStoredGUIManagedObject(loyaltyProgramID);
        
        if (loyaltyProgram != null && (force || !loyaltyProgram.getReadOnly()))
          {
            loyaltyPrograms.add(loyaltyProgram);
            validIDs.add(loyaltyProgramID);
          }
      }
        
  

    /*****************************************
    *
    *  remove
    *
    *****************************************/
    for (int i = 0; i < loyaltyPrograms.size(); i++)
      {

        GUIManagedObject loyaltyProgram = loyaltyPrograms.get(i);

        //
        // remove loyalty program
        //

        loyaltyProgramService.removeLoyaltyProgram(loyaltyProgram.getGUIManagedObjectID(), userID, tenantID);

        //
        // remove dynamic criterion fields
        //

        dynamicCriterionFieldService.removeLoyaltyProgramCriterionFields(loyaltyProgram);

        //
        // revalidate
        //

        revalidateSubscriberMessageTemplates(now, tenantID);
        revalidateOffers(now, tenantID);
        revalidateTargets(now, tenantID);
        revalidateJourneys(now, tenantID);
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
    response.put("removedloyaltyProgramIDS", JSONUtilities.encodeArray(validIDs));

    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
   *
   * processSetStatusLoyaltyProgram
   *
   *****************************************/

  JSONObject processSetStatusLoyaltyProgram(String userID, JSONObject jsonRoot, int tenantID)
  {
    /****************************************
     *
     * response
     *
     ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String, Object> response = new HashMap<String, Object>();
    JSONArray loyaltyProgramIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
    List<String> statusSetIDs = new ArrayList<>();
    Boolean status = JSONUtilities.decodeBoolean(jsonRoot, "active");
    long epoch = epochServer.getKey();

    for (int i = 0; i < loyaltyProgramIDs.size(); i++)
      {

        String loyaltyProgramID = loyaltyProgramIDs.get(i).toString();
        GUIManagedObject existingElement = loyaltyProgramService.getStoredLoyaltyProgram(loyaltyProgramID);
        if (existingElement != null && !(existingElement.getReadOnly()))
          {
            statusSetIDs.add(loyaltyProgramID);
            JSONObject elementRoot = (JSONObject) existingElement.getJSONRepresentation().clone();
            elementRoot.put("active", status);
            try
              {
                /****************************************
                 *
                 * instantiate LoyaltyProgram
                 *
                 ****************************************/

                LoyaltyProgram loyaltyProgram = null;
                switch (LoyaltyProgramType.fromExternalRepresentation(JSONUtilities.decodeString(elementRoot, "loyaltyProgramType", true)))
                {
                  case POINTS:
                    loyaltyProgram = new LoyaltyProgramPoints(elementRoot, epoch, existingElement, catalogCharacteristicService, tenantID);
                    break;

                  case CHALLENGE:
                    loyaltyProgram = new LoyaltyProgramChallenge(elementRoot, epoch, existingElement, catalogCharacteristicService, tenantID);
                    break;
                    
                  case MISSION:
                    loyaltyProgram = new LoyaltyProgramMission(elementRoot, epoch, existingElement, catalogCharacteristicService, tenantID);
                    break;

                  case Unknown:
                    throw new GUIManagerException("unsupported loyalty program type", JSONUtilities.decodeString(elementRoot, "loyaltyProgramType", false));
                }

                /*****************************************
                 *
                 * store
                 *
                 *****************************************/
                
                loyaltyProgramService.putLoyaltyProgram(loyaltyProgram, (existingElement == null), userID);

                /*****************************************
                 *
                 * revalidate
                 *
                 *****************************************/

                revalidateSubscriberMessageTemplates(now, tenantID);
                revalidateOffers(now, tenantID);
                revalidateTargets(now, tenantID);
                revalidateJourneys(now, tenantID);

              }
            catch (JSONUtilitiesException | GUIManagerException e)
              {
                //
                // incompleteObject
                //

                IncompleteObject incompleteObject = new IncompleteObject(elementRoot, epoch, tenantID);

                //
                // store
                //

                loyaltyProgramService.putLoyaltyProgram(incompleteObject, (existingElement == null), userID);

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
  *  processGetLoyaltyProgramList
  *
  *****************************************/

  JSONObject processGetLoyaltyProgramList(String userID, JSONObject jsonRoot,  LoyaltyProgramType loyaltyProgramType, boolean fullDetails, boolean includeArchived, int tenantID)
  {
    /*****************************************
    *
    *  retrieve loyalty program list
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> loyaltyProgramList = new ArrayList<JSONObject>();
    Collection <GUIManagedObject> loyaltyProgramObjects = new ArrayList<GUIManagedObject>();
    
    if (jsonRoot.containsKey("ids"))
      {
        JSONArray loyaltyProgramIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
        for (int i = 0; i < loyaltyProgramIDs.size(); i++)
          {
            String loyaltyProgramID = loyaltyProgramIDs.get(i).toString();
            GUIManagedObject loyaltyProgram = loyaltyProgramService.getStoredLoyaltyProgram(loyaltyProgramID, includeArchived);
            if (loyaltyProgram != null && loyaltyProgram.getTenantID() == tenantID)
              {
                loyaltyProgramObjects.add(loyaltyProgram);
              }
          }
      }
    else
      {
        loyaltyProgramObjects = loyaltyProgramService.getStoredGUIManagedObjects(includeArchived, tenantID);
      }
    for (GUIManagedObject loyaltyProgram : loyaltyProgramObjects)
      {
        long membersCount = 0L;
        JSONObject loyaltyProFull = loyaltyProgramService.generateResponseJSON(loyaltyProgram, true, now);
        boolean recurrence = JSONUtilities.decodeBoolean(loyaltyProFull, "recurrence", Boolean.FALSE);
        boolean active = JSONUtilities.decodeBoolean(loyaltyProFull, "active", Boolean.TRUE);
        
        if (loyaltyProgramType == LoyaltyProgramType.fromExternalRepresentation(JSONUtilities.decodeString(loyaltyProFull, "loyaltyProgramType")))
          {
            JSONObject loyaltyPro = loyaltyProgramService.generateResponseJSON(loyaltyProgram, fullDetails, now);
            loyaltyPro.put("active", active);
            try
              {
                membersCount = this.elasticsearch.getLoyaltyProgramCount(loyaltyProgram.getGUIManagedObjectID());
              } 
            catch (ElasticsearchClientException e)
              {
                log.warn("Exception processing REST api: {}", e);
              }
            loyaltyPro.put("programsMembersCount", membersCount);
            
            //
            //  CHALLENGE fields
            //
            
            if (loyaltyProgramType == LoyaltyProgramType.CHALLENGE)
              {
                loyaltyPro.put("recurrence", recurrence);
                if (loyaltyProgram instanceof LoyaltyProgramChallenge)
                  {
                    ChallengeLevel lastLevel = ((LoyaltyProgramChallenge) loyaltyProgram).getLastLevel();
                    loyaltyPro.put("topScore", lastLevel == null ? Integer.valueOf(0) : lastLevel.getScoreLevel());
                  }
              }
            else if (loyaltyProgramType == LoyaltyProgramType.MISSION)
              {
                if (loyaltyProgram instanceof LoyaltyProgramMission)
                  {
                    MissionStep lastStep = ((LoyaltyProgramMission) loyaltyProgram).getLastStep();
                    loyaltyPro.put("lastStep", lastStep == null ? null : lastStep.getStepName());
                  }
              }
            
            loyaltyProgramList.add(loyaltyPro);
          }
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("loyaltyPrograms", JSONUtilities.encodeArray(loyaltyProgramList));
    return JSONUtilities.encodeObject(response);
  }


  /*****************************************
  *
  *  getLoyaltyProgramPointsEvents
  *
  *****************************************/

  JSONObject processGetLoyaltyProgramPointsEvents(String userID, JSONObject jsonRoot, int tenantID)
  {
    /*****************************************
    *
    *  retrieve events
    *
    *****************************************/

    List<JSONObject> events = evaluateEnumeratedValues("loyaltyProgramPointsEventNames", SystemTime.getCurrentTime(), true, tenantID);

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("loyaltyProgramPointsEvents", JSONUtilities.encodeArray(events));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetLoyaltyProgramTypeList
  *
  *****************************************/

  JSONObject processGetLoyaltyProgramTypeList(String userID, JSONObject jsonRoot, int tenantID)
  {
    /*****************************************
    *
    *  retrieve loyalty program type list
    *
    *****************************************/

    List<JSONObject> programTypeList = new ArrayList<JSONObject>();
    for (LoyaltyProgramType programType : LoyaltyProgramType.values())
      {
        if(!programType.equals(LoyaltyProgramType.Unknown)){
          Map<String, Object> programTypeJSON = new HashMap<String, Object>();
          programTypeJSON.put("display", programType.getExternalRepresentation());
          programTypeJSON.put("name", programType.getExternalRepresentation());
          programTypeList.add(JSONUtilities.encodeObject(programTypeJSON));
        }
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("loyaltyProgramTypes", JSONUtilities.encodeArray(programTypeList));
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processGetBadgeTypeList
  *
  *****************************************/

  JSONObject processGetBadgeTypeList(String userID, JSONObject jsonRoot, int tenantID)
  {
    /*****************************************
    *
    *  retrieve badge type list
    *
    *****************************************/

    List<JSONObject> badgeTypeList = new ArrayList<JSONObject>();
    for (BadgeType badgeType : BadgeType.values())
      {
        if (!badgeType.equals(BadgeType.Unknown))
          {
            Map<String, Object> badgeTypeJSON = new HashMap<String, Object>();
            badgeTypeJSON.put("display", badgeType.name());
            badgeTypeJSON.put("name", badgeType.getExternalRepresentation());
            badgeTypeList.add(JSONUtilities.encodeObject(badgeTypeJSON));
          }
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("badgeTypes", JSONUtilities.encodeArray(badgeTypeList));
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processPutBadgeObjective
  *
  *****************************************/

  JSONObject processPutBadgeObjective(String userID, JSONObject jsonRoot, int tenantID)
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
    
    if (jsonRoot.containsKey("dryRun"))
      {
        dryRun = JSONUtilities.decodeBoolean(jsonRoot, "dryRun", false);
      }

    /*****************************************
    *
    *  badgeObjectiveID
    *
    *****************************************/

    String badgeObjectiveID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (badgeObjectiveID == null)
      {
        badgeObjectiveID = badgeObjectiveService.generateBadgeObjectiveID();
        jsonRoot.put("id", badgeObjectiveID);
      }

    /*****************************************
    *
    *  existing badgeObjective
    *
    *****************************************/

    GUIManagedObject existingBadgeObjective = badgeObjectiveService.getStoredBadgeObjective(badgeObjectiveID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingBadgeObjective != null && existingBadgeObjective.getReadOnly())
      {
        response.put("id", existingBadgeObjective.getGUIManagedObjectID());
        response.put("accepted", existingBadgeObjective.getAccepted());
        response.put("valid", existingBadgeObjective.getAccepted());
        response.put("processing", badgeObjectiveService.isActiveBadgeObjective(existingBadgeObjective, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process badgeObjective
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate badgeObjective
        *
        ****************************************/

        BadgeObjective badgeObjective = new BadgeObjective(jsonRoot, epoch, existingBadgeObjective, tenantID);

        /*****************************************
        *
        *  store
        *
        *****************************************/
        if (!dryRun)
          {

            badgeObjectiveService.putBadgeObjective(badgeObjective, (existingBadgeObjective == null), userID);

            /*****************************************
             *
             * revalidate dependent objects
             *
             *****************************************/

            revalidateBadges(now, tenantID);
          }

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", badgeObjective.getBadgeObjectiveID());
        response.put("accepted", badgeObjective.getAccepted());
        response.put("valid", badgeObjective.getAccepted());
        response.put("processing", badgeObjectiveService.isActiveBadgeObjective(badgeObjective, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch, tenantID);

        //
        //  store
        //
        if (!dryRun)
          {
            badgeObjectiveService.putBadgeObjective(incompleteObject, (existingBadgeObjective == null), userID);

            //
            // revalidate dependent objects
            //

            revalidateBadges(now, tenantID);
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
        response.put("responseCode", "badgeObjectiveNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }
  
  /*****************************************
  *
  *  processGetBadgeObjective
  *
  *****************************************/

  protected JSONObject processGetBadgeObjective(String userID, JSONObject jsonRoot, boolean includeArchived, int tenantID)
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

    String badgeObjectiveID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate scoring strategy
    *
    *****************************************/

    GUIManagedObject badgeObjective = badgeObjectiveService.getStoredBadgeObjective(badgeObjectiveID, includeArchived);
    JSONObject badgeObjectiveJSON = badgeObjectiveService.generateResponseJSON(badgeObjective, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (badgeObjective != null) ? "ok" : "badgeObjectiveNotFound");
    if (badgeObjective != null) response.put("badgeObjective", badgeObjectiveJSON);
    return JSONUtilities.encodeObject(response);
  
  }
  
  /*****************************************
  *
  *  processGetBadgeObjectiveList
  *
  *****************************************/

  protected JSONObject processGetBadgeObjectiveList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived, int tenantID)
  {
    /*****************************************
    *
    *  retrieve and convert badgeObjectives
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> badgeObjectives = new ArrayList<JSONObject>();
    Collection <GUIManagedObject> badgeObjectiveObjects = new ArrayList<GUIManagedObject>();
    
    if (jsonRoot.containsKey("ids"))
      {
        JSONArray badgeObjectiveIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
        for (int i = 0; i < badgeObjectiveIDs.size(); i++)
          {
            String badgeObjectiveID = badgeObjectiveIDs.get(i).toString();
            GUIManagedObject badgeObjective = badgeObjectiveService.getStoredBadgeObjective(badgeObjectiveID, includeArchived);
            if (badgeObjective != null && badgeObjective.getTenantID() == tenantID)
              {
                badgeObjectiveObjects.add(badgeObjective);
              }
          }
      }
    else
      {
        badgeObjectiveObjects = badgeObjectiveService.getStoredBadgeObjectives(includeArchived, tenantID);
      }
    for (GUIManagedObject badgeObjective : badgeObjectiveObjects)
      {
        badgeObjectives.add(badgeObjectiveService.generateResponseJSON(badgeObjective, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("badgeObjectives", JSONUtilities.encodeArray(badgeObjectives));
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  revalidateBadges
  *
  *****************************************/

  protected void revalidateBadges(Date date, int tenantID)
  {
    /****************************************
    *
    *  identify
    *
    ****************************************/

    Set<GUIManagedObject> modifiedBadges = new HashSet<GUIManagedObject>();
    for (GUIManagedObject existingBadge : badgeService.getStoredBadges(tenantID))
      {
        //
        //  modifiedBadge
        //

        long epoch = epochServer.getKey();
        GUIManagedObject modifiedBadge;
        try
          {
            Badge badge = new Badge(existingBadge.getJSONRepresentation(), epoch, existingBadge, catalogCharacteristicService, tenantID);
            badge.validate();
            modifiedBadge = badge;
          }
        catch (JSONUtilitiesException|GUIManagerException e)
          {
            modifiedBadge = new IncompleteObject(existingBadge.getJSONRepresentation(), epoch, tenantID);
          }

        //
        //  changed?
        //

        if (existingBadge.getAccepted() != modifiedBadge.getAccepted())
          {
            modifiedBadges.add(modifiedBadge);
          }
      }

    /****************************************
    *
    *  update
    *
    ****************************************/

    for (GUIManagedObject modifiedBadge : modifiedBadges)
      {
        badgeService.putGUIManagedObject(modifiedBadge, date, false, null);
      }
  }

  /*****************************************
  *
  *  processDownloadReport
  *
  *****************************************/

  void processDownloadReport(String userID, JSONObject jsonRoot, JSONObject jsonResponse, HttpExchange exchange) throws IOException
  {
    String reportID = JSONUtilities.decodeString(jsonRoot, "id", true);
    JSONArray filters = JSONUtilities.decodeJSONArray(jsonRoot, "criteria", false);
    Integer percentage = JSONUtilities.decodeInteger(jsonRoot, "percentage", false);
    Integer topRows = JSONUtilities.decodeInteger(jsonRoot, "topRows", false);
    JSONArray header = JSONUtilities.decodeJSONArray(jsonRoot, "header", false);
    Integer tenantID = JSONUtilities.decodeInteger(jsonRoot, "tenantID", true);

    GUIManagedObject report1 = reportService.getStoredReport(reportID);
    log.trace("Looking for "+reportID+" and got "+report1);
    String responseCode = null;

    if (report1 == null)
      {
        responseCode = "reportNotFound";
      }
    else
      {
        try
          {
            Report report = new Report(report1.getJSONRepresentation(), epochServer.getKey(), null, tenantID);
            String reportName = report.getName();

            String outputPath = ReportService.getReportOutputPath(tenantID);
            String fileExtension = Deployment.getDeployment(tenantID).getReportManagerFileExtension();

            File folder = new File(outputPath);
            String csvFilenameRegex = reportName+ "_"+ ".*"+ "\\."+ fileExtension+ReportUtils.ZIP_EXTENSION;

            File[] listOfFiles = folder.listFiles(new FileFilter(){
              @Override
                  public boolean accept(File f) {
                return Pattern.compile(csvFilenameRegex).matcher(f.getName()).matches();
              }});

              File reportFile = null;

              long lastMod = Long.MIN_VALUE;
              if(listOfFiles != null && listOfFiles.length != 0) {
                for (int i = 0; i < listOfFiles.length; i++) {
                  if (listOfFiles[i].isFile()) {
                    if(listOfFiles[i].lastModified() > lastMod) {
                      reportFile = listOfFiles[i];
                      lastMod = reportFile.lastModified();
                    }
                  }
                }
          } else {
            responseCode = "Cant find report with that name";
          }

          String finalFileName = reportFile.getAbsolutePath();
              
          File filterOutTmpFile = reportFile;
          File percentageOutTmpFile = reportFile;
          File topRowsOutTmpFile = reportFile;
          File headerOutTmpFile = reportFile;
          File finalZipFile = null;
          File internalFile = null;
          
          boolean isFilters = false;
          boolean isPercentage = false;
          boolean isTop = false;
          boolean isHeader = false;

          if(reportFile != null) {
            if(reportFile.length() > 0) {
              try {
                String unzippedFile = null;

                if (filters != null || percentage != null || topRows != null || header != null) {
                  filterOutTmpFile = File.createTempFile("tempReportFilter.", ".csv");
                  percentageOutTmpFile = File.createTempFile("tempReportPercentage.", ".csv");
                  topRowsOutTmpFile = File.createTempFile("tempReportTopRows.", ".csv");
                  headerOutTmpFile = File.createTempFile("tempReportHeader.", ".csv");
                  unzippedFile = ReportUtils.unzip(reportFile.getAbsolutePath());

                  if (filters != null && !filters.isEmpty())
                    {
                      List<String> colNames = new ArrayList<>();
                      List<List<String>> colsValues = new ArrayList<>();
                      for (int i=0; i<filters.size(); i++)
                        {
                          JSONObject filterJSON = (JSONObject) filters.get(i);
                          Object nameOfColumnObj = filterJSON.get("criterionField");
                          if (!(nameOfColumnObj instanceof String))
                            {
                              log.warn("criterionField is not a String : " + nameOfColumnObj.getClass().getName());
                              colNames.add("");
                              break;
                            }
                          String nameOfColumn = (String) nameOfColumnObj;
                          colNames.add(nameOfColumn);

                          Object argumentObj = filterJSON.get("argument");
                          if (!(argumentObj instanceof JSONObject))
                            {
                              log.warn("argument is not a JSONObject : " + argumentObj.getClass().getName());
                              colsValues.add(new ArrayList<>());
                              break;
                            }
                          JSONObject argument = (JSONObject) argumentObj;
                          String valueType = (String) argument.get("valueType");
                          Object value = (Object) argument.get("value");

                          List<String> valuesOfColumns;
                          switch (valueType) 
                          {
                            case "simpleSelect.string":
                              if (!(value instanceof String))
                                {
                                  log.warn("value of column " + nameOfColumn + " is not a String : " + value);
                                  colsValues.add(new ArrayList<>());
                                }
                              else
                                {
                                  String valueSimpleSelect = (String) value;
                                  valuesOfColumns = new ArrayList<>();
                                  valuesOfColumns.add(valueSimpleSelect);
                                  colsValues.add(valuesOfColumns);
                                }
                              break;

                            case "multiple.string":
                              valuesOfColumns = new ArrayList<>();
                              if (!(value instanceof JSONArray))
                                {
                                  log.warn("value of column " + nameOfColumn + " is not an array : " + value.getClass().getName());
                                }
                              else
                                {
                                  JSONArray valueMultiple = (JSONArray) value;
                                  for (int j=0; j<valueMultiple.size(); j++)
                                    {
                                      Object obj = valueMultiple.get(j);
                                      if (!(obj instanceof String))
                                        {
                                          log.warn("value is not a String : " + obj.getClass().getName());
                                        }
                                      else
                                        {
                                          valuesOfColumns.add((String) obj);
                                        }
                                    }
                                }
                              colsValues.add(valuesOfColumns);
                              break;

                            default:
                              log.info("Received unsupported valueType : " + valueType);
                              break;
                          }
                        }

                      ReportUtils.filterReport(unzippedFile, filterOutTmpFile.getAbsolutePath(), colNames, colsValues, Deployment.getReportManagerCsvSeparator(), Deployment.getReportManagerFieldSurrounder());
                      isFilters = true;
                    }
                  else
                    {
                      filterOutTmpFile = new File(unzippedFile);
                    }

                  if (percentage != null) 
                    {
                      ReportUtils.extractPercentageOfRandomRows(filterOutTmpFile.getAbsolutePath(), percentageOutTmpFile.getAbsolutePath(), percentage);
                      isPercentage = true;
                    }
                  else
                    {
                      percentageOutTmpFile = filterOutTmpFile;
                    }

                  if (topRows != null) 
                    {
                      ReportUtils.extractTopRows(percentageOutTmpFile.getAbsolutePath(), topRowsOutTmpFile.getAbsolutePath(), topRows);
                      isTop = true;
                    }
                  else
                    {
                      topRowsOutTmpFile = percentageOutTmpFile;
                    }

                  if (header != null)
                    {
                      List<String> columnNames = new ArrayList<>();
                      for (int i=0; i<header.size(); i++)
                        {
                          String nameOfColumn = (String) header.get(i);
                          columnNames.add(nameOfColumn);
                        }
                      ReportUtils.subsetOfCols(topRowsOutTmpFile.getAbsolutePath(), headerOutTmpFile.getAbsolutePath(), columnNames, Deployment.getReportManagerCsvSeparator(), Deployment.getReportManagerFieldSurrounder());
                      isHeader = true;
                    }
                  else
                    {
                      headerOutTmpFile = topRowsOutTmpFile;
                    }

                  finalFileName = finalFileName.substring(0, finalFileName.length()-8);
                  
                  if(isFilters) {
                    finalFileName = finalFileName+"_Filter";
                  }
                  
                  if(isPercentage) {
                    finalFileName = finalFileName+"_Percentage";
                  }
                  
                  if(isTop) {
                    finalFileName = finalFileName+"_Top";
                  }
                  
                  if(isHeader) {
                    finalFileName = finalFileName+"_Column";
                  }
                  
                  
                  internalFile = File.createTempFile(finalFileName + ".", ".csv");
                                 
                  try
                  {
                      String strLine;
                      BufferedReader br = new BufferedReader(new FileReader(headerOutTmpFile));
                      BufferedWriter bw = new BufferedWriter(new FileWriter(internalFile));
                      while ((strLine = br.readLine()) != null) 
                      {
                              bw.write(strLine);
                              bw.write("\n");
                          }
                      br.close();
                      bw.close();
                
                  }
                  catch (FileNotFoundException e) 
                  {
                      log.error("File doesn't exist", e);
                  }
                  
                  finalZipFile = File.createTempFile(finalFileName + ".", ".zip");
                  String finalZipFileName = finalZipFile.getAbsolutePath();
                  ReportUtils.zipFile(internalFile.getAbsolutePath(), finalZipFileName);
                  reportFile = new File(finalZipFileName);
                }

                FileInputStream fis = new FileInputStream(reportFile);
                exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                exchange.getResponseHeaders().add("Content-Disposition", "attachment; filename=" + reportFile.getName());
                exchange.sendResponseHeaders(200, reportFile.length());
                OutputStream os = exchange.getResponseBody();
                byte data[] = new byte[10_000]; // allow some bufferization
                int length;
                while ((length = fis.read(data)) != -1) {
                  os.write(data, 0, length);
                }
                fis.close();
                os.flush();
                os.close();
              } catch (Exception excp) {
                StringWriter stackTraceWriter = new StringWriter();
                excp.printStackTrace(new PrintWriter(stackTraceWriter, true));
                log.warn("Exception processing REST api: {}", stackTraceWriter.toString());
              }

              if (filters != null || percentage != null || topRows != null || header != null)
              {
                if (filterOutTmpFile != null) filterOutTmpFile.delete();
                if (percentageOutTmpFile != null) percentageOutTmpFile.delete();
                if (topRowsOutTmpFile != null) topRowsOutTmpFile.delete();
                if (headerOutTmpFile != null) headerOutTmpFile.delete();
                if (finalZipFile != null) finalZipFile.delete();
                if (internalFile != null) internalFile.delete();
              }

            } else {
              responseCode = "Report size is 0, report file is empty";
            }
          } else {
            responseCode = "Report is null, cant find this report";
          }
        } catch (GUIManagerException e)
        {
          log.info("Exception when building report from "+report1+" : "+e.getLocalizedMessage());
          responseCode = "internalError";
        }
      }
    
    if(responseCode != null) {
      try {
        jsonResponse.put("responseCode", responseCode);
        exchange.sendResponseHeaders(200, 0);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
        writer.write(jsonResponse.toString());
        writer.close();
        exchange.close();
      }catch(Exception e) {
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());
      }
    }
  }

  /*****************************************
  *
  *  processGetDashboardCounts
  *
  *****************************************/

  JSONObject processGetDashboardCounts(String userID, JSONObject jsonRoot, boolean includeArchived, int tenantID)
  {
    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("journeyCount", journeyCount(GUIManagedObjectType.Journey, tenantID));
    response.put("campaignCount", journeyCount(GUIManagedObjectType.Campaign, tenantID));
    //response.put("workflowCount", journeyCount(GUIManagedObjectType.Workflow, tenantID));
   //response.put("loyaltyWorkflowCount", journeyCount(GUIManagedObjectType.LoyaltyWorkflow, tenantID));
    response.put("bulkCampaignCount", journeyCount(GUIManagedObjectType.BulkCampaign, tenantID));
    response.put("segmentationDimensionCount", segmentationDimensionService.getStoredSegmentationDimensions(includeArchived, tenantID).size());
    response.put("pointCount", pointService.getStoredPoints(includeArchived, tenantID).size());
    response.put("offerCount", offerService.getStoredOffers(includeArchived, tenantID).size());
    response.put("scoringStrategyCount", scoringStrategyService.getStoredScoringStrategies(includeArchived, tenantID).size());
    response.put("presentationStrategyCount", presentationStrategyService.getStoredPresentationStrategies(includeArchived, tenantID).size());
    response.put("dnboMatrixCount", dnboMatrixService.getStoredDNBOMatrixes(includeArchived, tenantID).size());
    response.put("callingChannelCount", callingChannelService.getStoredCallingChannels(includeArchived, tenantID).size());
    response.put("salesChannelCount", salesChannelService.getStoredSalesChannels(includeArchived, tenantID).size());
    response.put("sourceAddressCount", sourceAddressService.getStoredSourceAddresses(includeArchived, tenantID).size());
    response.put("supplierCount", supplierService.getStoredSuppliers(includeArchived, tenantID).size());
    response.put("productCount", productService.getStoredProducts(includeArchived, tenantID).size());
    response.put("voucherTypeCount", voucherTypeService.getStoredVoucherTypes(includeArchived, tenantID).size());
    response.put("voucherCount", voucherService.getStoredVouchers(includeArchived, tenantID).size());
    response.put("catalogCharacteristicCount", catalogCharacteristicService.getStoredCatalogCharacteristics(includeArchived, tenantID).size());
    response.put("journeyObjectiveCount", journeyObjectiveService.getStoredJourneyObjectives(includeArchived, tenantID).size());
    response.put("offerObjectiveCount", offerObjectiveService.getStoredOfferObjectives(includeArchived, tenantID).size());
    response.put("productTypeCount", productTypeService.getStoredProductTypes(includeArchived, tenantID).size());
    response.put("deliverableCount", deliverableService.getStoredDeliverables(includeArchived, tenantID).size());    
    response.put("reportsCount", reportService.getStoredReports(includeArchived, tenantID).size());
    response.put("walletsCount", pointService.getStoredPoints(includeArchived, tenantID).size() + tokenTypeService.getStoredTokenTypes(includeArchived, tenantID).size() + voucherTypeService.getStoredVoucherTypes(includeArchived, tenantID).size());
    response.put("ucgRuleCount", ucgRuleService.getStoredUCGRules(includeArchived, tenantID).size());
    response.put("targetCount", targetService.getStoredTargets(includeArchived, tenantID).size());
    response.put("exclusionInclusionCount", exclusionInclusionTargetService.getStoredExclusionInclusionTargets(includeArchived, tenantID).size());
    response.put("segmentContactPolicies",segmentContactPolicyService.getStoredSegmentContactPolicys(includeArchived, tenantID).size());
    response.put("contactPolicyCount", contactPolicyService.getStoredContactPolicies(includeArchived, tenantID).size());
    response.put("communicationChannelCount", Deployment.getCommunicationChannels().size());
    response.put("communicationChannelBlackoutCount", communicationChannelBlackoutService.getStoredCommunicationChannelBlackouts(includeArchived, tenantID).size());
    response.put("resellerCount", resellerService.getStoredResellers(includeArchived, tenantID).size());
    
    //
    //  LoyaltyProgram
    //
    
    int loyaltyProgramCount = 0;
    int loyaltyProgramChallengeCount = 0;
    int loyaltyProgramMissionCount = 0;
    for (GUIManagedObject guiManagedObject : loyaltyProgramService.getStoredLoyaltyPrograms(includeArchived, tenantID))
      {
        JSONObject loyaltyProFull = loyaltyProgramService.generateResponseJSON(guiManagedObject, true, SystemTime.getCurrentTime());
        LoyaltyProgramType type = LoyaltyProgramType.fromExternalRepresentation(JSONUtilities.decodeString(loyaltyProFull, "loyaltyProgramType"));
        if (type == LoyaltyProgramType.POINTS)
          {
            loyaltyProgramCount++;
          }
        else if (type == LoyaltyProgramType.CHALLENGE)
          {
            loyaltyProgramChallengeCount++;
          }
        else if (type == LoyaltyProgramType.MISSION)
          {
            loyaltyProgramMissionCount++;
          }
      }
    response.put("loyaltyProgramCount", loyaltyProgramCount);
    response.put("loyaltyProgramChallengeCount", loyaltyProgramChallengeCount);
    response.put("loyaltyProgramMissionCount", loyaltyProgramMissionCount);
    
    //
    //  areaAvailablity
    //
    
    if (jsonRoot.containsKey("areaAvailability"))
      {
        int mailTemplateCount = 0;
        int smsTemplateCount = 0;
        int dialogTemplateCount = 0;
        int pushTemplateCount = 0;
        JSONArray areaAvailability = JSONUtilities.decodeJSONArray(jsonRoot, "areaAvailability", false);
        Collection<GUIManagedObject> mailTemplates = subscriberMessageTemplateService.getStoredMailTemplates(true,
            includeArchived, tenantID);
        if (mailTemplates.size() != 0 && mailTemplates != null)
          {
            for (GUIManagedObject template : mailTemplates)
              {
                if (template instanceof MailTemplate)
                  {
                    JSONArray templeteAreaAvailability = (JSONArray) ((MailTemplate) template).getJSONRepresentation()
                        .get("areaAvailability");
                    if (templeteAreaAvailability == null || templeteAreaAvailability.isEmpty())
                      {
                        mailTemplateCount += 1;
                      }
                    else
                      {
                        for (int i = 0; i < areaAvailability.size(); i++)
                          {
                            if (templeteAreaAvailability.contains(areaAvailability.get(i)))
                              {
                                mailTemplateCount += 1;
                              }
                            else
                              {
                                continue;
                              }
                          }

                      }
                  }
              }
          }
        Collection<GUIManagedObject> smsTemplates = subscriberMessageTemplateService.getStoredSMSTemplates(true,
            includeArchived, tenantID);
        if (smsTemplates.size() != 0 && smsTemplates != null)
          {
            for (GUIManagedObject template : smsTemplates)
              {
                if (template instanceof SMSTemplate)
                  {
                    JSONArray templeteAreaAvailability = (JSONArray) ((SMSTemplate) template).getJSONRepresentation()
                        .get("areaAvailability");
                    if (templeteAreaAvailability == null || templeteAreaAvailability.isEmpty())
                      {
                        smsTemplateCount += 1;
                      }
                    else
                      {
                        for (int i = 0; i < areaAvailability.size(); i++)
                          {
                            if (templeteAreaAvailability.contains(areaAvailability.get(i)))
                              {
                                smsTemplateCount += 1;
                              }
                            else
                              {
                                continue;
                              }
                          }

                      }
                  }
              }
          }
        Collection<GUIManagedObject> dialogTemplates = subscriberMessageTemplateService.getStoredDialogTemplates(true,
            includeArchived, tenantID);
        if (dialogTemplates.size() != 0 && dialogTemplates != null)
          {
            for (GUIManagedObject template : dialogTemplates)
              {
                if (template instanceof DialogTemplate)
                  {
                    JSONArray templeteAreaAvailability = (JSONArray)((DialogTemplate) template).getJSONRepresentation()
                        .get("areaAvailability");
                    if (templeteAreaAvailability == null || templeteAreaAvailability.isEmpty())
                      {
                        dialogTemplateCount += 1;
                      }
                    else
                      {
                        for (int i = 0; i < areaAvailability.size(); i++)
                          {
                            if (templeteAreaAvailability.contains(areaAvailability.get(i)))
                              {
                                dialogTemplateCount += 1;
                              }
                            else
                              {
                                continue;
                              }
                          }

                      }
                  }
              }
          }
        Collection<GUIManagedObject> pushTemplates = subscriberMessageTemplateService.getStoredPushTemplates(true,
            includeArchived, tenantID);
        if (pushTemplates.size() != 0 && pushTemplates != null)
          {
            for (GUIManagedObject template : pushTemplates)
              {
                if (template instanceof PushTemplate)
                  {
                    JSONArray templeteAreaAvailability = (JSONArray)((PushTemplate) template).getJSONRepresentation()
                        .get("areaAvailability");
                    if (templeteAreaAvailability == null || templeteAreaAvailability.isEmpty())
                      {
                        pushTemplateCount += 1;
                      }
                    else
                      {
                        for (int i = 0; i < areaAvailability.size(); i++)
                          {
                            if (templeteAreaAvailability.contains(areaAvailability.get(i)))
                              {
                                pushTemplateCount += 1;
                              }
                            else
                              {
                                continue;
                              }
                          }

                      }
                  }
              }
          }
        response.put("mailTemplateCount", mailTemplateCount);
        response.put("smsTemplateCount", smsTemplateCount);
        response.put("pushTemplateCount", pushTemplateCount);
        response.put("dialogTemplateCount", dialogTemplateCount);
      }
    else {
      response.put("mailTemplateCount", subscriberMessageTemplateService.getStoredMailTemplates(true, includeArchived, tenantID).size());
      response.put("smsTemplateCount", subscriberMessageTemplateService.getStoredSMSTemplates(true, includeArchived, tenantID).size());
      response.put("pushTemplateCount", subscriberMessageTemplateService.getStoredPushTemplates(true, includeArchived, tenantID).size());
      response.put("dialogTemplateCount", subscriberMessageTemplateService.getStoredDialogTemplates(true, includeArchived, tenantID).size());
      
    }
    
    if (jsonRoot.containsKey("areaAvailability"))
      {
        int workflowCount = 0;
        JSONArray areaAvailability = JSONUtilities.decodeJSONArray(jsonRoot, "areaAvailability", false);
        Collection<GUIManagedObject> journeys = journeyService.getStoredJourneys(includeArchived, tenantID);
        if (journeys.size() != 0 && journeys != null)
          {
            for (GUIManagedObject journey : journeys)
              {
                if (journey instanceof Journey && journey.getGUIManagedObjectType() == GUIManagedObjectType.Workflow && ((Journey) journey).getJSONRepresentation()
                    .get("areaAvailability") != null)
                  {
                    JSONArray journeyAreaAvailability = (JSONArray) ((Journey) journey).getJSONRepresentation()
                        .get("areaAvailability");
                    if (journeyAreaAvailability == null || journeyAreaAvailability.isEmpty())
                      {
                        workflowCount += 1;
                      }
                    else
                      {
                        for (int i = 0; i < areaAvailability.size(); i++)
                          {
                            if (journeyAreaAvailability.contains(areaAvailability.get(i)))
                              {
                                workflowCount += 1;
                                break;
                              }
                            }

                      }
                  }
              }
          }
        response.put("workflowCount", workflowCount);
      }
    else {
      response.put("workflowCount", journeyCount(GUIManagedObjectType.Workflow, tenantID));
    }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  journeyCount
  *
  *****************************************/

  private int journeyCount(GUIManagedObjectType journeyType, int tenantID)
  {
    int result = 0;
    for (GUIManagedObject journey : journeyService.getStoredJourneys(tenantID))
      {
        if (journey.getGUIManagedObjectType() == journeyType)
          {
            result += 1;
          }
      }
    return result;
  }
  

  /*****************************************
  *
  *  processGetReportGlobalConfiguration
  *
  *****************************************/

  JSONObject processGetReportGlobalConfiguration(String userID, JSONObject jsonRoot, int tenantID)
  {
    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();
    HashMap<String,Object> globalConfig = new HashMap<String,Object>();
    globalConfig.put("reportManagerZookeeperDir",   Deployment.getReportManagerZookeeperDir());
    globalConfig.put("reportManagerOutputPath",     ReportService.getReportOutputPath(tenantID));
    globalConfig.put("reportManagerFileDateFormat", Deployment.getDeployment(tenantID).getReportManagerFileDateFormat());
    globalConfig.put("reportManagerFileExtension",  Deployment.getDeployment(tenantID).getReportManagerFileExtension());
    globalConfig.put("reportManagerCsvSeparator",   Deployment.getReportManagerCsvSeparator());
    globalConfig.put("reportManagerStreamsTempDir", Deployment.getDeployment(tenantID).getReportManagerStreamsTempDir());
    response.put("reportGlobalConfiguration", JSONUtilities.encodeObject(globalConfig));
    response.put("responseCode", "ok");
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetReportList
  *
  *****************************************/

  JSONObject processGetReportList(String userID, JSONObject jsonRoot, boolean includeArchived, int tenantID)
  {
    if (log.isTraceEnabled())
      {
        log.trace("In processGetReportList : " + jsonRoot);
      }
    Date now = SystemTime.getCurrentTime();
    List<JSONObject> reports = new ArrayList<JSONObject>();
    Collection <GUIManagedObject> reportObjects = new ArrayList<GUIManagedObject>();
    
    if (jsonRoot.containsKey("ids"))
      {
  JSONArray reportIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
  for (int i = 0; i < reportIDs.size(); i++)
    {
      String reportID = reportIDs.get(i).toString();
      GUIManagedObject report = reportService.getStoredReport(reportID, includeArchived);
      if (report != null && report.getTenantID() == tenantID)
        {
          reportObjects.add(report);
        }
    }
      }
    else
      {
        reportObjects = reportService.getStoredReports(includeArchived, tenantID);
      }
    for (GUIManagedObject report : reportObjects)
      {
        if (log.isTraceEnabled())
          {
            log.trace("In processGetReportList, adding : " + report);
          }
        JSONObject reportResponse = reportService.generateResponseJSON(report, true, now);
        reportResponse.put("isRunning", reportService.isReportRunning(((Report)report).getName(), tenantID));
        reports.add(reportResponse);
      }
    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("reports", JSONUtilities.encodeArray(reports));
    if (log.isTraceEnabled())
      {
        log.trace("res : " + response.get("reports"));
      }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processLaunchReport
  *
  *****************************************/

  JSONObject processLaunchReport(String userID, JSONObject jsonRoot, int tenantID)
  {
    log.trace("In processLaunchReport : "+jsonRoot);
    HashMap<String,Object> response = new HashMap<String,Object>();
    String reportID = JSONUtilities.decodeString(jsonRoot, "id", true);
    boolean backendSimulator = JSONUtilities.decodeBoolean(jsonRoot, "backendsimulator", Boolean.FALSE);
    Report report = (Report) reportService.getStoredReport(reportID);
    log.trace("Looking for "+reportID+" and got "+report);
    String responseCode;
    if (report == null)
      {
        responseCode = "reportNotFound";
      }
    else
      {
        if (reportService.isReportRunning(report.getName(), tenantID))
          {
            responseCode = "reportIsAlreadyRunning";
          }
        else
          {
            reportService.launchReport(report.getName(), backendSimulator, tenantID);
            responseCode = "ok";
          }
      }
    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutReport
  *
  *****************************************/

  JSONObject processPutReport(String userID, JSONObject jsonRoot, int tenantID)
  {
    log.trace("In processPutReport : "+jsonRoot);
    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();
    String reportID = JSONUtilities.decodeString(jsonRoot, "id", false);
    Boolean dryRun = false;
    

    /*****************************************
    *
    *  dryRun
    *
    *****************************************/
    if (jsonRoot.containsKey("dryRun")) {
      dryRun = JSONUtilities.decodeBoolean(jsonRoot, "dryRun", false);
    }
    if (reportID == null)
      {
        reportID = reportService.generateReportID();
        jsonRoot.put("id", reportID);
      }
    log.trace("ID : "+reportID);
    GUIManagedObject existingReport = reportService.getStoredReport(reportID);
    if (existingReport != null && existingReport.getReadOnly())
      {
        log.trace("existingReport : "+existingReport);
        response.put("id", existingReport.getGUIManagedObjectID());
        response.put("accepted", existingReport.getAccepted());
        response.put("valid", existingReport.getAccepted());
        response.put("processing", reportService.isActiveReport(existingReport, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }
    if (existingReport != null)
      {
        if (existingReport instanceof Report)
          {
            Report existingRept = (Report) existingReport;
            // Is the new effective scheduling valid ?
            List<SchedulingInterval> availableScheduling = existingRept.getAvailableScheduling();
            if (availableScheduling != null) {
              // Check if the effectiveScheduling is valid
              JSONArray effectiveSchedulingJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, Report.EFFECTIVE_SCHEDULING, false);
              if (effectiveSchedulingJSONArray != null) { 
                for (int i=0; i<effectiveSchedulingJSONArray.size(); i++) {
                  String schedulingIntervalStr = (String) effectiveSchedulingJSONArray.get(i);
                  SchedulingInterval eSchedule = SchedulingInterval.fromExternalRepresentation(schedulingIntervalStr);
                  log.trace("Checking that "+eSchedule+" is allowed");
                  if (! availableScheduling.contains(eSchedule)) {
                	 
                    response.put("id", jsonRoot.get("id"));
                    response.put("responseCode", "reportNotValid");
                    response.put("responseMessage", "scheduling "+eSchedule+" is not valid");
                    StringBuffer respMsg = new StringBuffer("scheduling "+eSchedule+" should be part of [ ");
                    for (SchedulingInterval aSched : availableScheduling) {
                      respMsg.append(aSched+" ");
                    }
                    respMsg.append("]");
                    response.put("responseParameter", respMsg.toString());
                    return JSONUtilities.encodeObject(response);
                  }
                 

                }
              }
            }
          }
      }
    JSONArray effectiveScheduling = new JSONArray();
    JSONArray effectiveSchedulingJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, Report.EFFECTIVE_SCHEDULING, false);
    if (effectiveSchedulingJSONArray != null) { 
        for (int i=0; i<effectiveSchedulingJSONArray.size(); i++) {
        	 String schedulingIntervalStr = (String) effectiveSchedulingJSONArray.get(i);
        	 SchedulingInterval eSchedule = SchedulingInterval.fromExternalRepresentation(schedulingIntervalStr);
              if(eSchedule.equals(SchedulingInterval.NONE))
           		continue;
            effectiveScheduling.add(schedulingIntervalStr);
        }
        }
        
    jsonRoot.remove(Report.EFFECTIVE_SCHEDULING);
    jsonRoot.put(Report.EFFECTIVE_SCHEDULING, effectiveScheduling);  
    long epoch = epochServer.getKey();
    try
      {    	
        Report report = new Report(jsonRoot, epoch, existingReport, tenantID);
        log.trace("new report : "+report);
        if (!dryRun)
          {
        	            reportService.putReport(report, (existingReport == null), userID);
          }
      
        response.put("id", report.getReportID());
        response.put("accepted", report.getAccepted());
        response.put("valid", report.getAccepted());
        response.put("processing", reportService.isActiveReport(report, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());
        response.put("id", jsonRoot.get("id"));
        response.put("responseCode", "reportNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }  

  /*****************************************
  *
  *  getPartnerTypes
  *
  *****************************************/

  JSONObject processGetPartnerTypes(String userID, JSONObject jsonRoot, int tenantID)
  {
    /*****************************************
    *
    *  retrieve PartnerTypes
    *
    *****************************************/

    List<JSONObject> partnerTypes = new ArrayList<JSONObject>();
    for (PartnerType partnerType : Deployment.getDeployment(tenantID).getPartnerTypes().values())
      {
        JSONObject partnerTypeJSON = partnerType.getJSONRepresentation();
        partnerTypes.add(partnerTypeJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("partnerTypes", JSONUtilities.encodeArray(partnerTypes));
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  getBillingModes
  *
  *****************************************/

  JSONObject processGetBillingModes(String userID, JSONObject jsonRoot, int tenantID)
  {
    /*****************************************
    *
    *  retrieve BillingModes
    *
    *****************************************/

    List<JSONObject> billingModes = new ArrayList<JSONObject>();
    for (BillingMode billingMode : Deployment.getDeployment(tenantID).getBillingModes().values())
      {
        JSONObject billingModeJSON = billingMode.getJSONRepresentation();
        billingModes.add(billingModeJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("billingModes", JSONUtilities.encodeArray(billingModes));
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processLoyaltyProgramOptInOut
  *
  *****************************************/

  JSONObject processLoyaltyProgramOptInOut(JSONObject jsonRoot, boolean optIn, int tenantID)throws GUIManagerException {
    
    /****************************************
     *
     * response
     *
     ****************************************/

    HashMap<String, Object> response = new HashMap<String, Object>();

    /****************************************
     *
     * argument
     *
     ****************************************/
    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", false);

    /*****************************************
     *
     * resolve subscriberID
     *
     *****************************************/

    String subscriberID = resolveSubscriberID(customerID, tenantID);

    String loyaltyProgramID = JSONUtilities.decodeString(jsonRoot, "loyaltyProgram", false); 
    String loyaltyProgramRequestID = "";

    /*****************************************
     *
     * getSubscriberProfile - no history
     *
     *****************************************/

    try
      {
        SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false);
        if (subscriberProfile == null)
          {
            response.put("responseCode", RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
            response.put("responseMessage", RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
            if (log.isDebugEnabled())
              log.debug("SubscriberProfile is null for subscriberID {}", subscriberID);
            return JSONUtilities.encodeObject(response);
          }

        Date now = SystemTime.getCurrentTime();

        String activeLoyaltyProgramID = null;
        for (LoyaltyProgram loyaltyProgram : loyaltyProgramService.getActiveLoyaltyPrograms(now, tenantID))
          {
            if (loyaltyProgramID.equals(loyaltyProgram.getLoyaltyProgramID()))
              {
                activeLoyaltyProgramID = loyaltyProgram.getGUIManagedObjectID();
                break;
              }
          }
        if (activeLoyaltyProgramID == null)
          {
            response.put("responseCode", RESTAPIGenericReturnCodes.LOYALTY_PROJECT_NOT_FOUND.getGenericResponseCode());
            response.put("responseMessage",
                RESTAPIGenericReturnCodes.LOYALTY_PROJECT_NOT_FOUND.getGenericResponseMessage());
            return JSONUtilities.encodeObject(response);
          }

        Serializer<StringKey> keySerializer = StringKey.serde().serializer();
        Serializer<LoyaltyProgramRequest> valueSerializer = LoyaltyProgramRequest.serde().serializer();

        String featureID = (optIn ? API.loyaltyProgramOptIn : API.loyaltyProgramOptOut).getMethodIndex() + "";
        String operation = optIn ? "opt-in" : "opt-out";
        String moduleID = DeliveryRequest.Module.REST_API.getExternalRepresentation();
        loyaltyProgramRequestID = zuks.getStringKey();

        /*****************************************
         *
         * request
         *
         *****************************************/

        // Build a json doc to create the LoyaltyProgramRequest
        HashMap<String, Object> request = new HashMap<String, Object>();

        // Fields for LoyaltyProgramRequest
        request.put("operation", operation);
        request.put("loyaltyProgramRequestID", loyaltyProgramRequestID);
        request.put("loyaltyProgramID", activeLoyaltyProgramID);
        request.put("eventDate", now);

        // Fields for DeliveryRequest
        request.put("deliveryRequestID", loyaltyProgramRequestID);
        request.put("subscriberID", subscriberID);
        request.put("eventID", "0"); // No event here
        request.put("moduleID", moduleID);
        request.put("featureID", featureID);
        request.put("deliveryType", "loyaltyProgramFulfillment");
        JSONObject valueRes = JSONUtilities.encodeObject(request);

        LoyaltyProgramRequest loyaltyProgramRequest = new LoyaltyProgramRequest(subscriberProfile,subscriberGroupEpochReader,valueRes, null, tenantID);
        loyaltyProgramRequest.forceDeliveryPriority(DELIVERY_REQUEST_PRIORITY);
        String topic = Deployment.getDeliveryManagers().get(loyaltyProgramRequest.getDeliveryType()).getRequestTopic(loyaltyProgramRequest.getDeliveryPriority());

        // Write it to the right topic
        kafkaProducer
            .send(new ProducerRecord<byte[], byte[]>(topic, keySerializer.serialize(topic, new StringKey(subscriberID)),
                valueSerializer.serialize(topic, loyaltyProgramRequest)));

        //
        // TODO how do we deal with the offline errors ?
        //

        // TODO trigger event (for campaign) ?

      }
    catch (SubscriberProfileServiceException e)
      {
        log.error("unable to process request processLoyaltyProgramOptInOut {} ", e.getMessage());
        throw new GUIManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseMessage(), "21");
      }

    /*****************************************
     *
     * decorate and response
     *
     *****************************************/
    response.put("deliveryRequestID", loyaltyProgramRequestID);
    response.put("responseCode", RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
    response.put("responseMessage", RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
    return JSONUtilities.encodeObject(response);
 }
  
  
}

