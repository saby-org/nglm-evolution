/*****************************************************************************
*
*  GUIManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.elasticsearch.client.RestHighLevelClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.SubscriberIDService;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramType;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;
import com.evolving.nglm.evolution.Report.SchedulingInterval;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.sun.net.httpserver.HttpExchange;

public class GUIManagerLoyaltyReporting extends GUIManager
{

  private static final Logger log = LoggerFactory.getLogger(GUIManagerLoyaltyReporting.class);

  public GUIManagerLoyaltyReporting(JourneyService journeyService, SegmentationDimensionService segmentationDimensionService, PointService pointService, OfferService offerService, ReportService reportService, PaymentMeanService paymentMeanService, ScoringStrategyService scoringStrategyService, PresentationStrategyService presentationStrategyService, CallingChannelService callingChannelService, SalesChannelService salesChannelService, SourceAddressService sourceAddressService, SupplierService supplierService, ProductService productService, CatalogCharacteristicService catalogCharacteristicService, ContactPolicyService contactPolicyService, JourneyObjectiveService journeyObjectiveService, OfferObjectiveService offerObjectiveService, ProductTypeService productTypeService, UCGRuleService ucgRuleService, DeliverableService deliverableService, TokenTypeService tokenTypeService, VoucherTypeService voucherTypeService, VoucherService voucherService, SubscriberMessageTemplateService subscriberTemplateService, SubscriberProfileService subscriberProfileService, SubscriberIDService subscriberIDService, DeliverableSourceService deliverableSourceService, UploadedFileService uploadedFileService, TargetService targetService, CommunicationChannelBlackoutService communicationChannelBlackoutService, LoyaltyProgramService loyaltyProgramService, ResellerService resellerService, ExclusionInclusionTargetService exclusionInclusionTargetService, SegmentContactPolicyService segmentContactPolicyService, CriterionFieldAvailableValuesService criterionFieldAvailableValuesService, DNBOMatrixService dnboMatrixService, DynamicCriterionFieldService dynamicCriterionFieldService, DynamicEventDeclarationsService dynamicEventDeclarationsService, JourneyTemplateService journeyTemplateService, KafkaResponseListenerService<StringKey,PurchaseFulfillmentRequest> purchaseResponseListenerService, SharedIDService subscriberGroupSharedIDService, ZookeeperUniqueKeyServer zuks, int httpTimeout, KafkaProducer<byte[], byte[]> kafkaProducer, RestHighLevelClient elasticsearch, SubscriberMessageTemplateService subscriberMessageTemplateService, String getCustomerAlternateID, GUIManagerContext guiManagerContext, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, ReferenceDataReader<String,JourneyTrafficHistory> journeyTrafficReader, ReferenceDataReader<String,RenamedProfileCriterionField> renamedProfileCriterionFieldReader)
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
  *  processGetLoyaltyProgram
  *
  *****************************************/

  JSONObject processGetLoyaltyProgram(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (loyaltyProgram != null) ? "ok" : "loyaltyProgramNotFound");
    if (loyaltyProgram != null) response.put("loyaltyProgram", loyaltyProgramJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutLoyaltyProgram
  *
  *****************************************/

  JSONObject processPutLoyaltyProgram(String userID, JSONObject jsonRoot)
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
        switch (LoyaltyProgramType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "loyaltyProgramType", true)))
        {
          case POINTS:
            loyaltyProgram = new LoyaltyProgramPoints(jsonRoot, epoch, existingLoyaltyProgram, catalogCharacteristicService);
            break;

//          case BADGES:
//            // TODO
//            break;

          case Unknown:
            throw new GUIManagerException("unsupported loyalty program type", JSONUtilities.decodeString(jsonRoot, "loyaltyProgramType", false));
        }

        /*****************************************
        *
        *  store
        *
        *****************************************/

        loyaltyProgramService.putLoyaltyProgram(loyaltyProgram, (existingLoyaltyProgram == null), userID);
        
        /*****************************************
        *
        *  add dynamic criterion fields)
        *
        *****************************************/

        dynamicCriterionFieldService.addLoyaltyProgramCriterionFields(loyaltyProgram, (existingLoyaltyProgram == null));

        /*****************************************
        *
        *  revalidate
        *
        *****************************************/

        revalidateSubscriberMessageTemplates(now);
        revalidateOffers(now);
        revalidateTargets(now);
        revalidateJourneys(now);

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

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        loyaltyProgramService.putLoyaltyProgram(incompleteObject, (existingLoyaltyProgram == null), userID);

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
  *  processRemoveLoyaltyProgram
  *
  *****************************************/

  JSONObject processRemoveLoyaltyProgram(String userID, JSONObject jsonRoot){

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

    String loyaltyProgramID = JSONUtilities.decodeString(jsonRoot, "id", true);
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject existingLoyaltyProgram = loyaltyProgramService.getStoredGUIManagedObject(loyaltyProgramID);
    if (existingLoyaltyProgram != null && (force || !existingLoyaltyProgram.getReadOnly()))
      {
        //
        //  remove loyalty program
        //

        loyaltyProgramService.removeLoyaltyProgram(loyaltyProgramID, userID);

        //
        //  remove dynamic criterion fields
        //
        
        dynamicCriterionFieldService.removeLoyaltyProgramCriterionFields(existingLoyaltyProgram);

        //
        //  revalidate
        //

        revalidateSubscriberMessageTemplates(now);
        revalidateOffers(now);
        revalidateTargets(now);
        revalidateJourneys(now);
      }

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (existingLoyaltyProgram != null && (force || !existingLoyaltyProgram.getReadOnly()))
      {
        responseCode = "ok";
      }
    else if (existingLoyaltyProgram != null)
      {
        responseCode = "failedReadOnly";
      }
    else
      {
        responseCode = "loyaltyProgamNotFound";
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
  *  processGetLoyaltyProgramList
  *
  *****************************************/

  JSONObject processGetLoyaltyProgramList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve loyalty program list
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> loyaltyProgramList = new ArrayList<JSONObject>();
    for (GUIManagedObject loyaltyProgram : loyaltyProgramService.getStoredGUIManagedObjects(includeArchived))
      {
        JSONObject loyaltyPro = loyaltyProgramService.generateResponseJSON(loyaltyProgram, fullDetails, now);
        loyaltyProgramList.add(loyaltyPro);
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

  JSONObject processGetLoyaltyProgramPointsEvents(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve events
    *
    *****************************************/

    List<JSONObject> events = evaluateEnumeratedValues("loyaltyProgramPointsEventNames", SystemTime.getCurrentTime(), true);

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

  JSONObject processGetLoyaltyProgramTypeList(String userID, JSONObject jsonRoot)
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
  *  processDownloadReport
  *
  *****************************************/

  void processDownloadReport(String userID, JSONObject jsonRoot, JSONObject jsonResponse, HttpExchange exchange)
  {
    String reportID = JSONUtilities.decodeString(jsonRoot, "id", true);
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
            Report report = new Report(report1.getJSONRepresentation(), epochServer.getKey(), null);
            String reportName = report.getName();

            String outputPath = Deployment.getReportManagerOutputPath()+File.separator;
            String fileExtension = Deployment.getReportManagerFileExtension();

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
              }else {
                responseCode = "Cant find report with that name";
              }

              if(reportFile != null) {
                if(reportFile.length() > 0) {
                  try {
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
                }else {
                  responseCode = "Report size is 0, report file is empty";
                }
              }else {
                responseCode = "Report is null, cant find this report";
              }
          }
        catch (GUIManagerException e)
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

  JSONObject processGetDashboardCounts(String userID, JSONObject jsonRoot, boolean includeArchived)
  {
    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("journeyCount", journeyCount(GUIManagedObjectType.Journey));
    response.put("campaignCount", journeyCount(GUIManagedObjectType.Campaign));
    response.put("workflowCount", journeyCount(GUIManagedObjectType.Workflow));
    response.put("bulkCampaignCount", journeyCount(GUIManagedObjectType.BulkCampaign));
    response.put("segmentationDimensionCount", segmentationDimensionService.getStoredSegmentationDimensions(includeArchived).size());
    response.put("pointCount", pointService.getStoredPoints(includeArchived).size());
    response.put("offerCount", offerService.getStoredOffers(includeArchived).size());
    response.put("loyaltyProgramCount", loyaltyProgramService.getStoredLoyaltyPrograms(includeArchived).size());
    response.put("scoringStrategyCount", scoringStrategyService.getStoredScoringStrategies(includeArchived).size());
    response.put("presentationStrategyCount", presentationStrategyService.getStoredPresentationStrategies(includeArchived).size());
    response.put("dnboMatrixCount", dnboMatrixService.getStoredDNBOMatrixes(includeArchived).size());
    response.put("callingChannelCount", callingChannelService.getStoredCallingChannels(includeArchived).size());
    response.put("salesChannelCount", salesChannelService.getStoredSalesChannels(includeArchived).size());
    response.put("sourceAddressCount", sourceAddressService.getStoredSourceAddresses(includeArchived).size());
    response.put("supplierCount", supplierService.getStoredSuppliers(includeArchived).size());
    response.put("productCount", productService.getStoredProducts(includeArchived).size());
    response.put("voucherTypeCount", voucherTypeService.getStoredVoucherTypes(includeArchived).size());
    response.put("voucherCount", voucherService.getStoredVouchers(includeArchived).size());
    response.put("catalogCharacteristicCount", catalogCharacteristicService.getStoredCatalogCharacteristics(includeArchived).size());
    response.put("journeyObjectiveCount", journeyObjectiveService.getStoredJourneyObjectives(includeArchived).size());
    response.put("offerObjectiveCount", offerObjectiveService.getStoredOfferObjectives(includeArchived).size());
    response.put("productTypeCount", productTypeService.getStoredProductTypes(includeArchived).size());
    response.put("deliverableCount", deliverableService.getStoredDeliverables(includeArchived).size());
    response.put("mailTemplateCount", subscriberMessageTemplateService.getStoredMailTemplates(true, includeArchived).size());
    response.put("smsTemplateCount", subscriberMessageTemplateService.getStoredSMSTemplates(true, includeArchived).size());
    response.put("pushTemplateCount", subscriberMessageTemplateService.getStoredPushTemplates(true, includeArchived).size());
    response.put("dialogTemplateCount", subscriberMessageTemplateService.getStoredDialogTemplates(true, includeArchived).size());
    response.put("reportsCount", reportService.getStoredReports(includeArchived).size());
    response.put("walletsCount", pointService.getStoredPoints(includeArchived).size() + tokenTypeService.getStoredTokenTypes(includeArchived).size() + voucherTypeService.getStoredVoucherTypes(includeArchived).size());
    response.put("ucgRuleCount", ucgRuleService.getStoredUCGRules(includeArchived).size());
    response.put("targetCount", targetService.getStoredTargets(includeArchived).size());
    response.put("exclusionInclusionCount", exclusionInclusionTargetService.getStoredExclusionInclusionTargets(includeArchived).size());
    response.put("segmentContactPolicies",segmentContactPolicyService.getStoredSegmentContactPolicys(includeArchived).size());
    response.put("contactPolicyCount", contactPolicyService.getStoredContactPolicies(includeArchived).size());
    response.put("communicationChannelCount", Deployment.getCommunicationChannels().size());
    response.put("communicationChannelBlackoutCount", communicationChannelBlackoutService.getStoredCommunicationChannelBlackouts(includeArchived).size());
    response.put("resellerCount", resellerService.getStoredResellers(includeArchived).size());
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  journeyCount
  *
  *****************************************/

  private int journeyCount(GUIManagedObjectType journeyType)
  {
    int result = 0;
    for (GUIManagedObject journey : journeyService.getStoredJourneys())
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

  JSONObject processGetReportGlobalConfiguration(String userID, JSONObject jsonRoot)
  {
    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();
    HashMap<String,Object> globalConfig = new HashMap<String,Object>();
    globalConfig.put("reportManagerZookeeperDir",   Deployment.getReportManagerZookeeperDir());
    globalConfig.put("reportManagerOutputPath",     Deployment.getReportManagerOutputPath());
    globalConfig.put("reportManagerDateFormat",     Deployment.getReportManagerDateFormat());
    globalConfig.put("reportManagerFileExtension",  Deployment.getReportManagerFileExtension());
    globalConfig.put("reportManagerCsvSeparator",   Deployment.getReportManagerCsvSeparator());
    globalConfig.put("reportManagerStreamsTempDir", Deployment.getReportManagerStreamsTempDir());
    response.put("reportGlobalConfiguration", JSONUtilities.encodeObject(globalConfig));
    response.put("responseCode", "ok");
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetReportList
  *
  *****************************************/

  JSONObject processGetReportList(String userID, JSONObject jsonRoot, boolean includeArchived)
  {
    log.trace("In processGetReportList : "+jsonRoot);
    Date now = SystemTime.getCurrentTime();
    List<JSONObject> reports = new ArrayList<JSONObject>();
    for (GUIManagedObject report : reportService.getStoredReports(includeArchived))
      {
        log.trace("In processGetReportList, adding : "+report);
        JSONObject reportResponse = reportService.generateResponseJSON(report, true, now);
        reportResponse.put("isRunning", reportService.isReportRunning(((Report)report).getName()));
        reports.add(reportResponse);
      }
    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("reports", JSONUtilities.encodeArray(reports));
    log.trace("res : "+response.get("reports"));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processLaunchReport
  *
  *****************************************/

  JSONObject processLaunchReport(String userID, JSONObject jsonRoot)
  {
    log.trace("In processLaunchReport : "+jsonRoot);
    HashMap<String,Object> response = new HashMap<String,Object>();
    String reportID = JSONUtilities.decodeString(jsonRoot, "id", true);
    Report report = (Report) reportService.getStoredReport(reportID);
    log.trace("Looking for "+reportID+" and got "+report);
    String responseCode;
    if (report == null)
      {
        responseCode = "reportNotFound";
      }
    else
      {
        if (reportService.isReportRunning(report.getName()))
          {
            responseCode = "reportIsAlreadyRunning";
          }
        else
          {
            reportService.launchReport(report.getName());
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

  JSONObject processPutReport(String userID, JSONObject jsonRoot)
  {
    log.trace("In processPutReport : "+jsonRoot);
    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();
    String reportID = JSONUtilities.decodeString(jsonRoot, "id", false);
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

    long epoch = epochServer.getKey();
    try
      {
        Report report = new Report(jsonRoot, epoch, existingReport);
        log.trace("new report : "+report);
        reportService.putReport(report, (existingReport == null), userID);
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

  JSONObject processGetPartnerTypes(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve PartnerTypes
    *
    *****************************************/

    List<JSONObject> partnerTypes = new ArrayList<JSONObject>();
    for (PartnerType partnerType : Deployment.getPartnerTypes().values())
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

  JSONObject processGetBillingModes(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve BillingModes
    *
    *****************************************/

    List<JSONObject> billingModes = new ArrayList<JSONObject>();
    for (BillingMode billingMode : Deployment.getBillingModes().values())
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
  
}

