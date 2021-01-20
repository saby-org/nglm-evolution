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
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.elasticsearch.client.RestHighLevelClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SubscriberIDService;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramType;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;
import com.evolving.nglm.evolution.Report.SchedulingInterval;
import com.evolving.nglm.evolution.SubscriberProfileService.SubscriberProfileServiceException;
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
        if (!dryRun)
          {

            loyaltyProgramService.putLoyaltyProgram(loyaltyProgram, (existingLoyaltyProgram == null), userID);

            /*****************************************
             *
             * add dynamic criterion fields)
             *
             *****************************************/

            dynamicCriterionFieldService.addLoyaltyProgramCriterionFields(loyaltyProgram,
                (existingLoyaltyProgram == null));

            /*****************************************
             *
             * revalidate
             *
             *****************************************/

            revalidateSubscriberMessageTemplates(now);
            revalidateOffers(now);
            revalidateTargets(now);
            revalidateJourneys(now);
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

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

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

        loyaltyProgramService.removeLoyaltyProgram(loyaltyProgram.getGUIManagedObjectID(), userID);

        //
        // remove dynamic criterion fields
        //

        dynamicCriterionFieldService.removeLoyaltyProgramCriterionFields(loyaltyProgram);

        //
        // revalidate
        //

        revalidateSubscriberMessageTemplates(now);
        revalidateOffers(now);
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
    response.put("removedloyaltyProgramIDS", JSONUtilities.encodeArray(validIDs));

    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   * processSetStatusLoyaltyProgram
   *
   *****************************************/

  JSONObject processSetStatusLoyaltyProgram(String userID, JSONObject jsonRoot)
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
                switch (LoyaltyProgramType
                    .fromExternalRepresentation(JSONUtilities.decodeString(elementRoot, "loyaltyProgramType", true)))
                  {
                    case POINTS:
                      loyaltyProgram = new LoyaltyProgramPoints(elementRoot, epoch, existingElement,
                          catalogCharacteristicService);
                      break;

//          case BADGES:
//            // TODO
//            break;

                    case Unknown:
                      throw new GUIManagerException("unsupported loyalty program type",
                          JSONUtilities.decodeString(elementRoot, "loyaltyProgramType", false));
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

                revalidateSubscriberMessageTemplates(now);
                revalidateOffers(now);
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

  JSONObject processGetLoyaltyProgramList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
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
            if (loyaltyProgram != null)
              {
                loyaltyProgramObjects.add(loyaltyProgram);
              }
          }
      }
    else
      {
        loyaltyProgramObjects = loyaltyProgramService.getStoredGUIManagedObjects(includeArchived);
      }
    for (GUIManagedObject loyaltyProgram : loyaltyProgramObjects)
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
 * @throws IOException 
  *
  *****************************************/

  void processDownloadReport(String userID, JSONObject jsonRoot, JSONObject jsonResponse, HttpExchange exchange) throws IOException
  {
    String reportID = JSONUtilities.decodeString(jsonRoot, "id", true);
    JSONArray filters = JSONUtilities.decodeJSONArray(jsonRoot, "criteria", false);
    Integer percentage = JSONUtilities.decodeInteger(jsonRoot, "percentage", false);
    Integer topRows = JSONUtilities.decodeInteger(jsonRoot, "topRows", false);
    JSONArray header = JSONUtilities.decodeJSONArray(jsonRoot, "header", false);

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
          } else {
            responseCode = "Cant find report with that name";
          }

          File filterOutTmpFile = reportFile;
          File percentageOutTmpFile = reportFile;
          File topRowsOutTmpFile = reportFile;
          File headerOutTmpFile = reportFile;
          File finalZipFile = null;

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
                    }
                  else
                    {
                      filterOutTmpFile = new File(unzippedFile);
                    }

                  if (percentage != null) 
                    {
                      ReportUtils.extractPercentageOfRandomRows(filterOutTmpFile.getAbsolutePath(), percentageOutTmpFile.getAbsolutePath(), percentage);
                    }
                  else
                    {
                      percentageOutTmpFile = filterOutTmpFile;
                    }

                  if (topRows != null) 
                    {
                      ReportUtils.extractTopRows(percentageOutTmpFile.getAbsolutePath(), topRowsOutTmpFile.getAbsolutePath(), topRows);
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
                    }
                  else
                    {
                      headerOutTmpFile = topRowsOutTmpFile;
                    }

                  finalZipFile = File.createTempFile("reportFinal", "zip");
                  String finalZipFileName = finalZipFile.getAbsolutePath();
                  ReportUtils.zipFile(headerOutTmpFile.getAbsolutePath(), finalZipFileName);
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
      if (report != null)
        {
          reportObjects.add(report);
        }
    }
      }
    else
      {
        reportObjects = reportService.getStoredReports(includeArchived);
      }
    for (GUIManagedObject report : reportObjects)
      {
        if (log.isTraceEnabled())
          {
            log.trace("In processGetReportList, adding : " + report);
          }
        JSONObject reportResponse = reportService.generateResponseJSON(report, true, now);
        reportResponse.put("isRunning", reportService.isReportRunning(((Report)report).getName()));
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

    long epoch = epochServer.getKey();
    try
      {
        Report report = new Report(jsonRoot, epoch, existingReport);
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
  
  /*****************************************
  *
  *  processLoyaltyProgramOptInOut
  *
  *****************************************/

  JSONObject processLoyaltyProgramOptInOut(JSONObject jsonRoot, boolean optIn)throws GUIManagerException {
    
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

    String subscriberID = resolveSubscriberID(customerID);

    String loyaltyProgramID = JSONUtilities.decodeString(jsonRoot, "loyaltyProgram", false); 
    String loyaltyProgramRequestID = "";

    /*****************************************
     *
     * getSubscriberProfile - no history
     *
     *****************************************/

    try
      {
        SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
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
        for (LoyaltyProgram loyaltyProgram : loyaltyProgramService.getActiveLoyaltyPrograms(now))
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
        String topic = Deployment.getLoyaltyProgramRequestTopic();
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

        LoyaltyProgramRequest loyaltyProgramRequest = new LoyaltyProgramRequest(subscriberProfile,subscriberGroupEpochReader,valueRes, null);

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

