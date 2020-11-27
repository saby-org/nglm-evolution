package com.evolving.nglm.evolution;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import com.evolving.nglm.core.JSONUtilities;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

public class VoucherService extends GUIService {

  private enum VoucherJobAction{
    IMPORT_FILE("import_file"),
    REIMPORT_FILE("reimport_file"),
    UPDATE_DATE("update_date"),
    DELETE_FILE("delete_file"),
    DELETE_VOUCHER("delete_voucher"),
    UNKNOWN("unknown");
    private String action;
    VoucherJobAction(String action){this.action=action;}
    public String getExternalRepresentation(){return this.action;}
    public static VoucherJobAction fromExternalRepresentation(String externalRepresentation){for(VoucherJobAction enumeratedValue:VoucherJobAction.values()){if(enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) return enumeratedValue;}return UNKNOWN;}
  }

  public enum ImportVoucherFileStatus {
    NotProcessed("notProcessed"),
    Processing("processing"),
    Processed("processed"),
    Error("error"),
    Deleted("deleted"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private ImportVoucherFileStatus(String externalRepresentation){this.externalRepresentation = externalRepresentation;}
    public String getExternalRepresentation(){return externalRepresentation;}
    public static ImportVoucherFileStatus fromExternalRepresentation(String externalRepresentation) { for (ImportVoucherFileStatus enumeratedValue : ImportVoucherFileStatus.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  private static final Logger log = LoggerFactory.getLogger(VoucherService.class);

  private volatile boolean stopRequested = false;
  // queue of jobs to do, this got a persistent backup view in zookeeper, in case of restart only
  private BlockingQueue<VoucherJob> processVoucherFileJobsQueue = new LinkedBlockingQueue<VoucherJob>();
  private Thread processVoucherFileThread = null;
  private VoucherPersonalESService voucherPersonalESService;
  private UploadedFileService uploadedFileService;
  private ZooKeeper zookeeper;
  // the root path we saved some stats in zookeeper
  private static final String ZOOKEEPER_PERSONAL_VOUCHER_ROOT = Deployment.getZookeeperRoot() + "/voucherPersonalImport";
  // the "file", node, we saved a persitent view of the voucher job to do, in case of restart
  private static final String ZOOKEEPER_JOB_QUEUE = ZOOKEEPER_PERSONAL_VOUCHER_ROOT + "/job_to_process";
  // we fix a charset to store data in zookeeper just to avoid issue in case different default jvm ones
  private static final Charset ZOOKEEPER_ENCODING_CHARSET = StandardCharsets.UTF_8;

  // this is the UploadedFileService applicationID we use once we don't want the file to appear in GUI anymore for choice, while not deleting it yet
  // we keep the file available in GUI as long as not yet bound to a voucher
  // once bound, we removed it as a choice, but we keep it on file system to process it, then we delete only if ok. (on error we might want reprocess it)
  private static final String usedVoucherFileApplicationId = "used-vouchers";

  public VoucherService(String bootstrapServers, String groupID, String voucherTopic, boolean masterService, VoucherListener voucherListener, boolean notifyOnSignificantChange, ElasticsearchClientAPI elasticsearch, UploadedFileService uploadedFileService) {
    super(bootstrapServers, "voucherService", groupID, voucherTopic, masterService, getSuperListener(voucherListener), "putVoucher", "removeVoucher", notifyOnSignificantChange);
    this.voucherPersonalESService = new VoucherPersonalESService(elasticsearch,masterService,com.evolving.nglm.core.Deployment.getElasticsearchLiveVoucherShards(),com.evolving.nglm.core.Deployment.getElasticsearchLiveVoucherReplicas());
    this.uploadedFileService = uploadedFileService;

    if(masterService){
      initZookeeper();
      // a thread job handling "voucher modification" to do in ES
      processVoucherFileThread = new Thread(this::runProcessVoucherfile,"voucherService-processVoucherfile");
      processVoucherFileThread.start();
    }
  }

  public VoucherService(String bootstrapServers, String groupID, String voucherTopic, boolean masterService, ElasticsearchClientAPI elasticsearch, UploadedFileService uploadedFileService) {
    this(bootstrapServers, groupID, voucherTopic, masterService, (VoucherListener) null, true,elasticsearch,uploadedFileService);
  }

  public VoucherService(String bootstrapServers, String groupID, String voucherTopic, ElasticsearchClientAPI elasticsearch) {
    this(bootstrapServers, groupID, voucherTopic, false,elasticsearch,null);
  }

  public VoucherService(String bootstrapServers, String groupID, String voucherTopic) {
    this(bootstrapServers, groupID, voucherTopic, false,null,null);
  }

  private static GUIManagedObjectListener getSuperListener(VoucherListener voucherListener) {
    GUIManagedObjectListener superListener = null;
    if (voucherListener != null) {
      superListener = new GUIManagedObjectListener() {
        @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { voucherListener.voucherActivated((Voucher) guiManagedObject); }
        @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { voucherListener.voucherDeactivated(guiManagedObjectID); }
      };
    }
    return superListener;
  }


  public String generateVoucherID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredVoucher(String voucherID) { return getStoredGUIManagedObject(voucherID); }
  public GUIManagedObject getStoredVoucher(String voucherID, boolean includeArchived) { return getStoredGUIManagedObject(voucherID, includeArchived); }
  public Collection<GUIManagedObject> getStoredVouchers() { return getStoredGUIManagedObjects(); }
  public Collection<GUIManagedObject> getStoredVouchers(boolean includeArchived) { return getStoredGUIManagedObjects(includeArchived); }
  public boolean isActiveVoucher(GUIManagedObject voucherUnchecked, Date date) { return isActiveGUIManagedObject(voucherUnchecked, date); }
  public Voucher getActiveVoucher(String voucherID, Date date) { return (Voucher) getActiveGUIManagedObject(voucherID, date); }
  public Collection<Voucher> getActiveVouchers(Date date) { return (Collection<Voucher>) getActiveGUIManagedObjects(date); }

  //this call might trigger stock count, this for stock information for GUI, so DO NOT USE it for traffic calls, hopefuly ES will manage to keep that call quick
  public GUIManagedObject getStoredVoucherWithCurrentStocks(String voucherID, boolean includeArchived){

    GUIManagedObject uncheckedVoucher = getStoredVoucher(voucherID,includeArchived);
    if(!(uncheckedVoucher instanceof VoucherPersonal) && !(uncheckedVoucher instanceof VoucherShared)) return uncheckedVoucher;//cant do more than normal one

    // VoucherPersonal : get stock from ES and fill it per file
    if(uncheckedVoucher instanceof VoucherPersonal){
      VoucherPersonal voucher = (VoucherPersonal) uncheckedVoucher;
      // got stats data saved in Zookeeper
      for(VoucherFile voucherFile:voucher.getVoucherFiles()){
        voucherFile.setVoucherFileStats(getVoucherFileStatsFromZookeeper(voucherID,voucherFile.getFileId()));
      }
      // add the current live status from ES
      voucherPersonalESService.populateVoucherFileWithStockInformation(voucher);
      return voucher;
    }

    return uncheckedVoucher;
  }

  public VoucherPersonalESService getVoucherPersonalESService() {return voucherPersonalESService;}

  public void putVoucher(GUIManagedObject voucher, boolean newObject, String userID) {
    Date now = SystemTime.getCurrentTime();
    //save conf
    putGUIManagedObject(voucher, now, newObject, userID);
    //send process files order if voucher OK
    if(isActiveVoucher(voucher,now) && voucher instanceof VoucherPersonal){
      //changed the applicationId of the files, to make it unavailable to use any longer
      for(VoucherFile file:((VoucherPersonal) voucher).getVoucherFiles()){
        UploadedFile uploadedFile=(UploadedFile)uploadedFileService.getStoredUploadedFile(file.getFileId());
        if(uploadedFile!=null && uploadedFile.getApplicationID()!=null && !uploadedFile.getApplicationID().equals(usedVoucherFileApplicationId)){
          uploadedFileService.changeFileApplicationId(file.getFileId(),usedVoucherFileApplicationId);
        }
      }
      voucherPersonalESService.createSupplierVoucherIndexIfNotExist(((VoucherPersonal)voucher).getSupplierID());
      checkForVoucherModificationToApplyOnVoucherPersonalStore((VoucherPersonal)voucher);
    }
  }

  public void removeVoucher(String voucherID, String userID, UploadedFileService uploadedFileService) {
    GUIManagedObject storedVoucher = getStoredVoucher(voucherID);
    if(storedVoucher instanceof VoucherPersonal){
      VoucherPersonal voucher = (VoucherPersonal) storedVoucher;
      addVoucherJob(new VoucherJob(voucher.getSupplierID(),voucher.getVoucherID(),null,null,VoucherJobAction.DELETE_VOUCHER));
    }
    removeGUIManagedObject(voucherID, SystemTime.getCurrentTime(), userID);
  }
  
  public void cleanUpVouchersJob() {
    log.info("VoucherPersonalESService-cleanUpExpiredVouchers : start execution");
    Date now = SystemTime.getCurrentTime();

    // we delete the not allocated expired vouchers as soon as expired, per voucherId, fileId, to stored this stats
    // we delete as well change one, this is one is for now just for stats
    for(Voucher untypedVoucher : getActiveVouchers(SystemTime.getCurrentTime())){
      if(!(untypedVoucher instanceof VoucherPersonal)) continue;
      VoucherPersonal voucher = (VoucherPersonal) untypedVoucher;
      for(VoucherFile file:voucher.getVoucherFiles()){
        int nbExpired=voucherPersonalESService.deleteAvailableExpiredVoucher(voucher.getSupplierID(),voucher.getVoucherID(),file.getFileId(),now);
        log.info("VoucherPersonalESService-cleanUpExpiredVouchers : "+nbExpired+" unallocated expired vouchers deleted for voucher "+voucher.getVoucherID()+" and file "+file.getFileId());
        updateVoucherFileStatsInZookeeper(voucher.getVoucherID(),file.getFileId(),0,0,0,nbExpired);
      }
    }

    // this is a bulk delete of all vouchers a while after expiryDate is past, clean up of ES
    Date expiryDate=EvolutionUtilities.addTime(now,-1*com.evolving.nglm.core.Deployment.getElasticsearchRetentionDaysExpiredVouchers(), EvolutionUtilities.TimeUnit.Day,Deployment.getBaseTimeZone());
    // safety
    if(now.before(expiryDate)){
      log.error("VoucherPersonalESService-cleanUpExpiredVouchers : bug, expiryDate for cleanup is after now !! "+now+" vs "+expiryDate);
      return;
    }
    voucherPersonalESService.deleteExpiredVoucher(expiryDate);

    log.info("VoucherPersonalESService-cleanUpExpiredVouchers : stop execution");
  }

 @Override
 protected JSONObject getSummaryJSONRepresentation(GUIManagedObject guiManagedObject) {
   JSONObject result = super.getSummaryJSONRepresentation(guiManagedObject);
   result.put("voucherTypeId", guiManagedObject.getJSONRepresentation().get("voucherTypeId"));
   result.put("supplierID", guiManagedObject.getJSONRepresentation().get("supplierID"));
   result.put("imageURL", guiManagedObject.getJSONRepresentation().get("imageURL"));
   return result;
 }

  public interface VoucherListener {
    public void voucherActivated(Voucher voucher);
    public void voucherDeactivated(String guiManagedObjectID);
  }

  @Override
  public void stop(){
    // wakeup and let ends the processVoucherFileThread
    synchronized (this){
      this.stopRequested=true;
      if(processVoucherFileThread!=null) processVoucherFileThread.interrupt();
      if(voucherPersonalESService!=null) voucherPersonalESService.close();
    }
    // continue stop()
    super.stop();
  }

  private void checkForVoucherModificationToApplyOnVoucherPersonalStore(VoucherPersonal voucher){

    // for all voucherFiles
    for(VoucherFile voucherFile:voucher.getVoucherFiles()){

      // weird cases checks, should not happen
      if(voucherFile.getFileId()==null){
        log.warn("VoucherService.runProcessVoucherfile : voucherFileId is null?? : "+voucher.toString());
        continue;
      }

      // had this file already been processed ?
      boolean newFile=true;
      boolean expiryDateChanged=false;
      boolean toReprocessOnError=false;

      Iterator<VoucherFile> iterator=voucher.getPreviousVoucherFiles().iterator();
      while(iterator.hasNext()){
        VoucherFile previousVoucherFile= iterator.next();
        if(voucherFile.getFileId().equals(previousVoucherFile.getFileId())){
          newFile=false;

          // change on expiry date conf
          // note that we don't handle a relative expiry period changes from voucherType
          if(voucherFile.getExpiryDate()==null && previousVoucherFile.getExpiryDate()!=null){
            // date has been removed from file
            expiryDateChanged=true;
          }else if(voucherFile.getExpiryDate()!=null && previousVoucherFile.getExpiryDate()==null){
            // date has been added to file
            expiryDateChanged=true;
          }else if(voucherFile.getExpiryDate()!=null && previousVoucherFile.getExpiryDate()!=null && voucherFile.getExpiryDate().compareTo(previousVoucherFile.getExpiryDate())!=0){
            // date has been changed from file
            expiryDateChanged=true;
          }

          VoucherFileStats previousVoucherFileStats=getVoucherFileStatsFromZookeeper(voucher.getVoucherID(),previousVoucherFile.getFileId());
          if(previousVoucherFileStats.getImportStatus()!=ImportVoucherFileStatus.Processed){
            toReprocessOnError=true;
          }
          //remove it from "previous files" conf, the remaining ones would be to delete
          iterator.remove();
          break;
        }
      }

      if(log.isDebugEnabled()) log.debug("VoucherService.runProcessVoucherfile : newFile="+newFile+", expiryDateChanged="+expiryDateChanged+", toReprocessOnError="+toReprocessOnError);

      // nothing to do
      if(!newFile && !expiryDateChanged && !toReprocessOnError){
        log.info("VoucherService.runProcessVoucherfile : nothing to do for id "+voucherFile.getFileId());
        continue;
      }

      //get the uploaded file and check what we can do
      UploadedFile uploadedFile = (UploadedFile) uploadedFileService.getStoredUploadedFile(voucherFile.getFileId());
      if(uploadedFile==null && newFile){
        // the worst error case, new file, but not found
        log.error("VoucherService.runProcessVoucherfile : no uploaded file found for id "+voucherFile.getFileId());
        updateVoucherFileStatsInZookeeper(voucher.getVoucherID(),voucherFile.getFileId(),ImportVoucherFileStatus.Error);
        continue;
      }
      if(uploadedFile==null && toReprocessOnError && !expiryDateChanged){
        // file was previously not OK but is not there anymore, nothing we can do
        log.warn("VoucherService.runProcessVoucherfile : previous file was error, but is not found anymore for id "+voucherFile.getFileId());
        continue;
      }
      if(uploadedFile==null && toReprocessOnError && expiryDateChanged){
        // file was previously not OK but is not there anymore, but we can update the date
        log.warn("VoucherService.runProcessVoucherfile : previous file was error, but is not found anymore, we will just update new expiryDate for id "+voucherFile.getFileId());
        toReprocessOnError=false;
      }

      if(!toReprocessOnError) updateVoucherFileStatsInZookeeper(voucher.getVoucherID(),voucherFile.getFileId(),ImportVoucherFileStatus.Processing);

      final String supplierID=voucher.getSupplierID();
      final String voucherID=voucher.getVoucherID();
      final String fileID=voucherFile.getFileId();
      Date voucherExpiryDate=voucherFile.getExpiryDate()!=null?voucherFile.getExpiryDate():voucherFile.getRelativeLatestExpiryDate();
      String esVoucherExpiryDate=VoucherPersonalES.getESFormatedDate(VoucherPersonalES.ES_FIELDS.expiryDate,voucherExpiryDate);

      // do we have to update expiry date ? Do it before importing file
      if(expiryDateChanged){
        log.info("VoucherService.runProcessVoucherfile : need to update expiry date for "+voucherFile.getFileId()+" to "+voucherFile.getExpiryDate());
        addVoucherJob(new VoucherJob(supplierID,voucherID,fileID,esVoucherExpiryDate,VoucherJobAction.UPDATE_DATE));
        if(!toReprocessOnError) continue;//nothing more to do
      }
      // we have to (re-)process the file
      if(toReprocessOnError){
        log.info("VoucherService.runProcessVoucherfile : will re-process file "+UploadedFile.OUTPUT_FOLDER+uploadedFile.getDestinationFilename()+" for voucher "+voucher.toString());
        addVoucherJob(new VoucherJob(supplierID,voucherID,fileID,esVoucherExpiryDate,VoucherJobAction.REIMPORT_FILE));
      }
      if(newFile){
        log.info("VoucherService.runProcessVoucherfile : will process file "+UploadedFile.OUTPUT_FOLDER+uploadedFile.getDestinationFilename()+" for voucher "+voucher.toString());
        addVoucherJob(new VoucherJob(supplierID,voucherID,fileID,esVoucherExpiryDate,VoucherJobAction.IMPORT_FILE));
      }


    }

    if(voucher.getPreviousVoucherFiles()!=null && !voucher.getPreviousVoucherFiles().isEmpty()){
      for(VoucherFile voucherFile:voucher.getPreviousVoucherFiles()){
        log.info("VoucherService.runProcessVoucherfile : file "+voucherFile.toString()+", have been removed from voucher "+voucher.getVoucherID()+", vouchers need to be deleted from ES");
        // safety double check here, the file as really been removed:
        boolean skipDelete=false;
        for(VoucherFile existingFile:voucher.getVoucherFiles()){
          if(existingFile.getFileId().equals(voucherFile.getFileId())){
            log.warn("VoucherService.runProcessVoucherfile : ORDER TO DELETE file id "+voucherFile.getFileId()+" received while file still associated to voucher!! won't delete it");
            skipDelete=true;
            break;
          }
        }
        if(skipDelete) continue;
        // this file has been deleted
        addVoucherJob(new VoucherJob(voucher.getSupplierID(),voucher.getVoucherID(),voucherFile.getFileId(),null,VoucherJobAction.DELETE_FILE));
      }
    }else{
      if(log.isDebugEnabled()) log.debug("VoucherService.runProcessVoucherfile : no files to delete for voucher "+voucher.toString());
    }

  }

  // Thread job of processing voucher files to ingest
  // so far it is done with 1 thread only on 1 instance only ! DO NOT ALLOW parallelization without thinking
  private void runProcessVoucherfile() {
    while (!stopRequested) {

      try {

        // wait for a job
        VoucherJob job = processVoucherFileJobsQueue.take();

        // is the job a "delete voucher job" ?
        if(job.getAction()==VoucherJobAction.DELETE_VOUCHER){
          log.info("VoucherService.runProcessVoucherfile : need to delete entirely vouchers for voucherId "+job.getVoucherId());
          if(voucherPersonalESService.deleteRemoveVoucher(job.getSupplierId(),job.getVoucherId())){
            log.info("VoucherService.runProcessVoucherfile : deleted voucher "+job.getVoucherId()+" from ES");
            deleteAllVoucherFileStatsFromZookeeper(job.getVoucherId());
            removeVoucherJobFromZookeeper(job);
            continue;
          }else{
            log.warn("VoucherService.runProcessVoucherfile : error when trying to delete voucher "+job.getVoucherId()+" from ES");
          }
        }

        // is the job a "delete voucher file job" ?
        if(job.getAction()==VoucherJobAction.DELETE_FILE){
          log.info("VoucherService.runProcessVoucherfile : need to delete vouchers for fileId "+job.getFileId()+" and voucherId "+job.getVoucherId());
          if(voucherPersonalESService.deleteRemoveVoucherFile(job.getSupplierId(),job.getVoucherId(),job.getFileId())){
            log.info("VoucherService.runProcessVoucherfile : deleted file "+job.getFileId()+" from ES");
            // we do not delete the stats file in zookeeper here, but we mark it deleted
            updateVoucherFileStatsInZookeeper(job.getVoucherId(),job.getFileId(),ImportVoucherFileStatus.Deleted);
            removeVoucherJobFromZookeeper(job);
            continue;
          }else{
            log.warn("VoucherService.runProcessVoucherfile : error when trying to delete file "+job.getFileId()+" from ES");
          }
        }

        // is the job a "update expiry date job" ?
        if(job.getAction()==VoucherJobAction.UPDATE_DATE){
          log.info("VoucherService.runProcessVoucherfile : need to update vouchers expiryDate for fileId "+job.getFileId()+" and voucherId "+job.getVoucherId());
          if(voucherPersonalESService.updateExpiryDateForVoucherFile(job.getSupplierId(),job.getVoucherId(),job.getFileId(),job.getExpiryDate())){
            log.info("VoucherService.runProcessVoucherfile : updated expiryDate file "+job.getFileId()+" from ES");
            if(getVoucherFileStatsFromZookeeper(job.getVoucherId(),job.getFileId()).getImportStatus()==ImportVoucherFileStatus.Processing){
              updateVoucherFileStatsInZookeeper(job.getVoucherId(),job.getFileId(),ImportVoucherFileStatus.Processed);
            }
            removeVoucherJobFromZookeeper(job);
            continue;
          }else{
            log.warn("VoucherService.runProcessVoucherfile : error when trying to update file "+job.getFileId()+" from ES");
          }
        }

        // is the job a "import file job" ?
        if(job.getAction()==VoucherJobAction.IMPORT_FILE||job.getAction()==VoucherJobAction.REIMPORT_FILE){

          log.info("VoucherService.runProcessVoucherfile : need to import vouchers from fileId "+job.getFileId()+" and voucherId "+job.getVoucherId());

          UploadedFile uploadedFile = (UploadedFile) uploadedFileService.getStoredUploadedFile(job.getFileId());
          if(uploadedFile==null){
            // we need to import a file, but file is not found
            log.error("VoucherService.runProcessVoucherfile : no uploaded file found for id "+job.getFileId());
            updateVoucherFileStatsInZookeeper(job.getVoucherId(),job.getFileId(),ImportVoucherFileStatus.Error);
            removeVoucherJobFromZookeeper(job);
            continue;
          }

          int totalNbVouchersAdded=0;
          int nbOfLines=0;
          BufferedReader reader;
          Date expiryDate = VoucherPersonalES.getDateFromESDateFormated(VoucherPersonalES.ES_FIELDS.expiryDate,job.getExpiryDate());
          boolean alreadyExpired=expiryDate.before(SystemTime.getCurrentTime());
          try{
            List<VoucherPersonalES> vouchersToAdd = new ArrayList<VoucherPersonalES>();
            reader = new BufferedReader(new FileReader(UploadedFile.OUTPUT_FOLDER+uploadedFile.getDestinationFilename()));
            for (String line; (line = reader.readLine()) != null;) {
              if(line.trim().isEmpty()) {
                if(log.isDebugEnabled()) log.debug("VoucherService.run(skipping empty line)");
                continue;
              }
              nbOfLines++;
              if(log.isDebugEnabled()) log.debug("VoucherService.runProcessVoucherfile : need to process line ("+line+")");

              //if someone got the good idea to insert already expired voucher, we just don't insert them in ES...(we count line to stored expired)
              // note however that we let previously modifying already stored voucher to expired
              if(alreadyExpired) continue;

              VoucherPersonalES voucherES = new VoucherPersonalES(line,expiryDate,job.getVoucherId(),job.getFileId());
              vouchersToAdd.add(voucherES);

              if(vouchersToAdd.size()>=Deployment.getImportVoucherFileBulkSize()){
                int nbVouchersAdded=voucherPersonalESService.addVouchers(job.getSupplierId(),vouchersToAdd);
                updateVoucherFileStatsInZookeeper(job.getVoucherId(),job.getFileId(),nbVouchersAdded,0,0,0);
                totalNbVouchersAdded+=nbVouchersAdded;
                vouchersToAdd.clear();
              }

            }
            reader.close();
            if(!alreadyExpired){
              int nbVouchersAdded=voucherPersonalESService.addVouchers(job.getSupplierId(),vouchersToAdd);
              updateVoucherFileStatsInZookeeper(job.getVoucherId(),job.getFileId(),nbVouchersAdded,0,0,0);
              totalNbVouchersAdded+=nbVouchersAdded;
            }
          }catch(IOException e){
            log.warn("VoucherService.runProcessVoucherfile : problem with file reading", e);
            updateVoucherFileStatsInZookeeper(job.getVoucherId(),job.getFileId(),ImportVoucherFileStatus.Error);
            removeVoucherJobFromZookeeper(job);
            continue;
          }

          updateVoucherFileStatsInZookeeper(job.getVoucherId(),job.getFileId(),ImportVoucherFileStatus.Processed);
          updateVoucherFileStatsInZookeeper(job.getVoucherId(),job.getFileId(),nbOfLines);
          if(alreadyExpired&&job.getAction()!=VoucherJobAction.REIMPORT_FILE) updateVoucherFileStatsInZookeeper(job.getVoucherId(),job.getFileId(),0,0,0,nbOfLines);

          log.info("VoucherService.runProcessVoucherfile : processed file "+UploadedFile.OUTPUT_FOLDER+uploadedFile.getDestinationFilename()+", "+totalNbVouchersAdded+" vouchers added over "+nbOfLines+" lines in file");
          removeVoucherJobFromZookeeper(job);
          continue;
        }

      } catch (InterruptedException e) {
        // normal exception on interrupting thread waiting on queue
        log.info("VoucherService.runProcessVoucherfile : interrupted");
      }

    }
  }

  private void addVoucherJob(VoucherJob voucherJob){
    if(log.isDebugEnabled()) log.debug("VoucherService.addVoucherJob : job "+voucherJob);
    // first we modify the zookeeper (this is just a persistent backup of the job queue in case of restart)
    ensureZookeeperConnected();
    synchronized (zookeeper){
      try{
        if(zookeeper.exists(ZOOKEEPER_JOB_QUEUE,false)!=null){
          Stat stat=new Stat();
          //get the queue list saved
          byte[] rawJobQueue=zookeeper.getData(ZOOKEEPER_JOB_QUEUE,event->{return;},stat);
          JSONArray zookeeperJobQueue = (JSONArray) (new JSONParser()).parse(new String(rawJobQueue,ZOOKEEPER_ENCODING_CHARSET));
          if(log.isDebugEnabled()) log.debug("VoucherService.addVoucherJob : current stored job queue in zookeeper : "+zookeeperJobQueue.toJSONString());
          // add the job at the end
          zookeeperJobQueue.add(voucherJob.getJsonRepresentation());
          // store it back to zookeeper
          if(log.isDebugEnabled()) log.debug("VoucherService.addVoucherJob : will store job queue in zookeeper : "+zookeeperJobQueue.toJSONString());
          rawJobQueue=JSONUtilities.encodeArray(zookeeperJobQueue).toJSONString().getBytes(ZOOKEEPER_ENCODING_CHARSET);
          zookeeper.setData(ZOOKEEPER_JOB_QUEUE,rawJobQueue, stat.getVersion());
        }else{
          log.error("VoucherService.addVoucherJob : no persitent backup of the job queue in zookeeper");
        }
      }catch (KeeperException|InterruptedException|ParseException e){
        log.warn("VoucherService.addVoucherJob : ",e);
      }
    }
    // even if we got issue with zookeeper persistent job queue backup, push it to process
    processVoucherFileJobsQueue.add(voucherJob);
  }

  private void removeVoucherJobFromZookeeper(VoucherJob voucherJob){
    if(log.isDebugEnabled()) log.debug("VoucherService.removeVoucherJobFromZookeeper : job "+voucherJob);
    // first we modify the zookeeper (this is just a persistent backup of the job queue in case of restart)
    ensureZookeeperConnected();
    synchronized (zookeeper){
      try{
        if(zookeeper.exists(ZOOKEEPER_JOB_QUEUE,false)!=null){
          Stat stat=new Stat();
          //get the queue list saved
          byte[] rawJobQueue=zookeeper.getData(ZOOKEEPER_JOB_QUEUE,event->{return;},stat);
          JSONArray zookeeperJobQueue = (JSONArray) (new JSONParser()).parse(new String(rawJobQueue,ZOOKEEPER_ENCODING_CHARSET));
          if(log.isDebugEnabled()) log.debug("VoucherService.removeVoucherJobFromZookeeper : current stored job queue in zookeeper : "+zookeeperJobQueue.toJSONString());
          // add the job at the end
          Iterator iterator=zookeeperJobQueue.iterator();
          boolean removed=false;
          while(iterator.hasNext()){
            VoucherJob storedVoucherJob=new VoucherJob((JSONObject)iterator.next());
            if(storedVoucherJob.equals(voucherJob)){
              iterator.remove();
              removed=true;
              break;
            }
          }
          if(!removed){
            log.warn("VoucherService.removeVoucherJobFromZookeeper : job not found in zookeeper backup : "+voucherJob);
            return;
          }
          // store it back to zookeeper
          if(log.isDebugEnabled()) log.debug("VoucherService.removeVoucherJobFromZookeeper : will store job queue in zookeeper : "+zookeeperJobQueue.toJSONString());
          rawJobQueue=JSONUtilities.encodeArray(zookeeperJobQueue).toJSONString().getBytes(ZOOKEEPER_ENCODING_CHARSET);
          zookeeper.setData(ZOOKEEPER_JOB_QUEUE,rawJobQueue, stat.getVersion());
        }else{
          log.error("VoucherService.removeVoucherJobFromZookeeper : no persitent backup of the job queue in zookeeper");
        }
      }catch (KeeperException|InterruptedException|ParseException e){
        log.warn("VoucherService.removeVoucherJobFromZookeeper : ",e);
      }
    }
  }

  private VoucherFileStats getVoucherFileStatsFromZookeeper(String voucherId, String fileId){
    return getVoucherFileStatsFromZookeeper(voucherId,fileId,false);
  }

  private VoucherFileStats getVoucherFileStatsFromZookeeper(String voucherId, String fileId, boolean createIfNotExists){
    ensureZookeeperConnected();
    String statsNode = getZookeeperNodePathForVoucherFileStats(voucherId,fileId);
    VoucherFileStats initialVoucherFileStats = new VoucherFileStats();
    synchronized (zookeeper){
      try{
        if(zookeeper.exists(statsNode,false)!=null){
          Stat stat = new Stat();
          byte[] rawFileStats=zookeeper.getData(statsNode,event->{return;},stat);
          VoucherFileStats toRet=new VoucherFileStats((JSONObject)(new JSONParser()).parse(new String(rawFileStats,ZOOKEEPER_ENCODING_CHARSET)),stat);
          if(log.isDebugEnabled()) log.debug("VoucherService.getVoucherFileStatsFromZookeeper : will return saved stats "+toRet);
          return toRet;
        }else{
          log.info("VoucherService.getVoucherFileStatsFromZookeeper : no stats stored yet in zookeeper for file id "+fileId);
          if(createIfNotExists){
            log.info("VoucherService.getVoucherFileStatsFromZookeeper : will create initial stats in zookeeper for file id "+fileId);
            zookeeper.create(statsNode,initialVoucherFileStats.getJsonRepresentation().toJSONString().getBytes(ZOOKEEPER_ENCODING_CHARSET), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
          }
        }
      }catch (KeeperException|InterruptedException|ParseException e){
        log.warn("VoucherService.getVoucherFileStatsFromZookeeper : ",e);
      }
    }
    //else an init one
    return initialVoucherFileStats;
  }

  private void updateVoucherFileStatsInZookeeper(String voucherId, String fileId, ImportVoucherFileStatus status){
    updateVoucherFileStatsInZookeeper(voucherId,fileId,status,-1,0,0,0,0);
  }
  private void updateVoucherFileStatsInZookeeper(String voucherId, String fileId, int stockImportedToAdd, int stockDeliveredToAdd, int stockRedeemedToAdd, int stockExpiredToAdd){
    updateVoucherFileStatsInZookeeper(voucherId,fileId,null,-1,stockImportedToAdd,stockDeliveredToAdd,stockRedeemedToAdd,stockExpiredToAdd);
  }
  private void updateVoucherFileStatsInZookeeper(String voucherId, String fileId, int stockInfileToSet){
    updateVoucherFileStatsInZookeeper(voucherId,fileId,null,stockInfileToSet,0,0,0,0);
  }
  private void updateVoucherFileStatsInZookeeper(String voucherId, String fileId, ImportVoucherFileStatus status, int stockInfileToSet, int stockImportedToAdd, int stockDeliveredToAdd, int stockRedeemedToAdd, int stockExpiredToAdd){
    ensureZookeeperConnected();
    String statsNode = getZookeeperNodePathForVoucherFileStats(voucherId,fileId);
    synchronized (zookeeper){
      VoucherFileStats voucherFileStats = getVoucherFileStatsFromZookeeper(voucherId,fileId,true);
      if(log.isDebugEnabled()) log.debug("VoucherService.updateVoucherFileStatsInZookeeper : will update for file id "+fileId+" : "+voucherFileStats);
      if(status!=null) voucherFileStats.setImportStatus(status);
      if(stockInfileToSet>=0)voucherFileStats.setStockInFile(stockInfileToSet);//a dirty hack
      voucherFileStats.addStockImported(stockImportedToAdd);
      voucherFileStats.addStockDelivered(stockDeliveredToAdd);
      voucherFileStats.addStockRedeemed(stockRedeemedToAdd);
      voucherFileStats.addStockExpired(stockExpiredToAdd);
      if(log.isDebugEnabled()) log.debug("VoucherService.updateVoucherFileStatsInZookeeper : will store for file id "+fileId+" : "+voucherFileStats);
      try{
        zookeeper.setData(statsNode,voucherFileStats.getJsonRepresentation().toJSONString().getBytes(ZOOKEEPER_ENCODING_CHARSET),voucherFileStats.getZookeeperVersion());
      }catch (KeeperException|InterruptedException e){
        log.warn("VoucherService.initZookeeper : ",e);
      }
    }
  }

  private void deleteAllVoucherFileStatsFromZookeeper(String voucherId){
    log.info("VoucherService.deleteAllVoucherFileStatsFromZookeeper : will delete all file stats in zookeeper for voucher id "+voucherId);
    ensureZookeeperConnected();
    synchronized (zookeeper){
      try{
        for(String fileId:getAllZookeeperFileIdListForVoucher(voucherId)){
          VoucherFileStats statsFile = getVoucherFileStatsFromZookeeper(voucherId,fileId);
          zookeeper.delete(getZookeeperNodePathForVoucherFileStats(voucherId,fileId),statsFile.getZookeeperVersion());
        }
      }catch (KeeperException|InterruptedException e){
        log.warn("VoucherService.deleteAllVoucherFileStatsFromZookeeper : ",e);
      }
    }
  }


  // 2 very closely related methods, returning where are the stats store for file/voucher, BASED ON NAMED PATTERN
  // file "prefix file name" where we gonna keep some stats about voucher file
  // this is a bit dirty way of doing stuff, but that not the most important part
  private static final String ZOOKEEPER_VOUCHER_FILE_PATTERN = ZOOKEEPER_PERSONAL_VOUCHER_ROOT + "/voucher_VOUCHERID_file_FILEID";
  private String getZookeeperNodePathForVoucherFileStats(String voucherId, String fileId){
    return ZOOKEEPER_VOUCHER_FILE_PATTERN.replace("VOUCHERID",voucherId).replace("FILEID",fileId);
  }
  private List<String> getAllZookeeperFileIdListForVoucher(String voucherId){
    if(log.isDebugEnabled()) log.debug("VoucherService.getAllZookeeperFileIdListForVoucher : will get file stats list from zookeeper for voucherId "+voucherId);
    List<String> toRet = new ArrayList<>();
    ensureZookeeperConnected();
    synchronized (zookeeper){
      try{
        List<String> allNodes = zookeeper.getChildren(ZOOKEEPER_PERSONAL_VOUCHER_ROOT,false);
        for(String node:allNodes){
          if(log.isDebugEnabled()) log.debug("VoucherService.getAllZookeeperFileIdListForVoucher : checking name pattern of "+node);
          String voucherIdReplaced = ZOOKEEPER_VOUCHER_FILE_PATTERN.replace(ZOOKEEPER_PERSONAL_VOUCHER_ROOT+"/","").replace("VOUCHERID",voucherId);
          String startWithFilter = voucherIdReplaced.substring(0,voucherIdReplaced.indexOf("_file_")+1);
          if(log.isDebugEnabled()) log.debug("VoucherService.getAllZookeeperFileIdListForVoucher : will check file name using "+startWithFilter+","+voucherIdReplaced);
          if(node.startsWith(startWithFilter)){
            String fileIdToRet = node.replace(startWithFilter,"").replace("file_","");
            if(log.isDebugEnabled()) log.debug("VoucherService.getAllZookeeperFileIdListForVoucher : "+node+" starts with "+startWithFilter+", will return "+fileIdToRet);
            toRet.add(fileIdToRet);
          }
        }
      }catch (KeeperException|InterruptedException e){
        log.warn("VoucherService.getAllZookeeperFileIdListForVoucher : ",e);
      }
    }
    return toRet;
  }

  private void initZookeeper(){
    ensureZookeeperConnected();
    synchronized (zookeeper){
      try{
        if(zookeeper.exists(ZOOKEEPER_JOB_QUEUE,false)==null){
          // first time starting
          if(zookeeper.exists(ZOOKEEPER_PERSONAL_VOUCHER_ROOT,false)==null){
            log.info("VoucherService.initZookeeper : "+ZOOKEEPER_PERSONAL_VOUCHER_ROOT+" does not exists, creating");
            zookeeper.create(ZOOKEEPER_PERSONAL_VOUCHER_ROOT,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
          }
          log.info("VoucherService.initZookeeper : "+ZOOKEEPER_JOB_QUEUE+" does not exists, creating empty one");
          byte[] jobQueue=JSONUtilities.encodeArray(Collections.EMPTY_LIST).toJSONString().getBytes(ZOOKEEPER_ENCODING_CHARSET);
          zookeeper.create(ZOOKEEPER_JOB_QUEUE,jobQueue, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        }else{
          // if file exists, we restored pending jobs if there was some
          byte[] rawJobQueue=zookeeper.getData(ZOOKEEPER_JOB_QUEUE,event->{return;},new Stat());
          JSONArray zookeeperJobQueue = (JSONArray) (new JSONParser()).parse(new String(rawJobQueue,ZOOKEEPER_ENCODING_CHARSET));
          for(Object storedJob:zookeeperJobQueue){
            processVoucherFileJobsQueue.add(new VoucherJob((JSONObject)storedJob));
          }
        }
      }catch (KeeperException|InterruptedException|ParseException e){
        log.warn("VoucherService.initZookeeper : ",e);
      }
    }
  }

  private void ensureZookeeperConnected() {

    if (zookeeper != null && ! zookeeper.getState().isAlive()) {
      log.info("closing zookeeper client due to status "+zookeeper.getState());
      try { zookeeper.close(); } catch (InterruptedException e) { }
      zookeeper = null;
    }

    while (zookeeper == null || zookeeper.getState() != ZooKeeper.States.CONNECTED) {

      if(zookeeper!=null){
        log.info("looping in ensureZookeeperConnected while zookeeper not null "+zookeeper.getState().toString());
        try {
          zookeeper.close();
        } catch (InterruptedException e) {
          log.info("exception while closing ",e);
        }
      }

      try {
        CountDownLatch connectionWait = new CountDownLatch(1);
        zookeeper = new ZooKeeper(Deployment.getZookeeperConnect(), 3000,
          event->{
            log.info("zookeeper connection event received : "+event.getState());
            if(event.getState()== Watcher.Event.KeeperState.SyncConnected){
              connectionWait.countDown();
            }
          }
          , false);
          connectionWait.await();
      } catch (InterruptedException e) {
        // normal notify on connectionWait
      } catch (IOException e) {
        log.info("could not create zookeeper client using "+Deployment.getZookeeperConnect());
      }

    }

  }

  private class VoucherJob{
    private String supplierId;
    private String voucherId;
    private String fileId;
    private String expiryDate;
    private VoucherJobAction action;

    VoucherJob(String supplierId,String voucherId,String fileId,String expiryDate,VoucherJobAction action){
      this.supplierId=supplierId;
      this.voucherId=voucherId;
      this.fileId=fileId;
      this.expiryDate=expiryDate;
      this.action=action;
    }

    VoucherJob(JSONObject json){
      this.supplierId=JSONUtilities.decodeString(json,"supplierId",true);
      this.voucherId=JSONUtilities.decodeString(json,"voucherId",true);
      this.fileId=JSONUtilities.decodeString(json,"fileId",false);
      this.expiryDate=JSONUtilities.decodeString(json,"expiryDate",false);
      this.action=VoucherJobAction.fromExternalRepresentation(JSONUtilities.decodeString(json,"action",true));
    }

    public String getSupplierId() { return supplierId; }
    public String getVoucherId() { return voucherId; }
    public String getFileId() { return fileId; }
    public String getExpiryDate() { return expiryDate; }
    public VoucherJobAction getAction() { return action; }

    JSONObject getJsonRepresentation(){
      Map<String,String> json=new HashMap<>();
      json.put("supplierId",getSupplierId());
      json.put("voucherId",getVoucherId());
      json.put("fileId",getFileId());
      json.put("expiryDate",getExpiryDate());
      json.put("action",getAction().getExternalRepresentation());
      return JSONUtilities.encodeObject(json);
    }

    @Override
    public boolean equals(Object object){
      if(!(object instanceof VoucherJob)) return false;
      VoucherJob voucherJob = (VoucherJob)object;
      if(!Objects.equals(getSupplierId(),voucherJob.getSupplierId())) return false;
      if(!Objects.equals(getVoucherId(),voucherJob.getVoucherId())) return false;
      if(!Objects.equals(getFileId(),voucherJob.getFileId())) return false;
      if(!Objects.equals(getExpiryDate(),voucherJob.getExpiryDate())) return false;
      if(!Objects.equals(getAction(),voucherJob.getAction())) return false;
      return true;
    }

    @Override
    public String toString() {
      return "VoucherJob{" +
              "supplierId='" + supplierId + '\'' +
              ", voucherId='" + voucherId + '\'' +
              ", fileId='" + fileId + '\'' +
              ", expiryDate='" + expiryDate + '\'' +
              ", action=" + action.getExternalRepresentation() +
              '}';
    }

  }

  public class VoucherFileStats{
    private ImportVoucherFileStatus importStatus;
    private Integer stockInFile;
    private Integer stockImported;
    private Integer stockDelivered;
    private Integer stockRedeemed;
    private Integer stockExpired;
    // this one is never stored, always computed from ES view:
    private Integer stockAvailable;
    // need to keep this for zookeeper versioning concurrency control
    private int zookeeperVersion;

    public ImportVoucherFileStatus getImportStatus() { return importStatus; }
    public Integer getStockInFile() { return stockInFile; }
    public Integer getStockImported() { return stockImported; }
    public Integer getStockDelivered() { return stockDelivered; }
    public Integer getStockRedeemed() { return stockRedeemed; }
    public Integer getStockExpired() { return stockExpired; }
    public Integer getStockAvailable() { return stockAvailable; }
    public int getZookeeperVersion() { return zookeeperVersion; }

    public void setImportStatus(ImportVoucherFileStatus importStatus) { this.importStatus = importStatus; }
    public void setStockInFile(Integer stockInFile) { this.stockInFile = stockInFile; }
    public void addStockImported(Integer stockImported) { if(this.stockImported==null)this.stockImported=0;this.stockImported += stockImported; }
    public void addStockDelivered(Integer stockDelivered) { if(this.stockDelivered==null)this.stockDelivered=0;this.stockDelivered += stockDelivered; }
    public void addStockRedeemed(Integer stockRedeemed) { if(this.stockRedeemed==null)this.stockRedeemed=0;this.stockRedeemed = +stockRedeemed; }
    public void addStockExpired(Integer stockExpired) { if(this.stockExpired==null)this.stockExpired=0;this.stockExpired += stockExpired; }
    public void setStockAvailable(Integer stockAvailable) { this.stockAvailable = stockAvailable; }


    private VoucherFileStats() {
      this.importStatus = ImportVoucherFileStatus.Unknown;
      this.stockInFile = 0;
      this.stockImported = 0;
      this.stockDelivered = 0;
      this.stockRedeemed = 0;
      this.stockExpired = 0;
      this.stockAvailable = 0;
      this.zookeeperVersion = -1;
    }

    private VoucherFileStats(JSONObject json,Stat stat){
      this.importStatus = ImportVoucherFileStatus.fromExternalRepresentation(JSONUtilities.decodeString(json,"importStatus",true));
      this.stockInFile = JSONUtilities.decodeInteger(json,"stockInFile",0);
      this.stockImported = JSONUtilities.decodeInteger(json,"stockImported",0);
      this.stockDelivered = JSONUtilities.decodeInteger(json,"stockDelivered",0);
      this.stockRedeemed = JSONUtilities.decodeInteger(json,"stockRedeemed",0);
      this.stockExpired = JSONUtilities.decodeInteger(json,"stockExpired",0);
      this.stockAvailable = 0;
      this.zookeeperVersion = stat.getVersion();
    }

    JSONObject getJsonRepresentation(){
      Map<String,Object> json=new HashMap<>();
      json.put("importStatus",getImportStatus().getExternalRepresentation());
      json.put("stockInFile",getStockInFile());
      json.put("stockImported",getStockImported());
      json.put("stockDelivered",getStockDelivered());
      json.put("stockRedeemed",getStockRedeemed());
      json.put("stockExpired",getStockExpired());
      return JSONUtilities.encodeObject(json);
    }

    @Override
    public String toString() {
      return "VoucherFileStats{" +
              "importStatus=" + importStatus +
              ", stockInFile=" + stockInFile +
              ", stockImported=" + stockImported +
              ", stockDelivered=" + stockDelivered +
              ", stockRedeemed=" + stockRedeemed +
              ", stockExpired=" + stockExpired +
              ", stockAvailable=" + stockAvailable +
              ", zookeeperVersion=" + zookeeperVersion +
              '}';
    }

  }

}
