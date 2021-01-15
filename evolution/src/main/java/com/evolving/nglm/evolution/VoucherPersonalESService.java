package com.evolving.nglm.evolution;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class VoucherPersonalESService{

  private static final String LIVE_VOUCHER_INDEX_PREFIX = "voucher_live_";

  private static final Logger log = LoggerFactory.getLogger(VoucherPersonalESService.class);

  private ElasticsearchClientAPI elasticsearch;
  private boolean isMaster;
  // the indices configuration
  private int liveVoucherIndexNumberOfShards;
  private int liveVoucherIndexNumberOfReplicas;

  // try to get the concurrency update control for "allocatePendingVoucher" a bit smart
  private ConcurrentHashMap<Thread,Integer> threadsPosition = new ConcurrentHashMap<>();
  private ConcurrentLinkedQueue<String> zeroStockVoucherIdCached = new ConcurrentLinkedQueue<>();
  private Timer cacheCleaner = new Timer("VoucherPersonalESService-cacheCleaner",true);
  private static long cacheCleanerFrequencyInMilliSec=Deployment.getVoucherESCacheCleanerFrequencyInSec()*1000L;

  private static final long sleepUnit = 200;
  private static final int maxTries=5;

  public VoucherPersonalESService(ElasticsearchClientAPI elasticsearch, boolean masterService, int liveVoucherIndexNumberOfShards, int liveVoucherIndexNumberOfReplicas) {
    this.elasticsearch = elasticsearch;
    this.isMaster = masterService;
    this.liveVoucherIndexNumberOfShards=liveVoucherIndexNumberOfShards;
    this.liveVoucherIndexNumberOfReplicas=liveVoucherIndexNumberOfReplicas;

    // this entire service makes no sense without elasticsearch client, but we did allow "null" client if just "voucherService" conf is needed
    // in that case, no start of the scheduled task
    if(this.elasticsearch!=null){
      // a scheduled task, for concurrency position spreading "shrinking" back to 0, and cleaning "0 stock" cache
      this.cacheCleaner.scheduleAtFixedRate(new TimerTask() {
        @Override
        public void run() {
          if(log.isDebugEnabled()) log.debug("VoucherPersonalESService-cacheCleaner : start execution");
          // we just try get back slowly to 0 if no more concurrency
          threadsPosition.forEach((thread,position)->{
            Integer newPosition=position>0?position-1:0;
            if(log.isDebugEnabled()) log.debug("VoucherPersonalESService-cacheCleaner : thread "+thread.getName()+" position from "+position+" to "+newPosition);
            if(position!=newPosition) threadsPosition.put(thread,newPosition);
          });
          // we just clear the "zero stock cache"
          if(log.isDebugEnabled()) log.debug("VoucherPersonalESService-cacheCleaner : clearing zeroStockVoucherIdCached, size was "+zeroStockVoucherIdCached.size());
          zeroStockVoucherIdCached.clear();
          if(log.isDebugEnabled()) log.debug("VoucherPersonalESService-cacheCleaner : finish execution");
        }
      },cacheCleanerFrequencyInMilliSec,cacheCleanerFrequencyInMilliSec);
    }
  }

  public void close(){
    if(elasticsearch!=null){
      try {
        this.elasticsearch.close();
      } catch (IOException e) {
        log.warn("VoucherPersonalESService.close : error ",e);
      }
    }
  }

  // bulk delete all expired vouchers
  public boolean deleteExpiredVoucher(Date expiryDate, int tenantID){
    if(elasticsearch==null) log.error("VoucherPersonalESService.deleteExpiredVoucher : called with no elasticsearch client init");
    try{
      for(int i=0;i<maxTries;i++){
        try{
          BulkByScrollResponse response = elasticsearch.deleteByQuery(VoucherPersonalES.getDeleteByQueryExpirationDateRequest(LIVE_VOUCHER_INDEX_PREFIX+"*",expiryDate),RequestOptions.DEFAULT);
          log.info("VoucherPersonalESService.deleteExpiredVoucher : "+response.getDeleted()+" deleted over "+response.getTotal()+" in "+response.getTook().millis()+ "ms");
          if(response.getTotal()==response.getDeleted()){
            return true;
          }else{
            return false;
          }
        }catch (ElasticsearchStatusException e){
          if(e.status()==RestStatus.TOO_MANY_REQUESTS){
            //ES is over loaded, waiting
            try{
              long toSleep=sleepUnit*(i+1);
              log.warn("VoucherPersonalESService.deleteExpiredVoucher : ES overloaded, sleeping "+ toSleep + "ms");
              Thread.sleep(toSleep);
              continue;
            }catch(InterruptedException ie){
              log.warn("VoucherPersonalESService.deleteExpiredVoucher : ",ie);
              continue;
            }
          }else{
            log.warn("VoucherPersonalESService.deleteExpiredVoucher : unhandled ES error "+e.status().getStatus()+", "+e.status().name());
            return false;
          }
        }
      }
    }catch (IOException e){
      log.warn("VoucherPersonalESService.deleteExpiredVoucher : ",e);
      return false;
    }
    log.warn("VoucherPersonalESService.deleteExpiredVoucher : could not get voucher from ES after "+maxTries+" tries");
    return false;
  }

  // bulk delete expired vouchers not allocated
  public int deleteAvailableExpiredVoucher(String supplierId, String voucherId, String fileId,Date expiryDate, int tenantID){
    if(elasticsearch==null) log.error("VoucherPersonalESService.deleteAvailableExpiredVoucher : called with no elasticsearch client init");
    try{
      for(int i=0;i<maxTries;i++){
        try{
          BulkByScrollResponse response = elasticsearch.deleteByQuery(VoucherPersonalES.getDeleteByQueryAvailableExpirationDateRequest(getLiveIndexName(supplierId),voucherId,fileId,expiryDate),RequestOptions.DEFAULT);
          log.info("VoucherPersonalESService.deleteAvailableExpiredVoucher : "+response.getDeleted()+" deleted over "+response.getTotal()+" in "+response.getTook().millis()+ "ms");
          return (int)response.getDeleted();
        }catch (ElasticsearchStatusException e){
          if(e.status()==RestStatus.TOO_MANY_REQUESTS){
            //ES is over loaded, waiting
            try{
              long toSleep=sleepUnit*(i+1);
              log.warn("VoucherPersonalESService.deleteAvailableExpiredVoucher : ES overloaded, sleeping "+ toSleep + "ms");
              Thread.sleep(toSleep);
              continue;
            }catch(InterruptedException ie){
              log.warn("VoucherPersonalESService.deleteAvailableExpiredVoucher : ",ie);
              continue;
            }
          }else{
            log.warn("VoucherPersonalESService.deleteAvailableExpiredVoucher : unhandled ES error "+e.status().getStatus()+", "+e.status().name());
            return 0;
          }
        }
      }
    }catch (IOException e){
      log.warn("VoucherPersonalESService.deleteAvailableExpiredVoucher : ",e);
      return 0;
    }
    log.warn("VoucherPersonalESService.deleteAvailableExpiredVoucher : could not get voucher from ES after "+maxTries+" tries");
    return 0;
  }

  // bulk update voucher expiryDate on file conf change
  public boolean updateExpiryDateForVoucherFile(String supplierId, String voucherId, String fileId, String newExpiryDate){
    if(elasticsearch==null) log.error("VoucherPersonalESService.updateExpiryDateForVoucherFile : called with no elasticsearch client init");
    try{
      for(int i=0;i<maxTries;i++){
        try{
          BulkByScrollResponse response = elasticsearch.updateByQuery(VoucherPersonalES.getUpdateByQueryExpirationDateRequest(getLiveIndexName(supplierId),voucherId,fileId,newExpiryDate),RequestOptions.DEFAULT);
          log.info("VoucherPersonalESService.updateExpiryDateForVoucherFile : "+response.getUpdated()+" updated over "+response.getTotal()+" in "+response.getTook().millis()+ "ms");
          if(response.getTotal()==response.getUpdated()){
            return true;
          }else{
            return false;
          }
        }catch (ElasticsearchStatusException e){
          if(e.status()==RestStatus.TOO_MANY_REQUESTS){
            //ES is over loaded, waiting
            try{
              long toSleep=sleepUnit*(i+1);
              log.warn("VoucherPersonalESService.updateExpiryDateForVoucherFile : ES overloaded, sleeping "+ toSleep + "ms");
              Thread.sleep(toSleep);
              continue;
            }catch(InterruptedException ie){
              log.warn("VoucherPersonalESService.updateExpiryDateForVoucherFile : ",ie);
              continue;
            }
          }else{
            log.warn("VoucherPersonalESService.updateExpiryDateForVoucherFile : unhandled ES error "+e.status().getStatus()+", "+e.status().name());
            return false;
          }
        }
      }
    }catch (IOException e){
      log.warn("VoucherPersonalESService.updateExpiryDateForVoucherFile : ",e);
      return false;
    }
    log.warn("VoucherPersonalESService.updateExpiryDateForVoucherFile : could not get voucher from ES after "+maxTries+" tries");
    return false;
  }

  // bulk delete voucher file when deleted from voucher
  public boolean deleteRemoveVoucherFile(String supplierId, String voucherId, String fileId){
    if(elasticsearch==null) log.error("VoucherPersonalESService.deleteRemoveVoucherFile : called with no elasticsearch client init");
    try{
      for(int i=0;i<maxTries;i++){
        try{
          BulkByScrollResponse response = elasticsearch.deleteByQuery(VoucherPersonalES.getDeleteByQueryRemoveVoucherFileRequest(getLiveIndexName(supplierId),voucherId,fileId),RequestOptions.DEFAULT);
          log.info("VoucherPersonalESService.deleteRemovedVoucherFile : "+response.getDeleted()+" deleted over "+response.getTotal()+" in "+response.getTook().millis()+ "ms");
          if(response.getTotal()==response.getDeleted()){
            return true;
          }else{
            return false;
          }
        }catch (ElasticsearchStatusException e){
          if(e.status()==RestStatus.TOO_MANY_REQUESTS){
            //ES is over loaded, waiting
            try{
              long toSleep=sleepUnit*(i+1);
              log.warn("VoucherPersonalESService.deleteRemovedVoucherFile : ES overloaded, sleeping "+ toSleep + "ms");
              Thread.sleep(toSleep);
              continue;
            }catch(InterruptedException ie){
              log.warn("VoucherPersonalESService.deleteRemovedVoucherFile : ",ie);
              continue;
            }
          }else{
            log.warn("VoucherPersonalESService.deleteRemovedVoucherFile : unhandled ES error "+e.status().getStatus()+", "+e.status().name());
            return false;
          }
        }
      }
    }catch (IOException e){
      log.warn("VoucherPersonalESService.deleteRemovedVoucherFile : ",e);
      return false;
    }
    log.warn("VoucherPersonalESService.deleteRemovedVoucherFile : could not get voucher from ES after "+maxTries+" tries");
    return false;
  }

  // bulk delete voucher
  public boolean deleteRemoveVoucher(String supplierId, String voucherId){
    if(elasticsearch==null) log.error("VoucherPersonalESService.deleteRemoveVoucher : called with no elasticsearch client init");
    try{
      for(int i=0;i<maxTries;i++){
        try{
          BulkByScrollResponse response = elasticsearch.deleteByQuery(VoucherPersonalES.getDeleteByQueryRemoveVoucherRequest(getLiveIndexName(supplierId),voucherId),RequestOptions.DEFAULT);
          log.info("VoucherPersonalESService.deleteRemoveVoucher : "+response.getDeleted()+" deleted over "+response.getTotal()+" in "+response.getTook().millis()+ "ms");
          if(response.getTotal()==response.getDeleted()){
            return true;
          }else{
            return false;
          }
        }catch (ElasticsearchStatusException e){
          if(e.status()==RestStatus.TOO_MANY_REQUESTS){
            //ES is over loaded, waiting
            try{
              long toSleep=sleepUnit*(i+1);
              log.warn("VoucherPersonalESService.deleteRemoveVoucher : ES overloaded, sleeping "+ toSleep + "ms");
              Thread.sleep(toSleep);
              continue;
            }catch(InterruptedException ie){
              log.warn("VoucherPersonalESService.deleteRemoveVoucher : ",ie);
              continue;
            }
          }else{
            log.warn("VoucherPersonalESService.deleteRemoveVoucher : unhandled ES error "+e.status().getStatus()+", "+e.status().name());
            return false;
          }
        }
      }
    }catch (IOException e){
      log.warn("VoucherPersonalESService.deleteRemoveVoucher : ",e);
      return false;
    }
    log.warn("VoucherPersonalESService.deleteRemoveVoucher : could not get voucher from ES after "+maxTries+" tries");
    return false;
  }

  // to get the associate subscriberId to a voucher
  public VoucherPersonalES getESVoucherFromVoucherCode(String supplierId, String voucherCode, int tenantID){
    if(elasticsearch==null) log.error("VoucherPersonalESService.getESVoucherFromVoucherCode : called with no elasticsearch client init");
    try{
      for(int i=0;i<maxTries;i++){
        try{
          GetResponse response = elasticsearch.get(new GetRequest().index(getLiveIndexName(supplierId)).id(voucherCode),RequestOptions.DEFAULT);
          if(response.isExists()){
            VoucherPersonalES toRet = new VoucherPersonalES(response, tenantID);
            if(log.isDebugEnabled()) log.debug("VoucherPersonalESService.getESVoucherFromVoucherCode : found "+toRet);
            return toRet;
          }else{
            if(log.isDebugEnabled()) log.debug("VoucherPersonalESService.getESVoucherFromVoucherCode : voucher not found "+supplierId+", "+voucherCode);
            return null;
          }
        }catch (ElasticsearchStatusException e){
          if(e.status()==RestStatus.TOO_MANY_REQUESTS){
            //ES is over loaded, waiting
            try{
              long toSleep=sleepUnit*(i+1);
              log.warn("VoucherPersonalESService.getESVoucherFromVoucherCode : ES overloaded, sleeping "+ toSleep + "ms");
              Thread.sleep(toSleep);
              continue;
            }catch(InterruptedException ie){
              log.warn("VoucherPersonalESService.getESVoucherFromVoucherCode : ",ie);
              continue;
            }
          }else{
            log.warn("VoucherPersonalESService.getESVoucherFromVoucherCode : unhandled ES error "+e.status().getStatus()+", "+e.status().name());
            return null;
          }
        }
      }
    }catch (IOException e){
      log.warn("VoucherPersonalESService.getESVoucherFromVoucherCode : ",e);
      return null;
    }
    log.warn("VoucherPersonalESService.getESVoucherFromVoucherCode : could not get voucher from ES after "+maxTries+" tries");
    return null;
  }

  // to "un-allocate" a voucher
  public boolean voidReservation(String supplierId, String voucherCode){
    if(elasticsearch==null) log.error("VoucherPersonalESService.voidReservation : called with no elasticsearch client init");
    try{
      for(int i=0;i<maxTries;i++){
        try{
          GetResponse currentVoucher = elasticsearch.get(new GetRequest().index(getLiveIndexName(supplierId)).id(voucherCode),RequestOptions.DEFAULT);
          IndexResponse newVoucher = elasticsearch.index(VoucherPersonalES.getIndexVoucherRemovingSubscriberIdRequest(currentVoucher),RequestOptions.DEFAULT);
          if(log.isDebugEnabled()) log.debug("VoucherPersonalESService.voidReservation : old value "+currentVoucher.toString()+"; new value "+newVoucher.toString());
          return true;
        }catch (ElasticsearchStatusException e){
          if(e.status()==RestStatus.TOO_MANY_REQUESTS){
            //ES is over loaded, waiting
            try{
              long toSleep=sleepUnit*(i+1);
              log.warn("VoucherPersonalESService.voidReservation : ES overloaded, sleeping "+ toSleep + "ms");
              Thread.sleep(toSleep);
              continue;
            }catch(InterruptedException ie){
              log.warn("VoucherPersonalESService.voidReservation : ",ie);
              continue;
            }
          }else{
            log.warn("VoucherPersonalESService.voidReservation : unhandled ES error "+e.status().getStatus()+", "+e.status().name());
            return false;
          }
        }
      }
    }catch (IOException e){
      log.warn("VoucherPersonalESService.voidReservation : ",e);
      return false;
    }
    log.warn("VoucherPersonalESService.voidReservation : could not get voucher from ES after "+maxTries+" tries");
    return false;
  }

  // allocate in ES and return a voucher to a subscriber
  // concurrency control idea : https://www.elastic.co/guide/en/elasticsearch/reference/current/optimistic-concurrency-control.html
  public VoucherPersonalES allocatePendingVoucher(String supplierId,String voucherId, String subscriberId, int tenantID){
    if(elasticsearch==null) log.error("VoucherPersonalESService.allocatePendingVoucher : called with no elasticsearch client init");

    if(zeroStockVoucherIdCached.contains(voucherId)){
      if(log.isDebugEnabled()) log.debug("VoucherPersonalESService.allocatePendingVoucher : no voucher available from cache status for "+voucherId);
      return null;
    }

    String index = getLiveIndexName(supplierId);

    int maxTries=100;
    int myPosition=threadsPosition.get(Thread.currentThread())==null?0:threadsPosition.get(Thread.currentThread());
    int myMaxPosition=myPosition+Deployment.getNumberConcurrentVoucherAllocationToES();

    try{

      // retries loop
      for(int requestLoop=0;requestLoop<maxTries;requestLoop++){

        // search for some available vouchers

        SearchResponse response = null;
        Date minExpiryDate=EvolutionUtilities.addTime(SystemTime.getCurrentTime(),Deployment.getMinExpiryDelayForVoucherDeliveryInHours(), EvolutionUtilities.TimeUnit.Hour,Deployment.getDeployment(tenantID).getBaseTimeZone());
        try{
          response=elasticsearch.search(VoucherPersonalES.getSearchAvailableVouchersRequest(myMaxPosition+1,index, voucherId, minExpiryDate),RequestOptions.DEFAULT);
        }catch (ElasticsearchStatusException e){
          if(e.status()==RestStatus.TOO_MANY_REQUESTS){
            //ES is over loaded, waiting
            try{
              long toSleep=sleepUnit;
              log.warn("VoucherPersonalESService.allocatePendingVoucher : ES overloaded, sleeping "+ toSleep + "ms");
              Thread.sleep(toSleep);
              continue;
            }catch(InterruptedException ie){
              log.warn("VoucherPersonalESService.allocatePendingVoucher : ",ie);
              continue;
            }
          }else{
            log.warn("VoucherPersonalESService.allocatePendingVoucher : unhandled ES error "+e.status().getStatus()+", "+e.status().name());
            return null;
          }
        }

        if(log.isDebugEnabled()) log.debug("VoucherPersonalESService.allocatePendingVoucher : "+response.toString());
        if(response.getFailedShards()>0) log.info("VoucherPersonalESService.allocatePendingVoucher : "+response.getFailedShards()+" failed shards over "+response.getTotalShards()+" , INCOMPLETE LIST, but continue");
        if(response.getHits().getTotalHits().value<=0){
          // cache it for a while
          zeroStockVoucherIdCached.add(voucherId);
          log.info("VoucherPersonalESService.allocatePendingVoucher : no voucher available for "+voucherId+", will cache this");
          return null;
        }

        // try to reserve one of them
        int i=-1;
        List<SearchHit> results = new ArrayList<>(Arrays.asList(response.getHits().getHits()));
        while(!results.isEmpty()){
          if(i==-1){
            //first loop
            i=myPosition;
          }else{
            // not the first loop, we failed already, shifting position and try again
            i++;
            if(i>myMaxPosition) i=myMaxPosition;
            // we don't move the position on concurrency failed if not enough free voucher in ES
            if(i>myPosition && i<results.size()) threadsPosition.put(Thread.currentThread(),i);
          }
          // might be there is not enough results anymore, capping to still try getting one
          if(results.size()<=i) i=results.size()-1;

          SearchHit resp = results.remove(i);

          long primaryTerm = resp.getPrimaryTerm();//concurrency update control
          long seqNo = resp.getSeqNo();//concurrency update control

          VoucherPersonalES candidateToRet = new VoucherPersonalES(resp, tenantID);
          if(candidateToRet==null){
            log.warn("VoucherPersonalESService.allocatePendingVoucher : bug while searching available voucher, null");
            continue;
          }
          if(candidateToRet.getSubscriberId()!=null){
            log.warn("VoucherPersonalESService.allocatePendingVoucher : bug while searching available voucher, "+candidateToRet.getVoucherCode()+" belong to "+candidateToRet.getSubscriberId());
            continue;
          }
          if(candidateToRet.getExpiryDate().before(minExpiryDate)){
            log.warn("VoucherPersonalESService.allocatePendingVoucher : voucher expiry date is too soon "+candidateToRet.getVoucherCode()+" ,"+candidateToRet.getExpiryDate());
            continue;
          }

          // try to "allocate" into ES
          try{
            UpdateResponse updateResponse=elasticsearch.update(VoucherPersonalES.getUpdateVoucherWithSubscriberIdRequest(index,candidateToRet.getVoucherCode(),seqNo,primaryTerm,subscriberId),RequestOptions.DEFAULT);
            if(updateResponse.status()==RestStatus.OK && updateResponse.getResult()==DocWriteResponse.Result.UPDATED){
              //method success
              candidateToRet.setSubscriberId(subscriberId);
              return candidateToRet;
            }else{
              log.warn("VoucherPersonalESService.allocatePendingVoucher : unexpected result to handle ? "+updateResponse.getResult().name());
            }
          } catch (ElasticsearchStatusException e){
            if(e.status()==RestStatus.CONFLICT){
              //normal "optimistic update" failure, try an other one
              continue;
            }else if(e.status()==RestStatus.TOO_MANY_REQUESTS){
              try{
                Thread.sleep(sleepUnit*(requestLoop+1));
                continue;
              }catch(InterruptedException ie){
                log.warn("VoucherPersonalESService.allocatePendingVoucher : ",ie);
                continue;
              }
            }else{
              log.warn("VoucherPersonalESService.allocatePendingVoucher : unhandled ES error "+e.status().getStatus()+", "+e.status().name());
              return null;
            }
          }

        }

        log.info("VoucherPersonalESService.allocatePendingVoucher : could not reserved a voucher among "+myMaxPosition);

      }
    } catch (IOException e){
      log.warn("VoucherPersonalESService.allocatePendingVoucher : ",e);
      return null;
    }

    log.warn("VoucherPersonalESService.allocatePendingVoucher : could not reserved any vouchers voucher after "+maxTries+" tries!");

    return null;
  }

  // return numbers of really added vouchers
  public int addVouchers(String suplierID, List<VoucherPersonalES> vouchers){
    if(elasticsearch==null) log.error("VoucherPersonalESService.addVouchers : called with no elasticsearch client init");

    if(vouchers==null||vouchers.isEmpty()){
      log.info("VoucherPersonalESService.addVouchers : call without any voucher to add, returning 0");
      return 0;
    }

    String index=getLiveIndexName(suplierID);
    if(log.isDebugEnabled()) log.debug("VoucherPersonalESService.addVouchers : will try to bulk add "+vouchers.size()+" vouchers in "+index);

    int added=0;
    try{

      for(int requestLoop=0;requestLoop<maxTries;requestLoop++){

        BulkResponse response=null;
        try {
          response = elasticsearch.bulk(VoucherPersonalES.getAddVouchersBulkRequest(index,vouchers),RequestOptions.DEFAULT);
        }catch (ElasticsearchStatusException e){
          if(e.status()==RestStatus.TOO_MANY_REQUESTS){
            //ES is over loaded, waiting
            try{
              long toSleep=sleepUnit;
              log.warn("VoucherPersonalESService.addVouchers : ES overloaded, sleeping "+ toSleep + "ms");
              Thread.sleep(toSleep);
              continue;
            }catch(InterruptedException ie){
              log.warn("VoucherPersonalESService.addVouchers : ",ie);
              continue;
            }
          }else{
            log.warn("VoucherPersonalESService.addVouchers : unhandled ES error "+e.status().getStatus()+", "+e.status().name());
            return added;
          }
        }

        if(response!=null){
          Iterator<BulkItemResponse> it = response.iterator();
          while(it.hasNext()){
            BulkItemResponse rep = it.next();
            if(log.isDebugEnabled()) log.debug("VoucherPersonalESService.addVouchers : added : "+rep.toString());
            if(!rep.isFailed()){
              added++;
            }else{
              if(log.isDebugEnabled()) log.debug("VoucherPersonalESService.addVouchers : fail to add voucher "+rep.getId()+", "+rep.getFailureMessage());
            }
          }
        }

        if(log.isDebugEnabled()) log.debug("VoucherPersonalESService.addVouchers : "+added+" vouchers added to "+index);
        return added;

      }

    } catch (IOException e) {
      log.warn("VoucherPersonalESService.addVouchers : error ",e);
    }

    if(log.isDebugEnabled()) log.debug("VoucherPersonalESService.addVouchers : "+added+" vouchers added to "+index);

    return added;
  }

  public boolean createSupplierVoucherIndexIfNotExist(String supplierID) throws ElasticsearchException {
    if(elasticsearch==null) log.error("VoucherPersonalESService.createSupplierVoucherIndexIfNotExist : called with no elasticsearch client init");
    if(!this.isMaster){
      log.error("VoucherPersonalESService.createSupplierVoucherIndexIfNotExist : can not create index on none master service");
      return false;
    }
    // check if exists
    try{

      for(int i=0;i<maxTries;i++){
        try{
          if(elasticsearch.indices().exists(new GetIndexRequest(getLiveIndexName(supplierID)),RequestOptions.DEFAULT)){
            if(log.isDebugEnabled()) log.debug("VoucherPersonalESService.createSupplierVoucherIndexIfNotExist : "+getLiveIndexName(supplierID)+" already exists");
            return true;
          }
        }catch (ElasticsearchStatusException e){
          if(e.status()==RestStatus.TOO_MANY_REQUESTS){
            //ES is over loaded, waiting
            try{
              long toSleep=sleepUnit*(i+1);
              log.warn("VoucherPersonalESService.createSupplierVoucherIndexIfNotExist : ES overloaded, sleeping "+ toSleep + "ms");
              Thread.sleep(toSleep);
              continue;
            }catch(InterruptedException ie){
              log.warn("VoucherPersonalESService.createSupplierVoucherIndexIfNotExist : ",ie);
              continue;
            }
          }else{
            log.warn("VoucherPersonalESService.createSupplierVoucherIndexIfNotExist : unhandled ES error "+e.status().getStatus()+", "+e.status().name());
          }
        }
      }

      boolean toRet=false;
      CreateIndexResponse response=null;
      for(int i=0;i<maxTries;i++){
        try{
          response = elasticsearch.indices().create(VoucherPersonalES.getCreateLiveVoucherIndexRequest(getLiveIndexName(supplierID),this.liveVoucherIndexNumberOfShards,this.liveVoucherIndexNumberOfReplicas), RequestOptions.DEFAULT);
          toRet=response.isAcknowledged()&&response.isShardsAcknowledged();
          if(log.isDebugEnabled()) log.debug("VoucherPersonalESService.createSupplierVoucherIndexIfNotExist : created ES index "+response.index());
        }catch (ElasticsearchException e){
          //should be "alreadyexists" error case, normally should not happens, as only one thread on the entire platform should do this
          if(e.status()== RestStatus.BAD_REQUEST){
            if(log.isDebugEnabled()) log.debug("VoucherPersonalESService.createSupplierVoucherIndexIfNotExist : already exist ES index "+getLiveIndexName(supplierID));
            toRet=true;
          }else{
            log.error("VoucherPersonalESService.createSupplierVoucherIndexIfNotExist ",e);
            throw e;
          }
        }
        if(!toRet) log.error("VoucherPersonalESService.createSupplierVoucherIndexIfNotExist : isAcknowledged:"+response.isAcknowledged()+", isShardsAcknowledged:"+response.isShardsAcknowledged());
        return toRet;
      }

    }catch (IOException e){
      log.warn("VoucherPersonalESService.createSupplierVoucherIndexIfNotExist : exists check exception ",e);
    }
    return false;
  }

  // put the current stats for voucher from ES
  public void populateVoucherFileWithStockInformation(VoucherPersonal voucher, int tenantID){
    if(elasticsearch==null) log.error("VoucherPersonalESService.populateVoucherFileWithStockInformation : called with no elasticsearch client init");
    if(!this.isMaster){
      log.error("VoucherPersonalESService.populateVoucherFileWithStockInformation : is has not been meant to be executed in an non master service");
      return;
    }
    try{
      for(int i=0;i<maxTries;i++){
        try{
          SearchResponse response = elasticsearch.search(VoucherPersonalES.getSearchRequestGettingVoucherStocksInfo(getLiveIndexName(voucher.getSupplierID()),voucher.getVoucherID(), tenantID),RequestOptions.DEFAULT);
          log.info("VoucherPersonalESService.populateVoucherFileWithStockInformation : "+response.getSuccessfulShards()+" shards respond over "+response.getTotalShards()+" in "+response.getTook().millis()+ "ms");
          if(log.isDebugEnabled()) log.debug("VoucherPersonalESService.populateVoucherFileWithStockInformation : "+response.toString());
          if(response.getSuccessfulShards()!=response.getTotalShards()) log.warn("VoucherPersonalESService.populateVoucherFileWithStockInformation : incomplete shards response "+response.getSuccessfulShards()+" vs "+response.getTotalShards());
          // response parsing is highly dependent to the request, so putting code next to requests code
          VoucherPersonalES.processGettingVoucherStocksInfoResponse(response,voucher);
          return;
        }catch (ElasticsearchStatusException e){
          if(e.status()==RestStatus.TOO_MANY_REQUESTS){
            //ES is over loaded, waiting
            try{
              long toSleep=sleepUnit*(i+1);
              log.warn("VoucherPersonalESService.populateVoucherFileWithStockInformation : ES overloaded, sleeping "+ toSleep + "ms");
              Thread.sleep(toSleep);
              continue;
            }catch(InterruptedException ie){
              log.warn("VoucherPersonalESService.populateVoucherFileWithStockInformation : ",ie);
              continue;
            }
          }else{
            log.warn("VoucherPersonalESService.populateVoucherFileWithStockInformation : unhandled ES error "+e.status().getStatus()+", "+e.status().name());
            return;
          }
        }
      }
    }catch (IOException e){
      log.warn("VoucherPersonalESService.populateVoucherFileWithStockInformation : ",e);
      return;
    }
    log.warn("VoucherPersonalESService.populateVoucherFileWithStockInformation : could not get voucher from ES after "+maxTries+" tries");
    return;
  }

  private String getLiveIndexName(String supplierID){
    return LIVE_VOUCHER_INDEX_PREFIX+supplierID;
  }

}