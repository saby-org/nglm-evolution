package com.evolving.nglm.evolution;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SystemTime;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

public class VoucherPersonalES {

  private static final Logger log = LoggerFactory.getLogger(VoucherPersonalES.class);
  
  public enum ES_FIELDS {
    subscriberId("keyword"),
    voucherId("keyword"),
    fileId("keyword"),
    expiryDate("date", RLMDateUtils.DatePattern.ELASTICSEARCH_UNIVERSAL_TIMESTAMP.get());

    private String type;
    private String format;
    ES_FIELDS(String type){
      this(type,null);
    }
    ES_FIELDS(String type, String format){
      this.type=type;
      this.format=format;
    }
    public String getType(){
      return type;
    }
    public String getFormat(){
      return format;
    }
  }
  
  private static XContentBuilder mapping;
  static{
    try{
      XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("properties");
      for(ES_FIELDS field:ES_FIELDS.values()){
        builder.startObject(field.name()).field("type",field.getType());
        if(field.getFormat()!=null) builder.field("format",field.getFormat());
        builder.endObject();
      }
      builder.endObject().endObject();
      mapping=builder;
    }catch (IOException e){
      log.error("init mapping exception",e);
      NGLMRuntime.shutdown();
    }
  }

  public static XContentBuilder mapping() { return mapping; }

  private String voucherCode;
  private Date expiryDate;
  private String subscriberId;
  private String voucherId;
  private String fileId;

  public String getVoucherCode(){return voucherCode;}
  public String getVoucherId(){return voucherId;}
  public String getSubscriberId(){return subscriberId;}
  public Date getExpiryDate(){return expiryDate;}
  public String getFileId(){return fileId;}

  public void setSubscriberId(String subscriberId){this.subscriberId=subscriberId;}

  public VoucherPersonalES(String voucherCode, Date expiryDate, String voucherId, String fileId){
    this.voucherCode=voucherCode;
    this.expiryDate=expiryDate;
    this.voucherId=voucherId;
    this.fileId=fileId;
  }

  // return an instance from GetResponse
  public VoucherPersonalES(GetResponse getResponse, int tenantID){
    this.voucherCode=getResponse.getId();
    this.expiryDate=parseVoucherDate((String)getResponse.getSourceAsMap().get(ES_FIELDS.expiryDate.name()));
    this.voucherId=(String)getResponse.getSourceAsMap().get(ES_FIELDS.voucherId.name());
    this.subscriberId=(String)getResponse.getSourceAsMap().get(ES_FIELDS.subscriberId.name());
    this.fileId=(String)getResponse.getSourceAsMap().get(ES_FIELDS.fileId.name());
  }

  // return an instance from ES SearchHit
  public VoucherPersonalES(SearchHit searchHit, int tenantID){
    this.voucherCode=searchHit.getId();
    this.expiryDate=parseVoucherDate((String)searchHit.getSourceAsMap().get(ES_FIELDS.expiryDate.name()));
    this.voucherId=(String)searchHit.getSourceAsMap().get(ES_FIELDS.voucherId.name());
    this.subscriberId=(String)searchHit.getSourceAsMap().get(ES_FIELDS.subscriberId.name());
    this.fileId=(String)searchHit.getSourceAsMap().get(ES_FIELDS.fileId.name());
  }
  
  // Wrap the exception in a runtime one.
  public static Date parseVoucherDate(String date) {
    try
      {
        return RLMDateUtils.parseDateFromElasticsearch(date);
      } 
    catch (ParseException e)
      {
        throw new ServerRuntimeException(e);
      }
  }

  // construct the DeleteByQuery for deleting by voucherId, fileId and expiryDate
  public static DeleteByQueryRequest getDeleteByQueryAvailableExpirationDateRequest(String index, String voucherId, String fileId, Date expiryDate){
    DeleteByQueryRequest request = new DeleteByQueryRequest(index);
    request.setQuery(
            QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery(ES_FIELDS.voucherId.name(),voucherId))
                    .filter(QueryBuilders.termQuery(ES_FIELDS.fileId.name(),fileId))
                    .mustNot(QueryBuilders.existsQuery(ES_FIELDS.subscriberId.name()))//do not delete already allocated voucher
                    .filter(QueryBuilders.rangeQuery(ES_FIELDS.expiryDate.name()).lt(RLMDateUtils.formatDateForElasticsearchDefault(expiryDate))) 
    );
    request.setAbortOnVersionConflict(false);//don't stop entire update on version conflict
    return request;
  }

  // construct the DeleteByQuery for deleting all by expiryDate
  public static DeleteByQueryRequest getDeleteByQueryExpirationDateRequest(String index, Date expiryDate){
    DeleteByQueryRequest request = new DeleteByQueryRequest(index);
    request.setQuery(
      QueryBuilders.boolQuery()
        .filter(QueryBuilders.rangeQuery(ES_FIELDS.expiryDate.name()).lt(RLMDateUtils.formatDateForElasticsearchDefault(expiryDate)))
    );
    request.setAbortOnVersionConflict(false);//don't stop entire update on version conflict
    return request;
  }

  // construct the UpdateByQuery for updating the expiryDate for a voucher file (updating as well already "allocated" ones, in case of allocation rollback (and if not date in ES does not matter anymore, the one stored in voucher in  SubscriberProfile only is the real)
  public static UpdateByQueryRequest getUpdateByQueryExpirationDateRequest(String index, String voucherId, String fileId, String newExpiryDate){
    UpdateByQueryRequest request = new UpdateByQueryRequest(index);
    request.setQuery(
      QueryBuilders.boolQuery()
        .filter(QueryBuilders.termQuery(ES_FIELDS.voucherId.name(),voucherId))
        .filter(QueryBuilders.termQuery(ES_FIELDS.fileId.name(),fileId))
    );
    request.setScript(new Script(ScriptType.INLINE, "painless",
            "ctx._source."+ES_FIELDS.expiryDate.name()+"=\""+newExpiryDate+"\"",//the update expiryDate script
            Collections.emptyMap()));
    request.setAbortOnVersionConflict(false);//don't stop entire update on version conflict
    return request;
  }

  // construct the DeleteByQueryRequest for deleting voucher file
  public static DeleteByQueryRequest getDeleteByQueryRemoveVoucherFileRequest(String index, String voucherId, String fileId){
    DeleteByQueryRequest request = new DeleteByQueryRequest(index);
    request.setQuery(
            QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery(ES_FIELDS.voucherId.name(),voucherId))
                    .filter(QueryBuilders.termQuery(ES_FIELDS.fileId.name(),fileId))
                    .mustNot(QueryBuilders.existsQuery(ES_FIELDS.subscriberId.name()))//do not delete already allocated voucher
    );
    request.setAbortOnVersionConflict(false);//don't stop entire update on version conflict
    return request;
  }

  // construct the DeleteByQueryRequest for deleting a voucher
  public static DeleteByQueryRequest getDeleteByQueryRemoveVoucherRequest(String index, String voucherId){
    DeleteByQueryRequest request = new DeleteByQueryRequest(index);
    request.setQuery(
            QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery(ES_FIELDS.voucherId.name(),voucherId))
                    .mustNot(QueryBuilders.existsQuery(ES_FIELDS.subscriberId.name()))//do not delete already allocated voucher
    );
    request.setAbortOnVersionConflict(false);//don't stop entire update on version conflict
    return request;
  }

  // construct the SearchRequest searching for available vouchers
  public static SearchRequest getSearchAvailableVouchersRequest(int nbVouchers, String index, String voucherId, Date minExpiryDate){
    SearchRequest request = new SearchRequest();
    request.indices(index);

    BoolQueryBuilder builder = QueryBuilders.boolQuery();
    builder.mustNot(QueryBuilders.existsQuery(ES_FIELDS.subscriberId.name()));// no subscriberId associated, voucher is not allocated yet
    builder.filter().add(QueryBuilders.termQuery(ES_FIELDS.voucherId.name(),voucherId));
    builder.filter().add(QueryBuilders.rangeQuery(ES_FIELDS.expiryDate.name()).gte(RLMDateUtils.formatDateForElasticsearchDefault(minExpiryDate)));//with expiry date after minExpiryDate

    request.source(SearchSourceBuilder.searchSource().version(true).seqNoAndPrimaryTerm(true)//needed if a "reservation need to happen, "for optimistic concurrency update control"
    .query(builder)
    .sort(ES_FIELDS.expiryDate.name(), SortOrder.ASC)//sort by "oldest expiry"
    .from(0)
    .size(nbVouchers));//result size

    return request;
  }

  // constuct the UpdateRequest of voucher with a subscriberId, which "allocate" it
  public static UpdateRequest getUpdateVoucherWithSubscriberIdRequest(String index, String voucherCode, long seqNo, long primaryTerm, String subscriberId){
    UpdateRequest request=null;
    try{
      request = new UpdateRequest(index,voucherCode);
      request.setIfSeqNo(seqNo).setIfPrimaryTerm(primaryTerm)//this control the concurrency update
      .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)//this query we forced the refresh immediately (change visible to other right now)
      .doc(XContentFactory.jsonBuilder()
      .startObject()
        .field(ES_FIELDS.subscriberId.name(),subscriberId)
      .endObject());
    } catch (IOException e){
      log.error("VoucherPersonalES.getUpdateVoucherWithSubscriberIdRequest ",e);
    }
    return request;
  }

  // construct the IndexRequest of voucher removing subscriberId field, which "un-allocate" it
  public static IndexRequest getIndexVoucherRemovingSubscriberIdRequest(GetResponse getResponse){
    Map<String,Object> voucherSourceUpdated=new HashMap<>(getResponse.getSource());
    voucherSourceUpdated.remove(ES_FIELDS.subscriberId.name());//copy of source with subscriberId field removed
    return new IndexRequest(getResponse.getIndex()).create(false).id(getResponse.getId()).setIfSeqNo(getResponse.getSeqNo()).setIfPrimaryTerm(getResponse.getPrimaryTerm()).source(voucherSourceUpdated);
  }

  // construct the BulkRequest to store new vouchers in the index
  public static BulkRequest getAddVouchersBulkRequest(String index, List<VoucherPersonalES> vouchers){
    BulkRequest requests = new BulkRequest();
    vouchers.forEach((voucher)->{
      IndexRequest request = new IndexRequest().index(index).id(voucher.getVoucherCode()); // doc id is the voucherCode, then should be unique per index
      try {
        request.create(true).source(XContentFactory.jsonBuilder()
                .startObject()
                .field(ES_FIELDS.voucherId.name(),voucher.getVoucherId())
                .field(ES_FIELDS.fileId.name(),voucher.getFileId())
                .field(ES_FIELDS.expiryDate.name(),RLMDateUtils.formatDateForElasticsearchDefault(voucher.getExpiryDate()))
                .endObject()
        );
        requests.add(request);
      } catch (IOException e) {
        log.error("VoucherPersonalES.getAddVouchersBulkRequest ",e);
      }
    });
    return requests;
  }

  // construct the CreateIndexRequest to create live voucher index
  public static CreateIndexRequest getCreateLiveVoucherIndexRequest(String index, int liveVoucherIndexNumberOfShards, int liveVoucherIndexNumberOfReplicas) {
    CreateIndexRequest request = new CreateIndexRequest(index);
    request.settings(Settings.builder()
            .put("index.number_of_shards", liveVoucherIndexNumberOfShards)
            .put("index.number_of_replicas", liveVoucherIndexNumberOfReplicas)
    );
    request.mapping(VoucherPersonalES.mapping());
    return request;
  }

  // construct the SearchRequest to get stock info per voucherId->voucherFile->expired/expired
  public static SearchRequest getSearchRequestGettingVoucherStocksInfo(String index, String voucherId, int tenantID) {

    String dateConsideredForExpiry=RLMDateUtils.formatDateForElasticsearchDefault(EvolutionUtilities.addTime(SystemTime.getCurrentTime(),Deployment.getMinExpiryDelayForVoucherDeliveryInHours(), EvolutionUtilities.TimeUnit.Hour,Deployment.getDeployment(tenantID).getTimeZone()));

    int aggTermSize = 1000;// should be way lower, safety

    SearchRequest request = new SearchRequest(index).source(
      new SearchSourceBuilder()
      .query(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery(ES_FIELDS.voucherId.name(),voucherId))).size(0)//just desired voucherID (but do not returned any doc, we just care about aggregation result
      .aggregation(
        AggregationBuilders.terms("file").size(aggTermSize).field(ES_FIELDS.fileId.name())//group by filedId
        .subAggregation(AggregationBuilders.filters("expiry",//split into a "expiry" bucket
          new FiltersAggregator.KeyedFilter("expired",QueryBuilders.rangeQuery(ES_FIELDS.expiryDate.name()).lte(dateConsideredForExpiry)),//with "expired" ones
          new FiltersAggregator.KeyedFilter("not_expired",QueryBuilders.rangeQuery(ES_FIELDS.expiryDate.name()).gt(dateConsideredForExpiry)))//and "not_expired" ones
          .subAggregation(
            AggregationBuilders.missing("not_allocated").field(ES_FIELDS.subscriberId.name())//and aggregate the 2 previous bucket with "not_allocated" number
          )
        )
      )
    );
    return request;
  }
  // highly dependent with getSearchRequestGettingStocksInfo SearchRequest above, response parsing of it
  public static void processGettingVoucherStocksInfoResponse(SearchResponse response, VoucherPersonal voucher){
    // not all voucher or in ES, will put 0 in that case
    Set<String> voucherFilesNotInES=new HashSet<>();
    voucher.getVoucherFiles().stream().forEach(voucherFile -> voucherFilesNotInES.add(voucherFile.getFileId()));
    //parsin ES results
    Terms fileAggregationTerms = response.getAggregations().get("file");
    for(Terms.Bucket fileBucket:fileAggregationTerms.getBuckets()){
      Filters expiryFilters = fileBucket.getAggregations().get("expiry");
      Filters.Bucket expiredBucket = expiryFilters.getBucketByKey("expired");
      Filters.Bucket notExpiredBucket = expiryFilters.getBucketByKey("not_expired");
      Missing notAllocatedExpiredMissing = expiredBucket.getAggregations().get("not_allocated");
      Missing notAllocatedNotExpiredMissing = notExpiredBucket.getAggregations().get("not_allocated");
      //OK we parsed all what we could, processed answer
      String fileId = fileBucket.getKeyAsString();
      Integer expired = (int)notAllocatedExpiredMissing.getDocCount();//only the "not allocated expired"
      Integer available = (int)notAllocatedNotExpiredMissing.getDocCount();
      Integer allocated = ((int)expiredBucket.getDocCount() - expired) + ((int)notExpiredBucket.getDocCount() - available);//all the allocated
      if(log.isDebugEnabled()) log.debug("VoucherPersonalES.processGettingVoucherStocksInfoResponse : "+fileId+":"+expired+","+available+","+allocated);
      //is in ES
      voucherFilesNotInES.remove(fileId);
      // populate the right file
      for(VoucherFile voucherFile:voucher.getVoucherFiles()){
        if(voucherFile.getFileId().equals(fileId)){
          voucherFile.getVoucherFileStats().addStockExpired(expired);
          voucherFile.getVoucherFileStats().setStockAvailable(available);
          voucherFile.getVoucherFileStats().addStockDelivered(allocated);
          break;
        }
      }
    }
  }

  @Override
  public String toString() {
    return "VoucherPersonalES{" +
            "voucherCode='" + voucherCode + '\'' +
            ", expiryDate=" + expiryDate +
            ", subscriberId='" + subscriberId + '\'' +
            ", voucherId='" + voucherId + '\'' +
            ", fileId='" + fileId + '\'' +
            '}';
  }

}
