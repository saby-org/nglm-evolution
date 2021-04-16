/*****************************************************************************
*
*  PointBalance.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import com.evolving.nglm.core.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryOperation;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryRequest;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryStatus;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.EvolutionUtilities.RoundingSelection;


public class PointBalance
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(PointBalance.class);

  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema schema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("point_balance");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(3));
    schemaBuilder.field("expirationDates", SchemaBuilder.array(Timestamp.SCHEMA));
    schemaBuilder.field("points", SchemaBuilder.array(Schema.INT32_SCHEMA));
    schemaBuilder.field("earnedHistory", MetricHistory.schema());
    schemaBuilder.field("consumedHistory", MetricHistory.schema());
    schemaBuilder.field("expiredHistory", MetricHistory.schema());
    schemaBuilder.field("tenantID", Schema.INT16_SCHEMA);
    schemaBuilder.field("redemptionHistory", MetricHistory.schema()); // Number of time we *consume* (debit) a bunch of points.
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<PointBalance> serde = new ConnectSerde<PointBalance>(schema, false, PointBalance.class, PointBalance::pack, PointBalance::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<PointBalance> serde() { return serde; }
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private SortedMap<Date,Integer> balances;
  private MetricHistory earnedHistory;
  private MetricHistory consumedHistory;
  private MetricHistory expiredHistory;
  private int tenantID;
  private MetricHistory redemptionHistory;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public SortedMap<Date,Integer> getBalances() { return balances; }
  public MetricHistory getEarnedHistory() { return earnedHistory; }
  public MetricHistory getConsumedHistory() { return consumedHistory; }
  public MetricHistory getExpiredHistory() { return expiredHistory; }
  public int getTenantID() { return tenantID; }
  public MetricHistory getRedemptionHistory() { return redemptionHistory; }

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  public PointBalance()
  {
    this.balances = new TreeMap<Date,Integer>();
    this.earnedHistory = new MetricHistory(0, 0, tenantID);   // TODO : what are the right values for numberOfDailyBuckets and numberOfMonthlyBuckets ?
    this.consumedHistory = new MetricHistory(0, 0, tenantID); // TODO : what are the right values for numberOfDailyBuckets and numberOfMonthlyBuckets ?
    this.expiredHistory = new MetricHistory(0, 0, tenantID);  // TODO : what are the right values for numberOfDailyBuckets and numberOfMonthlyBuckets ?
    this.redemptionHistory = new MetricHistory(0, 0, tenantID);  // TODO : what are the right values for numberOfDailyBuckets and numberOfMonthlyBuckets ?
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  private PointBalance(SortedMap<Date,Integer> balances, MetricHistory earnedHistory, MetricHistory consumedHistory, MetricHistory expiredHistory, MetricHistory redemptionHistory, int tenantID)
  {
    this.balances = balances;
    this.earnedHistory = earnedHistory;
    this.consumedHistory = consumedHistory;
    this.expiredHistory = expiredHistory;
    this.tenantID = tenantID;
    this.redemptionHistory = redemptionHistory;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  public PointBalance(PointBalance pointBalance)
  {
    this.balances = new TreeMap<Date,Integer>(pointBalance.getBalances()); 
    this.earnedHistory = new MetricHistory(pointBalance.getEarnedHistory());
    this.consumedHistory = new MetricHistory(pointBalance.getConsumedHistory());
    this.expiredHistory = new MetricHistory(pointBalance.getExpiredHistory());
    this.tenantID = pointBalance.getTenantID();
    this.redemptionHistory = new MetricHistory(pointBalance.getRedemptionHistory());
  }

  /*****************************************
  *
  *  getBalance
  *
  *****************************************/

  public int getBalance(Date evaluationDate)
  {
    int result = 0;
    if (evaluationDate != null)
      {
        for (Date expirationDate : balances.keySet())
          {
            if (evaluationDate.compareTo(expirationDate) <= 0)
              {
                result += balances.get(expirationDate);
              }
          }
      }
    return result;
  }

  /*****************************************
  *
  *  getFirstExpirationDate
  *
  *****************************************/
  
  public Date getFirstExpirationDate(Date evaluationDate)
  {
    Date result = null;
    for (Date expirationDate : balances.keySet())
      {
        if (evaluationDate.compareTo(expirationDate) <= 0)
          {
            result = expirationDate;
            break;
          }
      }
    return result;
  }

  /*****************************************
  *
  *  update
  *
  *****************************************/

  public boolean update(EvolutionEventContext context, PointFulfillmentRequest pointFulfillmentResponse, String eventID, String moduleID, String featureID, String subscriberID, CommodityDeliveryOperation operation, int amount, Point point, Date evaluationDate, boolean generateBDR, String tier, int tenantID)
  {
    //
    //  validate
    //
    switch (operation)
      {
        case Credit:
          if (! point.getCreditable()) return false;
          break;

        case Debit:
          if (! point.getDebitable()) return false;
          if (amount > getBalance(evaluationDate)) return false;
          break;
      }

    //
    //  normalize
    //

    DeliveryManagerDeclaration pointManagerDeclaration = Deployment.getDeliveryManagers().get("pointFulfillment");
    JSONObject pointManagerJSON = (pointManagerDeclaration != null) ? pointManagerDeclaration.getJSONRepresentation() : null;
    String providerID = (pointManagerJSON != null) ? (String) pointManagerJSON.get("providerID") : null;
    Iterator<Date> expirationDates = balances.keySet().iterator();
    while (expirationDates.hasNext())
      {
        Date expirationDate = expirationDates.next();
        if (expirationDate.compareTo(evaluationDate) <= 0)
          {

            //
            //  update "expired" metric history
            //
            
            int expiredAmount = balances.get(expirationDate);
            if(expiredAmount > 0){

              //
              //  logs
              //
              
              if(log.isDebugEnabled()) log.debug(Thread.currentThread().getId()+" - PointBalance.update(...) : expired amount > 0 => NEED to update expired history and generate a BDR");

              //
              //  
              //
              
              expiredHistory.update(expirationDate, expiredAmount);
              
              //
              //  get related deliverable
              //
              
              Collection<Deliverable> deliverables = context.getDeliverableService().getActiveDeliverables(evaluationDate, tenantID);
              Deliverable searchedDeliverable = null;
              for(Deliverable deliverable : deliverables){
                if(deliverable.getFulfillmentProviderID().equals(providerID) && deliverable.getExternalAccountID().equals(point.getPointID())){
                  searchedDeliverable = deliverable;
                  break;
                }
              }
              
              if(searchedDeliverable != null)
                {

                  //
                  //  generate fake commodityDeliveryResponse (always generate BDR when bonuses expire, needed to get BDRs)
                  //
                  
                  generateCommodityDeliveryResponse(context, (eventID == null ? "bonusExpiration" : eventID), moduleID, (featureID == null ? "bonusExpiration" : featureID), subscriberID, CommodityDeliveryOperation.Expire, expiredAmount, point, searchedDeliverable, null, tier, tenantID);
                  
                }
              
            }
            
            //
            //  remove expired entry
            //
            
            expirationDates.remove();
            
          }
      }

    //
    //  operation
    //

    switch (operation)
      {
        case Credit:
          {
            //
            //  expiration date
            //

            Date expirationDate = EvolutionUtilities.addTime(evaluationDate, point.getValidity().getPeriodQuantity(), point.getValidity().getPeriodType(), Deployment.getDeployment(tenantID).getTimeZone(), point.getValidity().getRoundDown() ? RoundingSelection.RoundDown : RoundingSelection.NoRound);
            if(pointFulfillmentResponse != null){ pointFulfillmentResponse.setDeliverableExpirationDate(expirationDate); }

            //
            //  adjust (or create) the bucket
            //

            balances.put(expirationDate, (balances.get(expirationDate) != null) ? balances.get(expirationDate) + amount : amount);

            //
            //  merge (if necessary)
            //

            if (point.getValidity().getValidityExtension())
              {
                Date latestExpirationDate = evaluationDate;
                int balance = 0;
                for (Date bucketExpirationDate : balances.keySet())
                  {
                    latestExpirationDate = bucketExpirationDate.after(latestExpirationDate) ? bucketExpirationDate : latestExpirationDate;
                    balance += balances.get(bucketExpirationDate);
                  }
                balances.clear();
                if (balance > 0)
                  {
                    balances.put(latestExpirationDate, balance);
                  }
              }
            
            //
            //  update "earned" metric history
            //
            
            earnedHistory.update(evaluationDate, amount);
            
            //
            //  generate fake commodityDeliveryResponse (needed to get BDRs)
            //
            
            Collection<Deliverable> deliverables = context.getDeliverableService().getActiveDeliverables(evaluationDate, tenantID);
            Deliverable searchedDeliverable = null;
            for(Deliverable deliverable : deliverables){
              if(deliverable.getFulfillmentProviderID().equals(providerID) && deliverable.getExternalAccountID().equals(point.getPointID())){
                searchedDeliverable = deliverable;
                break;
              }
            }
            if(generateBDR){
              generateCommodityDeliveryResponse(context, eventID, moduleID, featureID, subscriberID, operation, amount, point, searchedDeliverable, expirationDate, tier, tenantID);
            }
           
          }
          break;

        case Debit:
          {
            int remainingAmount = amount;
            while (remainingAmount > 0)
              {
                //
                //  get earliest bucket
                //

                Date expirationDate = balances.firstKey();
                int bucket = balances.get(expirationDate);

                //
                //  adjust the bucket
                //

                if (bucket > remainingAmount)
                  {
                    bucket -= remainingAmount;
                    remainingAmount = 0;
                  }
                else
                  {
                    remainingAmount -= bucket;
                    bucket = 0;
                  }
                balances.put(expirationDate, bucket);

                //
                //  remove the bucket if empty
                //

                if (bucket == 0)
                  {
                    balances.remove(expirationDate);
                  }
              }
            
            //
            //  update "consumed" metric history
            //
            
            consumedHistory.update(evaluationDate, amount);
            redemptionHistory.update(evaluationDate, 1);
            
            //
            //  generate fake commodityDeliveryResponse (needed to get BDRs)
            //
            
            Collection<Deliverable> deliverables = context.getDeliverableService().getActiveDeliverables(evaluationDate, tenantID);
            Deliverable searchedDeliverable = null;
            for(Deliverable deliverable : deliverables){
              if(deliverable.getFulfillmentProviderID().equals(providerID) && deliverable.getExternalAccountID().equals(point.getPointID())){
                searchedDeliverable = deliverable;
                break;
              }
            }
            if(generateBDR){
              generateCommodityDeliveryResponse(context, eventID, moduleID, featureID, subscriberID, operation, amount, point, searchedDeliverable, null, tier, tenantID);
            }
            
          }
          break;
      }

    //
    //  return
    //

    return true;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    PointBalance pointBalance = (PointBalance) value;
    Struct struct = new Struct(schema);
    List<Date> expirationDates = new ArrayList<Date>();
    List<Integer> points = new ArrayList<Integer>();
    for (Date expirationDate : pointBalance.getBalances().keySet())
      {
        expirationDates.add(expirationDate);
        points.add(pointBalance.getBalances().get(expirationDate));
      }
    struct.put("expirationDates", expirationDates);
    struct.put("points", points);
    struct.put("earnedHistory", MetricHistory.pack(pointBalance.getEarnedHistory()));
    struct.put("consumedHistory", MetricHistory.pack(pointBalance.getConsumedHistory()));
    struct.put("expiredHistory", MetricHistory.pack(pointBalance.getExpiredHistory()));
    struct.put("tenantID", (short)pointBalance.getTenantID());
    struct.put("redemptionHistory", MetricHistory.pack(pointBalance.getRedemptionHistory()));
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static PointBalance unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    SortedMap<Date,Integer> balances = new TreeMap<Date,Integer>();
    List<Date> expirationDates = (List<Date>) valueStruct.get("expirationDates");
    List<Integer> points = (List<Integer>) valueStruct.get("points");
    for (int i=0; i<expirationDates.size(); i++)
      {
        balances.put(expirationDates.get(i), points.get(i));
      }
    MetricHistory earnedHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("earnedHistory").schema(), valueStruct.get("earnedHistory")));
    MetricHistory consumedHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("consumedHistory").schema(), valueStruct.get("consumedHistory")));
    MetricHistory expiredHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("expiredHistory").schema(), valueStruct.get("expiredHistory")));
    
    int tenantID = schema.field("tenantID") != null ? valueStruct.getInt16("tenantID") : 1;
    MetricHistory redemptionHistory = (schemaVersion >= 2)? MetricHistory.unpack(new SchemaAndValue(schema.field("redemptionHistory").schema(), valueStruct.get("redemptionHistory"))) : new MetricHistory(0, 0, tenantID);

    //  
    //  return
    //

    return new PointBalance(balances, earnedHistory, consumedHistory, expiredHistory, redemptionHistory, tenantID);
  }
  
  /*****************************************
  *
  *  generateCommodityDeliveryResponse
  *
  *****************************************/

  private void generateCommodityDeliveryResponse(EvolutionEventContext context, String eventID, String moduleID, String featureID, String subscriberID, CommodityDeliveryOperation operation, int amount, Point point, Deliverable deliverable, Date deliverableExpirationDate, String origin, int tenantID){
    
    //
    //  generate fake commodityDeliveryResponse (needed to get BDRs)
    //
    
    DeliveryManagerDeclaration commodityDeliveryManagerDeclaration = Deployment.getDeliveryManagers().get(CommodityDeliveryManager.COMMODITY_DELIVERY_TYPE);

    HashMap<String,Object> commodityDeliveryRequestData = new HashMap<String,Object>();
    
    commodityDeliveryRequestData.put("deliveryRequestID", context.getUniqueKey());
    commodityDeliveryRequestData.put("originatingRequest", true); 
    commodityDeliveryRequestData.put("deliveryType", CommodityDeliveryManager.COMMODITY_DELIVERY_TYPE);

    commodityDeliveryRequestData.put("eventID", (eventID == null ? "unknown" : eventID));
    commodityDeliveryRequestData.put("moduleID", moduleID);
    commodityDeliveryRequestData.put("featureID", featureID);

    commodityDeliveryRequestData.put("subscriberID", subscriberID);
    commodityDeliveryRequestData.put("pointID", point.getPointID());
    commodityDeliveryRequestData.put("providerID", deliverable.getFulfillmentProviderID());
    commodityDeliveryRequestData.put("commodityID", deliverable.getDeliverableID());
    commodityDeliveryRequestData.put("operation", operation.getExternalRepresentation());
    commodityDeliveryRequestData.put("amount", amount);
    commodityDeliveryRequestData.put("validityPeriodType", null);
    commodityDeliveryRequestData.put("validityPeriodQuantity", 0);

    commodityDeliveryRequestData.put("deliverableExpirationDate", deliverableExpirationDate);
    commodityDeliveryRequestData.put("origin", origin);

    commodityDeliveryRequestData.put("commodityDeliveryStatusCode", CommodityDeliveryStatus.SUCCESS.getReturnCode());

    if(log.isDebugEnabled()) log.debug(Thread.currentThread().getId()+" - PointBalance.update(...) : generating fake response DONE");
    if(log.isDebugEnabled()) log.debug(Thread.currentThread().getId()+" - PointBalance.update(...) : sending fake response ...");

    CommodityDeliveryRequest commodityDeliveryRequest = new CommodityDeliveryRequest(context.getSubscriberState().getSubscriberProfile(),context.getSubscriberGroupEpochReader(),JSONUtilities.encodeObject(commodityDeliveryRequestData), commodityDeliveryManagerDeclaration, tenantID);
    commodityDeliveryRequest.setCommodityDeliveryStatus(CommodityDeliveryStatus.SUCCESS);
    commodityDeliveryRequest.setDeliveryStatus(DeliveryStatus.Delivered);
    commodityDeliveryRequest.setStatusMessage("Success");
    commodityDeliveryRequest.setDeliveryDate(SystemTime.getCurrentTime());

    context.getSubscriberState().getDeliveryRequests().add(commodityDeliveryRequest);

  }
}
