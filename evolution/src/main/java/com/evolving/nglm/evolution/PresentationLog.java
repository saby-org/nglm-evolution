/*****************************************************************************
*
*  PresentationLog.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class PresentationLog implements SubscriberStreamEvent
{
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
    schemaBuilder.name("presentation_log");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("msisdn", Schema.STRING_SCHEMA);
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDate", Schema.INT64_SCHEMA);
    schemaBuilder.field("callUniqueIdentifier", Schema.STRING_SCHEMA);
    schemaBuilder.field("channelID", Schema.STRING_SCHEMA);
    schemaBuilder.field("salesChannelID", Schema.STRING_SCHEMA);
    schemaBuilder.field("userID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("presentationToken", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("presentationStrategyID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("transactionDurationMs", Schema.INT32_SCHEMA);
    schemaBuilder.field("offerIDs", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schemaBuilder.field("offerScores", SchemaBuilder.array(Schema.FLOAT64_SCHEMA));
    schemaBuilder.field("positions", SchemaBuilder.array(Schema.INT32_SCHEMA));
    schemaBuilder.field("controlGroupState", Schema.STRING_SCHEMA);
    schemaBuilder.field("scoringStrategyIDs", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schemaBuilder.field("retailerMsisdn", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("rechargeAmount", Schema.OPTIONAL_FLOAT64_SCHEMA);
    schemaBuilder.field("balance", Schema.OPTIONAL_FLOAT64_SCHEMA);
    schemaBuilder.field("moduleID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("featureID", Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<PresentationLog> serde = new ConnectSerde<PresentationLog>(schema, false, PresentationLog.class, PresentationLog::pack, PresentationLog::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<PresentationLog> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String msisdn;
  private String subscriberID;
  private Date eventDate;
  private String callUniqueIdentifier;
  private String channelID;
  private String salesChannelID;
  private String userID;
  private String presentationToken;
  private String presentationStrategyID;
  private Integer transactionDurationMs;
  private List<String> offerIDs;
  private List<Double> offerScores;
  private List<Integer> positions;  
  private String controlGroupState;
  private List<String> scoringStrategyIDs;
  private String retailerMsisdn;
  private Double rechargeAmount;
  private Double balance;
  private String moduleID;
  private String featureID;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getMsisdn() { return msisdn; }
  public String getSubscriberID() { return subscriberID; }
  public Date getEventDate() { return eventDate; }
  public String getCallUniqueIdentifier() { return callUniqueIdentifier; }
  public String getChannelID() { return channelID; }
  public String getSalesChannelID() { return salesChannelID; }
  public String getUserID() { return userID; }
  public String getPresentationToken() { return presentationToken; }
  public String getPresentationStrategyID() { return presentationStrategyID; }
  public Integer getTransactionDurationMs() { return transactionDurationMs; }
  public List<String> getOfferIDs() { return offerIDs; }
  public List<Double> getOfferScores() { return offerScores; }
  public List<Integer> getPositions() { return positions;   }
  public String getControlGroupState() { return controlGroupState; }
  public List<String> getScoringStrategyIDs() { return scoringStrategyIDs; }
  public String getRetailerMsisdn() { return retailerMsisdn; }
  public Double getRechargeAmount() { return rechargeAmount; }
  public Double getBalance() { return balance; }
  public String getModuleID() { return moduleID; }
  public String getFeatureID() { return featureID; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public PresentationLog(String msisdn, String subscriberID, Date eventDate, String callUniqueIdentifier, String channelID, String salesChannelID, String userID, String presentationToken, String presentationStrategyID, Integer transactionDurationMs, List<String> offerIDs, List<Double> offerScores, List<Integer> positions,   String controlGroupState, List<String> scoringStrategyIDs, String retailerMsisdn, Double rechargeAmount, Double balance, String moduleID, String featureID)
  {
    this.msisdn = msisdn;
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.callUniqueIdentifier = callUniqueIdentifier;
    this.channelID = channelID;
    this.salesChannelID = salesChannelID;
    this.userID = userID;
    this.presentationToken = presentationToken;
    this.presentationStrategyID = presentationStrategyID;
    this.transactionDurationMs = transactionDurationMs;
    this.offerIDs = offerIDs;
    this.offerScores = offerScores;
    this.positions = positions;  
    this.controlGroupState = controlGroupState;
    this.scoringStrategyIDs = scoringStrategyIDs;
    this.retailerMsisdn = retailerMsisdn;
    this.rechargeAmount = rechargeAmount;
    this.balance = balance;
    this.moduleID = moduleID;
    this.featureID = featureID;
}

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public PresentationLog(JSONObject jsonRoot)
  {
    //
    //  attributes
    //
    
    this.msisdn = JSONUtilities.decodeString(jsonRoot, "msisdn", true);
    this.subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
    this.eventDate = GUIManagedObject.parseDateField(JSONUtilities.decodeString(jsonRoot, "eventDate", true));
    this.callUniqueIdentifier = JSONUtilities.decodeString(jsonRoot, "callUniqueIdentifier", true);
    this.channelID = JSONUtilities.decodeString(jsonRoot, "channelID", true);
    this.salesChannelID = JSONUtilities.decodeString(jsonRoot, "salesChannelID", true);
    this.userID = JSONUtilities.decodeString(jsonRoot, "userID", false);
    this.presentationToken = JSONUtilities.decodeString(jsonRoot, "presentationToken", false);
    this.presentationStrategyID = JSONUtilities.decodeString(jsonRoot, "presentationStrategyID", false);
    this.transactionDurationMs = JSONUtilities.decodeInteger(jsonRoot, "transactionDurationMs", true);
    this.offerIDs = decodeOfferIDs(JSONUtilities.decodeJSONArray(jsonRoot, "offerIDs", true));
    this.offerScores = decodeOfferScores(JSONUtilities.decodeJSONArray(jsonRoot, "offerScores", true));
    this.positions = decodePositions(JSONUtilities.decodeJSONArray(jsonRoot, "positions", true));
    this.controlGroupState = JSONUtilities.decodeString(jsonRoot, "controlGroupState", true);
    this.scoringStrategyIDs = decodeScoringStrategyIDs(JSONUtilities.decodeJSONArray(jsonRoot, "scoringStrategyIDs", true));
    this.retailerMsisdn = JSONUtilities.decodeString(jsonRoot, "retailerMsisdn", false);
    this.rechargeAmount = JSONUtilities.decodeDouble(jsonRoot, "rechargeAmount", false);
    this.balance = JSONUtilities.decodeDouble(jsonRoot, "balance", false);
    this.moduleID = JSONUtilities.decodeString(jsonRoot, "moduleID", false);
    this.featureID = JSONUtilities.decodeString(jsonRoot, "featureID", false);
  }

  /*****************************************
  *
  *  decodeOfferIDs
  *
  *****************************************/

  private List<String> decodeOfferIDs(JSONArray jsonArray)
  {
    List<String> offerIDs = new ArrayList<String>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            String offerID = (String) jsonArray.get(i);
            offerIDs.add(offerID);
          }
      }
    return offerIDs;
  }

  /*****************************************
  *
  *  decodeOfferScores
  *
  *****************************************/

  private List<Double> decodeOfferScores(JSONArray jsonArray)
  {
    List<Double> offerScores = new ArrayList<Double>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            Double score = ((Number) jsonArray.get(i)).doubleValue();
            offerScores.add(score);
          }
      }
    return offerScores;
  }

  /*****************************************
  *
  *  decodePositions
  *
  *****************************************/

  private List<Integer> decodePositions(JSONArray jsonArray)
  {
    List<Integer> positions = null;
    if (jsonArray != null)
      {
        positions = new ArrayList<Integer>();
        for (int i=0; i<jsonArray.size(); i++)
          {
            Long positionLong = (Long) jsonArray.get(i);
            Integer position = (positionLong != null) ? new Integer(positionLong.intValue()) : null;
            positions.add(position);
          }
      }
    return positions;
  }

  /*****************************************
  *
  *  decodeScoringStrategyIDs
  *
  *****************************************/

  private List<String> decodeScoringStrategyIDs(JSONArray jsonArray)
  {
    List<String> scoringStrategyIDs = new ArrayList<String>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            String scoringStrategyID = (String) jsonArray.get(i);
            scoringStrategyIDs.add(scoringStrategyID);
          }
      }
    return scoringStrategyIDs;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    PresentationLog presentationLog = (PresentationLog) value;
    Struct struct = new Struct(schema);
    struct.put("msisdn", presentationLog.getMsisdn());
    struct.put("subscriberID", presentationLog.getSubscriberID());
    struct.put("eventDate", presentationLog.getEventDate().getTime());
    struct.put("callUniqueIdentifier", presentationLog.getCallUniqueIdentifier());
    struct.put("channelID", presentationLog.getChannelID());
    struct.put("salesChannelID", presentationLog.getSalesChannelID());
    struct.put("userID", presentationLog.getUserID());
    struct.put("presentationToken", presentationLog.getPresentationToken());
    struct.put("presentationStrategyID", presentationLog.getPresentationStrategyID());    
    struct.put("transactionDurationMs", presentationLog.getTransactionDurationMs());
    struct.put("offerIDs", presentationLog.getOfferIDs());
    struct.put("offerScores", presentationLog.getOfferScores());
    struct.put("positions", presentationLog.getPositions());
    struct.put("controlGroupState", presentationLog.getControlGroupState());
    struct.put("scoringStrategyIDs", presentationLog.getScoringStrategyIDs());
    struct.put("retailerMsisdn", presentationLog.getRetailerMsisdn());
    struct.put("rechargeAmount", presentationLog.getRechargeAmount());
    struct.put("balance", presentationLog.getBalance());
    struct.put("moduleID", presentationLog.getModuleID());
    struct.put("featureID", presentationLog.getFeatureID());
   return struct;
  }

  //
  //  subscriberStreamEventPack
  //

  public Object subscriberStreamEventPack(Object value) { return pack(value); }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static PresentationLog unpack(SchemaAndValue schemaAndValue)
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
    String msisdn = valueStruct.getString("msisdn");
    String subscriberID = valueStruct.getString("subscriberID");
    Date eventDate = new Date(valueStruct.getInt64("eventDate"));
    String callUniqueIdentifier = valueStruct.getString("callUniqueIdentifier");
    String channelID = valueStruct.getString("channelID");
    String salesChannelID = valueStruct.getString("salesChannelID");
    String userID = valueStruct.getString("userID");
    String presentationToken = valueStruct.getString("presentationToken");
    String presentationStrategyID = valueStruct.getString("presentationStrategyID");
    Integer transactionDurationMs = valueStruct.getInt32("transactionDurationMs");
    List<String> offerIDs = (List<String>) valueStruct.get("offerIDs");
    List<Double> offerScores = (List<Double>) valueStruct.get("offerScores");
    List<Integer> positions = (List<Integer>) valueStruct.get("positions");
    String controlGroupState = valueStruct.getString("controlGroupState");
    List<String> scoringStrategyIDs = (List<String>) valueStruct.get("scoringStrategyIDs");
    String retailerMsisdn = valueStruct.getString("retailerMsisdn");
    Double rechargeAmount = valueStruct.getFloat64("rechargeAmount");
    Double balance = valueStruct.getFloat64("balance");    
    String moduleID = valueStruct.getString("moduleID");
    String featureID = valueStruct.getString("featureID");

    //
    //  return
    //

    return new PresentationLog(msisdn, subscriberID, eventDate, callUniqueIdentifier, channelID, salesChannelID, userID, presentationToken, presentationStrategyID, transactionDurationMs, offerIDs, offerScores, positions, controlGroupState, scoringStrategyIDs, retailerMsisdn, rechargeAmount, balance, moduleID, featureID);
  }
}
