/****************************************************************************
*
*  DNBOToken.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Token;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.Token.TokenStatus;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;


public class DNBOToken extends Token
{

  /*****************************************
  *
  * schema
  *
  *****************************************/

  //
  // schema
  //

  private static Schema schema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("dnbo_token");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),6));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("presentationStrategyID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("scoringStrategyIDs", SchemaBuilder.array(Schema.STRING_SCHEMA).defaultValue(new ArrayList<String>()).schema());
    schemaBuilder.field("isAutoBounded", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("isAutoRedeemed", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("presentedOfferIDs", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schemaBuilder.field("presentedOffersSalesChannel", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("acceptedOfferID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("presentationDates", SchemaBuilder.array(Timestamp.SCHEMA).defaultValue(new ArrayList<Date>()).schema());
    schemaBuilder.field("presentationHistory", SchemaBuilder.array(Presentation.schema()).defaultValue(new ArrayList<Presentation>()).schema());
    schemaBuilder.field("purchaseDeliveryRequestID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("purchaseStatus", Schema.OPTIONAL_INT32_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  // serde
  //

  private static ConnectSerde<DNBOToken> serde = new ConnectSerde<DNBOToken>(schema, false, DNBOToken.class, DNBOToken::pack, DNBOToken::unpack);

  //
  // accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<DNBOToken> serde() { return serde; }

  /****************************************
  *
  * data
  *
  ****************************************/

  private String presentationStrategyID;        // reference to the {@link PresentationStrategy} used to configure the Token.
  private List<String> scoringStrategyIDs;      // reference to the {@link ScoringStrategy}s used to configure the Token.
  private boolean isAutoBound;
  private boolean isAutoRedeemed;
  private List<String> presentedOfferIDs;       // list of offersIDs last presented to the subscriber, in the same order they were presented.
  private String presentedOffersSalesChannel;
  private String acceptedOfferID;               // offer that has been accepted  if any, null otherwise.
  private List<Date> presentationDates;         // dates when this token has been bound (to be compared to max pres of the pres strategy)
  
  private List<Presentation> presentationHistory; // history of pres of offers, with dates/offerIds

  private String purchaseDeliveryRequestID;  // the delivery request id of the corresponding purchase request triggered
  private PurchaseFulfillmentManager.PurchaseFulfillmentStatus purchaseStatus; // the last known status for the corresponding purchase

  /****************************************
  *
  * accessors - basic
  *
  ****************************************/

  //
  // accessor
  //

  public String getPresentationStrategyID() { return presentationStrategyID; }
  public List<String> getScoringStrategyIDs() { return scoringStrategyIDs; }
  public boolean isAutoBound() { return isAutoBound; }
  public boolean isAutoRedeemed() { return isAutoRedeemed; }
  public List<String> getPresentedOfferIDs() { return presentedOfferIDs; }
  public String getPresentedOffersSalesChannel() { return presentedOffersSalesChannel; }
  public String getAcceptedOfferID() { return acceptedOfferID; }
  public List<Date> getPresentationDates() { return presentationDates;}
  public List<Presentation> getPresentationHistory() {return presentationHistory;}
  public String getPurchaseDeliveryRequestID() {return purchaseDeliveryRequestID;}
  public PurchaseFulfillmentManager.PurchaseFulfillmentStatus getPurchaseStatus() {return purchaseStatus;}

  //
  //  setters
  //

  public void setPresentationStrategyID(String presentationStrategyID) { this.presentationStrategyID = presentationStrategyID; }
  public void setScoringStrategyIDs(List<String> scoringStrategyIDs) { this.scoringStrategyIDs = scoringStrategyIDs; }
  public void setAutoBound(boolean isAutoBound) { this.isAutoBound = isAutoBound; }
  public void setAutoRedeemed(boolean isAutoRedeemed) { this.isAutoRedeemed = isAutoRedeemed; }
  public void setPresentedOfferIDs(List<String> presentedOfferIDs) { this.presentedOfferIDs = presentedOfferIDs; }
  public void setPresentedOffersSalesChannel(String presentedOffersSalesChannel) { this.presentedOffersSalesChannel = presentedOffersSalesChannel; }
  public void setAcceptedOfferID(String acceptedOfferID) { this.acceptedOfferID = acceptedOfferID; }
  public void setPresentationDates(List<Date> presentationDates) { this.presentationDates = presentationDates; }
  public void setPresentationHistory(List<Presentation> presentationHistory) { this.presentationHistory = presentationHistory; }
  public void setPurchaseDeliveryRequestID(String purchaseDeliveryRequestID) { this.purchaseDeliveryRequestID = purchaseDeliveryRequestID; }
  public void setPurchaseStatus(PurchaseFulfillmentManager.PurchaseFulfillmentStatus purchaseStatus) { this.purchaseStatus = purchaseStatus; }

  /*****************************************
  *
  * constructor (simple)
  *
  *****************************************/

  public DNBOToken(String tokenCode, TokenStatus tokenStatus, Date creationDate, Date boundedDate,
                   Date redeemedDate, Date tokenExpirationDate, int boundedCount, String eventID,
                   String subscriberID, String tokenTypeID, String moduleID, String featureID,
                   String presentationStrategyID, List<String> scoringStrategyIDs, boolean isAutoBound, boolean isAutoRedeemed,
                   List<String> presentedOfferIDs, String presentedOffersSalesChannel, String acceptedOfferID, List<Date> presentationDates, List<Presentation> presentationHistory) {
    super(tokenCode, tokenStatus, creationDate, boundedDate, redeemedDate, tokenExpirationDate,
          boundedCount, eventID, subscriberID, tokenTypeID, moduleID, featureID);
    this.presentationStrategyID = presentationStrategyID;
    this.scoringStrategyIDs = scoringStrategyIDs;
    this.isAutoBound = isAutoBound;
    this.isAutoRedeemed = isAutoRedeemed;
    this.presentedOfferIDs = presentedOfferIDs;
    this.presentedOffersSalesChannel = presentedOffersSalesChannel;
    this.acceptedOfferID = acceptedOfferID;
    this.presentationDates = presentationDates;
    this.presentationHistory = presentationHistory;
  }

  /*****************************************
  *
  * constructor (empty token created from outside)
  *
  *****************************************/

  public DNBOToken(String tokenCode, String subscriberID, TokenType tokenType) {
    this(tokenCode,
         TokenStatus.New,                                                 // status
         null,                                                            // creationDate
         null,                                                            // boundedDate
         null,                                                            // redeemedDate
         tokenType.getExpirationDate(SystemTime.getCurrentTime(), tokenType.getTenantID()), // tokenExpirationDate
         0,                                                               // boundedCount
         "",                                                              // eventID
         subscriberID,                                                    // subscriberID
         tokenType.getTokenTypeID(),                                      // tokenTypeID
         DeliveryRequest.Module.REST_API.getExternalRepresentation(),     // moduleID
         null,                                                            // featureID
         null,                                                            // presentationStrategyID
         Collections.<String>emptyList(),                                 // scoringStrategyIDs
         false,                                                           // isAutoBound
         false,                                                           // isAutoRedeemed
         new ArrayList<String>(),                                         // presentedOfferIDs
         null,                                                            // presentedOffersSalesChannel
         null,                                                            // acceptedOfferID
         new ArrayList<Date>(),                                           // presentationDates
         new ArrayList<Presentation>()                                    // presentationHistory
       );
  }

  /*****************************************
  *
  * constructor (unpack)
  *
  *****************************************/

  protected DNBOToken(SchemaAndValue schemaAndValue, String presentationStrategyID, List<String> scoringStrategyIDs, boolean isAutoBound, boolean isAutoRedeemed, List<String> presentedOfferIDs, String presentedOffersSalesChannel, String acceptedOfferID, List<Date> presentationDates, List<Presentation> presentationHistory, String purchaseDeliveryRequestID, PurchaseFulfillmentManager.PurchaseFulfillmentStatus purchaseStatus)
  {
    super(schemaAndValue);
    this.presentationStrategyID = presentationStrategyID;
    this.scoringStrategyIDs = scoringStrategyIDs;
    this.isAutoBound = isAutoBound;
    this.isAutoRedeemed = isAutoRedeemed;
    this.presentedOfferIDs = presentedOfferIDs;
    this.presentedOffersSalesChannel = presentedOffersSalesChannel;
    this.acceptedOfferID = acceptedOfferID;
    this.presentationDates = presentationDates;
    this.presentationHistory = presentationHistory;
    this.purchaseDeliveryRequestID = purchaseDeliveryRequestID;
    this.purchaseStatus = purchaseStatus;
  }

  /*****************************************
  *
  * pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    DNBOToken dnboToken = (DNBOToken) value;
    Struct struct = new Struct(schema);
    packCommon(struct, dnboToken);
    struct.put("presentationStrategyID",dnboToken.getPresentationStrategyID());
    struct.put("scoringStrategyIDs",dnboToken.getScoringStrategyIDs());
    struct.put("isAutoBounded", dnboToken.isAutoBound());
    struct.put("isAutoRedeemed", dnboToken.isAutoRedeemed());
    struct.put("presentedOfferIDs", dnboToken.getPresentedOfferIDs());
    struct.put("presentedOffersSalesChannel", dnboToken.getPresentedOffersSalesChannel());
    struct.put("acceptedOfferID", dnboToken.getAcceptedOfferID());
    struct.put("presentationDates", dnboToken.getPresentationDates());
    struct.put("presentationHistory", packPresentationHistory(dnboToken.getPresentationHistory()));
    if(dnboToken.getPurchaseDeliveryRequestID()!=null) struct.put("purchaseDeliveryRequestID", dnboToken.getPurchaseDeliveryRequestID());
    if(dnboToken.getPurchaseStatus()!=null)  struct.put("purchaseStatus", dnboToken.getPurchaseStatus().getReturnCode());
    return struct;
  }

  private static List<Object> packPresentationHistory(List<Presentation> presentationHistory)
  {
    List<Object> result = new ArrayList<Object>();
    for (Presentation presentation : presentationHistory)
      {
        result.add(Presentation.pack(presentation));
      }
    return result;
  }
  
  /*****************************************
  *
  * unpack
  *
  *****************************************/

  public static DNBOToken unpack(SchemaAndValue schemaAndValue)
  {

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    Struct valueStruct = (Struct) value;
    String presentationStrategyID = valueStruct.getString("presentationStrategyID");
    List<String> scoringStrategyIDs = (schemaVersion >= 2) ? (List<String>) valueStruct.get("scoringStrategyIDs") : Collections.<String>emptyList();
    boolean isAutoBound = valueStruct.getBoolean("isAutoBounded");
    boolean isAutoRedeemed = valueStruct.getBoolean("isAutoRedeemed");
    List<String> presentedOfferIDs = (List<String>) valueStruct.get("presentedOfferIDs");
    String presentedOffersSalesChannel = (schemaVersion >= 3) ? (String) valueStruct.get("presentedOffersSalesChannel") : null;
    String acceptedOfferID = valueStruct.getString("acceptedOfferID");
    List<Date> presentationDates = (schemaVersion >= 4) ? (List<Date>) valueStruct.get("presentationDates") : new ArrayList<Date>();
    List<Presentation> presentationHistory = (schemaVersion >= 5) ? unpackPresentationHistory(schema.field("presentationHistory").schema(), valueStruct.get("presentationHistory")) : new ArrayList<Presentation>();
    String purchaseDeliveryRequestID = (schemaVersion >= 6) ? (String) valueStruct.get("purchaseDeliveryRequestID") : null;
    PurchaseFulfillmentManager.PurchaseFulfillmentStatus purchaseStatus = (schemaVersion >= 6) ? (valueStruct.getInt32("purchaseStatus")!=null ? PurchaseFulfillmentManager.PurchaseFulfillmentStatus.fromReturnCode(valueStruct.getInt32("purchaseStatus")) : null) : null;

    return new DNBOToken(schemaAndValue, presentationStrategyID, scoringStrategyIDs, isAutoBound, isAutoRedeemed, presentedOfferIDs, presentedOffersSalesChannel, acceptedOfferID, presentationDates, presentationHistory, purchaseDeliveryRequestID, purchaseStatus);
  }
  
  private static List<Presentation> unpackPresentationHistory(Schema schema, Object value)
  {

    Schema presentationHistorySchema = schema.valueSchema();
    List<Presentation> result = new ArrayList<>();
    List<Object> valueArray = (List<Object>) value;
    for (Object presentation : valueArray)
      {
        result.add(Presentation.unpack(new SchemaAndValue(presentationHistorySchema, presentation)));
      }
    return result;
  }

  @Override
  public String toString()
  {
    return "DNBOToken [" + (presentationStrategyID != null ? "presentationStrategyID=" + presentationStrategyID + ", " : "")
        + (scoringStrategyIDs != null ? "scoringStrategyIDs=" + scoringStrategyIDs + ", " : "")
        + "isAutoBound=" + isAutoBound + ", isAutoRedeemed=" + isAutoRedeemed + ", "
        + (presentedOfferIDs != null ? "presentedOfferIDs=" + presentedOfferIDs + ", " : "")
        + (presentedOffersSalesChannel != null ? "presentedOffersSalesChannel=" + presentedOffersSalesChannel + ", " : "")
        + (acceptedOfferID != null ? "acceptedOfferID=" + acceptedOfferID + ", " : "")
        + (presentationHistory != null ? "presentationHistory=" + presentationHistory + ", " : "")
        + (presentationDates != null ? "presentationDates=" + presentationDates + ", " : "")
        + (purchaseDeliveryRequestID != null ? "purchaseDeliveryRequestID=" + purchaseDeliveryRequestID + ", " : "")
        + (purchaseStatus != null ? "purchaseStatus=" + purchaseStatus.name() : "")
        + "]";
  }
  
  
  public JSONObject getJSON()
  {
    JSONObject result = new JSONObject();
    result.put("tokenCode", getTokenCode());
    result.put("tokenType", getTokenTypeID());
    result.put("creationDate", getTimeOrNull(getCreationDate()));
    result.put("expirationDate", getTimeOrNull(getTokenExpirationDate()));
    result.put("redeemedDate", getTimeOrNull(getRedeemedDate()));
    result.put("lastAllocationDate", getTimeOrNull(getBoundDate()));
    result.put("presentationStrategyID", Objects.toString(getPresentationStrategyID(), ""));
    if (getScoringStrategyIDs() != null)
      {
        final JSONArray scoringStrategies = new JSONArray();
        getScoringStrategyIDs().stream().forEach(ssID -> scoringStrategies.add(ssID));
        result.put("scoringStrategyIDs", scoringStrategies);
      }
    else
      {
        result.put("scoringStrategyIDs", "");
      }
    result.put("acceptedOfferID", getAcceptedOfferID());
    result.put("qtyAllocations", getBoundCount());
    if (getPresentedOfferIDs() != null)
      {
        result.put("qtyAllocatedOffers", getPresentedOfferIDs().size());
        final JSONArray presentedOfferIDs = new JSONArray();
        getPresentedOfferIDs().stream().forEach(offerID -> presentedOfferIDs.add(offerID));
        result.put("presentedOfferIDs", presentedOfferIDs);
      }
    else
      {
        result.put("qtyAllocatedOffers", 0);
        result.put("presentedOfferIDs", "");
      }
    result.put("tokenStatus", getTokenStatus() != null ? getTokenStatus().getExternalRepresentation() : "");
    result.put("moduleID", getModuleID());
    result.put("featureID", getFeatureID());
    result.put("presentedOffersSalesChannel", getPresentedOffersSalesChannel());
    return result;
  }
  
  private Object getTimeOrNull(Date date)
  {
    return (date == null) ? null : date.getTime();
  }

}
