package com.evolving.nglm.evolution.offeroptimizer;

import java.util.HashMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.DNBOToken;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.Offer;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.SubscriberState;

/**
 * This class represents the result of an offer proposition from the algorithm
 * 
 * @author fduclos
 *
 */
public class ProposedOfferDetails implements Comparable<ProposedOfferDetails>
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
      schemaBuilder.name("proposed_offer_details");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("offerId", Schema.STRING_SCHEMA);
      schemaBuilder.field("offerScore", Schema.OPTIONAL_FLOAT32_SCHEMA);
      schema = schemaBuilder.build();
    };

  private String offerId; // evolution offerId not E4OOfferId
  private String salesChannelId;
  private double offerScore;
  private int offerRank;

  public ProposedOfferDetails(String offerId, String salesChannelId, double offerScore)
  {
    this.offerId = offerId;
    this.salesChannelId = salesChannelId;
    this.offerScore = offerScore;
  }
  
  //
  // serde
  //

  private static ConnectSerde<ProposedOfferDetails> serde = new ConnectSerde<ProposedOfferDetails>(schema, false, ProposedOfferDetails.class, ProposedOfferDetails::pack, ProposedOfferDetails::unpack);

  //
  // accessor
  //

  public static Schema schema()
  {
    return schema;
  }
  public static ConnectSerde<ProposedOfferDetails> serde() { return serde; }
  
  public String getOfferId()
  {
    return offerId;
  }

  @Override
  public String toString()
  {
    return "ProposedOfferDetails [" + (offerId != null ? "offerId=" + offerId + ", " : "") + (salesChannelId != null ? "salesChannelId=" + salesChannelId + ", " : "") + "offerScore=" + offerScore + ", offerRank=" + offerRank + "]";
  }

  public void setOfferId(String offerId)
  {
    this.offerId = offerId;
  }

  public String getSalesChannelId()
  {
    return salesChannelId;
  }

  public void setSalesChannelId(String salesChannelId)
  {
    this.salesChannelId = salesChannelId;
  }

  public double getOfferScore()
  {
    return offerScore;
  }

  public void setOfferScore(double offerScore)
  {
    this.offerScore = offerScore;
  }

  public int getOfferRank()
  {
    return offerRank;
  }

  public void setOfferRank(int offerRank)
  {
    this.offerRank = offerRank;
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

 public static Object pack(Object value)
 {
   ProposedOfferDetails proposedOfferDetails = (ProposedOfferDetails) value;
   Struct struct = new Struct(schema);
   struct.put("offerId", proposedOfferDetails.getOfferId());
   struct.put("offerScore", proposedOfferDetails.getOfferScore());
   return struct;
 }
 
 /*****************************************
  *
  * unpack
  *
  *****************************************/

 public static ProposedOfferDetails unpack(SchemaAndValue schemaAndValue)
 {
   //
   // data
   //

   Schema schema = schemaAndValue.schema();
   Object value = schemaAndValue.value();
   Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

   //
   // unpack
   //

   Struct valueStruct = (Struct) value;
   String offerId = valueStruct.getString("offerId");
   double offerScore = valueStruct.getFloat64("offerScore");

   //
   // return
   //

   return new ProposedOfferDetails(offerId, null, offerScore);
 }

  @Override public int compareTo(ProposedOfferDetails o)
  {
    // Make this method "consistent with equals", so that ordering works
    double res = (o.getOfferScore() - this.getOfferScore());
    if (res > 0.0)
      return 1;
    if (res < 0.0)
      return -1;
    int result = o.getOfferId().compareTo(this.getOfferId());
    if (result != 0)
      return result;
    result = o.getSalesChannelId().compareTo(this.getSalesChannelId());
    if (result != 0)
      return result;
    result = o.getOfferRank() - this.getOfferRank();
    if (result != 0)
      return result;
    if (o.equals(this))
      result = 0;
    else
      // Should not happen, by construction. Return anything
      result = 1;
    return result;
  }

  /*****************************************
   *
   * getJSONRepresentation
   *
   *****************************************/

  public JSONObject getJSONRepresentation()
  {
    HashMap<String, Object> jsonRepresentation = new HashMap<String, Object>();
    jsonRepresentation.put("offerID", offerId);
    jsonRepresentation.put("salesChannelId", salesChannelId);
    jsonRepresentation.put("offerScore", offerScore);
    jsonRepresentation.put("offerRank", offerRank);
    return JSONUtilities.encodeObject(jsonRepresentation);
  }

  // enriched
  public JSONObject getJSONRepresentation(OfferService offerService)
  {
    JSONObject jsonObject = getJSONRepresentation();
    GUIManagedObject offerObject = offerService.getStoredOffer(offerId);
    if (offerObject instanceof Offer)
      {
        Offer offer = (Offer) offerObject;
        jsonObject.put("remainingStock", offer.getApproximateRemainingStock());
      }
    return jsonObject;
  }

}
