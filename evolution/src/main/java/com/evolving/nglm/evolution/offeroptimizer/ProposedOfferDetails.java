package com.evolving.nglm.evolution.offeroptimizer;

import java.util.HashMap;

import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.Offer;
import com.evolving.nglm.evolution.OfferService;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.JSONUtilities;

/**
 * This class represents the result of an offer proposition from the algorithm
 * @author fduclos
 *
 */
public class ProposedOfferDetails implements Comparable<ProposedOfferDetails>
{
  private String offerId; // evolution offerId not E4OOfferId
  private String salesChannelId;
  private double offerScore;
  private int offerRank;
  
  public ProposedOfferDetails(String offerId, String salesChannelId, double offerScore){
    this.offerId = offerId;
    this.salesChannelId = salesChannelId;
    this.offerScore = offerScore;
  }

  public String getOfferId()
  {
    return offerId;
  }

  @Override
  public String toString()
  {
    return "ProposedOfferDetails ["
        + (offerId != null ? "offerId=" + offerId + ", " : "") 
        + (salesChannelId != null ? "salesChannelId=" + salesChannelId + ", " : "") 
        + "offerScore=" + offerScore + ", offerRank=" + offerRank + "]";
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

  @Override
  public int compareTo(ProposedOfferDetails o)
  {
    // Make this method "consistent with equals", so that ordering works
    double res = (o.getOfferScore() - this.getOfferScore());
    if (res > 0.0)
      return 1;
    if (res < 0.0)
      return -1;
    int result = o.getOfferId().compareTo(this.getOfferId());
    if (result != 0) return result;
    result = o.getSalesChannelId().compareTo(this.getSalesChannelId());
    if (result != 0) return result;
    result = o.getOfferRank() - this.getOfferRank();
    if (result != 0) return result;
    if (o.equals(this))
      result = 0;
    else
      // Should not happen, by construction. Return anything
      result = 1;
    return result;
  }  
  
  /*****************************************
  *
  *  getJSONRepresentation
  *
  *****************************************/

  public JSONObject getJSONRepresentation()
  {
    HashMap<String,Object> jsonRepresentation = new HashMap<String,Object>();
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
    if(offerObject instanceof Offer){
      Offer offer = (Offer) offerObject;
      jsonObject.put("remainingStock", offer.getApproximateRemainingStock());
    }
    return jsonObject;
  }
  
}
