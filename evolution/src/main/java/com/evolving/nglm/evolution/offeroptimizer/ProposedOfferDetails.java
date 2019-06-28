package com.evolving.nglm.evolution.offeroptimizer;

import java.util.HashMap;

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
    return (int) (o.getOfferScore() - this.getOfferScore());
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
  
}
