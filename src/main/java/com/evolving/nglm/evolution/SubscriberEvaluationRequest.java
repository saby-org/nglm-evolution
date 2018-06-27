/****************************************************************************
*
*  SubscriberEvaluationRequest.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ReferenceDataReader;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SubscriberEvaluationRequest
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private SubscriberProfile subscriberProfile;
  private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
  private Date evaluationDate;
  private List<String> traceDetails;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SubscriberEvaluationRequest(SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, Date evaluationDate)
  {
    this.subscriberProfile = subscriberProfile;
    this.subscriberGroupEpochReader = subscriberGroupEpochReader;
    this.evaluationDate = evaluationDate;
    this.traceDetails = new ArrayList<String>();
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public SubscriberProfile getSubscriberProfile() { return subscriberProfile; }
  public ReferenceDataReader<String,SubscriberGroupEpoch> getSubscriberGroupEpochReader() { return subscriberGroupEpochReader; }
  public Date getEvaluationDate() { return evaluationDate; }
  public List<String> getTraceDetails() { return traceDetails; }
  public boolean getSubscriberTraceEnabled() { return subscriberProfile.getSubscriberTraceEnabled(); }
  
  /*****************************************
  *
  *  subscriberTrace
  *
  *****************************************/

  public void subscriberTrace(String messageFormatString, Object... args)
  {
    if (getSubscriberTraceEnabled())
      {
        traceDetails.add(MessageFormat.format(messageFormatString, args));
      }
  }
}  
