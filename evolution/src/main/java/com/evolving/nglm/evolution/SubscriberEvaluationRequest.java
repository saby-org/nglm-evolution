/****************************************************************************
*
*  SubscriberEvaluationRequest.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SubscriberStreamEvent;

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
  private JourneyState journeyState;
  private JourneyNode journeyNode;
  private JourneyLink journeyLink;
  private SubscriberStreamEvent subscriberStreamEvent;
  private Date evaluationDate;
  private List<String> traceDetails;

  /*****************************************
  *
  *  constructors
  *
  *****************************************/

  //
  //  constructor -- complete
  //  

  public SubscriberEvaluationRequest(SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, JourneyState journeyState, JourneyNode journeyNode, JourneyLink journeyLink, SubscriberStreamEvent subscriberStreamEvent, Date evaluationDate)
  {
    this.subscriberProfile = subscriberProfile;
    this.subscriberGroupEpochReader = subscriberGroupEpochReader;
    this.journeyState = journeyState;
    this.journeyNode = journeyNode;
    this.journeyLink = journeyLink;
    this.subscriberStreamEvent = subscriberStreamEvent;
    this.evaluationDate = evaluationDate;
    this.traceDetails = new ArrayList<String>();
  }

  //
  //  constructor -- standard
  //

  public SubscriberEvaluationRequest(SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, Date evaluationDate)
  {
    this(subscriberProfile, subscriberGroupEpochReader, null, null, null, null, evaluationDate);
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public SubscriberProfile getSubscriberProfile() { return subscriberProfile; }
  public ReferenceDataReader<String,SubscriberGroupEpoch> getSubscriberGroupEpochReader() { return subscriberGroupEpochReader; }
  public JourneyState getJourneyState() { return journeyState; }
  public JourneyNode getJourneyNode() { return journeyNode; }
  public JourneyLink getJourneyLink() { return journeyLink; }
  public SubscriberStreamEvent getSubscriberStreamEvent() { return subscriberStreamEvent; }
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
