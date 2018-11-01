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
import java.util.SortedSet;
import java.util.TreeSet;

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
  private SubscriberStreamEvent subscriberStreamEvent;
  private Date evaluationDate;
  private SortedSet<Date> nextEvaluationDates;
  private List<String> traceDetails;

  /*****************************************
  *
  *  constructors
  *
  *****************************************/

  //
  //  constructor -- complete
  //  

  public SubscriberEvaluationRequest(SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, JourneyState journeyState, JourneyNode journeyNode, SubscriberStreamEvent subscriberStreamEvent, Date evaluationDate)
  {
    this.subscriberProfile = subscriberProfile;
    this.subscriberGroupEpochReader = subscriberGroupEpochReader;
    this.journeyState = journeyState;
    this.journeyNode = journeyNode;
    this.subscriberStreamEvent = subscriberStreamEvent;
    this.evaluationDate = evaluationDate;
    this.nextEvaluationDates = new TreeSet<Date>();
    this.traceDetails = new ArrayList<String>();
  }

  //
  //  constructor -- standard
  //

  public SubscriberEvaluationRequest(SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, Date evaluationDate)
  {
    this(subscriberProfile, subscriberGroupEpochReader, null, null, null, evaluationDate);
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
  public SubscriberStreamEvent getSubscriberStreamEvent() { return subscriberStreamEvent; }
  public Date getEvaluationDate() { return evaluationDate; }
  public List<String> getTraceDetails() { return traceDetails; }
  public SortedSet<Date> getNextEvaluationDates() { return nextEvaluationDates; }
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
