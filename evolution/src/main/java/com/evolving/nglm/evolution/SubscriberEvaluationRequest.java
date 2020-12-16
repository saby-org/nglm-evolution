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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private ExtendedSubscriberProfile extendedSubscriberProfile;
  private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
  private JourneyState journeyState;
  private JourneyNode journeyNode;
  private JourneyLink journeyLink;
  private SubscriberStreamEvent subscriberStreamEvent;
  private Date evaluationDate;
  private SortedSet<Date> nextEvaluationDates;
  private ParameterMap evaluationVariables;
  private List<String> traceDetails;
  private HashMap<String, String> miscData = new HashMap(); // for data provided in special cases (like tags...)
  private int tenantID;

  /*****************************************
  *
  *  constructors
  *
  *****************************************/

  //
  //  constructor -- complete
  //  

  public SubscriberEvaluationRequest(SubscriberProfile subscriberProfile, ExtendedSubscriberProfile extendedSubscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, JourneyState journeyState, JourneyNode journeyNode, JourneyLink journeyLink, SubscriberStreamEvent subscriberStreamEvent, Date evaluationDate, int tenantID)
  {
    this.subscriberProfile = subscriberProfile;
    this.extendedSubscriberProfile = extendedSubscriberProfile;
    this.subscriberGroupEpochReader = subscriberGroupEpochReader;
    this.journeyState = journeyState;
    this.journeyNode = journeyNode;
    this.journeyLink = journeyLink;
    this.subscriberStreamEvent = subscriberStreamEvent;
    this.evaluationDate = evaluationDate;
    this.nextEvaluationDates = new TreeSet<Date>();
    this.evaluationVariables = new ParameterMap();
    this.traceDetails = new ArrayList<String>();
    this.tenantID = tenantID;
  }

  //
  //  constructor -- standard
  //

  public SubscriberEvaluationRequest(SubscriberProfile subscriberProfile, ExtendedSubscriberProfile extendedSubscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, Date evaluationDate, int tenantID)
  {
    this(subscriberProfile, extendedSubscriberProfile, subscriberGroupEpochReader, null, null, null, null, evaluationDate, tenantID);
  }

  //
  //  constructor -- standard
  //

  public SubscriberEvaluationRequest(SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, Date evaluationDate, int tenantID)
  {
    this(subscriberProfile, (ExtendedSubscriberProfile) null, subscriberGroupEpochReader, evaluationDate, tenantID);
  }
  
  //
  //  constructor -- for targetting based on Trigger event
  //

  public SubscriberEvaluationRequest(SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, SubscriberStreamEvent subscriberStreamEvent, Date evaluationDate, int tenantID)
  {
    this(subscriberProfile, null, subscriberGroupEpochReader, null, null, null, subscriberStreamEvent, evaluationDate, tenantID);
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
  public ParameterMap getEvaluationVariables() { return evaluationVariables; }
  public List<String> getTraceDetails() { return traceDetails; }
  public SortedSet<Date> getNextEvaluationDates() { return nextEvaluationDates; }
  public boolean getSubscriberTraceEnabled() { return subscriberProfile.getSubscriberTraceEnabled(); }
  public Map<String, String> getMiscData() { return miscData; }
  public int getTenantID() { return tenantID; }
  
  /*****************************************
  *
  *  getExtendedSubscriberProfile
  *
  *****************************************/

  public ExtendedSubscriberProfile getExtendedSubscriberProfile() { return extendedSubscriberProfile; }

  /*****************************************
  *
  *  getLanguage
  *
  *****************************************/

  public String getLanguage()
  {
    String languageID = (String) CriterionContext.Profile.getCriterionFields().get("subscriber.language").retrieve(this);
    String language = (languageID != null && Deployment.getSupportedLanguages().get(languageID) != null) ? Deployment.getSupportedLanguages().get(languageID).getName() : Deployment.getBaseLanguage();
    return language;
  }

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

  /*****************************************
  *
  *  clearTrace
  *
  *****************************************/

  public void clearSubscriberTrace()
  {
    if (getSubscriberTraceEnabled())
      {
        traceDetails.clear();
      }
  }

  @Override
  public String toString()
  {
    return "SubscriberEvaluationRequest [subscriberProfile=" + subscriberProfile + ", extendedSubscriberProfile=" + extendedSubscriberProfile + ", subscriberGroupEpochReader=" + subscriberGroupEpochReader + ", journeyState=" + journeyState + ", journeyNode=" + journeyNode + ", journeyLink=" + journeyLink + ", subscriberStreamEvent=" + subscriberStreamEvent + ", evaluationDate=" + evaluationDate + ", nextEvaluationDates=" + nextEvaluationDates + ", evaluationVariables=" + evaluationVariables + ", traceDetails=" + traceDetails + "]";
  }
  
  
}  
