package com.evolving.nglm.evolution;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.stream.Collectors;

public class ContactPolicyProcessor
{

  /*****************************************
   *
   *  services
   *
   *****************************************/
  private JourneyService journeyService;
  private JourneyObjectiveService journeyObjectiveService;
  private DynamicCriterionFieldService dynamicCriterionFieldsService;
  private ContactPolicyService contactPolicyService;

  /*****************************************
   *
   *  constructor
   *
   *****************************************/

  ContactPolicyProcessor(String groupIdBaseName,String key)
  {

     dynamicCriterionFieldsService = new DynamicCriterionFieldService(Deployment.getBrokerServers(), groupIdBaseName+"dynamiccriterionfieldservice-" + key, Deployment.getDynamicCriterionFieldTopic(), false);
     dynamicCriterionFieldsService.start();
     CriterionContext.initialize(dynamicCriterionFieldsService);
    //
    //  instantiate journey service
    //

    journeyService = new JourneyService(Deployment.getBrokerServers(), groupIdBaseName+"journeyservice-" + key, Deployment.getJourneyTopic(), false);
    journeyService.start();

    //
    //  instantiate journey sobjective service
    //

    journeyObjectiveService = new JourneyObjectiveService(Deployment.getBrokerServers(), groupIdBaseName+"journeyobjectiveservice-" + key, Deployment.getJourneyObjectiveTopic(), false);
    journeyObjectiveService.start();

    //
    //  instantiate journey sobjective service
    //

    contactPolicyService = new ContactPolicyService(Deployment.getBrokerServers(), groupIdBaseName+"contactpolicyservice-" + key, Deployment.getContactPolicyTopic(), false);
    contactPolicyService.start();

  }

  /*****************************************
   *
   *  ensureContactPolicy
   *  validate campaign contact policy based on current metric history
   *
   *****************************************/

  public boolean ensureContactPolicy(DeliveryRequest request, DeliveryManager deliveryManager, Logger log)
  {
    Date now = SystemTime.getCurrentTime();
    boolean returnValue = false;
    try
      {
        String channelId = Deployment.getDeliveryTypeCommunicationChannelIDMap().get(request.getDeliveryType());
        MetricHistory requestMetricHistory = request.getNotificationStatus();
        //validate segment contact policy
        if(request.getSegmentContactPolicyID() != null && isBlockedByContactPolicy(request.getSegmentContactPolicyID(),channelId,now,requestMetricHistory))
        {
          request.setDeliveryStatus(DeliveryManager.DeliveryStatus.Failed);
          return true;
        }

        Journey journey = journeyService.getActiveJourney(request.getFeatureID(), now);
        Set<JourneyObjectiveInstance> journeyObjectivesInstances = journey.getJourneyObjectiveInstances();
        for (JourneyObjectiveInstance journeyObjectiveInstance : journeyObjectivesInstances)
          {
            JourneyObjective journeyObjective = journeyObjectiveService.getActiveJourneyObjective(journeyObjectiveInstance.getJourneyObjectiveID(), now);
            if(this.evaluateChildParentsContactPolicies(channelId,journeyObjective,requestMetricHistory,now))
            {
              returnValue = true;
              request.setDeliveryStatus(DeliveryManager.DeliveryStatus.Failed);
              deliveryManager.completeRequest(request);
              break;
            }
          }
      }
    catch (Exception ex)
      {
        request.setDeliveryStatus(DeliveryManager.DeliveryStatus.Failed);
        deliveryManager.completeRequest(request);
        log.warn("Exception processing contact policy campaignID="+request.getFeatureID()+" subscriberID="+request.getSubscriberID(),ex);
      }
    return returnValue;
  }

  /*****************************************
   *
   *  evaluateChildParentsContactPolicies
   *  evaluate contact policies for JourneyObjective and it's parents
   *
   *****************************************/

  private boolean evaluateChildParentsContactPolicies(String channelID,JourneyObjective journeyObjective,MetricHistory requestMetricHistory,Date evaluationDate)
  {
    boolean returnValue = false;
    while(!returnValue && journeyObjective !=null)
    {
      String contactPolicyId = journeyObjective.getContactPolicyID();
      if(returnValue = isBlockedByContactPolicy(contactPolicyId,channelID,evaluationDate,requestMetricHistory)) break;
      journeyObjective = journeyObjective.getParentJourneyObjectiveID() != null ? journeyObjectiveService.getActiveJourneyObjective(journeyObjective.getParentJourneyObjectiveID(), evaluationDate) : null;
    }
    return returnValue;
  }

  /*****************************************
   *
   *  isBlockedByContactPolicy
   *  validate each ContactPolicy for desired channelID is blocking or not
   *
   *****************************************/

  private boolean isBlockedByContactPolicy(String contactPolicyId,String channelID,Date evaluationDate,MetricHistory requestMetricHistory)
  {
    boolean returnValue = false;
    ContactPolicy journeyContactPolicy = contactPolicyService.getActiveContactPolicy(contactPolicyId, evaluationDate);
    List<ContactPolicyCommunicationChannels> contactPolicyCommunicationChannels = journeyContactPolicy.getContactPolicyCommunicationChannels().stream().filter(p -> p.getCommunicationChannelID().equals(channelID)).collect(Collectors.toList());
    for (ContactPolicyCommunicationChannels channels : contactPolicyCommunicationChannels)
      {
        for (MessageLimits limit : channels.getMessageLimits())
          {
            Date startDate = null;
            switch (limit.getTimeUnit())
              {
                case "day":
                  startDate = RLMDateUtils.addDays(evaluationDate, -limit.getDuration(), Deployment.getBaseTimeZone());
                  break;
                case "week":
                  startDate = RLMDateUtils.addWeeks(evaluationDate, -limit.getDuration(), Deployment.getBaseTimeZone());
                  break;
                case "month":
                  startDate = RLMDateUtils.addMonths(evaluationDate, -limit.getDuration(), Deployment.getBaseTimeZone());
                  break;
              }
            //start date null means that no valid interval defined for processing list. An exception will be thrown that will be handled at upper level
            if (requestMetricHistory.getValue(RLMDateUtils.truncate(startDate, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone()), RLMDateUtils.truncate(evaluationDate, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone())) >= limit.getMaxMessages())
              {
                returnValue = true;
                break;
              }
          }
      }
    return returnValue;
  }

  /*****************************************
   *
   *  getParentChildObjectives
   *  get all parent objectives for an JourneyObjective
   *
   *****************************************/

  private List<JourneyObjective> getParentChildObjectives(JourneyObjective journeyObjective,Date date)
  {
    List<JourneyObjective> returnObjectives = new ArrayList<>();
    returnObjectives.add(journeyObjective);
    if(journeyObjective.getParentJourneyObjectiveID() != null)
    {
      returnObjectives.addAll(getParentChildObjectives(journeyObjectiveService.getActiveJourneyObjective(journeyObjective.getParentJourneyObjectiveID(),date),date));
    }
    return returnObjectives;
  }

  /*****************************************
   *
   *  class ContactPolityProcessorException
   *
   *****************************************/

  public class ContactPolityProcessorException extends Exception
  {
    public ContactPolityProcessorException(String message) { super(message); }
    public ContactPolityProcessorException(Throwable t) { super(t); }
  }
}
