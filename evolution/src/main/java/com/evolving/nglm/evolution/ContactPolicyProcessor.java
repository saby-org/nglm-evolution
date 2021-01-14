package com.evolving.nglm.evolution;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import org.slf4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

/**
 * ContactPolicyProcessor class
 * responsible for processing delivery request and validate if an request is blocked by any contact policy rule
 */
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

  ContactPolicyProcessor()
  {

    dynamicCriterionFieldsService = new DynamicCriterionFieldService(Deployment.getBrokerServers(), "NOT_USED", Deployment.getDynamicCriterionFieldTopic(), false);
    dynamicCriterionFieldsService.start();
    CriterionContext.initialize(dynamicCriterionFieldsService);
    //
    //  instantiate journey service
    //

    journeyService = new JourneyService(Deployment.getBrokerServers(), "NOT_USED", Deployment.getJourneyTopic(), false);
    journeyService.start();

    //
    //  instantiate journey sobjective service
    //

    journeyObjectiveService = new JourneyObjectiveService(Deployment.getBrokerServers(), "NOT_USED", Deployment.getJourneyObjectiveTopic(), false);
    journeyObjectiveService.start();

    //
    //  instantiate journey sobjective service
    //

    contactPolicyService = new ContactPolicyService(Deployment.getBrokerServers(), "NOT_USED", Deployment.getContactPolicyTopic(), false);
    contactPolicyService.start();

  }

  /*****************************************
   *
   *  ensureContactPolicy validate campaign contact policy based on current metric history
   * @param request DeliveryRequest that is evaluated
   * @return boolean true if any contact policy rule meet for input delivery request
   *
   *****************************************/

  public boolean ensureContactPolicy(INotificationRequest request, int tenantID) throws ContactPolityProcessorException
  {
    Date now = SystemTime.getCurrentTime();
    try
      {
        String channelId = Deployment.getDeployment(tenantID).getDeliveryTypeCommunicationChannelIDMap().get(request.getDeliveryType());
        MetricHistory requestMetricHistory = request.getNotificationHistory();
        //validate segment contact policy
        if (request.getSegmentContactPolicyID() != null && isBlockedByContactPolicy(request.getSegmentContactPolicyID(), channelId, now, requestMetricHistory))
          {
            request.setDeliveryStatus(DeliveryManager.DeliveryStatus.Failed);
            return true;
          }

        // if not a campaign, don't check for journey objectives
        if(!request.getModule().equals(DeliveryRequest.Module.Journey_Manager)) return false;

        Journey journey = journeyService.getActiveJourney(request.getFeatureID(), now);
        Set<JourneyObjectiveInstance> journeyObjectivesInstances = journey.getJourneyObjectiveInstances();
        for (JourneyObjectiveInstance journeyObjectiveInstance : journeyObjectivesInstances)
          {
            JourneyObjective journeyObjective = journeyObjectiveService.getActiveJourneyObjective(journeyObjectiveInstance.getJourneyObjectiveID(), now);
            if (this.evaluateChildParentsContactPolicies(channelId, journeyObjective, requestMetricHistory, now))
              {
                return true;
              }
          }
      }
    catch (Exception ex)
      {
        throw new ContactPolityProcessorException(ex);
      }
    return false;
  }

  /*****************************************
   *
   *  evaluateChildParentsContactPolicies evaluate contact policies for JourneyObjective and it's parents
   *  When first contact policy rule is meet the method return true
   * @param channelID specify the channelID
   * @param journeyObjective the journey objective to be evaluated
   * @param requestMetricHistory metric history that is evaluated
   * @param evaluationDate specify the date for evaluation
   * @return boolean. True if contact policy is meet
   *
   *****************************************/

  private boolean evaluateChildParentsContactPolicies(String channelID, JourneyObjective journeyObjective, MetricHistory requestMetricHistory, Date evaluationDate)
  {
    boolean returnValue = false;
    while (!returnValue && journeyObjective != null)
      {
        String contactPolicyId = journeyObjective.getContactPolicyID();
        if (returnValue = isBlockedByContactPolicy(contactPolicyId, channelID, evaluationDate, requestMetricHistory))
          break;
        journeyObjective = journeyObjective.getParentJourneyObjectiveID() != null ? journeyObjectiveService.getActiveJourneyObjective(journeyObjective.getParentJourneyObjectiveID(), evaluationDate) : null;
      }
    return returnValue;
  }

  /*****************************************
   *
   *  isBlockedByContactPolicy validate each ContactPolicy for desired channelID is blocking or not
   * @param contactPolicyId specify the contact policyId
   * @param channelID specify the channelId
   * @param evaluationDate
   * @param requestMetricHistory specify the MetricHistory to be evaluated
   *
   *****************************************/

  private boolean isBlockedByContactPolicy(String contactPolicyId, String channelID, Date evaluationDate, MetricHistory requestMetricHistory)
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

  private List<JourneyObjective> getParentChildObjectives(JourneyObjective journeyObjective, Date date)
  {
    List<JourneyObjective> returnObjectives = new ArrayList<>();
    returnObjectives.add(journeyObjective);
    if (journeyObjective.getParentJourneyObjectiveID() != null)
      {
        returnObjectives.addAll(getParentChildObjectives(journeyObjectiveService.getActiveJourneyObjective(journeyObjective.getParentJourneyObjectiveID(), date), date));
      }
    return returnObjectives;
  }

  /*****************************************
   *
   *  class ContactPolityProcessorException
   *  used to throw specific exception for contact policy
   *
   *****************************************/

  public class ContactPolityProcessorException extends Exception
  {
    public ContactPolityProcessorException(String message) { super(message); }
    public ContactPolityProcessorException(Throwable t) { super(t); }
  }
}