/*****************************************************************************
*
*  DeliveryManagerDeclaration.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityType;
import com.evolving.nglm.evolution.DeliveryRequest.DeliveryPriority;

import com.evolving.nglm.core.ConnectSerde;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;

import com.evolving.nglm.evolution.kafka.Topic;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

public class DeliveryManagerDeclaration
{

  private static final Logger log = LoggerFactory.getLogger(DeliveryManagerDeclaration.class);

  /****************************************
  *
  *  data
  *
  ****************************************/

  private JSONObject jsonRepresentation;
  private String deliveryType;
  private String requestClassName;
  private Topic[] requestTopics;
  private Topic[] responseTopics;
  private Topic routingTopic;
  private int deliveryRatePerMinute;
  private int retries;
  private int correlatorUpdateTimeoutSeconds;
  private int correlatorCleanerFrequencySeconds;
  private String providerID;
  private String providerName;
  private Map<String,CriterionField> subscriberProfileFields;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public JSONObject getJSONRepresentation() { return jsonRepresentation; }
  public String getDeliveryType() { return deliveryType; }
  public String getRequestClassName() { return requestClassName; }
  public int getDeliveryRatePerMinute() { return deliveryRatePerMinute; }
  public int getRetries() { return retries; }
  public int getCorrelatorUpdateTimeoutSeconds() { return correlatorUpdateTimeoutSeconds; }
  public int getCorrelatorCleanerFrequencyMilliSeconds() { return correlatorCleanerFrequencySeconds * 1000; }
  public String getProviderID() { return providerID; }
  public String getProviderName() { return providerName; }
  public Map<String,CriterionField> getSubscriberProfileFields() { return subscriberProfileFields; }

  public CommodityType getProviderType() { return CommodityType.fromExternalRepresentation(getRequestClassName()); }
  public String getRequestTopic(DeliveryPriority priority){return requestTopics[priority.getTopicIndex()].getName();}
  public String getResponseTopic(DeliveryPriority priority){return responseTopics[priority.getTopicIndex()].getName();}
  public List<Topic> getRequestTopics(){return Arrays.asList(requestTopics);}
  public List<Topic> getResponseTopics(){return Arrays.asList(responseTopics);}
  public Topic getRoutingTopic(){return routingTopic;}
  public List<String> getRequestTopicsList() {
    Set<String> topics = new HashSet<>();
    for(Topic topic:requestTopics) topics.add(topic.getName());
    return new ArrayList<>(topics);
  }
  public List<String> getResponseTopicsList() {
    Set<String> topics = new HashSet<>();
    for(Topic topic:responseTopics) topics.add(topic.getName());
    return new ArrayList<>(topics);
  }
  public boolean isProcessedByEvolutionEngine(){
    return deliveryType.equals("pointFulfillment") || deliveryType.equals("journeyFulfillment") || deliveryType.equals("loyaltyProgramFulfillment");
  }

  //
  //  getRequestSerde
  //

  public ConnectSerde<? extends DeliveryRequest> getRequestSerde()
  {
    try
      {
        Class<? extends DeliveryRequest> requestClass = (Class<? extends DeliveryRequest>) Class.forName(requestClassName);
        Method serdeMethod = requestClass.getMethod("serde");
        ConnectSerde<? extends DeliveryRequest> requestSerde = (ConnectSerde<? extends DeliveryRequest>) serdeMethod.invoke(null);
        return requestSerde;
      }
    catch (ClassNotFoundException|NoSuchMethodException|IllegalAccessException|InvocationTargetException e)
      {
        throw new RuntimeException(e);
      }
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public DeliveryManagerDeclaration(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    //
    //  data
    //

    this.jsonRepresentation = jsonRoot;
    this.deliveryType = JSONUtilities.decodeString(jsonRoot, "deliveryType", true);
    this.requestClassName = JSONUtilities.decodeString(jsonRoot, "requestClass", true);

    // old
    List<String> oldRequestTopics = decodeRequestTopics(jsonRoot);
    String oldResponseTopic = JSONUtilities.decodeString(jsonRoot, "responseTopic", false);
    String oldRoutingTopic = JSONUtilities.decodeString(jsonRoot, "routingTopic", false);
    // empty/null if not used anymore
    boolean isNewRequestTopic = oldRequestTopics.isEmpty();
    boolean isNewResponseTopic = oldResponseTopic==null;
    boolean isNewRoutingTopic =  oldRoutingTopic==null;
    // new init
    this.requestTopics = new Topic[DeliveryPriority.values().length];
    this.responseTopics = new Topic[DeliveryPriority.values().length];
    this.routingTopic = null;
    // init topics

    String warnLog = "topic configuration for {} is old way, please migrate, it will be not supported soon. {} messages will ends in {}";
    for(DeliveryPriority priority:DeliveryPriority.values()){
      // request
      if(isNewRequestTopic){
        // new
        this.requestTopics[priority.getTopicIndex()] = new Topic(deliveryType+"_request_"+priority.getExternalRepresentation(), Topic.TYPE.traffic, true);
      }else{
        // old
        String topic = oldRequestTopics.get(Math.min(oldRequestTopics.size()-1,priority.getTopicIndex()));
        log.warn(warnLog,deliveryType,priority.getExternalRepresentation(),topic);
        this.requestTopics[priority.getTopicIndex()] = new Topic(topic, Topic.TYPE.traffic/*type is irrelevant, won't create*/,false);
      }
      // response
      if(isNewResponseTopic){
        //new
        this.responseTopics[priority.getTopicIndex()] = new Topic(deliveryType+"_response_"+priority.getExternalRepresentation(), Topic.TYPE.traffic, true);
      }else{
        //old (no priority)
        log.warn(warnLog,deliveryType,priority.getExternalRepresentation(),oldResponseTopic);
        this.responseTopics[priority.getTopicIndex()] = new Topic(oldResponseTopic, Topic.TYPE.traffic/*type is irrelevant, won't create*/,false);
      }
    }
    // routing can not have priority topics by definition (we don't know when corelatorID ONLY come back, what was original request priority
    if(isNewRoutingTopic){
      //new
      // no routing for evolution engine
      if(!isProcessedByEvolutionEngine()) this.routingTopic = new Topic(deliveryType+"_routing", Topic.TYPE.traffic, true);
    }else{
      //old
      log.warn(warnLog,deliveryType,"(no priority for routing)",oldRoutingTopic);
      this.routingTopic = new Topic(oldRoutingTopic, Topic.TYPE.traffic/*type is irrelevant, won't create*/,false);
    }

    this.deliveryRatePerMinute = JSONUtilities.decodeInteger(jsonRoot, "deliveryRatePerMinute", Integer.MAX_VALUE);
    this.retries = JSONUtilities.decodeInteger(jsonRoot, "retries", 0);
    this.correlatorUpdateTimeoutSeconds = JSONUtilities.decodeInteger(jsonRoot, "correlatorUpdateTimeoutSeconds", 86400);
    this.correlatorCleanerFrequencySeconds = JSONUtilities.decodeInteger(jsonRoot, "correlatorCleanerFrequencySeconds", 3600);
    this.providerID = JSONUtilities.decodeString(jsonRoot, "providerID", false);
    this.providerName = JSONUtilities.decodeString(jsonRoot, "providerName", false);
    this.subscriberProfileFields = decodeSubscriberProfileFields(jsonRoot);
  }

  /*****************************************
  *
  *  decodeRequestTopics
  *
  *****************************************/

  private List<String> decodeRequestTopics(JSONObject jsonRoot) throws JSONUtilitiesException
  {
    List<String> requestTopics = new ArrayList<String>();
    if (JSONUtilities.decodeJSONArray(jsonRoot, "requestTopics", false) != null)
      {
        JSONArray jsonArray = JSONUtilities.decodeJSONArray(jsonRoot, "requestTopics", true);
        for (int i=0; i<jsonArray.size(); i++)
          {
            requestTopics.add((String) jsonArray.get(i));
          }
      }
    else
      {
        String requestTopic = JSONUtilities.decodeString(jsonRoot, "requestTopic", false);
        if(requestTopic!=null) requestTopics.add(requestTopic);
      }
    return requestTopics;
  }

  /*****************************************
  *
  *  decodeRequestTopics
  *
  *****************************************/

  private Map<String,CriterionField> decodeSubscriberProfileFields(JSONObject jsonRoot) throws JSONUtilitiesException
  {
    Map<String,CriterionField> subscriberProfileFields = new LinkedHashMap<>();
    JSONArray jsonArray = JSONUtilities.decodeJSONArray(jsonRoot, "subscriberProfileFields", false);
    if(jsonArray==null) return subscriberProfileFields;
    for (int i=0; i<jsonArray.size(); i++)
      {
        String fieldId = (String)jsonArray.get(i);
        CriterionField criterionField = Deployment.getProfileCriterionFields().get(fieldId);
        if(criterionField!=null){
          subscriberProfileFields.put(fieldId,criterionField);
        }else{
          log.error("subscriberProfileFields {} in deliveryManagerDeclaration is not a profileCriterionField",fieldId);
        }
    }
    return subscriberProfileFields;
  }
}
