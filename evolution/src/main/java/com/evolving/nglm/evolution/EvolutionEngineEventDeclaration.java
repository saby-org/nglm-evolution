/*****************************************************************************
*
*  EvolutionEngineEventDeclaration.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.DeploymentManagedObject;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class EvolutionEngineEventDeclaration
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  public enum EventRule
  {
    Standard("standard"),
    Extended("extended"),
    All("all"),
    Internal("internal"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private EventRule(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static EventRule fromExternalRepresentation(String externalRepresentation) { for (EventRule enumeratedValue : EventRule.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private JSONObject jsonRepresentation;
  private String name;
  private String eventClassName;
  private String eventTopic;
  private EventRule eventRule;
  private Map<String,CriterionField> eventCriterionFields;

  //
  //  derived
  //

  private ConnectSerde<EvolutionEngineEvent> eventSerde;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  protected JSONObject getJSONRepresentation() { return jsonRepresentation; }
  public String getName() { return name; }
  public String getEventClassName() { return eventClassName; }
  public String getEventTopic() { return eventTopic; }
  public EventRule getEventRule() { return eventRule; }
  public Map<String,CriterionField> getEventCriterionFields() { return eventCriterionFields; }
  public ConnectSerde<EvolutionEngineEvent> getEventSerde() { return eventSerde; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public EvolutionEngineEventDeclaration(JSONObject jsonRoot) throws GUIManagerException
  {
    //
    //  data
    //

    this.jsonRepresentation = jsonRoot;
    this.name = JSONUtilities.decodeString(jsonRoot, "name", true);
    this.eventRule = EventRule.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "eventRule", "standard"));
    this.eventClassName = JSONUtilities.decodeString(jsonRoot, "eventClass", this.eventRule != EventRule.Internal);
    this.eventTopic = JSONUtilities.decodeString(jsonRoot, "eventTopic", this.eventRule != EventRule.Internal);
    this.eventCriterionFields = decodeEventCriterionFields(JSONUtilities.decodeJSONArray(jsonRoot, "eventCriterionFields", false));

    //
    //  validate
    //

    switch (this.eventRule)
      {
        case Standard:
        case Extended:
        case All:
          try
            {
              Class<? extends EvolutionEngineEvent> eventClass = (Class<? extends EvolutionEngineEvent>) Class.forName(eventClassName);
              if (! EvolutionEngineEvent.class.isAssignableFrom(eventClass)) throw new GUIManagerException("invalid EvolutionEngineEventDeclaration", eventClassName);
              Method serdeMethod = eventClass.getMethod("serde");
              this.eventSerde = (ConnectSerde<EvolutionEngineEvent>) serdeMethod.invoke(null);
            }
          catch (ClassNotFoundException|NoSuchMethodException|IllegalAccessException|InvocationTargetException e)
            {
              throw new GUIManagerException(e);
            }
          break;

        case Internal:
          if (this.eventClassName != null) throw new GUIManagerException("non-null eventClassName on internal event", this.eventClassName);
          if (this.eventTopic != null) throw new GUIManagerException("non-null eventTopic on internal event", this.eventTopic);
          break;
      }
    
  }
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public EvolutionEngineEventDeclaration(String name, String eventClassName, String eventTopic, EventRule eventRule, Map<String, CriterionField> eventCriterionFields) throws GUIManagerException
  {
    //
    // data
    //

    this.name = name;
    this.eventClassName = eventClassName;
    this.eventTopic = eventTopic;
    this.eventRule = eventRule;
    this.eventCriterionFields = eventCriterionFields;
    
    //
    //  validate
    //

    switch (this.eventRule)
      {
        case Standard:
        case Extended:
        case All:
          try
            {
              Class<? extends EvolutionEngineEvent> eventClass = (Class<? extends EvolutionEngineEvent>) Class.forName(eventClassName);
              if (! EvolutionEngineEvent.class.isAssignableFrom(eventClass)) throw new GUIManagerException("invalid EvolutionEngineEventDeclaration", eventClassName);
              Method serdeMethod = eventClass.getMethod("serde");
              this.eventSerde = (ConnectSerde<EvolutionEngineEvent>) serdeMethod.invoke(null);
            }
          catch (ClassNotFoundException|NoSuchMethodException|IllegalAccessException|InvocationTargetException e)
            {
              throw new GUIManagerException(e);
            }
          break;

        case Internal:
          if (this.eventClassName != null) throw new GUIManagerException("non-null eventClassName on internal event", this.eventClassName);
          if (this.eventTopic != null) throw new GUIManagerException("non-null eventTopic on internal event", this.eventTopic);
          break;
      }
  }

  /*****************************************
  *
  *  decodeEventCriterionFields
  *
  *****************************************/

  public static Map<String,CriterionField> decodeEventCriterionFields(JSONArray jsonArray) throws GUIManagerException
  {
    Map<String,CriterionField> eventCriterionFields = new LinkedHashMap<String,CriterionField>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            JSONObject eventCriterionFieldJSON = (JSONObject) jsonArray.get(i);
            CriterionField eventCriterionField = new CriterionField(eventCriterionFieldJSON);
            eventCriterionFields.put(eventCriterionField.getID(), eventCriterionField);
          }
      }
    return eventCriterionFields;
  }
}
