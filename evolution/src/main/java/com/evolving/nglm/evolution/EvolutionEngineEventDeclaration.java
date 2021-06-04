/*****************************************************************************
*
*  EvolutionEngineEventDeclaration.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.evolution.kafka.Topic;
import com.evolving.nglm.evolution.preprocessor.PreprocessorEvent;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

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
  private Class<? extends EvolutionEngineEvent> eventClass;
  private String eventTopic;
  private Topic preprocessTopic;
  private EventRule eventRule;
  private Map<String, CriterionField> eventCriterionFields;
  private Map<String, CriterionField> edrCriterionFieldsMapping;

  //
  //  derived
  //

  private ConnectSerde<EvolutionEngineEvent> eventSerde;
  private ConnectSerde<PreprocessorEvent> preprocessorSerde;
  private boolean isTriggerEvent = false;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  protected JSONObject getJSONRepresentation() { return jsonRepresentation; }
  public String getName() { return name; }
  public String getEventClassName() { return eventClassName; }
  public Class<? extends EvolutionEngineEvent> getEventClass() { return eventClass; }//null if internal event
  public String getEventTopic() { return eventTopic; }
  public Topic getPreprocessTopic() { return preprocessTopic; }
  public EventRule getEventRule() { return eventRule; }
  public Map<String,CriterionField> getEventCriterionFields() { return eventCriterionFields; }
  public ConnectSerde<EvolutionEngineEvent> getEventSerde() { return eventSerde; }
  public ConnectSerde<PreprocessorEvent> getPreprocessorSerde() { return preprocessorSerde; }
  public boolean isTriggerEvent() { return isTriggerEvent; }
  public void markAsTriggerEvent(){ this.isTriggerEvent=true; }
  public Map<String, CriterionField> getEdrCriterionFieldsMapping() { return edrCriterionFieldsMapping; }

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
    this.eventTopic = JSONUtilities.decodeString(jsonRoot, "eventTopic", false); // topic can be null
    String preprocessTopicName = JSONUtilities.decodeString(jsonRoot, "preprocessTopic", false);
    if(preprocessTopicName!=null) this.preprocessTopic = new Topic(preprocessTopicName, Topic.TYPE.traffic, true);// preprocessorTopic can be null
    this.eventCriterionFields = decodeEventCriterionFields(JSONUtilities.decodeJSONArray(jsonRoot, "eventCriterionFields", false));
    this.edrCriterionFieldsMapping = decodeEdrCriterionFieldsMapping(eventCriterionFields, JSONUtilities.decodeJSONArray(jsonRoot, "edrCriterionFieldsMapping", false));

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
              this.eventClass = (Class<? extends EvolutionEngineEvent>) Class.forName(eventClassName);
              if (! EvolutionEngineEvent.class.isAssignableFrom(eventClass)) throw new GUIManagerException("invalid EvolutionEngineEventDeclaration, need to implement "+EvolutionEngineEvent.class.getCanonicalName(), eventClassName);
              Method serdeMethod = eventClass.getMethod("serde");
              this.eventSerde = (ConnectSerde<EvolutionEngineEvent>) serdeMethod.invoke(null);
              if(this.getPreprocessTopic()!=null)
			    {
			  	  String preprocessClassName = JSONUtilities.decodeString(jsonRoot, "preprocessClass", false);
			  	  if(preprocessClassName!=null)
				    {
					  Class<? extends PreprocessorEvent> preprocessClass = (Class<? extends PreprocessorEvent>) Class.forName(preprocessClassName);
					  if (! PreprocessorEvent.class.isAssignableFrom(preprocessClass)) throw new GUIManagerException("invalid EvolutionEngineEventDeclaration", preprocessClassName);
					  Method preprocessSerdeMethod = eventClass.getMethod("serde");
					  this.preprocessorSerde = (ConnectSerde<PreprocessorEvent>) preprocessSerdeMethod.invoke(null);
				    }
				  else
				    {
				      if (! PreprocessorEvent.class.isAssignableFrom(eventClass)) throw new GUIManagerException("invalid EvolutionEngineEventDeclaration", eventClassName);
				  	  this.preprocessorSerde = (ConnectSerde<PreprocessorEvent>) serdeMethod.invoke(null);
				    }
			    }
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
              this.eventClass = (Class<? extends EvolutionEngineEvent>) Class.forName(eventClassName);
              if (! EvolutionEngineEvent.class.isAssignableFrom(eventClass)) throw new GUIManagerException("invalid EvolutionEngineEventDeclaration, need to implement "+EvolutionEngineEvent.class.getCanonicalName(), eventClassName);
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
  
  /*****************************************
  *
  *  decodeEventCriterionFields
  *
  *****************************************/
  
  private Map<String, CriterionField> decodeEdrCriterionFieldsMapping(Map<String, CriterionField> eventCriterionFields, JSONArray edrArray)
  {
    Map<String, CriterionField> edrCriterionFields = new LinkedHashMap<String, CriterionField>();
    if (edrArray != null && eventCriterionFields != null)
      {
        for (int i=0; i<edrArray.size(); i++)
          {
            JSONObject edrCriterionFieldJSON = (JSONObject) edrArray.get(i);
            String criterionFieldID = JSONUtilities.decodeString(edrCriterionFieldJSON, "id", true);
            if (eventCriterionFields.get(criterionFieldID) != null)
              {
                String edrField = JSONUtilities.decodeString(edrCriterionFieldJSON, "edrField", true);
                edrCriterionFields.put(edrField, eventCriterionFields.get(criterionFieldID));
              }
          }
      }
    return edrCriterionFields;
  }
}
