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
  /****************************************
  *
  *  data
  *
  ****************************************/

  private JSONObject jsonRepresentation;
  private String name;
  private String eventClassName;
  private String eventTopic;
  private Map<String,CriterionField> eventCriterionFields;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  protected JSONObject getJSONRepresentation() { return jsonRepresentation; }
  public String getName() { return name; }
  public String getEventClassName() { return eventClassName; }
  public String getEventTopic() { return eventTopic; }
  public Map<String,CriterionField> getEventCriterionFields() { return eventCriterionFields; }

  //
  //  getEventSerde
  //

  public ConnectSerde<? extends SubscriberStreamEvent> getEventSerde()
  {
    try
      {
        Class<? extends SubscriberStreamEvent> eventClass = (Class<? extends SubscriberStreamEvent>) Class.forName(eventClassName);
        Method serdeMethod = eventClass.getMethod("serde");
        ConnectSerde<? extends SubscriberStreamEvent> eventSerde = (ConnectSerde<? extends SubscriberStreamEvent>) serdeMethod.invoke(null);
        return eventSerde;
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

  public EvolutionEngineEventDeclaration(JSONObject jsonRoot) throws GUIManagerException
  {
    //
    //  data
    //

    this.jsonRepresentation = jsonRoot;
    this.name = JSONUtilities.decodeString(jsonRoot, "name", true);
    this.eventClassName = JSONUtilities.decodeString(jsonRoot, "eventClass", true);
    this.eventTopic = JSONUtilities.decodeString(jsonRoot, "eventTopic", true);
    this.eventCriterionFields = decodeEventCriterionFields(JSONUtilities.decodeJSONArray(jsonRoot, "eventCriterionFields", false));
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
