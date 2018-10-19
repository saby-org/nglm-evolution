/*****************************************************************************
*
*  EvolutionEngineEventDeclaration.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.DeploymentManagedObject;
import com.evolving.nglm.core.SubscriberStreamEvent;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;

import org.json.simple.JSONObject;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  protected JSONObject getJSONRepresentation() { return jsonRepresentation; }
  public String getName() { return name; }
  public String getEventClassName() { return eventClassName; }
  public String getEventTopic() { return eventTopic; }

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

  public EvolutionEngineEventDeclaration(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    //
    //  data
    //

    this.jsonRepresentation = jsonRoot;
    this.name = JSONUtilities.decodeString(jsonRoot, "name", true);
    this.eventClassName = JSONUtilities.decodeString(jsonRoot, "eventClass", true);
    this.eventTopic = JSONUtilities.decodeString(jsonRoot, "eventTopic", true);
  }
}
