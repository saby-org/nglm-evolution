/*****************************************
*
*  SubscriberProfileForceUpdate.java
*
*****************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;
import com.evolving.nglm.evolution.CustomerMetaData.MetaData;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import com.evolving.nglm.core.JSONUtilities;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class SubscriberProfileForceUpdate implements SubscriberStreamEvent, Action
{
  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema schema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("subscriber_profile_force_update");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("parameterMap", ParameterMap.schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<SubscriberProfileForceUpdate> serde = new ConnectSerde<SubscriberProfileForceUpdate>(schema, false, SubscriberProfileForceUpdate.class, SubscriberProfileForceUpdate::pack, SubscriberProfileForceUpdate::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SubscriberProfileForceUpdate> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  *  data
  *
  *****************************************/

  private String subscriberID;
  private Date eventDate;
  private ParameterMap parameterMap;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getSubscriberID() { return subscriberID; }
  public Date getEventDate() { return eventDate; }
  public ParameterMap getParameterMap() { return parameterMap; }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public SubscriberProfileForceUpdate(JSONObject jsonRoot) throws GUIManagerException
  {
    /*****************************************
    *
    *  simple attributes
    *
    *****************************************/

    this.subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
    String date = JSONUtilities.decodeString(jsonRoot, "eventDate", false);
    if(date != null)
      {
        this.eventDate = GUIManagedObject.parseDateField(date);
      }
    else
      {
        this.eventDate = SystemTime.getCurrentTime();
      }

    /*****************************************
    *
    *  parameterMap
    *
    *****************************************/

    this.parameterMap = new ParameterMap();
    for (MetaData metaData : Deployment.getCustomerMetaData().getGeneralDetailsMetaData())
      {
        if (metaData.getEditable() && jsonRoot.containsKey(metaData.getName()))
          {
            Object value = null;
            switch (metaData.getDataType())
              {
                case IntegerCriterion:
                  value = JSONUtilities.decodeInteger(jsonRoot, metaData.getName(), false);
                  break;
                  
                case DoubleCriterion:
                  value = JSONUtilities.decodeDouble(jsonRoot, metaData.getName(), false);
                  break;
                  
                case StringCriterion:
                  value = JSONUtilities.decodeString(jsonRoot, metaData.getName(), false);
                  break;
                  
                case BooleanCriterion:
                  value = JSONUtilities.decodeBoolean(jsonRoot, metaData.getName(), false);
                  break;
                  
                case DateCriterion:
                  value = GUIManagedObject.parseDateField(JSONUtilities.decodeString(jsonRoot, metaData.getName(), false));
                  break;
                  
                case StringSetCriterion:
                  Set<String> stringSetValue = new HashSet<String>();
                  JSONArray stringSetArray = JSONUtilities.decodeJSONArray(jsonRoot, metaData.getName(), new JSONArray());
                  for (int j=0; j<stringSetArray.size(); j++)
                    {
                      stringSetValue.add((String) stringSetArray.get(j));
                    }
                  value = stringSetValue;
                  break;

                default:
                  throw new GUIManagerException("unsupported data type", metaData.getDataType().getExternalRepresentation());
              }
            this.parameterMap.put(metaData.getName(), value);
          }
      }
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public SubscriberProfileForceUpdate(String subscriberID, Date eventDate, ParameterMap parameterMap)
  {
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.parameterMap = parameterMap;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    SubscriberProfileForceUpdate subscriberProfileForceUpdate = (SubscriberProfileForceUpdate) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", subscriberProfileForceUpdate.getSubscriberID());
    struct.put("eventDate", subscriberProfileForceUpdate.getEventDate());
    struct.put("parameterMap", ParameterMap.pack(subscriberProfileForceUpdate.getParameterMap()));
    return struct;
  }

  //
  //  subscriberStreamEventPack
  //

  public Object subscriberStreamEventPack(Object value) { return pack(value); }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static SubscriberProfileForceUpdate unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String subscriberID = valueStruct.getString("subscriberID");
    Date eventDate = (Date) valueStruct.get("eventDate");
    ParameterMap parameterMap = ParameterMap.unpack(new SchemaAndValue(schema.field("parameterMap").schema(), valueStruct.get("parameterMap")));
    
    //
    //  return
    //

    return new SubscriberProfileForceUpdate(subscriberID, eventDate, parameterMap);
  }
  
  @Override
  public ActionType getActionType()
  {
    return ActionType.UpdateProfile;
  }
}
