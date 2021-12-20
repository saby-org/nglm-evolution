/*****************************************
*
*  SubscriberProfileForceUpdate.java
*
*****************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;
import com.evolving.nglm.evolution.CustomerMetaData.MetaData;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.evolving.nglm.core.JSONUtilities;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.HashSet;
import java.util.Set;

public class SubscriberProfileForceUpdate extends SubscriberStreamOutput implements SubscriberStreamEvent, Action
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(),3));
    for (Field field : subscriberStreamOutputSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("parameterMap", ParameterMap.schema());
    schemaBuilder.field("subscriberProfileForceUpdateRequestID", Schema.OPTIONAL_STRING_SCHEMA);
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
  private ParameterMap parameterMap;
  private String subscriberProfileForceUpdateRequestID;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getSubscriberID() { return subscriberID; }
  public ParameterMap getParameterMap() { return parameterMap; }
  public String getSubscriberProfileForceUpdateRequestID() { return subscriberProfileForceUpdateRequestID; }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public SubscriberProfileForceUpdate(JSONObject jsonRoot) throws GUIManagerException
  {
    this.subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
    this.parameterMap = buildParameterMap(jsonRoot);
    this.subscriberProfileForceUpdateRequestID = JSONUtilities.decodeString(jsonRoot, "subscriberProfileForceUpdateRequestID", false);
  }

  public static ParameterMap buildParameterMap(JSONObject jsonRoot) throws GUIManagerException{
    ParameterMap toRet = new ParameterMap();
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
            toRet.put(metaData.getName(), value);
          }
      }

    //
    // score can be updated // hack
    //

    Integer score = JSONUtilities.decodeInteger(jsonRoot, "score", false);
    String challengeID = JSONUtilities.decodeString(jsonRoot, "challengeID", false);
    if (challengeID != null && !challengeID.isEmpty() && score != null)
      {
        toRet.put("score", score);
        toRet.put("challengeID", challengeID);
      }

    return toRet;

  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public SubscriberProfileForceUpdate(SchemaAndValue schemaAndValue, String subscriberID, ParameterMap parameterMap, String subscriberProfileForceUpdateRequestID)
  {
    super(schemaAndValue);
    this.subscriberID = subscriberID;
    this.parameterMap = parameterMap;
    this.subscriberProfileForceUpdateRequestID = subscriberProfileForceUpdateRequestID;
  }

  public SubscriberProfileForceUpdate(String subscriberID, ParameterMap parameterMap, String subscriberProfileForceUpdateRequestID)
  {
    this.subscriberID = subscriberID;
    this.parameterMap = parameterMap;
    this.subscriberProfileForceUpdateRequestID = subscriberProfileForceUpdateRequestID;
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
    packSubscriberStreamOutput(struct,subscriberProfileForceUpdate);
    struct.put("subscriberID", subscriberProfileForceUpdate.getSubscriberID());
    struct.put("parameterMap", ParameterMap.pack(subscriberProfileForceUpdate.getParameterMap()));
    struct.put("subscriberProfileForceUpdateRequestID", subscriberProfileForceUpdate.getSubscriberProfileForceUpdateRequestID());
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
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String subscriberID = valueStruct.getString("subscriberID");
    ParameterMap parameterMap = ParameterMap.unpack(new SchemaAndValue(schema.field("parameterMap").schema(), valueStruct.get("parameterMap")));
    String subscriberProfileForceUpdateRequestID = (schema.field("subscriberProfileForceUpdateRequestID") != null) ? valueStruct.getString("subscriberProfileForceUpdateRequestID") : null;
    
    //
    //  return
    //

    return new SubscriberProfileForceUpdate(schemaAndValue, subscriberID, parameterMap, subscriberProfileForceUpdateRequestID);
  }
  
  @Override
  public ActionType getActionType()
  {
    return ActionType.UpdateProfile;
  }
}
