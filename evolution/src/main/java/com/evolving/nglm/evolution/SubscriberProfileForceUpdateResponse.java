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
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;
import com.evolving.nglm.evolution.CustomerMetaData.MetaData;
import com.evolving.nglm.evolution.DeliveryManagerForNotifications.MessageStatus;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.RLMDateUtils;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class SubscriberProfileForceUpdateResponse extends SubscriberStreamOutput
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
    schemaBuilder.name("subscriber_profile_force_update_response");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(),1));
    for (Field field : subscriberStreamOutputSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("returnCode", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("subscriberProfileForceUpdateRequestID", Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<SubscriberProfileForceUpdateResponse> serde = new ConnectSerde<SubscriberProfileForceUpdateResponse>(schema, false, SubscriberProfileForceUpdateResponse.class, SubscriberProfileForceUpdateResponse::pack, SubscriberProfileForceUpdateResponse::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SubscriberProfileForceUpdateResponse> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  *  data
  *
  *****************************************/

  private String returnCode;
  private String subscriberProfileForceUpdateRequestID;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getReturnCode() { return returnCode; }
  public String getSubscriberProfileForceUpdateRequestID() { return subscriberProfileForceUpdateRequestID; }
  

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public SubscriberProfileForceUpdateResponse(JSONObject jsonRoot) throws GUIManagerException
  {
    /*****************************************
    *
    *  simple attributes
    *
    *****************************************/

    this.returnCode = JSONUtilities.decodeString(jsonRoot, "returnCode", false);
    this.subscriberProfileForceUpdateRequestID = JSONUtilities.decodeString(jsonRoot, "subscriberProfileForceUpdateRequestID", false);
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public SubscriberProfileForceUpdateResponse(SchemaAndValue schemaAndValue, String returnCode, String subscriberProfileForceUpdateRequestID)
  {
    super(schemaAndValue);
    this.returnCode = returnCode;
    this.subscriberProfileForceUpdateRequestID = subscriberProfileForceUpdateRequestID;
  }

  public SubscriberProfileForceUpdateResponse(String returnCode, String subscriberProfileForceUpdateRequestID)
  {
    this.returnCode = returnCode;
    this.subscriberProfileForceUpdateRequestID = subscriberProfileForceUpdateRequestID;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    SubscriberProfileForceUpdateResponse subscriberProfileForceUpdateResponse = (SubscriberProfileForceUpdateResponse) value;
    Struct struct = new Struct(schema);
    packSubscriberStreamOutput(struct,subscriberProfileForceUpdateResponse);
    struct.put("returnCode", subscriberProfileForceUpdateResponse.getReturnCode());
    struct.put("subscriberProfileForceUpdateRequestID", subscriberProfileForceUpdateResponse.getSubscriberProfileForceUpdateRequestID());
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

  public static SubscriberProfileForceUpdateResponse unpack(SchemaAndValue schemaAndValue)
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
    String returnCode = valueStruct.getString("returnCode");
    String subscriberProfileForceUpdateRequestID = valueStruct.getString("subscriberProfileForceUpdateRequestID");
        
    //
    //  return
    //

    return new SubscriberProfileForceUpdateResponse(schemaAndValue, returnCode, subscriberProfileForceUpdateRequestID);
  }
}
