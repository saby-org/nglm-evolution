/*****************************************************************************
*
*  INFulfillmentManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;

import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import com.rii.utilities.SystemTime;
import com.rii.utilities.JSONUtilities;

import org.json.simple.JSONArray;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
  
public class INFulfillmentManager extends DeliveryManager
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(INFulfillmentManager.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private Set<String> availableAccounts;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public Set<String> getAvailableAccounts() { return availableAccounts; }
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public INFulfillmentManager()
  {
    //
    //  superclass
    //
    
    super("fulfillmentmanager-infulfillment", Deployment.getBrokerServers(), INFulfillmentRequest.serde(), Deployment.getFulfillmentManagers().get("inFulfillment"));

    //
    //  manager
    //

    availableAccounts = new HashSet<String>();
    JSONArray availableAccountsArray = JSONUtilities.decodeJSONArray(Deployment.getFulfillmentManagers().get("inFulfillment").getJSONRepresentation(), "availableAccounts", true);
    for (int i=0; i<availableAccountsArray.size(); i++)
      {
        availableAccounts.add((String) availableAccountsArray.get(i));
      }
  }

  /*****************************************
  *
  *  class INFulfillmentRequest
  *
  *****************************************/

  public static class INFulfillmentRequest extends FulfillmentRequest
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
      schemaBuilder.name("service_infulfillment_request");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
      for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
      schemaBuilder.field("account", Schema.STRING_SCHEMA);
      schemaBuilder.field("amount", Schema.INT32_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //
        
    private static ConnectSerde<INFulfillmentRequest> serde = new ConnectSerde<INFulfillmentRequest>(schema, false, INFulfillmentRequest.class, INFulfillmentRequest::pack, INFulfillmentRequest::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<INFulfillmentRequest> serde() { return serde; }
    public Schema subscriberStreamEventSchema() { return schema(); }
        
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String account;
    private int amount;

    //
    //  accessors
    //

    public String getAccount() { return account; }
    public int getAmount() { return amount; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public INFulfillmentRequest(EvolutionEventContext context, JourneyState journeyState, String account, int amount)
    {
      super(context, "inFulfillment", journeyState);
      this.account = account;
      this.amount = amount;
    }

    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    private INFulfillmentRequest(SchemaAndValue schemaAndValue, String account, int amount)
    {
      super(schemaAndValue);
      this.account = account;
      this.amount = amount;
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      INFulfillmentRequest inFulfillmentRequest = (INFulfillmentRequest) value;
      Struct struct = new Struct(schema);
      packCommon(struct, inFulfillmentRequest);
      struct.put("account", inFulfillmentRequest.getAccount());
      struct.put("amount", inFulfillmentRequest.getAmount());
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

    public static INFulfillmentRequest unpack(SchemaAndValue schemaAndValue)
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
      String account = valueStruct.getString("account");
      int amount = valueStruct.getInt32("amount");

      //
      //  return
      //

      return new INFulfillmentRequest(schemaAndValue, account, amount);
    }
  }

  /*****************************************
  *
  *  run
  *
  *****************************************/

  public void run()
  {
    startDelivery();
    while (isProcessing())
      {
        DeliveryRequest deliveryRequest = nextRequest();
        deliveryRequest.setDeliveryStatus(DeliveryStatus.Delivered);
        deliveryRequest.setDeliveryDate(SystemTime.getCurrentTime());
        markProcessed(deliveryRequest);
      }
  }
  
  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  instance  
    //

    INFulfillmentManager manager = new INFulfillmentManager();

    //
    //  run
    //

    manager.run();
  }
}
