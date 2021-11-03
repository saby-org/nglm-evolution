/*****************************************************************************
*
*  EDRDetails.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;

public class EDRDetails extends SubscriberStreamOutput implements SubscriberStreamEvent
{
  //
  // logger
  //

  private static final Logger log = LoggerFactory.getLogger(EDRDetails.class);

  /*****************************************
   *
   * schema
   *
   *****************************************/

  //
  // schema
  //

  private static Schema schema = null;
  private static int currentSchemaVersion = 2;
  static
    {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      schemaBuilder.name("edr_details");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(), currentSchemaVersion));
      for (Field field : subscriberStreamOutputSchema().fields()) schemaBuilder.field(field.name(), field.schema());
      schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
      schemaBuilder.field("eventName", Schema.STRING_SCHEMA);
      schemaBuilder.field("tenantID", Schema.INT16_SCHEMA);
      schemaBuilder.field("parameterMap", ParameterMap.schema());
      schema = schemaBuilder.build();
    };
    
    //
    //  serde
    //

    private static ConnectSerde<EDRDetails> serde = new ConnectSerde<EDRDetails>(schema, false, EDRDetails.class, EDRDetails::pack, EDRDetails::unpack);
    
    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<EDRDetails> serde() { return serde; }
    public Schema subscriberStreamEventSchema() { return schema(); }
    
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String subscriberID;
    private String eventName;
    private int tenantID;
    private ParameterMap parameterMap;
    
    public String getSubscriberID()
    {
      return subscriberID;
    }
    public String getEventName()
    {
      return eventName;
    }
    public ParameterMap getParameterMap()
    {
      return parameterMap;
    }
    public int getTenantID() { return tenantID; }
    
    /*****************************************
    *
    *  constructor -- enter
    *
    *****************************************/

    public EDRDetails(EvolutionEventContext context, EvolutionEngineEvent event, ParameterMap parameterMap, int tenantID)
    {
      this.subscriberID = context.getSubscriberState().getSubscriberID();
      this.eventName = event.getEventName();
      this.parameterMap = parameterMap;
      this.tenantID = tenantID;
    }
    
    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    private EDRDetails(SchemaAndValue schemaAndValue, String subscriberID, String eventName, ParameterMap parameterMap, int tenantID)
    {
      super(schemaAndValue);
      this.subscriberID = subscriberID;
      this.eventName = eventName;
      this.parameterMap = parameterMap;
      this.tenantID = tenantID;
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      EDRDetails edrDetails = (EDRDetails) value;
      Struct struct = new Struct(schema);
      packSubscriberStreamOutput(struct, edrDetails);
      struct.put("subscriberID", edrDetails.getSubscriberID());
      struct.put("eventName", edrDetails.getEventName());
      struct.put("parameterMap", ParameterMap.pack(edrDetails.getParameterMap()));
      struct.put("tenantID", (short) edrDetails.getTenantID());
      return struct;
    }
    
    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static EDRDetails unpack(SchemaAndValue schemaAndValue)
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
      String eventName = valueStruct.getString("eventName");
      ParameterMap parameterMap = ParameterMap.unpack(new SchemaAndValue(schema.field("parameterMap").schema(), valueStruct.get("parameterMap")));
      int tenantID = valueStruct.getInt16("tenantID");
      
      //
      //  return
      //

      return new EDRDetails(schemaAndValue, subscriberID, eventName, parameterMap, tenantID);
    }
    
    @Override public Object subscriberStreamEventPack(Object value) { return pack(value); }
}
