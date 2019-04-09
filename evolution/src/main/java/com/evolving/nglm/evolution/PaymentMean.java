
/*****************************************************************************
*
*  PaymentMean.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class PaymentMean extends GUIManagedObject 
{

  /*****************************************
  *
  *  variables
  *
  *****************************************/

  private static final String FULFILLMENT_PROVIDER_ID = "fulfillmentProviderID";
  private static final String DISPLAY = "display";
  private static final String ACTION_MANAGER_CLASS = "actionManagerClass";
  private static final String OTHERCONFIG = "otherconfig";

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
    schemaBuilder.name("paymentMean");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field(FULFILLMENT_PROVIDER_ID, Schema.STRING_SCHEMA);
    schemaBuilder.field(DISPLAY, Schema.STRING_SCHEMA);
    schemaBuilder.field(ACTION_MANAGER_CLASS, Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field(OTHERCONFIG, Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<PaymentMean> serde = new ConnectSerde<PaymentMean>(schema, false, PaymentMean.class, PaymentMean::pack, PaymentMean::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<PaymentMean> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/
  /*
   *  { 
   *    "id" : "1", 
   *    "fulfillmentProviderID" : "1", 
   *    "name" : "na", 
   *    "display" : "(not used)", 
   *    "actionManagerClass" : "", 
   *    "otherconfig" : ""
   *  }
   */

  private String fulfillmentProviderID = "";
  private String display = "";
  private String actionManagerClass = "";
  private String otherconfig = "";

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //

  public String getPaymentMeanID() { return getGUIManagedObjectID(); }
  public String getPaymentMeanName() { return getGUIManagedObjectName(); }
  public String getFulfillmentProviderID() { return fulfillmentProviderID; }
  public String getDisplay() { return display; }
  public String getActionManagerClass() { return actionManagerClass; }
  public String getOtherconfig() { return otherconfig; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/
  
  public PaymentMean(SchemaAndValue schemaAndValue, String fulfillmentProviderID, String display, String actionManagerClass, String otherconfig) {
    super(schemaAndValue);
    this.fulfillmentProviderID = fulfillmentProviderID;
    this.display = display;
    this.actionManagerClass = actionManagerClass;
    this.otherconfig = otherconfig;
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public PaymentMean(JSONObject jsonRoot, long epoch, GUIManagedObject existingPaymentMeanUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingPaymentMeanUnchecked != null) ? existingPaymentMeanUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingPaymentMean
    *
    *****************************************/

    PaymentMean existingPaymentMean = (existingPaymentMeanUnchecked != null && existingPaymentMeanUnchecked instanceof PaymentMean) ? (PaymentMean) existingPaymentMeanUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.fulfillmentProviderID = JSONUtilities.decodeString(jsonRoot, FULFILLMENT_PROVIDER_ID, true);
    this.display = JSONUtilities.decodeString(jsonRoot, DISPLAY, false);
    this.actionManagerClass = JSONUtilities.decodeString(jsonRoot, ACTION_MANAGER_CLASS, false);
    this.otherconfig = JSONUtilities.decodeString(jsonRoot, OTHERCONFIG, false);
    
    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingPaymentMean))
      {
        this.setEpoch(epoch);
      }
  }
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    PaymentMean paymentMean = (PaymentMean) value;
    Struct struct = new Struct(schema);
    packCommon(struct, paymentMean);
    struct.put(FULFILLMENT_PROVIDER_ID, paymentMean.getFulfillmentProviderID());
    struct.put(DISPLAY, paymentMean.getDisplay());
    struct.put(ACTION_MANAGER_CLASS, paymentMean.getActionManagerClass());
    struct.put(OTHERCONFIG, paymentMean.getOtherconfig());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static PaymentMean unpack(SchemaAndValue schemaAndValue)
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
    String fulfillmentProviderID = valueStruct.getString(FULFILLMENT_PROVIDER_ID);
    String display = valueStruct.getString(DISPLAY);
    String actionManagerClass = valueStruct.getString(ACTION_MANAGER_CLASS);
    String otherconfig = valueStruct.getString(OTHERCONFIG);

    //
    //  return
    //

    return new PaymentMean(schemaAndValue, fulfillmentProviderID, display, actionManagerClass, otherconfig);
  }


  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(PaymentMean existingPaymentMean)
  {
    if (existingPaymentMean != null && existingPaymentMean.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getPaymentMeanID(), existingPaymentMean.getPaymentMeanID());
        epochChanged = epochChanged || ! Objects.equals(getPaymentMeanName(), existingPaymentMean.getPaymentMeanName());
        epochChanged = epochChanged || ! Objects.equals(getFulfillmentProviderID(), existingPaymentMean.getFulfillmentProviderID());
        epochChanged = epochChanged || ! Objects.equals(getDisplay(), existingPaymentMean.getDisplay());
        epochChanged = epochChanged || ! Objects.equals(getActionManagerClass(), existingPaymentMean.getActionManagerClass());
        epochChanged = epochChanged || ! Objects.equals(getOtherconfig(), existingPaymentMean.getOtherconfig());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }

  /*****************************************
  *
  *  toString
  *
  *****************************************/

  @Override
  public String toString() {
    return "PaymentMean ["
        + "id=" + getPaymentMeanID()
        + ", name=" + getPaymentMeanName()
        + ", fulfillmentProviderID=" + getFulfillmentProviderID() 
        + ", display=" + getDisplay()
        + ", actionManagerClass=" + getActionManagerClass() 
        + ", otherconfig=" + getOtherconfig()
        + "]";
  }

}
