
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
    schemaBuilder.field("fulfillmentProviderID", Schema.STRING_SCHEMA);
    schemaBuilder.field("externalAccountID", Schema.STRING_SCHEMA);
    schemaBuilder.field("display", Schema.STRING_SCHEMA);
    schemaBuilder.field("actionManagerClass", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("otherConfig", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("generatedFromAccount", Schema.BOOLEAN_SCHEMA);
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

  private String fulfillmentProviderID;
  private String externalAccountID;
  private String display;
  private String actionManagerClass;
  private String otherConfig;
  private boolean generatedFromAccount;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getPaymentMeanID() { return getGUIManagedObjectID(); }
  public String getPaymentMeanName() { return getGUIManagedObjectName(); }
  public String getFulfillmentProviderID() { return fulfillmentProviderID; }
  public String getExternalAccountID() { return externalAccountID; }
  public String getDisplay() { return display; }
  public String getActionManagerClass() { return actionManagerClass; }
  public String getOtherConfig() { return otherConfig; }
  public boolean getGeneratedFromAccount() { return generatedFromAccount; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/
  
  public PaymentMean(SchemaAndValue schemaAndValue, String fulfillmentProviderID, String externalAccountID, String display, String actionManagerClass, String otherConfig, boolean generatedFromAccount)
  {
    super(schemaAndValue);
    this.fulfillmentProviderID = fulfillmentProviderID;
    this.externalAccountID = externalAccountID;
    this.display = display;
    this.actionManagerClass = actionManagerClass;
    this.otherConfig = otherConfig;
    this.generatedFromAccount = generatedFromAccount;
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

    this.fulfillmentProviderID = JSONUtilities.decodeString(jsonRoot, "fulfillmentProviderID", true);
    this.externalAccountID = JSONUtilities.decodeString(jsonRoot, "externalAccountID", true);
    this.display = JSONUtilities.decodeString(jsonRoot, "display", false);
    this.actionManagerClass = JSONUtilities.decodeString(jsonRoot, "actionManagerClass", false);
    this.otherConfig = JSONUtilities.decodeString(jsonRoot, "otherConfig", false);
    this.generatedFromAccount = JSONUtilities.decodeBoolean(jsonRoot, "generatedFromAccount", Boolean.FALSE);
    
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
    struct.put("fulfillmentProviderID", paymentMean.getFulfillmentProviderID());
    struct.put("externalAccountID", paymentMean.getExternalAccountID());
    struct.put("display", paymentMean.getDisplay());
    struct.put("actionManagerClass", paymentMean.getActionManagerClass());
    struct.put("otherConfig", paymentMean.getOtherConfig());
    struct.put("generatedFromAccount", paymentMean.getGeneratedFromAccount());
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
    String fulfillmentProviderID = valueStruct.getString("fulfillmentProviderID");
    String externalAccountID = valueStruct.getString("externalAccountID");
    String display = valueStruct.getString("display");
    String actionManagerClass = valueStruct.getString("actionManagerClass");
    String otherConfig = valueStruct.getString("otherConfig");
    boolean generatedFromAccount = valueStruct.getBoolean("generatedFromAccount");

    //
    //  return
    //

    return new PaymentMean(schemaAndValue, fulfillmentProviderID, externalAccountID, display, actionManagerClass, otherConfig, generatedFromAccount);
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
        epochChanged = epochChanged || ! Objects.equals(getExternalAccountID(), existingPaymentMean.getExternalAccountID());
        epochChanged = epochChanged || ! Objects.equals(getDisplay(), existingPaymentMean.getDisplay());
        epochChanged = epochChanged || ! Objects.equals(getActionManagerClass(), existingPaymentMean.getActionManagerClass());
        epochChanged = epochChanged || ! Objects.equals(getOtherConfig(), existingPaymentMean.getOtherConfig());
        epochChanged = epochChanged || ! generatedFromAccount == existingPaymentMean.getGeneratedFromAccount();
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

  @Override public String toString()
  {
    return "PaymentMean ["
        + "id=" + getPaymentMeanID()
        + ", name=" + getPaymentMeanName()
        + ", fulfillmentProviderID=" + getFulfillmentProviderID() 
        + ", externalAccountID=" + getExternalAccountID() 
        + ", display=" + getDisplay()
        + ", actionManagerClass=" + getActionManagerClass() 
        + ", otherConfig=" + getOtherConfig()
        + ", generatedFromAccount=" + getGeneratedFromAccount()
        + "]";
  }
}
