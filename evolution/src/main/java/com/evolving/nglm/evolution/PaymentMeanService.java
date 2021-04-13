/****************************************************************************
*
*  PaymentMeanService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Collection;
import java.util.Date;
import java.util.Objects;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class PaymentMeanService extends GUIService
{
/*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(PaymentMeanService.class);

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public PaymentMeanService(String bootstrapServers, String groupID, String paymentMeanTopic, boolean masterService, PaymentMeanListener paymentMeanListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "PaymentMeanService", groupID, paymentMeanTopic, masterService, getSuperListener(paymentMeanListener), "putPaymentMean", "removePaymentMean", notifyOnSignificantChange);
  }

  //
  //  constructor
  //
  
  public PaymentMeanService(String bootstrapServers, String groupID, String paymentMeanTopic, boolean masterService, PaymentMeanListener paymentMeanListener)
  {
    this(bootstrapServers, groupID, paymentMeanTopic, masterService, paymentMeanListener, true);
  }

  //
  //  constructor
  //

  public PaymentMeanService(String bootstrapServers, String groupID, String paymentMeanTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, paymentMeanTopic, masterService, (PaymentMeanListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(PaymentMeanListener paymentMeanListener)
  {
    GUIManagedObjectListener superListener = null;
    if (paymentMeanListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { paymentMeanListener.paymentMeanActivated((PaymentMean) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { paymentMeanListener.paymentMeanDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getSummaryJSONRepresentation
  *
  *****************************************/

  @Override protected JSONObject getSummaryJSONRepresentation(GUIManagedObject guiManagedObject)
  {
    JSONObject result = super.getSummaryJSONRepresentation(guiManagedObject);
    result.put("fulfillmentProviderID", guiManagedObject.getJSONRepresentation().get("fulfillmentProviderID"));
    return result;
  }
  
  /*****************************************
  *
  *  getReports
  *
  *****************************************/

  public String generatePaymentMeanID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredPaymentMean(String paymentMeanID) { return getStoredGUIManagedObject(paymentMeanID); }
  public GUIManagedObject getStoredPaymentMean(String paymentMeanID, boolean includeArchived) { return getStoredGUIManagedObject(paymentMeanID, includeArchived); }
  public Collection<GUIManagedObject> getStoredPaymentMeans(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredPaymentMeans(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActivePaymentMean(GUIManagedObject paymentMeanUnchecked, Date date) { return isActiveGUIManagedObject(paymentMeanUnchecked, date); }
  public PaymentMean getActivePaymentMean(String paymentMeanID, Date date) { return (PaymentMean) getActiveGUIManagedObject(paymentMeanID, date); }
  public Collection<PaymentMean> getActivePaymentMeans(Date date, int tenantID) { return (Collection<PaymentMean>) getActiveGUIManagedObjects(date, tenantID); }
  
  //
  //  getStoredPaymentMeanByName
  //
  
  public GUIManagedObject getStoredPaymentMeanByName(String paymentMeanName, boolean includeArchived, int tenantID)
  {
    GUIManagedObject result = null;
    for (GUIManagedObject guiManagedObject : getStoredPaymentMeans(includeArchived, tenantID))
      {
        if (Objects.equals(paymentMeanName, guiManagedObject.getGUIManagedObjectName()))
          {
            result = guiManagedObject;
            break;
          }
      }
    return result;
  }

  //
  //  getStoredPaymentMeanByName
  //

  public GUIManagedObject getStoredPaymentMeanByName(String paymentMeanName, int tenantID) { return getStoredPaymentMeanByName(paymentMeanName, false, tenantID); }

  /*****************************************
  *
  *  putPaymentMean
  *
  *****************************************/

  public void putPaymentMean(PaymentMean paymentMean, boolean newObject, String userID) { putGUIManagedObject(paymentMean, SystemTime.getCurrentTime(), newObject, userID); }

  /*****************************************
  *
  *  putPaymentMean
  *
  *****************************************/

  public void putIncompletePaymentMean(IncompleteObject paymentMean, boolean newObject, String userID)
  {
    Date now = SystemTime.getCurrentTime();
    putGUIManagedObject(paymentMean, now, newObject, userID);
  }

  /*****************************************
  *
  *  removePaymentMean
  *
  *****************************************/

  public void removePaymentMean(String paymentMeanID, String userID, int tenantID) { removeGUIManagedObject(paymentMeanID, SystemTime.getCurrentTime(), userID, tenantID); }

  /*****************************************
  *
  *  interface PaymentMeanListener
  *
  *****************************************/

  public interface PaymentMeanListener
  {
    public void paymentMeanActivated(PaymentMean paymentMean);
    public void paymentMeanDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  reportListener
    //

    PaymentMeanListener paymentMeanListener = new PaymentMeanListener()
    {
      @Override public void paymentMeanActivated(PaymentMean paymentMean) {
        log.trace("paymentMean activated: " + paymentMean.getPaymentMeanID());
      }
      @Override public void paymentMeanDeactivated(String guiManagedObjectID) { log.trace("paymentMean deactivated: " + guiManagedObjectID); }
    };

    //
    //  paymentMeanService
    //

    PaymentMeanService paymentMeanService = new PaymentMeanService(Deployment.getBrokerServers(), "example-001", Deployment.getPaymentMeanTopic(), false, paymentMeanListener);
    paymentMeanService.start();

    //
    //  sleep forever
    //

    while (true)
      {
        try
          {
            Thread.sleep(Long.MAX_VALUE);
          }
        catch (InterruptedException e)
          {
            //
            //  ignore
            //
          }
      }
  }

}
