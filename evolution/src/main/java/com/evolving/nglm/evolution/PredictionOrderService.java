/****************************************************************************
*
*  PredictionOrderService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.PredictionOrderMetadata;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;

public class PredictionOrderService extends GUIService
{
  private static final Logger log = LoggerFactory.getLogger(PredictionOrderService.class);
  
  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private ReferenceDataReader<String,PredictionOrderMetadata> metadata;
  
  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public PredictionOrderService(String bootstrapServers, String predictionOrderTopic, boolean masterService, PredictionOrderListener predictionOrderListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "PredictionOrderService", predictionOrderTopic, masterService, getSuperListener(predictionOrderListener), "putPredictionOrder", "removePredictionOrder", notifyOnSignificantChange);
    
    // Init metadata ReferenceDataReader
    metadata = ReferenceDataReader.<String,PredictionOrderMetadata>startReader("predictionorderservice-predictionordermetadata", Deployment.getBrokerServers(), Deployment.getPredictionOrderMetadataTopic(), PredictionOrderMetadata::unpack);
  }
  
  public PredictionOrderService(String bootstrapServers, String predictionOrderTopic, boolean masterService, PredictionOrderListener predictionOrderListener)
  {
    this(bootstrapServers, predictionOrderTopic, masterService, predictionOrderListener, true);
  }

  public PredictionOrderService(String bootstrapServers, String predictionOrderTopic, boolean masterService)
  {
    this(bootstrapServers, predictionOrderTopic, masterService, (PredictionOrderListener) null, true);
  }

  //
  //  getSuperListener
  //
  private static GUIManagedObjectListener getSuperListener(PredictionOrderListener predictionOrderListener)
  {
    GUIManagedObjectListener superListener = null;
    if (predictionOrderListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { predictionOrderListener.predictionOrderActivated((PredictionOrder) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { predictionOrderListener.predictionOrderDeactivated(guiManagedObjectID); }
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
    return result;
  }

  /*****************************************
  *
  *  getPredictionOrder
  *
  *****************************************/
  public String generatePredictionOrderID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredPredictionOrder(String predictionOrderID) { return getStoredGUIManagedObject(predictionOrderID); }
  public GUIManagedObject getStoredPredictionOrder(String predictionOrderID, boolean includeArchived) { return getStoredGUIManagedObject(predictionOrderID, includeArchived); }
  public Collection<GUIManagedObject> getStoredPredictionOrders(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredPredictionOrders(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActivePredictionOrder(GUIManagedObject predictionOrderUnchecked, Date date) { return isActiveGUIManagedObject(predictionOrderUnchecked, date); }
  public PredictionOrder getActivePredictionOrder(String predictionOrderID, Date date) { return (PredictionOrder) getActiveGUIManagedObject(predictionOrderID, date); }
  public Collection<PredictionOrder> getActivePredictionOrders(Date date, int tenantID) { return (Collection<PredictionOrder>) getActiveGUIManagedObjects(date, tenantID); }

  /*****************************************
  *
  *  getPredictionOrderMetaData
  *
  *****************************************/
  public PredictionOrderMetadata getPredictionOrderMetadata(String predictionOrderID)
  {
    PredictionOrderMetadata result = metadata.get(predictionOrderID);
    if(result == null) {
      result = new PredictionOrderMetadata(predictionOrderID);
    }
    return result;
  }
  
  /*****************************************
  *
  *  putPredictionOrder
  *
  *****************************************/
  public void putPredictionOrder(GUIManagedObject predictionOrder, boolean newObject, String userID) throws GUIManagerException
  {
    Date now = SystemTime.getCurrentTime();

    //
    //  validate
    //
    if (predictionOrder instanceof PredictionOrder)
      {
        ((PredictionOrder) predictionOrder).validate();
      }
    
    putGUIManagedObject(predictionOrder, now, newObject, userID);
  }
  
  public void putPredictionOrder(IncompleteObject predictionOrder, boolean newObject, String userID)
  {
    try
      {
        putPredictionOrder((GUIManagedObject) predictionOrder, newObject, userID);
      }
    catch (GUIManagerException e)
      {
        throw new RuntimeException(e);
      }
  }

  /*****************************************
  *
  *  removePredictionOrder
  *
  *****************************************/
  public void removePredictionOrder(String predictionOrder, String userID, int tenantID) { 
    removeGUIManagedObject(predictionOrder, SystemTime.getCurrentTime(), userID, tenantID); 
    
    // TODO: no deletion of Metadata for the moment...
  }

  /*****************************************
  *
  *  interface PredictionOrderListener
  *
  *****************************************/
  public interface PredictionOrderListener
  {
    public void predictionOrderActivated(PredictionOrder predictionOrder);
    public void predictionOrderDeactivated(String guiManagedObjectID);
  }
}
