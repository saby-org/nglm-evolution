/****************************************************************************
*
*  PredictionSettingsService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.PredictionSettingsMetadata;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;

public class PredictionSettingsService extends GUIService
{
  private static final Logger log = LoggerFactory.getLogger(PredictionSettingsService.class);
  
  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private ReferenceDataReader<String,PredictionSettingsMetadata> metadata;
  
  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public PredictionSettingsService(String bootstrapServers, String predictionSettingsTopic, boolean masterService, PredictionSettingsListener predictionSettingsListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "PredictionSettingsService", predictionSettingsTopic, masterService, getSuperListener(predictionSettingsListener), "putPredictionSettings", "removePredictionSettings", notifyOnSignificantChange);
    
    // Init metadata ReferenceDataReader
    metadata = ReferenceDataReader.<String,PredictionSettingsMetadata>startReader("predictionsettingsservice-predictionsettingsmetadata", Deployment.getBrokerServers(), Deployment.getPredictionSettingsMetadataTopic(), PredictionSettingsMetadata::unpack);
  }
  
  public PredictionSettingsService(String bootstrapServers, String predictionSettingsTopic, boolean masterService, PredictionSettingsListener predictionSettingsListener)
  {
    this(bootstrapServers, predictionSettingsTopic, masterService, predictionSettingsListener, true);
  }

  public PredictionSettingsService(String bootstrapServers, String predictionSettingsTopic, boolean masterService)
  {
    this(bootstrapServers, predictionSettingsTopic, masterService, (PredictionSettingsListener) null, true);
  }

  //
  //  getSuperListener
  //
  private static GUIManagedObjectListener getSuperListener(PredictionSettingsListener predictionSettingsListener)
  {
    GUIManagedObjectListener superListener = null;
    if (predictionSettingsListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { predictionSettingsListener.predictionSettingsActivated((PredictionSettings) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { predictionSettingsListener.predictionSettingsDeactivated(guiManagedObjectID); }
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
  *  getPredictionSettings
  *
  *****************************************/
  public String generatePredictionSettingsID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredPredictionSettings(String predictionSettingsID) { return getStoredGUIManagedObject(predictionSettingsID); }
  public GUIManagedObject getStoredPredictionSettings(String predictionSettingsID, boolean includeArchived) { return getStoredGUIManagedObject(predictionSettingsID, includeArchived); }
  public Collection<GUIManagedObject> getStoredPredictionSettings(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredPredictionSettings(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActivePredictionSettings(GUIManagedObject predictionSettingsUnchecked, Date date) { return isActiveGUIManagedObject(predictionSettingsUnchecked, date); }
  public PredictionSettings getActivePredictionSettings(String predictionSettingsID, Date date) { return (PredictionSettings) getActiveGUIManagedObject(predictionSettingsID, date); }
  public Collection<PredictionSettings> getActivePredictionSettings(Date date, int tenantID) { return (Collection<PredictionSettings>) getActiveGUIManagedObjects(date, tenantID); }

  /*****************************************
  *
  *  getPredictionSettingsMetaData
  *
  *****************************************/
  public PredictionSettingsMetadata getPredictionSettingsMetadata(String predictionSettingsID)
  {
    PredictionSettingsMetadata result = metadata.get(predictionSettingsID);
    if(result == null) {
      result = new PredictionSettingsMetadata(predictionSettingsID);
    }
    return result;
  }
  
  /*****************************************
  *
  *  putPredictionSettings
  *
  *****************************************/
  public void putPredictionSettings(GUIManagedObject predictionSettings, boolean newObject, String userID) throws GUIManagerException
  {
    Date now = SystemTime.getCurrentTime();

    //
    //  validate
    //
    if (predictionSettings instanceof PredictionSettings)
      {
        ((PredictionSettings) predictionSettings).validate();
      }
    
    putGUIManagedObject(predictionSettings, now, newObject, userID);
  }
  
  public void putPredictionSettings(IncompleteObject predictionSettings, boolean newObject, String userID)
  {
    try
      {
        putPredictionSettings((GUIManagedObject) predictionSettings, newObject, userID);
      }
    catch (GUIManagerException e)
      {
        throw new RuntimeException(e);
      }
  }

  /*****************************************
  *
  *  removePredictionSettings
  *
  *****************************************/
  public void removePredictionSettings(String predictionSettings, String userID, int tenantID) { 
    removeGUIManagedObject(predictionSettings, SystemTime.getCurrentTime(), userID, tenantID); 
    
    // TODO: no deletion of Metadata for the moment...
  }

  /*****************************************
  *
  *  interface PredictionSettingsListener
  *
  *****************************************/
  public interface PredictionSettingsListener
  {
    public void predictionSettingsActivated(PredictionSettings predictionSettings);
    public void predictionSettingsDeactivated(String guiManagedObjectID);
  }
}
