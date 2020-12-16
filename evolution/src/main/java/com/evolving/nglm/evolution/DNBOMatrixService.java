/****************************************************************************
*
*  DNBOMatrixService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Collection;
import java.util.Date;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class DNBOMatrixService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(DNBOMatrixService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private DNBOMatrixListener dnboMatrixListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public DNBOMatrixService(String bootstrapServers, String groupID, String dnboMatrixTopic, boolean masterService, DNBOMatrixListener dnboMatrixListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "DNBOMatrixService", groupID, dnboMatrixTopic, masterService, getSuperListener(dnboMatrixListener), "putDNBOMatrix", "removeDNBOMatrix", notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public DNBOMatrixService(String bootstrapServers, String groupID, String dnboMatrixTopic, boolean masterService, DNBOMatrixListener dnboMatrixListener)
  {
    this(bootstrapServers, groupID, dnboMatrixTopic, masterService, dnboMatrixListener, true);
  }

  //
  //  constructor
  //

  public DNBOMatrixService(String bootstrapServers, String groupID, String dnboMatrixTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, dnboMatrixTopic, masterService, (DNBOMatrixListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(DNBOMatrixListener dnboMatrixListener)
  {
    GUIManagedObjectListener superListener = null;
    if (dnboMatrixListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { dnboMatrixListener.dnboMatrixActivated((DNBOMatrix) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { dnboMatrixListener.dnboMatrixDeactivated(guiManagedObjectID); }
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
  *  generateDNBOMatrixID
  *
  *****************************************/

  public String generateDNBOMatrixID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredDNBOMatrix(String dnboMatrixID, int tenantID) { return getStoredGUIManagedObject(dnboMatrixID, tenantID); }
  public GUIManagedObject getStoredDNBOMatrix(String dnboMatrixID, boolean includeArchived, int tenantID) { return getStoredGUIManagedObject(dnboMatrixID, includeArchived, tenantID); }
  public Collection<GUIManagedObject> getStoredDNBOMatrixes(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredDNBOMatrixes(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActiveDNBOMatrix(GUIManagedObject dnboMatrixUnchecked, Date date) { return isActiveGUIManagedObject(dnboMatrixUnchecked, date); }
  public DNBOMatrix getActiveDNBOMatrix(String dnboMatrixID, Date date, int tenantID) { return (DNBOMatrix) getActiveGUIManagedObject(dnboMatrixID, date, tenantID); }
  public Collection<DNBOMatrix> getActiveDNBOMatrixes(Date date, int tenantID) { return (Collection<DNBOMatrix>) getActiveGUIManagedObjects(date, tenantID); }

  /*****************************************
  *
  *  putDNBOMatrix
  *
  *****************************************/

  public void putDNBOMatrix(GUIManagedObject dnboMatrix, boolean newObject, String userID, int tenantID) throws GUIManagerException
  {
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    //  put
    //

    putGUIManagedObject(dnboMatrix, now, newObject, userID, tenantID);
  }

  /*****************************************
  *
  *  putDNBOMatrix
  *
  *****************************************/

  public void putDNBOMatrix(IncompleteObject dnboMatrix, boolean newObject, String userID, int tenantID)
  {
    try
      {
        putDNBOMatrix((GUIManagedObject) dnboMatrix, newObject, userID, tenantID);
      }
    catch (GUIManagerException e)
      {
        throw new RuntimeException(e);
      }
  }
  
  /*****************************************
  *
  *  removeDNBOMatrix
  *
  *****************************************/

  public void removeDNBOMatrix(String dnboMatrixID, String userID, int tenantID) { removeGUIManagedObject(dnboMatrixID, SystemTime.getCurrentTime(), userID, tenantID); }

  /*****************************************
  *
  *  interface DNBOMatrixListener
  *
  *****************************************/

  public interface DNBOMatrixListener
  {
    public void dnboMatrixActivated(DNBOMatrix dnboMatrix);
    public void dnboMatrixDeactivated(String guiManagedObjectID);
  }
}
