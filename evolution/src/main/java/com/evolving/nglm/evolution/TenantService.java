/****************************************************************************
*
*  TenantService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.core.SystemTime;

import org.json.simple.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;

public class TenantService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(TenantService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private TenantListener tenantListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public TenantService(String bootstrapServers, String groupID, String tenantTopic, boolean masterService, TenantListener tenantListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "TenantService", groupID, tenantTopic, masterService, getSuperListener(tenantListener), "putTenant", "removeTenant", notifyOnSignificantChange);
  }

  //
  //  constructor
  //
  
  public TenantService(String bootstrapServers, String groupID, String tenantTopic, boolean masterService, TenantListener tenantListener)
  {
    this(bootstrapServers, groupID, tenantTopic, masterService, tenantListener, true);
  }

  //
  //  constructor
  //

  public TenantService(String bootstrapServers, String groupID, String tenantTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, tenantTopic, masterService, (TenantListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(TenantListener tenantListener)
  {
    GUIManagedObjectListener superListener = null;
    if (tenantListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { tenantListener.tenantActivated((Tenant) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { tenantListener.tenantDeactivated(guiManagedObjectID); }
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
    result.put("default", guiManagedObject.getJSONRepresentation().get("default"));
    result.put("serviceTypeID", guiManagedObject.getJSONRepresentation().get("serviceTypeID"));
    result.put("imageURL", guiManagedObject.getJSONRepresentation().get("imageURL"));
    result.put("tenantObjectives", guiManagedObject.getJSONRepresentation().get("tenantObjectives"));
    return result;
  }
  
  /*****************************************
  *
  *  getTenants
  *
  *****************************************/

  public String generateTenantID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredTenant(String tenantID) { return getStoredGUIManagedObject(tenantID); } // tenants are administrator objects
  public GUIManagedObject getStoredTenant(String tenantID, boolean includeArchived) { return getStoredGUIManagedObject(tenantID, includeArchived); }
  public Collection<GUIManagedObject> getStoredTenants() { return getStoredGUIManagedObjects(0); }
  public Collection<GUIManagedObject> getStoredTenants(boolean includeArchived) { return getStoredGUIManagedObjects(includeArchived, 0); }
  public boolean isActiveTenant(GUIManagedObject tenantUnchecked, Date date) { return isActiveGUIManagedObject(tenantUnchecked, date); }
  public Tenant getActiveTenant(String tenantID, Date date) { return (Tenant) getActiveGUIManagedObject(tenantID, date); }
  public Collection<Tenant> getActiveTenants(Date date) { return (Collection<Tenant>) getActiveGUIManagedObjects(date, 0); }

  /*****************************************
  *
  *  putTenant
  *
  *****************************************/

  public void putTenant(GUIManagedObject tenant, CallingChannelService callingChannelService, SalesChannelService salesChannelService, ProductService productService, VoucherService voucherService, boolean newObject, String userID) throws GUIManagerException
  {
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    //  put
    //

    putGUIManagedObject(tenant, now, newObject, userID);
  }

  /*****************************************
  *
  *  putTenant
  *
  *****************************************/

  public void putTenant(IncompleteObject tenant, CallingChannelService callingChannelService, SalesChannelService salesChannelService, ProductService productService, VoucherService voucherService, boolean newObject, String userID)
  {
    try
      {
        putTenant((GUIManagedObject) tenant, callingChannelService, salesChannelService, productService, voucherService, newObject, userID);
      }
    catch (GUIManagerException e)
      {
        throw new RuntimeException(e);
      }
  }
  
  /*****************************************
  *
  *  removeTenant
  *
  *****************************************/

  public void removeTenant(String tenantID, String userID) { removeGUIManagedObject(tenantID, SystemTime.getCurrentTime(), userID, 0); }

  /*****************************************
  *
  *  interface TenantListener
  *
  *****************************************/

  public interface TenantListener
  {
    public void tenantActivated(Tenant tenant);
    public void tenantDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  tenantListener
    //

    TenantListener tenantListener = new TenantListener()
    {
      @Override public void tenantActivated(Tenant tenant) { System.out.println("tenant activated: " + tenant.getTenantID()); }
      @Override public void tenantDeactivated(String guiManagedObjectID) { System.out.println("tenant deactivated: " + guiManagedObjectID); }
    };

    //
    //  tenantService
    //

    TenantService tenantService = new TenantService(Deployment.getBrokerServers(), "example-001", "tenant", false, tenantListener);
    tenantService.start();

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
