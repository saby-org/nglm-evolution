/****************************************************************************
*
*  SalesPartnerService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.SystemTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;

public class SalesPartnerService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SalesPartnerService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private SalesPartnerListener SalesPartnerListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SalesPartnerService(String bootstrapServers, String groupID, String salesPartnerTopic, boolean masterService, SalesPartnerListener salesPartnerListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "SalesPartnerService", groupID, salesPartnerTopic, masterService, getSuperListener(salesPartnerListener), "putSalesPartner", "removeSalesPartner", notifyOnSignificantChange);
  }

  //
  //  constructor
  //
  
  public SalesPartnerService(String bootstrapServers, String groupID, String salesPartnerTopic, boolean masterService, SalesPartnerListener salesPartnerListener)
  {
    this(bootstrapServers, groupID, salesPartnerTopic, masterService, salesPartnerListener, true);
  }

  //
  //  constructor
  //

  public SalesPartnerService(String bootstrapServers, String groupID, String salesPartnerTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, salesPartnerTopic, masterService, (SalesPartnerListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(SalesPartnerListener salesPartnerListener)
  {
    GUIManagedObjectListener superListener = null;
    if (salesPartnerListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { salesPartnerListener.salesPartnerActivated((SalesPartner) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { salesPartnerListener.salesPartnerDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getSalesPartners
  *
  *****************************************/

  public String generateSalesPartnerID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredSalesPartner(String SalesPartnerID) { return getStoredGUIManagedObject(SalesPartnerID); }
  public Collection<GUIManagedObject> getStoredSalesPartners() { return getStoredGUIManagedObjects(); }
  public boolean isActiveSalesPartner(GUIManagedObject SalesPartnerUnchecked, Date date) { return isActiveGUIManagedObject(SalesPartnerUnchecked, date); }
  public SalesPartner getActiveSalesPartner(String SalesPartnerID, Date date) { return (SalesPartner) getActiveGUIManagedObject(SalesPartnerID, date); }
  public Collection<SalesPartner> getActiveSalesPartners(Date date) { return (Collection<SalesPartner>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putSalesPartner
  *
  *****************************************/

  public void putSalesPartner(GUIManagedObject salesPartner, boolean newObject, String userID) throws GUIManagerException
  {
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    //  validate
    //

    if (salesPartner instanceof SalesPartner)
      {
        ((SalesPartner) salesPartner).validate(now);
      }
    
    //
    //  put
    //

    putGUIManagedObject(salesPartner, now, newObject, userID);
  }
  
  /*****************************************
  *
  *  putSalesPartner
  *
  *****************************************/

  public void putSalesPartner(IncompleteObject salesPartner, boolean newObject, String userID)
  {
    try
      {
        putSalesPartner((GUIManagedObject) salesPartner, newObject, userID);
      }
    catch (GUIManagerException e)
      {
        throw new RuntimeException(e);
      }
  }
  
  /*****************************************
  *
  *  removeSalesPartner
  *
  *****************************************/

  public void removeSalesPartner(String SalesPartnerID, String userID) { removeGUIManagedObject(SalesPartnerID, SystemTime.getCurrentTime(), userID); }

  /*****************************************
  *
  *  interface SalesPartnerListener
  *
  *****************************************/

  public interface SalesPartnerListener
  {
    public void salesPartnerActivated(SalesPartner SalesPartner);
    public void salesPartnerDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  salesPartnerListener
    //

    SalesPartnerListener salesPartnerListener = new SalesPartnerListener()
    {
      @Override public void salesPartnerActivated(SalesPartner salesPartner) { System.out.println("salesPartner activated: " + salesPartner.getGUIManagedObjectID()); }
      @Override public void salesPartnerDeactivated(String guiManagedObjectID) { System.out.println("salesPartner deactivated: " + guiManagedObjectID); }
    };

    //
    //  salesPartnerService
    //

    SalesPartnerService salesPartnerService = new SalesPartnerService(Deployment.getBrokerServers(), "example-SalesPartnerservice-001", Deployment.getSalesPartnerTopic(), false, salesPartnerListener);
    salesPartnerService.start();

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
