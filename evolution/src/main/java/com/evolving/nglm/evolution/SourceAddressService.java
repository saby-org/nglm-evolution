package com.evolving.nglm.evolution;

import java.util.Collection;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class SourceAddressService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SourceAddress.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  @Deprecated // groupID not used
  public SourceAddressService(String bootstrapServers, String groupID, String sourceAddressTopic, boolean masterService, SourceAddressListener sourceAddressListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "SourceAddressService", groupID, sourceAddressTopic, masterService, getSuperListener(sourceAddressListener), "putSourceAddress", "removeSourceAddress", notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  @Deprecated // groupID not used
  public SourceAddressService(String bootstrapServers, String groupID, String sourceAddressTopic, boolean masterService, SourceAddressListener sourceAddressListener)
  {
    this(bootstrapServers, groupID, sourceAddressTopic, masterService, sourceAddressListener, true);
  }

  //
  //  constructor
  //

  @Deprecated // groupID not used
  public SourceAddressService(String bootstrapServers, String groupID, String sourceAddressTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, sourceAddressTopic, masterService, (SourceAddressListener) null, true);
  }
  
  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(SourceAddressListener sourceAddressListener)
  {
    GUIManagedObjectListener superListener = null;
    if (sourceAddressListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { sourceAddressListener.sourceAddressActivated((SourceAddress) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { sourceAddressListener.sourceAddressDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }
  
  /*****************************************
  *
  *  getSourceAddresses
  *
  *****************************************/

  public String generateSourceAddressID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredSourceAddress(String sourceAddressID) { return getStoredGUIManagedObject(sourceAddressID); }
  public GUIManagedObject getStoredSourceAddress(String sourceAddressID, boolean includeArchived) { return getStoredGUIManagedObject(sourceAddressID, includeArchived); }
  public Collection<GUIManagedObject> getStoredSourceAddresss() { return getStoredGUIManagedObjects(); }
  public Collection<GUIManagedObject> getStoredSourceAddresses(boolean includeArchived) { return getStoredGUIManagedObjects(includeArchived); }
  public boolean isActiveSourceAddress(GUIManagedObject sourceAddressUnchecked, Date date) { return isActiveGUIManagedObject(sourceAddressUnchecked, date); }
  public SourceAddress getActiveSourceAddress(String sourceAddressID, Date date) { return (SourceAddress) getActiveGUIManagedObject(sourceAddressID, date); }
  public Collection<SourceAddress> getActiveSourceAddresses(Date date) { return (Collection<SourceAddress>) getActiveGUIManagedObjects(date); }
  
  /*****************************************
  *
  *  putSourceAddress
  *
  *****************************************/

  public void putSourceAddress(GUIManagedObject sourceAddress, boolean newObject, String userID) throws GUIManagerException
  {
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    //  validate
    //

    if (sourceAddress instanceof SourceAddress)      
      {
        ((SourceAddress) sourceAddress).validate(now);
      }
    
    //
    //  put
    //

    putGUIManagedObject(sourceAddress, now, newObject, userID);
  }
  
  /*****************************************
  *
  *  putSourceAddress
  *
  *****************************************/

  public void putSourceAddress(IncompleteObject sourceAddress, boolean newObject, String userID)
  {
    try
      {
        putSourceAddress((GUIManagedObject) sourceAddress, newObject, userID);
      }
    catch (GUIManagerException e)
      {
        throw new RuntimeException(e);
      }
  }
  
  /*****************************************
  *
  *  removeSourceAddress
  *
  *****************************************/

  public void removeSourceAddress(String sourceAddressID, String userID) { removeGUIManagedObject(sourceAddressID, SystemTime.getCurrentTime(), userID); }

  /*****************************************
  *
  *  interface SourceAddressListener
  *
  *****************************************/

  public interface SourceAddressListener
  {
    public void sourceAddressActivated(SourceAddress sourceAddress);
    public void sourceAddressDeactivated(String guiManagedObjectID);
  }
  
  /*****************************************
  *
  *  example main
  *
  *****************************************/
  
  public static void main(String[] args)
  {
    //
    //  SourceAddressListener
    //

    SourceAddressListener sourceAddressListener = new SourceAddressListener()
    {
      @Override public void sourceAddressActivated(SourceAddress sourceAddress) { System.out.println("SourceAddress activated: " + sourceAddress.getSourceAddressId()); }
      @Override public void sourceAddressDeactivated(String guiManagedObjectID) { System.out.println("SourceAddress deactivated: " + guiManagedObjectID); }
    };

    //
    //  SourceAddressService
    //

    SourceAddressService sourceAddressService = new SourceAddressService(Deployment.getBrokerServers(), "example-sourceAddressservice-001", Deployment.getSourceAddressTopic(), false, sourceAddressListener);
    sourceAddressService.start();

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
