/****************************************************************************
*
*  OfferService.java
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

public class OfferService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(OfferService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private OfferListener offerListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public OfferService(String bootstrapServers, String groupID, String offerTopic, boolean masterService, OfferListener offerListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "OfferService", groupID, offerTopic, masterService, getSuperListener(offerListener), "putOffer", "removeOffer", notifyOnSignificantChange);
  }

  //
  //  constructor
  //
  
  public OfferService(String bootstrapServers, String groupID, String offerTopic, boolean masterService, OfferListener offerListener)
  {
    this(bootstrapServers, groupID, offerTopic, masterService, offerListener, true);
  }

  //
  //  constructor
  //

  public OfferService(String bootstrapServers, String groupID, String offerTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, offerTopic, masterService, (OfferListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(OfferListener offerListener)
  {
    GUIManagedObjectListener superListener = null;
    if (offerListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { offerListener.offerActivated((Offer) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { offerListener.offerDeactivated(guiManagedObjectID); }
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
    result.put("offerObjectives", guiManagedObject.getJSONRepresentation().get("offerObjectives"));
    return result;
  }
  
  /*****************************************
  *
  *  getOffers
  *
  *****************************************/

  public String generateOfferID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredOffer(String offerID, int tenantID) { return getStoredGUIManagedObject(offerID, tenantID); }
  public GUIManagedObject getStoredOffer(String offerID, boolean includeArchived, int tenantID) { return getStoredGUIManagedObject(offerID, includeArchived, tenantID); }
  public Collection<GUIManagedObject> getStoredOffers(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredOffers(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActiveOffer(GUIManagedObject offerUnchecked, Date date) { return isActiveGUIManagedObject(offerUnchecked, date); }
  public Offer getActiveOffer(String offerID, Date date, int tenantID) { return (Offer) getActiveGUIManagedObject(offerID, date, tenantID); }
  public Collection<Offer> getActiveOffers(Date date, int tenantID) { return (Collection<Offer>) getActiveGUIManagedObjects(date, tenantID); }

  /*****************************************
  *
  *  putOffer
  *
  *****************************************/

  public void putOffer(GUIManagedObject offer, CallingChannelService callingChannelService, SalesChannelService salesChannelService, ProductService productService, VoucherService voucherService, boolean newObject, String userID, int tenantID) throws GUIManagerException
  {
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    //  validate
    //

    if (offer instanceof Offer)
      {
        ((Offer) offer).validate(callingChannelService, salesChannelService, productService, voucherService, now, tenantID);
      }

    //
    //  put
    //

    putGUIManagedObject(offer, now, newObject, userID, tenantID);
  }

  /*****************************************
  *
  *  putOffer
  *
  *****************************************/

  public void putOffer(IncompleteObject offer, CallingChannelService callingChannelService, SalesChannelService salesChannelService, ProductService productService, VoucherService voucherService, boolean newObject, String userID, int tenantID)
  {
    try
      {
        putOffer((GUIManagedObject) offer, callingChannelService, salesChannelService, productService, voucherService, newObject, userID, tenantID);
      }
    catch (GUIManagerException e)
      {
        throw new RuntimeException(e);
      }
  }
  
  /*****************************************
  *
  *  removeOffer
  *
  *****************************************/

  public void removeOffer(String offerID, String userID, int tenantID) { removeGUIManagedObject(offerID, SystemTime.getCurrentTime(), userID, tenantID); }

  /*****************************************
  *
  *  interface OfferListener
  *
  *****************************************/

  public interface OfferListener
  {
    public void offerActivated(Offer offer);
    public void offerDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  offerListener
    //

    OfferListener offerListener = new OfferListener()
    {
      @Override public void offerActivated(Offer offer) { System.out.println("offer activated: " + offer.getOfferID()); }
      @Override public void offerDeactivated(String guiManagedObjectID) { System.out.println("offer deactivated: " + guiManagedObjectID); }
    };

    //
    //  offerService
    //

    OfferService offerService = new OfferService(Deployment.getBrokerServers(), "example-001", Deployment.getOfferTopic(), false, offerListener);
    offerService.start();

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
