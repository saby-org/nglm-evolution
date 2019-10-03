/****************************************************************************
*
*  OfferService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;

import com.evolving.nglm.core.SystemTime;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import org.json.simple.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { offerListener.offerDeactivated(guiManagedObjectID); }
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
    return result;
  }
  
  /*****************************************
  *
  *  getOffers
  *
  *****************************************/

  public String generateOfferID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredOffer(String offerID) { return getStoredGUIManagedObject(offerID); }
  public GUIManagedObject getStoredOffer(String offerID, boolean includeArchived) { return getStoredGUIManagedObject(offerID, includeArchived); }
  public Collection<GUIManagedObject> getStoredOffers() { return getStoredGUIManagedObjects(); }
  public Collection<GUIManagedObject> getStoredOffers(boolean includeArchived) { return getStoredGUIManagedObjects(includeArchived); }
  public boolean isActiveOffer(GUIManagedObject offerUnchecked, Date date) { return isActiveGUIManagedObject(offerUnchecked, date); }
  public Offer getActiveOffer(String offerID, Date date) { return (Offer) getActiveGUIManagedObject(offerID, date); }
  public Collection<Offer> getActiveOffers(Date date) { return (Collection<Offer>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putOffer
  *
  *****************************************/

  public void putOffer(GUIManagedObject offer, CallingChannelService callingChannelService, SalesChannelService salesChannelService, ProductService productService, boolean newObject, String userID) throws GUIManagerException
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
        ((Offer) offer).validate(callingChannelService, salesChannelService, productService, now);
      }

    //
    //  put
    //

    putGUIManagedObject(offer, now, newObject, userID);
  }

  /*****************************************
  *
  *  putOffer
  *
  *****************************************/

  public void putOffer(IncompleteObject offer, CallingChannelService callingChannelService, SalesChannelService salesChannelService, ProductService productService, boolean newObject, String userID)
  {
    try
      {
        putOffer((GUIManagedObject) offer, callingChannelService, salesChannelService, productService, newObject, userID);
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

  public void removeOffer(String offerID, String userID) { removeGUIManagedObject(offerID, SystemTime.getCurrentTime(), userID); }

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
