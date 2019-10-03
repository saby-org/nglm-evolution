/****************************************************************************
*
*  ProductTypeService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

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

public class ProductTypeService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(ProductTypeService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private ProductTypeListener productTypeListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public ProductTypeService(String bootstrapServers, String groupID, String productTypeTopic, boolean masterService, ProductTypeListener productTypeListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "ProductTypeService", groupID, productTypeTopic, masterService, getSuperListener(productTypeListener), "putProductType", "removeProductType", notifyOnSignificantChange);
  }
  //
  //  constructor
  //

  public ProductTypeService(String bootstrapServers, String groupID, String productTypeTopic, boolean masterService, ProductTypeListener productTypeListener)
  {
    this(bootstrapServers, groupID, productTypeTopic, masterService, productTypeListener, true);
  }

  //
  //  constructor
  //

  public ProductTypeService(String bootstrapServers, String groupID, String productTypeTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, productTypeTopic, masterService, (ProductTypeListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(ProductTypeListener productTypeListener)
  {
    GUIManagedObjectListener superListener = null;
    if (productTypeListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { productTypeListener.productTypeActivated((ProductType) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { productTypeListener.productTypeDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getProductTypes
  *
  *****************************************/

  public String generateProductTypeID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredProductType(String productTypeID) { return getStoredGUIManagedObject(productTypeID); }
  public GUIManagedObject getStoredProductType(String productTypeID, boolean includeArchived) { return getStoredGUIManagedObject(productTypeID, includeArchived); }
  public Collection<GUIManagedObject> getStoredProductTypes() { return getStoredGUIManagedObjects(); }
  public Collection<GUIManagedObject> getStoredProductTypes(boolean includeArchived) { return getStoredGUIManagedObjects(includeArchived); }
  public boolean isActiveProductType(GUIManagedObject productTypeUnchecked, Date date) { return isActiveGUIManagedObject(productTypeUnchecked, date); }
  public ProductType getActiveProductType(String productTypeID, Date date) { return (ProductType) getActiveGUIManagedObject(productTypeID, date); }
  public Collection<ProductType> getActiveProductTypes(Date date) { return (Collection<ProductType>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putProductType
  *
  *****************************************/

  public void putProductType(GUIManagedObject productType, boolean newObject, String userID) { putGUIManagedObject(productType, SystemTime.getCurrentTime(), newObject, userID); }

  /*****************************************
  *
  *  removeProductType
  *
  *****************************************/

  public void removeProductType(String productTypeID, String userID) { removeGUIManagedObject(productTypeID, SystemTime.getCurrentTime(), userID); }

  /*****************************************
  *
  *  interface ProductTypeListener
  *
  *****************************************/

  public interface ProductTypeListener
  {
    public void productTypeActivated(ProductType productType);
    public void productTypeDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  productTypeListener
    //

    ProductTypeListener productTypeListener = new ProductTypeListener()
    {
      @Override public void productTypeActivated(ProductType productType) { System.out.println("productType activated: " + productType.getProductTypeID()); }
      @Override public void productTypeDeactivated(String guiManagedObjectID) { System.out.println("productType deactivated: " + guiManagedObjectID); }
    };

    //
    //  productTypeService
    //

    ProductTypeService productTypeService = new ProductTypeService(Deployment.getBrokerServers(), "example-productTypeservice-001", Deployment.getProductTypeTopic(), false, productTypeListener);
    productTypeService.start();

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
