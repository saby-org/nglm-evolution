/*****************************************************************************
*
*  Product.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.StockMonitor.StockableItem;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import com.evolving.nglm.core.JSONUtilities;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.Objects;
import java.util.Set;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

@GUIDependencyDef(objectType = "product", serviceClass = ProductService.class, dependencies = { "supplier" , "point" , "campaign" , "producttype" })
public class Product extends GUIManagedObject implements StockableItem
{
  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema schema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("product");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),4));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("supplierID", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliverableID", Schema.STRING_SCHEMA);
    schemaBuilder.field("stock", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("productTypes", SchemaBuilder.array(ProductTypeInstance.schema()).schema());
    schemaBuilder.field("simpleOffer", Schema.OPTIONAL_BOOLEAN_SCHEMA);
    schemaBuilder.field("workflowID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("deliverableQuantity", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("deliverableValidity", ProductValidity.schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<Product> serde = new ConnectSerde<Product>(schema, false, Product.class, Product::pack, Product::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<Product> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String supplierID;
  private String deliverableID;
  private Integer stock;
  private Set<ProductTypeInstance> productTypes;
  private boolean simpleOffer;
  private String workflowID;
  private int deliverableQuantity;
  private ProductValidity deliverableValidity;

  //
  //  derived
  //

  private String stockableItemID;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getProductID() { return getGUIManagedObjectID(); }
  public String getDisplay() { return getGUIManagedObjectDisplay(); }
  public String getSupplierID() { return supplierID; }
  public String getDeliverableID() { return deliverableID; }
  public Integer getStock() { return stock; }
  public Set<ProductTypeInstance> getProductTypes() { return productTypes;  }
  public String getStockableItemID() { return stockableItemID; }
  public boolean getSimpleOffer() { return simpleOffer; }
  public String getWorkflowID() { return workflowID; }
  public int getDeliverableQuantity() { return deliverableQuantity; }
  public ProductValidity getDeliverableValidity(){ return deliverableValidity; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Product(SchemaAndValue schemaAndValue, String supplierID, String deliverableID, Integer stock, Set<ProductTypeInstance> productTypes, boolean simpleOffer, String workflowID, int deliverableQuantity, ProductValidity deliverableValidity)
  {
    super(schemaAndValue);
    this.supplierID = supplierID;
    this.deliverableID = deliverableID;
    this.stock = stock;
    this.productTypes = productTypes;
    this.stockableItemID = "product-" + getProductID();
    this.simpleOffer = simpleOffer;
    this.workflowID = workflowID;
    this.deliverableQuantity = deliverableQuantity;
    this.deliverableValidity = deliverableValidity;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    Product product = (Product) value;
    Struct struct = new Struct(schema);
    packCommon(struct, product);
    struct.put("supplierID", product.getSupplierID());
    struct.put("deliverableID", product.getDeliverableID());
    struct.put("stock", product.getStock());
    struct.put("productTypes", packProductTypes(product.getProductTypes()));
    struct.put("simpleOffer", product.getSimpleOffer());
    struct.put("workflowID", product.getWorkflowID());
    struct.put("deliverableQuantity", product.getDeliverableQuantity());
    struct.put("deliverableValidity", ProductValidity.pack(product.getDeliverableValidity()));
    return struct;
  }
  
  /****************************************
  *
  *  packProductTypes
  *
  ****************************************/

  private static List<Object> packProductTypes(Set<ProductTypeInstance> productTypes)
  {
    List<Object> result = new ArrayList<Object>();
    for (ProductTypeInstance productType : productTypes)
      {
        result.add(ProductTypeInstance.pack(productType));
      }
    return result;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static Product unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String supplierID = valueStruct.getString("supplierID");
    String deliverableID = valueStruct.getString("deliverableID");
    Integer stock = valueStruct.getInt32("stock");
    Set<ProductTypeInstance> productTypes = unpackProductTypes(schema.field("productTypes").schema(), valueStruct.get("productTypes"));
    boolean simpleOffer = (schemaVersion >= 2) ? valueStruct.getBoolean("simpleOffer") : false;
    String workflowID = (schema.field("workflowID") != null) ? valueStruct.getString("workflowID") : null;
    int deliverableQuantity = (schema.field("deliverableQuantity")!= null) ? valueStruct.getInt32("deliverableQuantity") : 0;
    ProductValidity deliverableValidity = (schema.field("deliverableValidity")!= null) ? ProductValidity.unpack(new SchemaAndValue(schema.field("deliverableValidity").schema(), valueStruct.get("deliverableValidity"))) : null;
    
    //
    //  return
    //

    return new Product(schemaAndValue, supplierID, deliverableID, stock, productTypes, simpleOffer, workflowID, deliverableQuantity, deliverableValidity);
  }
  
  /*****************************************
  *
  *  unpackProductTypes
  *
  *****************************************/

  private static Set<ProductTypeInstance> unpackProductTypes(Schema schema, Object value)
  {
    //
    //  get schema for ProductType
    //

    Schema productTypeSchema = schema.valueSchema();

    //
    //  unpack
    //

    Set<ProductTypeInstance> result = new HashSet<ProductTypeInstance>();
    List<Object> valueArray = (List<Object>) value;
    for (Object productType : valueArray)
      {
        result.add(ProductTypeInstance.unpack(new SchemaAndValue(productTypeSchema, productType)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public Product(JSONObject jsonRoot, long epoch, GUIManagedObject existingProductUnchecked, DeliverableService deliverableService, CatalogCharacteristicService catalogCharacteristicService, int tenantID) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingProductUnchecked != null) ? existingProductUnchecked.getEpoch() : epoch, tenantID);

    /*****************************************
    *
    *  existingProduct
    *
    *****************************************/

    Product existingProduct = (existingProductUnchecked != null && existingProductUnchecked instanceof Product) ? (Product) existingProductUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.supplierID = JSONUtilities.decodeString(jsonRoot, "supplierID", true);
    this.stock = JSONUtilities.decodeInteger(jsonRoot, "stock", false);
    this.productTypes = decodeProductTypes(JSONUtilities.decodeJSONArray(jsonRoot, "productTypes", true), catalogCharacteristicService);
    this.stockableItemID = "product-" + getProductID();
    this.simpleOffer = JSONUtilities.decodeBoolean(jsonRoot, "simpleOffer", Boolean.FALSE);
    this.workflowID = JSONUtilities.decodeString(jsonRoot, "workflowId", false);

    //
    //  deliverable
    //

    this.deliverableID = JSONUtilities.decodeString(jsonRoot, "deliverableID", false);
    if (deliverableID == null)
      {
        String deliverableName = JSONUtilities.decodeString(jsonRoot, "deliverableName", true);
        GUIManagedObject deliverableUnchecked = deliverableService.getStoredDeliverableByName(deliverableName, tenantID);
        Deliverable deliverable = (deliverableUnchecked != null && deliverableUnchecked.getAccepted()) ? (Deliverable) deliverableUnchecked : null;
        if (deliverable == null) JSONUtilities.decodeString(jsonRoot, "deliverableID", true);
        this.deliverableID = deliverable.getDeliverableID();
      }
    this.deliverableQuantity = JSONUtilities.decodeInteger(jsonRoot, "deliverableQuantity", 1);
    JSONObject jsonValue = JSONUtilities.decodeJSONObject(jsonRoot, "deliverableValidity", false);
    this.deliverableValidity = (jsonValue != null) ? new ProductValidity(JSONUtilities.decodeJSONObject(jsonRoot, "deliverableValidity", false)) : null;

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingProduct))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(Product existingProduct)
  {
    if (existingProduct != null && existingProduct.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingProduct.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(supplierID, existingProduct.getSupplierID());
        epochChanged = epochChanged || ! Objects.equals(deliverableID, existingProduct.getDeliverableID());
        epochChanged = epochChanged || ! Objects.equals(stock, existingProduct.getStock());
        epochChanged = epochChanged || ! Objects.equals(productTypes, existingProduct.getProductTypes());
        epochChanged = epochChanged || ! Objects.equals(simpleOffer, existingProduct.getSimpleOffer());
        epochChanged = epochChanged || ! Objects.equals(workflowID, existingProduct.getWorkflowID());
        epochChanged = epochChanged || ! Objects.equals(deliverableQuantity, existingProduct.getDeliverableQuantity());
        epochChanged = epochChanged || ! Objects.equals(deliverableValidity, existingProduct.getDeliverableValidity());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
  
  /*****************************************
  *
  *  decodeProductTypes
  *
  *****************************************/

  private Set<ProductTypeInstance> decodeProductTypes(JSONArray jsonArray, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
  {
    Set<ProductTypeInstance> result = new HashSet<ProductTypeInstance>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new ProductTypeInstance((JSONObject) jsonArray.get(i), catalogCharacteristicService));
          }
      }
    return result;
  }
  
  /*****************************************
  *
  *  validate
  *
  *****************************************/

  public void validate(SupplierService supplierService, ProductTypeService productTypeService, DeliverableService deliverableService, Date date) throws GUIManagerException
  {
    /*****************************************
    *
    *  validate supplier exists and is active
    *
    *****************************************/

    if (supplierService.getActiveSupplier(supplierID, date) == null) throw new GUIManagerException("unknown supplier", supplierID);
    
    /*****************************************
    *
    *  validate all product types exists and are active
    *
    *****************************************/

    for (ProductTypeInstance productTypeInstance : productTypes)
      {
        if (productTypeService.getActiveProductType(productTypeInstance.getProductTypeID(), date) == null) throw new GUIManagerException("unknown product type", productTypeInstance.getProductTypeID());
      }

    /****************************************
    *
    *  ensure active deliverable
    *
    ****************************************/

    //
    //  retrieve deliverable
    //
    
    GUIManagedObject deliverable = deliverableService.getStoredDeliverable(deliverableID);

    //
    //  validate the deliverable exists
    //

    if (deliverable == null) throw new GUIManagerException("unknown deliverable", deliverableID);
    
    //
    //  validate the deliverable start/end dates include the entire product active period
    //

    if (! deliverableService.isActiveDeliverableThroughInterval(deliverable, this.getEffectiveStartDate(), this.getEffectiveEndDate())) throw new GUIManagerException("invalid deliverable (start/end dates)", deliverableID);
  }
  
  @Override public Map<String, List<String>> getGUIDependencies(int tenantID)
  {
    Map<String, List<String>> result = new HashMap<String, List<String>>();
    List<String> supplierIDs = new ArrayList<String>();
    List<String> pointIDs = new ArrayList<String>();
    List<String> campaignIDs = new ArrayList<String>();
    List<String> productTypeIDs = new ArrayList<String>();
    supplierIDs.add(getSupplierID());
    result.put("supplier", supplierIDs);
    String pointID=getDeliverableID().startsWith(CommodityDeliveryManager.POINT_PREFIX)?getDeliverableID().replace(CommodityDeliveryManager.POINT_PREFIX, ""):"";
    pointIDs.add(pointID);
    String campaignID=getDeliverableID().startsWith(CommodityDeliveryManager.JOURNEY_PREFIX)?getDeliverableID().replace(CommodityDeliveryManager.JOURNEY_PREFIX, ""):"";
    campaignIDs.add(campaignID);
    for(ProductTypeInstance prdIn:getProductTypes()) {
    	productTypeIDs.add(prdIn.getProductTypeID());	
    }
    result.put("point", pointIDs);
    result.put("campaign", campaignIDs);
    result.put("producttype", productTypeIDs);
    return result;
  }
}
