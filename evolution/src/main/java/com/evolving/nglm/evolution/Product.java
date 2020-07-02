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

@GUIDependencyDef(objectType = "product", serviceClass = ProductService.class, dependencies = { "supplier" , "point" })
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("supplierID", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliverableID", Schema.STRING_SCHEMA);
    schemaBuilder.field("stock", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("productTypes", SchemaBuilder.array(ProductTypeInstance.schema()).schema());
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

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Product(SchemaAndValue schemaAndValue, String supplierID, String deliverableID, Integer stock, Set<ProductTypeInstance> productTypes)
  {
    super(schemaAndValue);
    this.supplierID = supplierID;
    this.deliverableID = deliverableID;
    this.stock = stock;
    this.productTypes = productTypes;
    this.stockableItemID = "product-" + getProductID();
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
    
    //
    //  return
    //

    return new Product(schemaAndValue, supplierID, deliverableID, stock, productTypes);
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

  public Product(JSONObject jsonRoot, long epoch, GUIManagedObject existingProductUnchecked, DeliverableService deliverableService, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingProductUnchecked != null) ? existingProductUnchecked.getEpoch() : epoch);

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

    //
    //  deliverable
    //

    this.deliverableID = JSONUtilities.decodeString(jsonRoot, "deliverableID", false);
    if (deliverableID == null)
      {
        String deliverableName = JSONUtilities.decodeString(jsonRoot, "deliverableName", true);
        GUIManagedObject deliverableUnchecked = deliverableService.getStoredDeliverableByName(deliverableName);
        Deliverable deliverable = (deliverableUnchecked != null && deliverableUnchecked.getAccepted()) ? (Deliverable) deliverableUnchecked : null;
        if (deliverable == null) JSONUtilities.decodeString(jsonRoot, "deliverableID", true);
        this.deliverableID = deliverable.getDeliverableID();
      }

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
  
  @Override public Map<String, List<String>> getGUIDependencies()
  {
    Map<String, List<String>> result = new HashMap<String, List<String>>();
    List<String> supplierIDs = new ArrayList<String>();
    List<String> pointIDs = new ArrayList<String>();
    supplierIDs.add(getSupplierID());
    result.put("supplier", supplierIDs);
    String pointID=getDeliverableID().startsWith(CommodityDeliveryManager.POINT_PREFIX)?getDeliverableID().replace(CommodityDeliveryManager.POINT_PREFIX, ""):"";
    pointIDs.add(pointID);
    result.put("point", pointIDs);
    return result;
  }
}
