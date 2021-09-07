package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

// at this time, ProductType is actually nothing else than OfferContentType but ready to diverge
@GUIDependencyDef(objectType = "productType", serviceClass = ProductTypeService.class, dependencies = {"catalogcharacteristic"})
public class ProductType extends OfferContentType {

  private static Schema schema = null;
  static {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("producttype");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(OfferContentType.commonSchema().version(),2));
    for (Field field : OfferContentType.commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schema = schemaBuilder.build();
  }

  private static ConnectSerde<ProductType> serde = new ConnectSerde<ProductType>(schema, false, ProductType.class, ProductType::pack, ProductType::unpack);

  public static Schema schema() { return schema; }
  public static ConnectSerde<ProductType> serde() { return serde; }
  private List<String> catalogCharacteristics;
  public List<String> getCatalogCharacteristics() { return catalogCharacteristics; }
  
  public static Object pack(Object value) {
    ProductType productType = (ProductType) value;
    Struct struct = new Struct(schema);
    OfferContentType.packCommon(struct, productType);
    struct.put("catalogCharacteristics", productType.getCatalogCharacteristics());
    return struct;
  }

  public static ProductType unpack(SchemaAndValue schemaAndValue) {
	  Struct valueStruct = (Struct) schemaAndValue.value();
	    List<String> catalogCharacteristics = (List<String>) valueStruct.get("catalogCharacteristics");
	   
    return new ProductType(schemaAndValue,catalogCharacteristics);
  }


  public ProductType(SchemaAndValue schemaAndValue,List<String> catalogCharacteristics) {
    super(schemaAndValue);
    this.catalogCharacteristics = catalogCharacteristics;
  }

  public ProductType(JSONObject jsonRoot, long epoch, GUIManagedObject existingProductTypeUnchecked, int tenantID) throws GUIManagerException {
    super(jsonRoot, epoch, existingProductTypeUnchecked, tenantID);
    this.catalogCharacteristics = decodeCatalogCharacteristics(JSONUtilities.decodeJSONArray(jsonRoot, "catalogCharacteristics", true));

  }
  private List<String> decodeCatalogCharacteristics(JSONArray jsonArray) throws GUIManagerException
  {
    List<String> catalogCharacteristics = new ArrayList<String>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject catalogCharacteristicJSON = (JSONObject) jsonArray.get(i);
        catalogCharacteristics.add(JSONUtilities.decodeString(catalogCharacteristicJSON, "catalogCharacteristicID", true));
      }
    return catalogCharacteristics;
  }
  
  @Override public Map<String, List<String>> getGUIDependencies(List<GUIService> guiServiceList, int tenantID)
  {
    Map<String, List<String>> result = new HashMap<String, List<String>>();
    result.put("catalogcharacteristic".toLowerCase(), getCatalogCharacteristics());
    return result;
  }

}
