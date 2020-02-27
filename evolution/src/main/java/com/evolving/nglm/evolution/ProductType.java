package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.json.simple.JSONObject;

// at this time, ProductType is actually nothing else than OfferContentType but ready to diverge
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


  public static Object pack(Object value) {
    ProductType productType = (ProductType) value;
    Struct struct = new Struct(schema);
    OfferContentType.packCommon(struct, productType);
    return struct;
  }

  public static ProductType unpack(SchemaAndValue schemaAndValue) {
    return new ProductType(schemaAndValue);
  }


  public ProductType(SchemaAndValue schemaAndValue) {
    super(schemaAndValue);
  }

  public ProductType(JSONObject jsonRoot, long epoch, GUIManagedObject existingProductTypeUnchecked) throws GUIManagerException {
    super(jsonRoot, epoch, existingProductTypeUnchecked);
  }

}
