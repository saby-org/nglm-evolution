package com.evolving.nglm.evolution;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import org.apache.kafka.connect.data.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

public abstract class OfferContentType extends GUIManagedObject {

  private static Schema commonSchema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("offer_content_type");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(GUIManagedObject.commonSchema().version(),1));
    for (Field field : GUIManagedObject.commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("catalogCharacteristics", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().schema());
    commonSchema = schemaBuilder.build();
  }

  public static Schema commonSchema() { return commonSchema; }


  private List<String> catalogCharacteristics;

  public String getID() { return getGUIManagedObjectID(); }
  public String getName() { return getGUIManagedObjectName(); }
  public String getDisplay() { return getGUIManagedObjectDisplay(); }
  public List<String> getCatalogCharacteristics() { return catalogCharacteristics; }


  public static Object packCommon(Struct struct, OfferContentType offerContentType) {
    GUIManagedObject.packCommon(struct, offerContentType);
    if(offerContentType.getCatalogCharacteristics()!=null) struct.put("catalogCharacteristics", offerContentType.getCatalogCharacteristics());
    return struct;
  }


  public OfferContentType(SchemaAndValue schemaAndValue) {
    super(schemaAndValue);
    Object value = schemaAndValue.value();
    Struct valueStruct = (Struct) value;
    this.catalogCharacteristics = (List<String>) valueStruct.get("catalogCharacteristics");
  }

  public OfferContentType(JSONObject jsonRoot, long epoch, GUIManagedObject existingOfferContentTypeUnchecked, int tenantID) throws GUIManagerException {

    super(jsonRoot, (existingOfferContentTypeUnchecked != null) ? existingOfferContentTypeUnchecked.getEpoch() : epoch, tenantID);

    OfferContentType existingOfferContentType = (existingOfferContentTypeUnchecked != null && existingOfferContentTypeUnchecked instanceof OfferContentType) ? (OfferContentType) existingOfferContentTypeUnchecked : null;
    
    this.catalogCharacteristics = decodeCatalogCharacteristics(JSONUtilities.decodeJSONArray(jsonRoot, "catalogCharacteristics", false));

    if (getRawEffectiveStartDate() != null) throw new GUIManagerException("unsupported start date", JSONUtilities.decodeString(jsonRoot, "effectiveStartDate", false));
    if (getRawEffectiveEndDate() != null) throw new GUIManagerException("unsupported end date", JSONUtilities.decodeString(jsonRoot, "effectiveEndDate", false));

    if (epochChanged(existingOfferContentType)) {
      this.setEpoch(epoch);
    }
  }

  private List<String> decodeCatalogCharacteristics(JSONArray jsonArray) throws GUIManagerException {
    if(jsonArray==null) return null;
    List<String> catalogCharacteristics = new ArrayList<String>();
    for (int i=0; i<jsonArray.size(); i++) {
        JSONObject catalogCharacteristicJSON = (JSONObject) jsonArray.get(i);
        catalogCharacteristics.add(JSONUtilities.decodeString(catalogCharacteristicJSON, "catalogCharacteristicID", true));
    }
    return catalogCharacteristics;
  }

  private boolean epochChanged(OfferContentType existingOfferContentType) {
    if (existingOfferContentType != null && existingOfferContentType.getAccepted()) {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingOfferContentType.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(catalogCharacteristics, existingOfferContentType.getCatalogCharacteristics());
        return epochChanged;
    } else {
      return true;
    }
  }

  public void validate(CatalogCharacteristicService catalogCharacteristicService, Date date, int tenantID) throws GUIManagerException {
    if(this.catalogCharacteristics==null) return;
    for (String catalogCharacteristicID : this.catalogCharacteristics) {
      CatalogCharacteristic catalogCharacteristic = catalogCharacteristicService.getActiveCatalogCharacteristic(catalogCharacteristicID, date, tenantID);
      if (catalogCharacteristic == null) throw new GUIManagerException("unknown catalog characteristic", catalogCharacteristicID);
    }
  }

  @Override
  public String toString() {
    return getJSONRepresentation().toJSONString();
  }

}
