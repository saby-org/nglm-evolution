package com.evolving.nglm.evolution;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import com.evolving.nglm.core.ChangeLogESSinkTask;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SimpleESSinkConnector;

public class PartnersESSinkConnector extends SimpleESSinkConnector
{
  public enum PartnerType { Reseller, Supplier } 

  @Override
  public Class<? extends Task> taskClass()
  {
    return PartnersESSinkTask.class;
  }
  
  public static class PartnersESSinkTask extends ChangeLogESSinkTask<GUIManagedObject>
  {

    @Override
    public GUIManagedObject unpackRecord(SinkRecord sinkRecord)
    {
      GUIManagedObject partner = null;
      Object guiManagedObjectValue = sinkRecord.value();
      Schema guiManagedObjectValueSchema = sinkRecord.valueSchema();
      GUIManagedObject guiManagedObject = GUIManagedObject.commonSerde().unpack(new SchemaAndValue(guiManagedObjectValueSchema, guiManagedObjectValue));
      if (guiManagedObject instanceof Supplier || guiManagedObject instanceof Reseller) partner = guiManagedObject;
      return partner;
    }

    @Override
    public String getDocumentID(GUIManagedObject item)
    {
      if (item instanceof Supplier)
        {
          return "_".concat(PartnerType.Supplier.name()).concat("_").concat(item.getGUIManagedObjectID());
        }
      else if (item instanceof Reseller)
        {
          return "_".concat(PartnerType.Reseller.name()).concat("_").concat(item.getGUIManagedObjectID());
        }
      else
        {
          throw new ServerRuntimeException("invalid partner " + item.getClass().getName());
        }
    }

    @Override
    public Map<String, Object> getDocumentMap(GUIManagedObject item)
    {
      Map<String,Object> documentMap = new HashMap<String,Object>();
      return documentMap;
    }
    
  }

}
