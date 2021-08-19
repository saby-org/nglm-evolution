package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import com.evolving.nglm.core.ChangeLogESSinkTask;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.SystemTime;

public class BadgeMappingsESSinkConnector extends SimpleESSinkConnector
{

  @Override public Class<? extends Task> taskClass()
  {
    return BadgeMappingsESSinkTask.class;
  }

  public static class BadgeMappingsESSinkTask extends ChangeLogESSinkTask<GUIManagedObject>
  {
    @Override public GUIManagedObject unpackRecord(SinkRecord sinkRecord)
    {
      GUIManagedObject badge = null;
      Object guiManagedObjectValue = sinkRecord.value();
      Schema guiManagedObjectValueSchema = sinkRecord.valueSchema();
      GUIManagedObject guiManagedObject = GUIManagedObject.commonSerde().unpack(new SchemaAndValue(guiManagedObjectValueSchema, guiManagedObjectValue));
      if (guiManagedObject instanceof Badge) badge = guiManagedObject;
      return badge;
    }

    @Override public String getDocumentID(GUIManagedObject item)
    {
      return "_" + "badge-".concat(item.getGUIManagedObjectID()).hashCode();
    }

    @Override public Map<String, Object> getDocumentMap(GUIManagedObject item)
    {
      Map<String, Object> documentMap = new HashMap<String, Object>();
      Date now = SystemTime.getCurrentTime();
      if (item instanceof Badge)
        {
          Badge badge = (Badge) item;
          documentMap.put("id", badge.getGUIManagedObjectID());
          documentMap.put("display", badge.getGUIManagedObjectDisplay());
          documentMap.put("active", badge.getActive());
          documentMap.put("badgeType", badge.getBadgeType().getExternalRepresentation());
          documentMap.put("createdDate", RLMDateUtils.formatDateForElasticsearchDefault(badge.getCreatedDate()));
          documentMap.put("timestamp", RLMDateUtils.formatDateForElasticsearchDefault(now));
        }
      return documentMap;
    }

  }

}
