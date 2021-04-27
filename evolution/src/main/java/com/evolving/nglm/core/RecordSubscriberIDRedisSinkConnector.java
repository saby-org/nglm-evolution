/****************************************************************************
*
*  RecordSubscriberIDRedisSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.core;

import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Longs;

import java.nio.charset.StandardCharsets;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

public class RecordSubscriberIDRedisSinkConnector extends SimpleRedisSinkConnector
{
  /*****************************************
  *
  *  config
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(RecordSubscriberIDRedisSinkConnector.class);

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return RecordSubscriberIDRedisSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class RecordSubscriberIDRedisSinkTask extends SimpleRedisSinkTask
  {
    /****************************************
    *
    *  getCacheEntries
    *
    ****************************************/

    @Override public List<CacheEntry> getCacheEntries(SinkRecord sinkRecord)
    {
      /****************************************
      *
      *  extract RecordSubscriberID
      *
      ****************************************/

      Object recordSubscriberIDValue = sinkRecord.value();
      Schema recordSubscriberIDValueSchema = sinkRecord.valueSchema();
      RecordSubscriberID recordSubscriberID = RecordSubscriberID.unpack(new SchemaAndValue(recordSubscriberIDValueSchema, recordSubscriberIDValue));
          
      log.info("RecordSubscriberIDRedisSinkTask recordSubscriberID " + recordSubscriberID);
      
      /****************************************
      *
      *  process all subscriberIDs - one cacheEntry for each subscriber ID:
      *  -- key is subscriberID
      *  -- value is alternateID
      *  -- reverseDBIndex is optionally specified in configuration of alternateID
      *
      ****************************************/

      AlternateID alternateID = Deployment.getAlternateIDs().get(recordSubscriberID.getIDField());
      List<CacheEntry> cacheEntries = new ArrayList<CacheEntry>();
      if (alternateID != null && alternateID.getReverseRedisCacheIndex() != null)
        {
          //
          //  package alternateID and internalSubscriberID into byte arrays
          //
          try{
            byte[] subscriberIDBytes = Longs.toByteArray(Long.parseLong(recordSubscriberID.getSubscriberID()));
            byte[] alternateIDBytes = (recordSubscriberID.getAlternateID() != null) ? recordSubscriberID.getAlternateID().getBytes(StandardCharsets.UTF_8) : null;
          
            log.info("RecordSubscriberIDRedisSinkTask subscriberIDBytes " + Hex.encodeHexString( subscriberIDBytes ) );
            log.info("RecordSubscriberIDRedisSinkTask alternateIDBytes " + (alternateIDBytes != null ? Hex.encodeHexString( alternateIDBytes ) : null) );
      
            Integer reverseDBIndex = alternateID.getReverseRedisCacheIndex();
            cacheEntries.add(new CacheEntry(subscriberIDBytes, alternateIDBytes, reverseDBIndex, null, recordSubscriberID.getTenantID()));
          }catch (NumberFormatException ex){
            log.error("ignoring not numerical subscriberID: {}",recordSubscriberID.getSubscriberID());
            return Collections.<CacheEntry>emptyList();
          }
        }
      else if (alternateID != null)
        {
          cacheEntries = Collections.<CacheEntry>emptyList();
        }
      else
        {
          log.error("ignoring unspecified alternateID: {}", recordSubscriberID.getIDField());
          cacheEntries = Collections.<CacheEntry>emptyList();
        }

      /****************************************
      *
      *  return
      *
      ****************************************/
      
      if(cacheEntries != null)
        {
          for(CacheEntry cacheEntry : cacheEntries)
            {
              log.info("RecordSubscriberIDRedisSinkTask cachEntry : " + cacheEntry );
            }
        }
      
      return cacheEntries;
    }
  }
}
