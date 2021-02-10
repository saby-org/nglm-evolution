/****************************************************************************
*
*  RecordAlternateIDRedisSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import java.nio.charset.StandardCharsets;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

public class RecordAlternateIDRedisSinkConnector extends com.evolving.nglm.core.SimpleRedisSinkConnector
{
  /*****************************************
  *
  *  config
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(RecordAlternateIDRedisSinkConnector.class);

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return RecordAlternateIDRedisSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class RecordAlternateIDRedisSinkTask extends SimpleRedisSinkTask
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
      *  extract RecordAlternateID
      *
      ****************************************/

      Object recordAlternateIDValue = sinkRecord.value();
      Schema recordAlternateIDValueSchema = sinkRecord.valueSchema();
      RecordAlternateID recordAlternateID = RecordAlternateID.unpack(new SchemaAndValue(recordAlternateIDValueSchema, recordAlternateIDValue));
          
      log.info("Handle RecordAlternateID " +  recordAlternateID);
      
      /****************************************
      *
      *  process all alternateIDs - one cacheEntry for each alternate ID:
      *  -- key is alternateID
      *  -- value is subscriberID
      *  -- dbIndex is specified in configuration of alternateID 
      *
      ****************************************/

      AlternateID alternateID = Deployment.getAlternateIDs().get(recordAlternateID.getIDField());
      List<CacheEntry> cacheEntries = new ArrayList<CacheEntry>();
      if (alternateID != null)
        {
          //
          //  package list of subscriberIDs into byte array
          //
          log.info("Size of recoredAlternateID " + recordAlternateID.getAllSubscriberIDs().size());
          byte[] subscriberIDBytes = new byte[2 + 8*recordAlternateID.getAllSubscriberIDs().size()];
          System.arraycopy(Shorts.toByteArray((short) recordAlternateID.getAllSubscriberIDs().size()), 0, subscriberIDBytes, 0, 2);
          int numberOfSubscriberIDs = 0;
          for (String subscriberID : recordAlternateID.getAllSubscriberIDs())
            {
              try{
                System.arraycopy(Longs.toByteArray(Long.parseLong(subscriberID)), 0, subscriberIDBytes, 2+numberOfSubscriberIDs*8, 8);
                numberOfSubscriberIDs += 1;
              }catch (NumberFormatException ex){
                log.error("ignoring not numerical subscriberID: {}",subscriberID);
                return Collections.<CacheEntry>emptyList();
              }
            }
          
          //
          //  package alternateID into byte array
          //

          byte[] alternateIDBytes = (recordAlternateID.getAlternateID() != null) ? recordAlternateID.getAlternateID().getBytes(StandardCharsets.UTF_8) : null;
          
          //
          //  mapping: alternateID -> subscriberID
          //
          
          int dbIndex = alternateID.getRedisCacheIndex();
          Integer ttlOnDelete = alternateID.getRedisCacheTTLOnDelete();
          if (alternateIDBytes != null) cacheEntries.add(new CacheEntry(alternateIDBytes, ((numberOfSubscriberIDs > 0) ? subscriberIDBytes : null), dbIndex, ((recordAlternateID.getSubscriberAction() != SubscriberStreamEvent.SubscriberAction.Delete && !alternateID.getSharedID()) ? ttlOnDelete : null), recordAlternateID.getTenantID()));
        }
      else
        {
          log.error("ignoring unspecified alternateID: {}", recordAlternateID.getIDField());
          cacheEntries = Collections.<CacheEntry>emptyList();
        }

      /****************************************
      *
      *  return
      *
      ****************************************/

      return cacheEntries;
    }
  }
}
