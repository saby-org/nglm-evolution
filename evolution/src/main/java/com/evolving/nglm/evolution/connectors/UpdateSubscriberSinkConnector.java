package com.evolving.nglm.evolution.connectors;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.evolution.SingletonServices;
import com.evolving.nglm.evolution.event.MapperUtils;
import com.evolving.nglm.evolution.event.SubscriberUpdated;
import com.evolving.nglm.evolution.redis.UpdateAlternateIDSubscriberIDs;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class UpdateSubscriberSinkConnector extends SinkConnector {

  protected static final Logger log = LoggerFactory.getLogger(UpdateSubscriberSinkConnector.class);

  private static final AtomicBoolean stopRequested = new AtomicBoolean(false);

  private String connectorName = null;

  @Override public String version() {return "0.1";}
  @Override public Class<? extends Task> taskClass() {return UpdateSubscriberSinkTask.class;}
  @Override public ConfigDef config() {return new ConfigDef();}

  @Override public void start(Map<String, String> properties) {
    connectorName = properties.get("name");
    log.info(connectorName+" START");
  }
  @Override public void stop() {
    stopRequested.set(true);
    log.info(connectorName+" stop()");
  }

  @Override public List<Map<String, String>> taskConfigs(int maxTasks) {
    log.info(connectorName+" maxTasks "+maxTasks);
    List<Map<String, String>> result = new ArrayList<Map<String,String>>();
    for (int i = 0; i < maxTasks; i++)
      {
        Map<String, String> taskConfig = new HashMap<>();
        taskConfig.put("connectorName", connectorName);
        taskConfig.put("taskNumber", Integer.toString(i));
        result.add(taskConfig);
      }
    return result;
  }

  public static class UpdateSubscriberSinkTask extends SinkTask {

    protected static final Logger log = LoggerFactory.getLogger(UpdateSubscriberSinkTask.class);

    private String taskName = null;

    @Override public String version() {return "0.1";}

    @Override public void start(Map<String, String> taskConfig) {
      Integer taskNumber = Integer.parseInt(taskConfig.get("taskNumber"));
      this.taskName=taskConfig.get("connectorName")+"-"+taskNumber;
      log.info(taskName+" start");
    }
    @Override public void stop() {
      log.info(taskName+" stop");
      stopRequested.set(true);
    }

    @Override public void put(Collection<SinkRecord> sinkRecords) {

      if(log.isDebugEnabled()) log.debug(taskName+" put "+sinkRecords.size());

      // prepare the delete alternateID request
      List<UpdateAlternateIDSubscriberIDs> alternateIDsToUpdate = new ArrayList<>();
      for (SinkRecord sinkRecord : sinkRecords) {
        SubscriberUpdated subscriberUpdated = SubscriberUpdated.unpack(new SchemaAndValue(sinkRecord.valueSchema(), sinkRecord.value()));
        // need to remove all alternateIDs
        if(subscriberUpdated.getSubscriberDeleted()){
          for(Map.Entry<AlternateID,String> entry:SubscriberStreamOutput.unpackAlternateIDs(subscriberUpdated.getAlternateIDs()).entrySet()){
            alternateIDsToUpdate.add(new UpdateAlternateIDSubscriberIDs(entry.getKey(),entry.getValue(),subscriberUpdated.getTenantID()).setSubscriberIDToRemove(Long.valueOf(subscriberUpdated.getSubscriberID())));
          }
        }
        else if (!subscriberUpdated.getAlternateIDsToRemove().isEmpty() || !subscriberUpdated.getAlternateIDsToAdd().isEmpty()){
          // need to remove some alternateIDs
          for(Map.Entry<AlternateID,String> entry:subscriberUpdated.getAlternateIDsToRemove().entrySet()){
            alternateIDsToUpdate.add(new UpdateAlternateIDSubscriberIDs(entry.getKey(),entry.getValue(),subscriberUpdated.getTenantID()).setSubscriberIDToRemove(Long.valueOf(subscriberUpdated.getSubscriberID())));
          }
          // need to add some alternateIDs
          for(Map.Entry<AlternateID,String> entry:subscriberUpdated.getAlternateIDsToAdd().entrySet()){
            alternateIDsToUpdate.add(new UpdateAlternateIDSubscriberIDs(entry.getKey(),entry.getValue(),subscriberUpdated.getTenantID()).setSubscriberIDToAdd(Long.valueOf(subscriberUpdated.getSubscriberID())));
          }
        }
      }
      MapperUtils.updateAlternateIDs(alternateIDsToUpdate, SingletonServices.getSubscriberIDService(),stopRequested);

    }

  }
}
