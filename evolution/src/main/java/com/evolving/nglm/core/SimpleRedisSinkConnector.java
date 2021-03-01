/****************************************************************************
*
*  SimpleRedisSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.core;

import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisException;

public abstract class SimpleRedisSinkConnector extends SinkConnector
{
  /*****************************************
  *
  *  config
  *
  *****************************************/

  //
  //  defaults
  //

  private final static String DEFAULT_DB_INDEX = "0";
  private final static String DEFAULT_PIPELINED = "false";
  
  //
  //  logger
  //

  protected static final Logger log = LoggerFactory.getLogger(SimpleRedisSinkConnector.class);
  
  //
  //  configuration
  //

  private String connectorName = null;
  private String redisSentinels;
  private String redisInstance;
  private String topic;
  private String defaultDBIndex;
  private String pipelined;

  //
  //  version
  //

  final static String SimpleRedisSinkVersion = "0.1";

  /****************************************
  *
  *  version
  *
  ****************************************/

  @Override public String version()
  {
    return SimpleRedisSinkVersion;
  }

  /****************************************
  *
  *  start
  *
  ****************************************/

  @Override public void start(Map<String, String> properties)
  {
    /*****************************************
    *
    *  configuration -- connectorName
    *
    *****************************************/

    connectorName = properties.get("name");

    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("{} -- Connector.start() START", connectorName);

    /*****************************************
    *
    *  configuration -- redisSentinels
    *
    *****************************************/

    redisSentinels = properties.get("redisSentinels");
    if (redisSentinels == null || redisSentinels.trim().length() == 0) throw new ConnectException("SimpleRedisSinkConnector configuration must specify 'redisSentinels'");

    /*****************************************
    *
    *  configuration -- redisInstance
    *
    *****************************************/

    redisInstance = properties.get("redisInstance");
    if (redisInstance == null || redisInstance.trim().length() == 0) throw new ConnectException("SimpleRedisSinkConnector configuration must specify 'redisInstance'");

    /*****************************************
    *
    *  configuration -- topic
    *
    *****************************************/

    topic = properties.get("topics");
    if (topic == null || topic.trim().length() == 0) throw new ConnectException("SimpleRedisSinkConnector configuration must specify 'topics'");

    /*****************************************
    *
    *  configuration -- defaultDBIndex
    *
    *****************************************/

    defaultDBIndex = properties.get("defaultDBIndex");
    if (defaultDBIndex != null && defaultDBIndex.trim().length() > 0)
      {
        Integer defaultDBIndexInteger = null;
        try
          {
            defaultDBIndex = defaultDBIndex.trim();
            defaultDBIndexInteger = Integer.parseInt(defaultDBIndex);
          }
        catch (NumberFormatException e)
          {
            throw new ConnectException("SimpleRedisSinkConnector improperly formatted defaultDBIndex: " + defaultDBIndex);
          }
        if (defaultDBIndexInteger < 0)
          {
            throw new ConnectException("SimpleRedisSinkConnector improperly formatted defaultDBIndex: " + defaultDBIndex);
          }
      }
    else
      {
        defaultDBIndex = DEFAULT_DB_INDEX;
      }

    /*****************************************
    *
    *  configuration -- pipelined
    *
    *****************************************/

    pipelined = properties.get("pipelined");
    if (pipelined != null && pipelined.trim().length() > 0)
      {
        pipelined = pipelined.trim();
        if (pipelined.equalsIgnoreCase("t") || pipelined.equalsIgnoreCase("true") || pipelined.equalsIgnoreCase("y") || pipelined.equalsIgnoreCase("yes"))
          pipelined = "true";
        else if (pipelined.equalsIgnoreCase("f") || pipelined.equalsIgnoreCase("false") || pipelined.equalsIgnoreCase("n") || pipelined.equalsIgnoreCase("no"))
          pipelined = "false";
        else
          throw new ConnectException("SimpleRedisSinkConnector improperly formatted pipelined: " + pipelined);
      }
    else
      {
        pipelined = DEFAULT_PIPELINED;
      }
    
    /*****************************************
    *
    *  log
    *
    *****************************************/
    
    log.info("{} -- Connector.start() END", connectorName);
  }

  /****************************************
  *
  *  abstract
  *
  ****************************************/
  
  @Override public abstract Class<? extends Task> taskClass();

  /****************************************
  *
  *  taskConfigs
  *
  ****************************************/

  @Override public List<Map<String, String>> taskConfigs(int maxTasks)
  {
    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("taskConfigs() START");
    log.info("taskConfigs() {} maxTasks", maxTasks);

    /*****************************************
    *
    *  create N task configs
    *
    *****************************************/
    
    List<Map<String, String>> result = new ArrayList<Map<String,String>>();
    for (int i = 0; i < maxTasks; i++)
      {
        Map<String, String> taskConfig = new HashMap<>();
        taskConfig.put("connectorName", connectorName);
        taskConfig.put("redisSentinels", redisSentinels);
        taskConfig.put("redisInstance", redisInstance);
        taskConfig.put("topic", topic);
        taskConfig.put("defaultDBIndex", defaultDBIndex);
        taskConfig.put("pipelined", pipelined);
        taskConfig.put("taskNumber", Integer.toString(i));
        result.add(taskConfig);
      }

    /*****************************************
    *
    *  return
    *
    *****************************************/

    //
    //  log
    //

    log.info("taskConfigs() END");

    //
    //  return
    //

    return result;
  }

  /****************************************
  *
  *  stop
  *
  ****************************************/

  @Override public void stop()
  {
    /****************************************
    *
    *  log
    *
    ****************************************/
    
    log.info("{} -- Connector.stop()", connectorName);
  }

  /****************************************
  *
  *  config
  *
  ****************************************/

  @Override public ConfigDef config()
  {
    ConfigDef result = new ConfigDef();
    result.define("redisSentinels", Type.STRING, Importance.HIGH, "redis sentinels");
    result.define("redisInstance", Type.STRING, Importance.HIGH, "redis instance");
    return result;
  }

  /****************************************************************************
  *
  *  class SimpleRedisSinkTask
  *
  ****************************************************************************/

  public abstract static class SimpleRedisSinkTask extends SinkTask
  {
    /*****************************************
    *
    *  config
    *
    *****************************************/

    //
    //  configuration
    //

    private String connectorName = null;
    private Set<String> redisSentinels;
    private String redisInstance;
    private String topic;
    private int defaultDBIndex;
    private boolean pipelined;
    private int taskNumber;

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public String getTopic() { return topic; }
    public int getTaskNumber() { return taskNumber; }

    /****************************************
    *
    *  attributes
    *
    ****************************************/

    private List<SinkRecord> sinkRecords = new ArrayList<SinkRecord>();
    private JedisSentinelPool jedisSentinelPool = null;

    /*****************************************
    *
    *  abstract
    *
    *****************************************/

    public abstract List<CacheEntry> getCacheEntries(SinkRecord sinkRecord);
    
    /*****************************************
    *
    *  version
    *
    *****************************************/

    @Override public String version()
    {
      return SimpleRedisSinkVersion;
    }
    
    /*****************************************
    *
    *  start
    *
    *****************************************/

    @Override public void start(Map<String, String> taskConfig)
    {
      /*****************************************
      *
      *  log
      *
      *****************************************/

      log.trace("{} -- Task.start()", connectorName);
      
      /*****************************************
      *
      *  config
      *
      *****************************************/

      connectorName = taskConfig.get("connectorName");
      redisSentinels = new HashSet<String>(Arrays.asList(taskConfig.get("redisSentinels").split("\\s*,\\s*")));
      redisInstance = taskConfig.get("redisInstance");
      topic = taskConfig.get("topic");
      taskNumber = parseIntegerConfig(taskConfig.get("taskNumber"));
      defaultDBIndex = parseIntegerConfig(taskConfig.get("defaultDBIndex"));
      pipelined = parseBooleanConfig(taskConfig.get("pipelined"));
      
      /*****************************************
      *
      *  initialize jedis client
      *
      *****************************************/

      JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
      jedisPoolConfig.setTestOnBorrow(true);
      jedisPoolConfig.setTestOnReturn(false);
      jedisPoolConfig.setMaxTotal(2);
      jedisPoolConfig.setJmxNamePrefix("SimpleRedisSinkConnector");
      jedisPoolConfig.setJmxNameBase(connectorName + "-" + Integer.toString(taskNumber));
      
      String password = System.getProperty("redis.password");
      if(password != null && !password.trim().equals("") && !password.trim().equals("none")) {
        log.info("SimpleRedisSinkConnector() Use Redis Password " + password);
        jedisSentinelPool = new JedisSentinelPool(redisInstance, redisSentinels, jedisPoolConfig, Protocol.DEFAULT_TIMEOUT, Protocol.DEFAULT_TIMEOUT, null, password, 0, null,
            Protocol.DEFAULT_TIMEOUT, Protocol.DEFAULT_TIMEOUT, null, password, null);        
      }
      else {
        log.info("SimpleRedisSinkConnector() No Redis Password");
        jedisSentinelPool = new JedisSentinelPool(redisInstance, redisSentinels, jedisPoolConfig);
      }  
    }

    /*****************************************
    *
    *  put
    *
    *****************************************/

    @Override public void put(Collection<SinkRecord> sinkRecords)
    {
      BinaryJedis jedis = null;
      Pipeline pipeline = null;
      try
        {
          if (sinkRecords.isEmpty())
            log.trace("{} -- Task.put() 0 records", connectorName);
          else
            log.info("{} -- Task.put() {} records", connectorName, sinkRecords.size());
          
          /*****************************************
          *
          *  get jedis connection
          *
          *****************************************/

          jedis = jedisSentinelPool.getResource();
          pipeline = (pipelined) ? jedis.pipelined() : null;

          /****************************************
          *
          *  process all records
          *  - use pipline (i.e., batching) if specified
          *  - otherwise use record-by-record processing
          *
          ****************************************/

          for (SinkRecord sinkRecord : sinkRecords)
            {
              if (sinkRecord.value() != null)
                {
                  //
                  //  serialized record
                  //

                  List<CacheEntry> cacheEntries = getCacheEntries(sinkRecord);

                  //
                  //  send
                  //

                  for (CacheEntry cacheEntry : cacheEntries)
                    {
                      pipeline.select(cacheEntry.getDBIndex());
                      if (pipelined && cacheEntry.getValue() != null)
                        {
                          byte[] value = cacheEntry.getValue();
                          log.info("1 Cache Entry Value " + Hex.encodeHexString(value));
                          pipeline.set(cacheEntry.getKey(), value);
                        }
                      else if (pipelined && cacheEntry.getValue() == null && cacheEntry.getTTLOnDelete() != null)
                        {
                          pipeline.expireAt(cacheEntry.getKey(), expirationDate(cacheEntry.getTTLOnDelete(), cacheEntry.getTenantID()));
                        }
                      else if (pipelined && cacheEntry.getValue() == null && cacheEntry.getTTLOnDelete() == null)
                        {
                          pipeline.del(cacheEntry.getKey());
                        }
                      else if (cacheEntry.getValue() != null)
                        {
                          byte[] value = cacheEntry.getValue();
                          log.info("2 Cache Entry Value " + Hex.encodeHexString(value));
                          jedis.set(cacheEntry.getKey(), value);
                        }
                      else if (cacheEntry.getValue() == null && cacheEntry.getTTLOnDelete() != null)
                        {
                          jedis.expireAt(cacheEntry.getKey(), expirationDate(cacheEntry.getTTLOnDelete(), cacheEntry.getTenantID()));
                        }
                      else
                        {
                          jedis.del(cacheEntry.getKey());
                        }
                    }
                }
            }

          /*****************************************
          *
          *  sync pipeline (if necessary)
          *
          *****************************************/

          if (pipelined)
            {
              pipeline.sync();
            }

          /*****************************************
          *
          *  statistics
          *
          *****************************************/

          updatePutCount(connectorName, taskNumber, 1);
          updateRecordCount(connectorName, taskNumber, sinkRecords.size());
        }
      catch (JedisException e)
        {
          //
          //  log
          //

          log.error("JEDIS error: {}", connectorName);
          StringWriter stackTraceWriter = new StringWriter();
          e.printStackTrace(new PrintWriter(stackTraceWriter, true));
          log.error(stackTraceWriter.toString());

          //
          //  abort (and retry)
          //

          throw new RetriableException(e);
        }
      finally
        {
          //
          //  return
          //
          
          if (pipeline != null) try { pipeline.close(); } catch (JedisException e1) { }
          if (jedis != null) try { jedis.close(); } catch (JedisException e1) { }
        }
    }

    /*****************************************
    *
    *  expirationDate
    *
    *****************************************/

    private long expirationDate(int ttlOnDelete, int tenantID)
    {
      Date now = SystemTime.getCurrentTime();
      Date day = RLMDateUtils.truncate(now, Calendar.DATE, Calendar.SUNDAY, Deployment.getDeployment(tenantID).getTimeZone());
      Date expiration = RLMDateUtils.addDays(day, ttlOnDelete + 1, Deployment.getDeployment(tenantID).getTimeZone());
      return expiration.getTime() / 1000L;
    }

    /*****************************************
    *
    *  flush
    *
    *****************************************/

    @Override public void flush(Map<TopicPartition, OffsetAndMetadata> map)
    {
      log.trace("{} -- Task.flush()", connectorName);
    }
    
    /*****************************************
    *
    *  stop
    *
    *****************************************/

    @Override public void stop()
    {
      log.info("{} -- Task.stop()", connectorName);
      if (jedisSentinelPool != null) try { jedisSentinelPool.close(); } catch (JedisException e) { }
      stopStatisticsCollection();
    }
    
    /****************************************************************************
    *
    *  statistics
    *    - updateBatchCount
    *    - updatePutCount
    *    - updateRecordCount
    *    - stopStatisticsCollection
    *
    ****************************************************************************/

    /*****************************************
    *
    *  statistics data (transient)
    *
    *****************************************/

    private String statisticsKey(String connector, int taskNumber) { return connector + "-" + taskNumber; }
    private static Map<String,SinkTaskStatistics> allTaskStatistics = new HashMap<String, SinkTaskStatistics>();

    /*****************************************
    *
    *  getStatistics
    *
    *****************************************/

    private SinkTaskStatistics getStatistics(String connectorName, int taskNumber)
    {
      synchronized (allTaskStatistics)
        {
          SinkTaskStatistics taskStatistics = allTaskStatistics.get(statisticsKey(connectorName, taskNumber));
          if (taskStatistics == null)
            {
              try
                {
                  taskStatistics = new SinkTaskStatistics(connectorName, "Redis", taskNumber);
                }
              catch (ServerException se)
                {
                  throw new ServerRuntimeException("Could not create statistics object", se);
                }
              allTaskStatistics.put(statisticsKey(connectorName, taskNumber), taskStatistics);
            }
          return taskStatistics;
        }
    }    

    /*****************************************
    *
    *  updateBatchCount
    *
    *****************************************/

    private void updateBatchCount(String connectorName, int taskNumber, int amount)
    {
      synchronized (allTaskStatistics)
        {
          SinkTaskStatistics taskStatistics = getStatistics(connectorName, taskNumber);
          taskStatistics.updateBatchCount(amount);
        }
    }

    /*****************************************
    *
    *  updatePutCount
    *
    *****************************************/

    private void updatePutCount(String connectorName, int taskNumber, int amount)
    {
      synchronized (allTaskStatistics)
        {
          SinkTaskStatistics taskStatistics = getStatistics(connectorName, taskNumber);
          taskStatistics.updatePutCount(amount);
        }
    }

    /*****************************************
    *
    *  updateRecordCount
    *
    *****************************************/

    private void updateRecordCount(String connectorName, int taskNumber, int amount)
    {
      synchronized (allTaskStatistics)
        {
          SinkTaskStatistics taskStatistics = getStatistics(connectorName, taskNumber);
          taskStatistics.updateRecordCount(amount);
        }
    }

    /*****************************************
    *
    *  stopStatisticsCollection
    *
    *****************************************/

    private void stopStatisticsCollection()
    {
      synchronized (allTaskStatistics)
        {
          for (SinkTaskStatistics taskStatistics : allTaskStatistics.values())
            {
              taskStatistics.unregister();
            }
          allTaskStatistics.clear();
        }
    }

    /*****************************************
    *
    *  inner class - CacheEntry
    *
    *****************************************/

    public class CacheEntry
    {
      //
      //  attributes
      //

      private byte[] key;
      private byte[] value;
      private int dbIndex;
      private Integer ttlOnDelete;
      private int tenantID;

      //
      //  accessors
      //

      public byte[] getKey() { return key; }
      public byte[] getValue() { return value; }
      public int getDBIndex() { return dbIndex; }
      public Integer getTTLOnDelete() { return ttlOnDelete; }
      public int getTenantID() { return tenantID; }

      //
      //  constructor
      //

      public CacheEntry(byte[] key, byte[] value, Integer dbIndex, Integer ttlOnDelete, int tenantID)
      {
        this.key = key;
        this.value = value;
        this.dbIndex = (dbIndex != null) ? dbIndex : SimpleRedisSinkTask.this.defaultDBIndex;
        this.ttlOnDelete = ttlOnDelete;
        this.tenantID = tenantID;
      }

      //
      //  constructor (default dbIndex)
      //

      public CacheEntry(byte[] key, byte[] value, int tenantID)
      {
        this(key, value, null, null, tenantID);
      }
      
      @Override
      public String toString()
      {
        return "CacheEntry [key=" + Arrays.toString(key) + ", value=" + Arrays.toString(value) + ", dbIndex=" + dbIndex + ", ttlOnDelete=" + ttlOnDelete + ", tenantID=" + tenantID + "]";
      }      
    }
  }
  
  /*****************************************
  *
  *  parseIntegerConfig
  *
  *****************************************/

  protected static Integer parseIntegerConfig(String attribute)
  {
    try
      {
        return (attribute != null) ? Integer.parseInt(attribute) : null;
      }
    catch (NumberFormatException e)
      {
        return null;
      }
  }
  
  /*****************************************
  *
  *  parseBooleanConfig
  *
  *****************************************/

  protected static boolean parseBooleanConfig(String attribute)
  {
    return Objects.equals(attribute, "true");
  }
}
