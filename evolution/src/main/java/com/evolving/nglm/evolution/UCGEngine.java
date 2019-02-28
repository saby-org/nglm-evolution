/****************************************************************************
*
*  UCGEngine.java 
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.CronFormat;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.core.utilities.UtilitiesException;
import com.evolving.nglm.evolution.UCGRuleService.UCGRuleListener;
import com.evolving.nglm.evolution.UCGState.UCGGroup;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

public class UCGEngine
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(UCGEngine.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  private volatile boolean stopRequested = false;
  private UCGRuleService ucgRuleService = null;
  private SegmentationDimensionService segmentationDimensionService = null;
  private ReferenceDataReader<String,UCGState> ucgStateReader = null;
  private KafkaProducer<byte[], byte[]> kafkaProducer = null;
  private BlockingQueue<UCGRule> evaluationRequests = new LinkedBlockingQueue<UCGRule>();
  private Thread ucgEvaluatorThread = null;
  private UCGEngineStatistics ucgEngineStatistics = null;
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public UCGEngine(String[] args)
  {
    /*****************************************
    *
    *  runtime
    *
    *****************************************/

    NGLMRuntime.initialize();

    /*****************************************
    *
    *  shutdown hook
    *
    *****************************************/
    
    NGLMRuntime.addShutdownHook(new ShutdownHook(this));

    /*****************************************
    *
    *  ucgRuleService
    *
    *****************************************/

    UCGRuleListener ucgListener = new UCGRuleListener()
    {
      @Override public void ucgRuleActivated(UCGRule ucgRule) { ruleActivated(ucgRule); }
      @Override public void ucgRuleDeactivated(String guiManagedObjectID) { }
    };
    ucgRuleService = new UCGRuleService(Deployment.getBrokerServers(), "ucgengine-ucgruleservice", Deployment.getUCGRuleTopic(), false, ucgListener);
    ucgRuleService.start();

    /*****************************************
    *
    *  segmentationDimensionService
    *
    *****************************************/

    segmentationDimensionService = new SegmentationDimensionService(Deployment.getBrokerServers(), "ucgending-segmentationdimensionservice", Deployment.getSegmentationDimensionTopic(), false);
    segmentationDimensionService.start();
    
    /*****************************************
    *
    *  ucgStateReader
    *
    *****************************************/

    ucgStateReader = ReferenceDataReader.<String,UCGState>startReader("ucgengine-ucgstate", "001", Deployment.getBrokerServers(), Deployment.getUCGStateTopic(), UCGState::unpack);

    /*****************************************
    *
    *  kafka producer
    *
    *****************************************/

    Properties kafkaProducerProperties = new Properties();
    kafkaProducerProperties.put("bootstrap.servers", Deployment.getBrokerServers());
    kafkaProducerProperties.put("acks", "all");
    kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProducer = new KafkaProducer<byte[], byte[]>(kafkaProducerProperties);

    /*****************************************
    *
    *  monitoring object
    *
    *****************************************/

    ucgEngineStatistics = new UCGEngineStatistics("ucgengine");

    /*****************************************
    *
    *  ucgEvaluator
    *
    *****************************************/

    Runnable ucgEvaluator = new Runnable() { @Override public void run() { runUCGEvaluator(); } };
    ucgEvaluatorThread = new Thread(ucgEvaluator, "UCGEvaluator");
    ucgEvaluatorThread.start();
  }

  /*****************************************
  *
  *  ruleActivated
  *
  *****************************************/

  private void ruleActivated(UCGRule activeUCGRule)
  {
    UCGState ucgState = ucgStateReader.get(UCGState.getSingletonKey());
    boolean evaluate = true;
    evaluate = evaluate && activeUCGRule.getRefreshEpoch() != null;
    evaluate = evaluate && (ucgState == null || ! Objects.equals(activeUCGRule.getUCGRuleID(), ucgState.getUCGRuleID()) || ! Objects.equals(activeUCGRule.getRefreshEpoch(), ucgState.getRefreshEpoch()));
    if (evaluate)
      {
        evaluationRequests.add(activeUCGRule);
      }
  }

  /*****************************************
  *
  *  runUCGEvaluator
  *
  *****************************************/

  private void runUCGEvaluator()
  {
    /*****************************************
    *
    *  abort if ucg evaluation is not configured
    *
    *****************************************/

    if (Deployment.getUCGEvaluationCronEntry() == null) return;

    /*****************************************
    *
    *  cron
    *
    *****************************************/

    CronFormat ucgEvaluation = null;
    try
      {
        ucgEvaluation = new CronFormat(Deployment.getUCGEvaluationCronEntry(), TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
      }
    catch (UtilitiesException e)
      {
        log.error("bad ucgEvaluationCronEntry {}", e.getMessage());
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.info(stackTraceWriter.toString());
        return;
      }

    /*****************************************
    *
    *  main loop
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    Date nextUCGEvaluation = ucgEvaluation.previous(now);
    while (! stopRequested)
      {
        /*****************************************
        *
        *  wait until reload schedule is needed
        *
        *****************************************/

        synchronized (this)
          {
            while (! stopRequested && now.before(nextUCGEvaluation))
              {
                try
                  {
                    this.wait(nextUCGEvaluation.getTime() - now.getTime());
                  }
                catch (InterruptedException e)
                  {
                  }
                now = SystemTime.getCurrentTime();
              }
          }

        //
        //  abort if stopping
        //

        if (stopRequested)
          {
            continue;
          }
          
        /*****************************************
        *
        *  log start
        *
        *****************************************/

        log.info("ucgEvaluation {} (start)", nextUCGEvaluation);

        /*****************************************
        *
        *  ucg evaluation
        *
        *****************************************/

        //
        //  active rle
        //

        UCGRule activeUCGRule = null;
        for (UCGRule ucgRule : ucgRuleService.getActiveUCGRules(now))
          {
            activeUCGRule = ucgRule;
          }

        //
        //  evaluate (if necessary)
        //

        boolean evaluate = true;
        evaluate = evaluate && activeUCGRule != null;
        evaluate = evaluate && activeUCGRule.getRefreshEpoch() != null;
        evaluate = evaluate && (ucgStateReader.get(UCGState.getSingletonKey()) == null || ucgStateReader.get(UCGState.getSingletonKey()).getEvaluationDate().before(nextUCGEvaluation));
        if (evaluate)
          {
            evaluationRequests.add(activeUCGRule);
          }

        /*****************************************
        *
        *  log end
        *
        *****************************************/

        log.info("ucgEvaluation {} (end)", nextUCGEvaluation);

        /*****************************************
        *
        *  nextUCGEvaluation
        *
        *****************************************/

        nextUCGEvaluation = ucgEvaluation.next(RLMDateUtils.addSeconds(nextUCGEvaluation,1));
        log.info("ucgEvaluation {} (next)", nextUCGEvaluation);
      }
  }

  /*****************************************
  *
  *  run
  *
  *****************************************/

  public void run()
  {
    ConnectSerde<StringKey> stringKeySerde = StringKey.serde();
    ConnectSerde<UCGState> ucgStateSerde = UCGState.serde();
    while (! stopRequested)
      {
        /*****************************************
        *
        *  get next request -- drain additional requests and keep the last
        *
        *****************************************/

        UCGRule ucgRule = null;
        while (! stopRequested && ucgRule == null)
          {
            try
              {
                ucgRule = evaluationRequests.take();
                while (evaluationRequests.size() > 0)
                  {
                    ucgRule = evaluationRequests.take();
                  }
              }
            catch (InterruptedException e)
              {

              }
          }

        //
        //  stopRequested
        //

        if (stopRequested)
          {
            continue;
          }

        /*****************************************
        *
        *  now
        *
        *****************************************/

        Date now = SystemTime.getCurrentTime();

        /*****************************************
        *
        *  segmentsByDimension
        *
        *****************************************/

        List<Set<String>> segmentsByDimension = new LinkedList<Set<String>>();
        for (String dimensionID : ucgRule.getSelectedDimensions())
          {
            Set<String> segmentIDs = new HashSet<String>();
            SegmentationDimension dimension = segmentationDimensionService.getActiveSegmentationDimension(dimensionID, now);
            if (dimension != null)
              {
                for (Segment segment : dimension.getSegments())
                  {
                    segmentIDs.add(segment.getID());
                  }
              }
            segmentsByDimension.add(segmentIDs);
          }

        /*****************************************
        *
        *  strata
        *
        *****************************************/

        Set<List<String>> strata = Sets.cartesianProduct(segmentsByDimension);

        /*****************************************
        *
        *  evaluate
        *
        *****************************************/

        Set<UCGGroup> ucgGroups = new HashSet<UCGGroup>();
        for (List<String> stratum : strata)
          {
            ucgGroups.add(new UCGGroup(new HashSet<String>(stratum), 100, 10));
          }

        /*****************************************
        *
        *  ucgState
        *
        *****************************************/

        UCGState ucgState = new UCGState(ucgRule, ucgGroups, now);
        kafkaProducer.send(new ProducerRecord<byte[], byte[]>(Deployment.getUCGStateTopic(), stringKeySerde.serializer().serialize(Deployment.getUCGStateTopic(), new StringKey(UCGState.getSingletonKey())), ucgStateSerde.serializer().serialize(Deployment.getUCGStateTopic(), ucgState)));
      }
  }

  /*****************************************
  *
  *  class ShutdownHook
  *
  *****************************************/

  private static class ShutdownHook implements NGLMRuntime.NGLMShutdownHook
  {
    //
    //  data
    //

    private UCGEngine ucgEngine;

    //
    //  constructor
    //

    private ShutdownHook(UCGEngine ucgEngine)
    {
      this.ucgEngine = ucgEngine;
    }

    //
    //  shutdown
    //

    @Override public void shutdown(boolean normalShutdown)
    {
      ucgEngine.shutdownUCGEngine(normalShutdown);
    }
  }

  /****************************************
  *
  *  shutdownUCGEngine
  *
  ****************************************/
  
  private void shutdownUCGEngine(boolean normalShutdown)
  {
    /*****************************************
    *
    *  stop threads
    *
    *****************************************/

    synchronized (this)
      {
        stopRequested = true;
        if (ucgEvaluatorThread != null) ucgEvaluatorThread.interrupt();
        this.notifyAll();
      }

    /*****************************************
    *
    *  close resources
    *
    *****************************************/

    if (ucgRuleService != null) ucgRuleService.stop();
    if (segmentationDimensionService != null) segmentationDimensionService.stop();
    if (ucgStateReader != null) ucgStateReader.close();
    if (kafkaProducer != null) kafkaProducer.close();

    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("Stopped UCGEngine");
  }

  /*****************************************
  *
  *  engine
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  instance  
    //

    UCGEngine engine = new UCGEngine(args);

    //
    //  run
    //

    engine.run();
  }
}
