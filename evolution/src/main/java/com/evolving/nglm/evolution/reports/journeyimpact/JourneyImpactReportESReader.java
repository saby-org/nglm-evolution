package com.evolving.nglm.evolution.reports.journeyimpact;

import com.evolving.nglm.evolution.reports.journeycustomerstates.JourneyCustomerStatesReportObjects;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.Journey;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.reports.ReportEsReader;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.LinkedHashMap;

/**
 * This implements phase 1 of the Journey report. All it does is specifying
 * <ol>
 * <li>Which field in Elastic Search is used as the key in the Kafka topic that
 * is produced ({@link JourneyCustomerStatesReportObjects#KEY_STR}), and
 * <li>Which Elastic Search indexes have to be read (passed as an array to the
 * {@link ReportEsReader} constructor.
 * </ol>
 * <p>
 * All data is written to a single topic.
 */
public class JourneyImpactReportESReader
{

  //
  // logger
  //

  private static final Logger log = LoggerFactory.getLogger(JourneyImpactReportESReader.class);
  private static String elasticSearchDateFormat = Deployment.getElasticSearchDateFormat();
  private static DateFormat dateFormat = new SimpleDateFormat(elasticSearchDateFormat);

  public static void main(String[] args)
  {
    log.info("received " + args.length + " args");
    for (String arg : args)
      {
        log.info("JourneyImpactReportESReader: arg " + arg);
      }

    if (args.length < 5)
      {
        log.warn("Usage : JourneyImpactReportESReader <Output Topic> <KafkaNodeList> <ZKhostList> <ESNode> <ES customer index> <ES journey index>");
        return;
      }

    String topicName = args[0];
    String kafkaNodeList = args[1];
    String kzHostList = args[2];
    String esNode = args[3];
    String esIndexJourneyStats = args[4];
    String esIndexJourneyMetric = args[5];
    
    JourneyService journeyService = new JourneyService(kafkaNodeList, "JourneyImpactReportESReader-journeyservice-" + topicName, Deployment.getJourneyTopic(), false);
    journeyService.start();
    
    Collection<Journey> activeJourneys = journeyService.getActiveJourneys(SystemTime.getCurrentTime());
    StringBuilder activeJourneyEsIndex = new StringBuilder();
    boolean firstEntry = true;
    for (Journey journey : activeJourneys)
      {
        if (!firstEntry) activeJourneyEsIndex.append(",");
        String indexName = esIndexJourneyStats + journey.getJourneyID();
        activeJourneyEsIndex.append(indexName);
        firstEntry = false;
      }

    log.info("Reading data from ES in (" + activeJourneyEsIndex.toString() + ") and " + esIndexJourneyMetric + " index and writing to " + topicName + " topic.");

    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    esIndexWithQuery.put(activeJourneyEsIndex.toString(), QueryBuilders.matchAllQuery());
    esIndexWithQuery.put(esIndexJourneyMetric, QueryBuilders.matchAllQuery());

    ReportEsReader reportEsReader = new ReportEsReader("journeyID", topicName, kafkaNodeList, kzHostList, esNode, esIndexWithQuery);

    reportEsReader.start();
    log.info("Finished JourneyImpactReportESReader");
  }

}
