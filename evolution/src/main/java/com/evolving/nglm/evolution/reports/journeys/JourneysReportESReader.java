package com.evolving.nglm.evolution.reports.journeys;

import com.evolving.nglm.evolution.reports.journeyimpact.JourneyImpactReportESReader;
import com.evolving.nglm.evolution.reports.ReportEsReader;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.LinkedHashMap;

public class JourneysReportESReader
{
  //
  // logger
  //

  private static final Logger log = LoggerFactory.getLogger(JourneyImpactReportESReader.class);

  /****************************************
   * 
   * read
   * 
   ****************************************/
  
  public static void read(String topicName, String kafkaNodeList, String kzHostList, String esNode, String esIndexJourney, final Date reportGenerationDate, Integer defaultReportPeriodQuantity, String defaultReportPeriodUnit)
  {
    log.info("starting JourneysReportESReader - Reading data from ES in " + esIndexJourney + " index and writing to " + topicName + " topic.");
    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    esIndexWithQuery.put(esIndexJourney, QueryBuilders.matchAllQuery());
    log.info("RAJ K ES indexes to read {}", esIndexWithQuery.keySet());
    
    //
    //  reportEsReader
    //
    
    ReportEsReader reportEsReader = new ReportEsReader("journeyID", topicName, kafkaNodeList, kzHostList, esNode, esIndexWithQuery);
    reportEsReader.start();
    log.info("Finished JourneysReportESReader");
  }

  public static void main(String[] args)
  {

  }
}