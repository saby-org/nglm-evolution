/****************************************************************************
 *
 * Token ReportESReader.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports.tokenOffer;

import com.evolving.nglm.evolution.reports.ReportEsReader;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.LinkedHashMap;

public class TokenOfferReportESReader
{

  private static final Logger log = LoggerFactory.getLogger(TokenOfferReportESReader.class);

  /****************************************
   * 
   * read
   * 
   ****************************************/
  
  public static void read(String topicName, String kafkaNodeList, String kzHostList, String esNode, String esIndexCustomer, final Date reportGenerationDate, Integer reportPeriodQuantity, String reportPeriodUnit)
  {
    log.info("starting TokenReportESReader - Reading data from ES in " + esIndexCustomer + "  index and writing to " + topicName + " topic.");
    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    esIndexWithQuery.put(esIndexCustomer, QueryBuilders.matchAllQuery());
    log.info("RAJ K ES indexes to read {}", esIndexWithQuery.keySet());
    
    //
    //  reportEsReader
    //
    
    ReportEsReader reportEsReader = new ReportEsReader(TokenOfferReportObjects.KEY_STR, topicName, kafkaNodeList, kzHostList, esNode, esIndexWithQuery, true);
    reportEsReader.start();
    log.info("Finished TokenReportESReader");
  
    
  }
}
