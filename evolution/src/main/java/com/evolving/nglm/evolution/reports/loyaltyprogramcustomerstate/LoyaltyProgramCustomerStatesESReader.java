package com.evolving.nglm.evolution.reports.loyaltyprogramcustomerstate;

import com.evolving.nglm.evolution.reports.ReportEsReader;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.LinkedHashMap;

public class LoyaltyProgramCustomerStatesESReader
{
  //
  // logger
  //

  private static final Logger log = LoggerFactory.getLogger(LoyaltyProgramCustomerStatesESReader.class);
  
  /****************************************
   * 
   * read
   * 
   ****************************************/
  
  public static void read(String topicName, String kafkaNodeList, String kzHostList, String esNode, String esIndexSubscriber, final Date reportGenerationDate, int defaultReportPeriodQuantity, String defaultReportPeriodUnit)
  {
    log.info("starting LoyaltyProgramESReader - Reading data from ES in " + esIndexSubscriber + " index and writing to " + topicName + " topic.");
    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    esIndexWithQuery.put(esIndexSubscriber, QueryBuilders.matchAllQuery());
    if(log.isDebugEnabled()) log.debug("ES indexes to read {}", esIndexWithQuery.keySet());

    ReportEsReader reportEsReader = new ReportEsReader(
        "subscriberID",
        topicName,
        kafkaNodeList,
        kzHostList,
        esNode,
        esIndexWithQuery,
        true, true
        );
    reportEsReader.start();
    log.info("Finished LoyaltyProgramESReader");
  }

  public static void main(String[] args)
  {

  }
}
