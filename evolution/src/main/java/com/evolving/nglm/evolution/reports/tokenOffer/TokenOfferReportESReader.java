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

import java.util.LinkedHashMap;

public class TokenOfferReportESReader {

	private static final Logger log = LoggerFactory.getLogger(TokenOfferReportESReader.class);
	
	public static void main(String[] args) {
	  log.info("received " + args.length + " args");
	  for(String arg : args){
	    log.info("TokenReportESReader: arg " + arg);
	  }

	  if (args.length < 6) {
	    log.warn(
	        "Usage : TokenReportESReader <Output Topic> <KafkaNodeList> <ZKhostList> <ESNode> <ES customer index> <ES journey index>");
	    return;
	  }
	  String topicName       = args[0];
	  String kafkaNodeList   = args[1];
	  String kzHostList      = args[2];
	  String esNode          = args[3];
	  String esIndexCustomer = args[4];

	  log.info("Reading data from ES in "+esIndexCustomer+"  index and writing to "+topicName+" topic.");	

	  LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
      esIndexWithQuery.put(esIndexCustomer, QueryBuilders.matchAllQuery());
      
      ReportEsReader reportEsReader = new ReportEsReader(
              TokenOfferReportObjects.KEY_STR,
              topicName,
              kafkaNodeList,
              kzHostList,
              esNode,
              esIndexWithQuery
          );
      
      reportEsReader.start();
	  log.info("Finished TokenReportESReader");
	}
	
}
