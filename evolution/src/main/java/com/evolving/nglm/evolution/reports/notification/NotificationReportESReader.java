package com.evolving.nglm.evolution.reports.notification;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.reports.ReportEsReader;
import com.evolving.nglm.evolution.reports.ReportEsReader.PERIOD;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedHashMap;

public class NotificationReportESReader
{

  private static final Logger log = LoggerFactory.getLogger(NotificationReportESReader.class);
  private static String elasticSearchDateFormat = Deployment.getElasticSearchDateFormat();
  private static DateFormat dateFormat = new SimpleDateFormat(elasticSearchDateFormat);
  
  public static void main(String[] args) {
    log.info("received " + args.length + " args");
    for(String arg : args){
      log.info("NotificationReportESReader: arg " + arg);
    }

    if (args.length < 6) {
      log.warn(
          "Usage : NotificationReportESReader <Output Topic> <KafkaNodeList> <ZKhostList> <ESNode> <ES customer index> <ES journey index>");
      return;
    }
    String topicName       = args[0];
    String kafkaNodeList   = args[1];
    String kzHostList      = args[2];
    String esNode          = args[3];
    String esIndexNotif = args[4];
    String esIndexCustomer = args[5];
    
    Integer reportPeriodQuantity = 0;
    String reportPeriodUnit = null;
    if(args.length > 6 && args[6] != null && args[7] != null){
      reportPeriodQuantity = Integer.parseInt(args[6]);
      reportPeriodUnit = args[7];
    }

    log.info("Reading data from ES in "+esIndexNotif+"  index and writing to "+topicName+" topic.");    

    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    
    QueryBuilder query = null;
    if(reportPeriodUnit != null && !reportPeriodUnit.isEmpty()){
      Calendar cal = SystemTime.getCalendar();
      if(reportPeriodUnit.equalsIgnoreCase(PERIOD.DAYS.getExternalRepresentation())){
        cal.set(Calendar.DAY_OF_MONTH, -reportPeriodQuantity);
      }
      else if(reportPeriodUnit.equalsIgnoreCase(PERIOD.WEEKS.getExternalRepresentation())){
        cal.set(Calendar.WEEK_OF_YEAR, -reportPeriodQuantity);
      }else if(reportPeriodUnit.equalsIgnoreCase(PERIOD.MONTHS.getExternalRepresentation())){
        cal.set(Calendar.MONTH, -reportPeriodQuantity);
      }
      query = QueryBuilders.rangeQuery("deliveryDate").format(elasticSearchDateFormat).gte(dateFormat.format(cal.getTime()));

    }else{
      query = QueryBuilders.matchAllQuery();
    }
    
    esIndexWithQuery.put(esIndexNotif, query);
    esIndexWithQuery.put(esIndexCustomer, QueryBuilders.matchAllQuery());
    
    ReportEsReader reportEsReader = new ReportEsReader(
            "subscriberID",
            topicName,
            kafkaNodeList,
            kzHostList,
            esNode,
            esIndexWithQuery
        );
    
    reportEsReader.start();
    log.info("Finished NotificationReportESReader");
  }
}
