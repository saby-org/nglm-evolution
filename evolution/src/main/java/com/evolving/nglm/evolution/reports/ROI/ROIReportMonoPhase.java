/****************************************************************************
 *
 *  TokenReportMonoPhase.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports.ROI;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.zip.ZipOutputStream;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.DeliveryRequest;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.PresentationStrategyService;
import com.evolving.nglm.evolution.ScoringStrategyService;
import com.evolving.nglm.evolution.TokenTypeService;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportMonoPhase;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.evolution.reports.subscriber.SubscriberReportMonoPhase;

public class ROIReportMonoPhase implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(ROIReportMonoPhase.class);
  final private static String CSV_SEPARATOR = ReportUtils.getSeparator();

  private static final String dateTime = "dateTime";
  private static final String nbCustomerUCG = "nbCustomerUCG";
  private static final String nbCustomerTarget = "nbCustomerTarget";
  private static final String average_metric_UCG = "average_metric_UCG";
  private static final String average_metric_Target = "average_metric_Target";
  private static final String metric_ROI = "metric_ROI";
  private static final String nbRetainedCustomers = "nbRetainedCustomers";
  private static final String churnRate = "churnRate";
  private static final String churnROI = "churnROI";
  
  
  
  
  static List<String> headerFieldsOrder = new ArrayList<String>();
  static
  {
    headerFieldsOrder.add(dateTime);
    headerFieldsOrder.add(nbCustomerUCG);
    headerFieldsOrder.add(nbCustomerTarget);
    headerFieldsOrder.add(average_metric_UCG);
    headerFieldsOrder.add(average_metric_Target);
    headerFieldsOrder.add(metric_ROI);
    headerFieldsOrder.add(nbRetainedCustomers);
    headerFieldsOrder.add(churnRate);
    headerFieldsOrder.add(churnROI);
  }

  private OfferService offerService = null;
  private TokenTypeService tokenTypeService = null;
  private PresentationStrategyService presentationStrategyService = null;
  private ScoringStrategyService scoringStrategyService = null;
  private JourneyService journeyService = null;
  private LoyaltyProgramService loyaltyProgramService = null;
  private int tenantID = 0;

  /****************************************
   *
   * dumpElementToCsv
   *
   ****************************************/

 
  public boolean dumpElementToCsvMono(Map<String,Object> map, ZipOutputStream writer, boolean addHeaders) throws IOException
  {
    LinkedHashMap<String, Object> result = new LinkedHashMap<>();
    LinkedHashMap<String, Object> commonFields = new LinkedHashMap<>();
    Map<String, Object> subscriberFields = map;

    if (subscriberFields != null)
      {
        String subscriberID = Objects.toString(subscriberFields.get("subscriberID"));
        Date now = SystemTime.getCurrentTime();
        if (subscriberID != null)
          {
        	
          }
      }
    return addHeaders;
  }

  private String dateOrEmptyString(Object time)
  {
    return (time == null) ? "" : ReportsCommonCode.getDateString(new Date((long) time));
  }
  
  /****************************************
   *
   * addHeaders
   *
   ****************************************/

  private void addHeaders(ZipOutputStream writer, Set<String> headers, int offset) throws IOException
  {
    if (headers != null && !headers.isEmpty())
      {
        String header = "";
        for (String field : headers)
          {
            header += field + CSV_SEPARATOR;
          }
        header = header.substring(0, header.length() - offset);
        writer.write(header.getBytes());
        if (offset == 1)
          {
            writer.write("\n".getBytes());
          }
      }
  }

  /****************************************
   *
   * main
   *
   ****************************************/

  public static void main(String[] args, final Date reportGenerationDate)
  {
    ROIReportMonoPhase tokenReportMonoPhase = new ROIReportMonoPhase();
    tokenReportMonoPhase.start(args, reportGenerationDate);
  }
  
  private void start(String[] args, final Date reportGenerationDate)
  {
    log.info("received " + args.length + " args");
    for (String arg : args)
      {
        log.info("TokenReportESReader: arg " + arg);
      }

    if (args.length < 3) {
      log.warn(
          "Usage : TokenReportMonoPhase <ESNode> <ES customer index> <csvfile>");
      return;
    }
    String esNode          = args[0];
    String esIndexCustomer = args[1];
    String csvfile         = args[2];
    if (args.length > 3) tenantID = Integer.parseInt(args[3]);

    log.info("Reading data from ES in "+esIndexCustomer+"  index and writing to "+csvfile+" file.");  

    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    esIndexWithQuery.put(esIndexCustomer, QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("tenantID", tenantID)));
      
    ReportMonoPhase reportMonoPhase = new ReportMonoPhase(
              esNode,
              esIndexWithQuery,
              this,
              csvfile
          );

    offerService = new OfferService(Deployment.getBrokerServers(), "report-offerService-tokenReportMonoPhase", Deployment.getOfferTopic(), false);
    scoringStrategyService = new ScoringStrategyService(Deployment.getBrokerServers(), "report-scoringstrategyservice-tokenReportMonoPhase", Deployment.getScoringStrategyTopic(), false);
    presentationStrategyService = new PresentationStrategyService(Deployment.getBrokerServers(), "report-presentationstrategyservice-tokenReportMonoPhase", Deployment.getPresentationStrategyTopic(), false);
    tokenTypeService = new TokenTypeService(Deployment.getBrokerServers(), "report-tokentypeservice-tokenReportMonoPhase", Deployment.getTokenTypeTopic(), false);
    journeyService = new JourneyService(Deployment.getBrokerServers(), "report-journeyservice-tokenReportMonoPhase",Deployment.getJourneyTopic(), false);
    loyaltyProgramService = new LoyaltyProgramService(Deployment.getBrokerServers(), "report-loyaltyprogramservice-tokenReportMonoPhase", Deployment.getLoyaltyProgramTopic(), false);

    offerService.start();
    scoringStrategyService.start();
    presentationStrategyService.start();
    tokenTypeService.start();
    journeyService.start();
    loyaltyProgramService.start();

    try {
      if (!reportMonoPhase.startOneToOne())
        {
          log.warn("An error occured, the report might be corrupted");
          throw new RuntimeException("An error occurred, report must be restarted");
        }
    } finally {
      offerService.stop();
      scoringStrategyService.stop();
      presentationStrategyService.stop();
      tokenTypeService.stop();
      journeyService.stop();
      loyaltyProgramService.stop();
    }
  }

}
