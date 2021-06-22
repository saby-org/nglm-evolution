package com.evolving.nglm.evolution.reports.bdr;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ColumnType;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportDriver.ReportTypeDef;

import java.util.Date;

@ReportTypeDef(reportType = "detailedrecords")
public class BDRReportDriver extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(BDRReportDriver.class);
  public static final String ES_INDEX_BDR_INITIAL = "detailedrecords_bonuses-";
  
  @Override public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params, int tenantID)
  {
    log.debug("Processing " + report.getName());
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    BDRReportMonoPhase.main(new String[] { elasticSearch, ES_INDEX_BDR_INITIAL, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit }, reportGenerationDate);
    log.debug("Finished with BDR Report");
  }

  @Override
  public List<FilterObject> reportFilters()
  {
    List<FilterObject> result = new ArrayList<>();
    result.add(new FilterObject(BDRReportMonoPhase.moduleName, ColumnType.MULTIPLE_STRING, new String[] { "Journey_Manager", "Loyalty_Program" }));
    result.add(new FilterObject(BDRReportMonoPhase.deliverableDisplay, ColumnType.MULTIPLE_STRING, new String[] { "OnNetMinutes" }));
    return result;
  }

  @Override
  public List<String> reportHeader()
  {
    List<String> result = new ArrayList<>();
    result.add(BDRReportMonoPhase.moduleId);
    result.add(BDRReportMonoPhase.featureId);
    result.add(BDRReportMonoPhase.deliverableID);
    result.add(BDRReportMonoPhase.deliverableQty);
    result.add(BDRReportMonoPhase.moduleName);
    result.add(BDRReportMonoPhase.featureDisplay);
    result.add(BDRReportMonoPhase.deliverableDisplay);
    result.add(BDRReportMonoPhase.customerID);
    result.add(BDRReportMonoPhase.deliverableExpirationDate);
    result.add(BDRReportMonoPhase.eventDatetime);
    result.add(BDRReportMonoPhase.operation);
    result.add(BDRReportMonoPhase.orgin);
    result.add(BDRReportMonoPhase.providerName);
    result.add(BDRReportMonoPhase.returnCode);
    result.add(BDRReportMonoPhase.returnCodeDescription);
    result.add(BDRReportMonoPhase.returnCodeDetails);
    result.add(BDRReportMonoPhase.deliveryRequestID);
    result.add(BDRReportMonoPhase.originatingDeliveryRequestID);
    result.add(BDRReportMonoPhase.eventID);
    result.add(BDRReportMonoPhase.deliveryStatus);
    return result;
  }

}
