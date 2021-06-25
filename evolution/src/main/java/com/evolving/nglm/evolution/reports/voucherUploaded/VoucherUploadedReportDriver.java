package com.evolving.nglm.evolution.reports.voucherUploaded;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.VoucherService;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.bdr.BDRReportMonoPhase;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class VoucherUploadedReportDriver extends ReportDriver
{

  private static final Logger log = LoggerFactory.getLogger(VoucherUploadedReportDriver.class);

  @Override public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params, int tenantID)
  {

    log.debug("Processing Journey Customer States Report with " + report + " and " + params);
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    String Voucher_ES_INDEX = "voucher_live_";    
    log.debug("PHASE 1 : read ElasticSearch");
    VoucherUploadedReportMonoPhase.main(new String[] { elasticSearch, Voucher_ES_INDEX, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit }, reportGenerationDate);
   
    log.debug("Finished with Journey Customer States Report");

  }

  @Override
  public List<FilterObject> reportFilters()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> reportHeader()
  {
    List<String> result = new ArrayList<>();
    result = VoucherUploadedReportMonoPhase.headerFieldsOrder;
    return result;
  }
}