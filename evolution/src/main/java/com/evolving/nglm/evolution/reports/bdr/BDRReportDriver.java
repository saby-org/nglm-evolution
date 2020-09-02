package com.evolving.nglm.evolution.reports.bdr;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;

public class BDRReportDriver extends ReportDriver{
  private static final Logger log = LoggerFactory.getLogger(BDRReportDriver.class);

  @Override
  public void produceReport(
      Report report,
      String zookeeper,
      String kafka,
      String elasticSearch,
      String csvFilename,
      String[] params)
  {
    log.debug("Processing "+report.getName());
    String esIndexBdr = "detailedrecords_bonuses-";
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    BDRReportMonoPhase.main(new String[]{
        elasticSearch, esIndexBdr, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit 
    });         
    log.debug("Finished with BDR Report");
  }

  @Override
  public JSONArray reportFilters() {
	  JSONArray responseJSON = new JSONArray();

	  JSONObject filterJSON, argumentJSON;

	  filterJSON = new JSONObject();
	  filterJSON.put("criterionField", "moduleName");
	  argumentJSON = new JSONObject();
	  argumentJSON.put("expression", "");
	  argumentJSON.put("valueType", "string");
	  argumentJSON.put("value", new JSONArray());
	  argumentJSON.put("timeUnit", null);
	  filterJSON.put("argument", argumentJSON);
	  responseJSON.add(argumentJSON);

	  filterJSON = new JSONObject();
	  filterJSON.put("criterionField", "deliveryStatus");
	  argumentJSON = new JSONObject();
	  argumentJSON.put("expression", "");
	  argumentJSON.put("valueType", "string");
	  argumentJSON.put("value", new JSONArray());
	  argumentJSON.put("timeUnit", null);
	  filterJSON.put("argument", argumentJSON);
	  responseJSON.add(argumentJSON);

	  return responseJSON;
  }
}
