/****************************************************************************
 *
 *  TokenReportDriver.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports.ROI;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.datacubes.generator.SubscriberProfileDatacubeGenerator;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportDriver.ReportTypeDef;
import com.evolving.nglm.evolution.reports.subscriber.SubscriberReportMonoPhase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

@ReportTypeDef(reportType = "ROI")
public class ROIReportDriver extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(ROIReportDriver.class);

  @Override
  public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params, int tenantID)
  {
    log.debug("Processing ROI Report with " + report.getName());

    String esIndexSubscriber = SubscriberProfileDatacubeGenerator.DATACUBE_ES_INDEX(tenantID);

    ROIReportMonoPhase.main(new String[] { elasticSearch, esIndexSubscriber, csvFilename, tenantID+"" }, reportGenerationDate);

    log.debug("Finished with ROI Report");
  }

  @Override
  public List<FilterObject> reportFilters()
  {
    return null;
  }

  @Override
  public List<String> reportHeader()
  {
    List<String> result = ROIReportMonoPhase.headerFieldsOrder;
    return result;
  }
}