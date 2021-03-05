/*****************************************************************************
 *
 *  ReportCsvFactory.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.reports;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.zip.ZipOutputStream;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;

/**
 * A factory that must be implemented to produce phase 3 of the Report
 * Generation.
 *
 */
public interface ReportCsvFactory
{

  /**
   * Write a {@link ReportElement} to the report (csv file).
   * 
   * @param key
   *          The key that this record had in Kafka, for reference.
   * @param re
   *          {@link ReportElement} to write.
   * @param gzipOutputStream
   *          {@link Writer} to write to.
   * @throws IOException
   *           When an error related to filesystem access occurs.
   */
  default boolean dumpElementToCsv(String key, ReportElement re, ZipOutputStream writer, boolean addHeader) throws IOException {return false;}
  default boolean dumpElementToCsvMono(Map<String,Object> map, ZipOutputStream writer, boolean addHeader) throws IOException {return false;}
  default void dumpLineToCsv(Map<String, Object> lineMap, ZipOutputStream writer, boolean addHeaders)  {}
  default Map<String, List<Map<String, Object>>> getSplittedReportElementsForFile(ReportElement reportElement) {return null;}
  default Map<String, List<Map<String, Object>>> getSplittedReportElementsForFileMono(Map<String,Object> map) {return null;}
  
  /*******************************
   * 
   * will return in yyyy-ww format
   * 
   *******************************/
  
  public static Set<String> getEsIndexWeeks(final Date fromDate, Date toDate)
  {
    Date tempfromDate = fromDate;
    Set<String> esIndexList = new HashSet<String>();
    while(tempfromDate.getTime() < toDate.getTime())
      {
        esIndexList.add(RLMDateUtils.printWeek(tempfromDate));
        tempfromDate = RLMDateUtils.addDays(tempfromDate, 1, Deployment.getBaseTimeZone());
      }
    return esIndexList;
  }
}
