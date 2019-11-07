/*****************************************************************************
 *
 *  ReportCsvFactory.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.reports;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.zip.ZipOutputStream;

import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;

/**
 * A factory that must be implemented to produce phase 3 of the Report Generation. 
 *
 */
public interface ReportCsvFactory {

	/**
	 * Write a {@link ReportElement} to the report (csv file).
	 * @param key     The key that this record had in Kafka, for reference.
	 * @param re      {@link ReportElement} to write.
	 * @param gzipOutputStream  {@link Writer} to write to.
	 * @throws IOException When an error related to filesystem access occurs.
	 */
	void dumpElementToCsv(String key, ReportElement re, ZipOutputStream writer) throws IOException;
	
	 
  //
  // format
  //
  default String format(String s)
  {
    // Surround s with double quotes, and escape double quotes inside s
    return "\""+s.replaceAll("\"", Matcher.quoteReplacement("\\\""))+"\"";
  }

}
