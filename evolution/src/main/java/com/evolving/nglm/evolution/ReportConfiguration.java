package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;

public class ReportConfiguration {
	
	private static final Logger log = LoggerFactory.getLogger(ReportConfiguration.class);

	public ReportConfiguration(JSONObject jsonRoot) throws JSONUtilitiesException {
		reportName        = JSONUtilities.decodeString(jsonRoot, Report.REPORT_NAME, true);
		reportClass       = JSONUtilities.decodeString(jsonRoot, "reportClass", true);
		reportDisplayName = JSONUtilities.decodeString(jsonRoot, Report.REPORT_DISPLAY_NAME, true);
		reportDescription = JSONUtilities.decodeString(jsonRoot, Report.REPORT_DESCRIPTION, true);

		String availableSchedulingStr = JSONUtilities.decodeString(jsonRoot, Report.AVAILABLE_SCHEDULING, false);
		log.trace("availableSchedulingStr = "+availableSchedulingStr);
		if (availableSchedulingStr != null) {
			String[] values = availableSchedulingStr.split("\\|");
			log.trace("values.length = "+values.length);
			for (int i=0; i<values.length; i++) {
				try {
					Report.SchedulingInterval val = Report.SchedulingInterval.valueOf(values[i].trim());
					availableScheduling.add(val);
					log.trace("  added "+val);
				} catch (IllegalArgumentException e) {
					log.error("Illegal Scheduling value : "+values[i]);
				}
			}
		}
		if (availableScheduling.isEmpty()) {
			availableScheduling.add(Report.SchedulingInterval.ANY);
		}
	}
	private String reportName;
	private String reportClass;
	private String reportDisplayName;
	private String reportDescription;

	public List<Report.SchedulingInterval> getAvailableScheduling() {
		return availableScheduling;
	}
	private List<Report.SchedulingInterval> availableScheduling = new ArrayList<>(); 

	public String getReportDisplayName() {
		return reportDisplayName;
	}
	public String getReportDescription() {
		return reportDescription;
	}

	@Override
	public String toString() {
		return "ReportConfiguration [reportName=" + reportName + ", reportClass=" + reportClass + ", reportDisplayName="
				+ reportDisplayName + ", reportDescription=" + reportDescription + "]";
	}
	public String getReportName() {
		return reportName;
	}
	public String getReportClass() {
		return reportClass;
	}
}


