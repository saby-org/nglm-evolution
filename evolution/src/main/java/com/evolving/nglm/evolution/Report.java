/*****************************************************************************
*
*  Offer.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.OfferCallingChannel.OfferCallingChannelProperty;
import com.evolving.nglm.evolution.StockMonitor.StockableItem;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import com.evolving.nglm.core.JSONUtilities;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;


import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class Report extends GUIManagedObject 
{
	// Following fields need to be aligned with ReportConfiguration fields (for initial populating)
	public static final String REPORT_DESCRIPTION = "reportDescription";
	public static final String REPORT_DISPLAY_NAME = "reportDisplayName";
	public static final String REPORT_NAME = "reportName";
	public static final String AVAILABLE_SCHEDULING = "availableScheduling";
	public static final String EFFECTIVE_SCHEDULING = "effectiveScheduling";

	@Override
	public String toString() {
		return "Report ["
			+"reportID="+getReportID()
			+", "+REPORT_NAME+"="+getName()
			+", "+REPORT_DISPLAY_NAME+"="+getDisplayName()
			+", "+REPORT_DESCRIPTION+"="+getDescription()
			+", "+EFFECTIVE_SCHEDULING+"="+getEffectiveScheduling()
			+", "+AVAILABLE_SCHEDULING+"="+getPossibleScheduling()
			+", epoch=" + getEpoch()
			+", active=" + getActive()
			+", accepted=" + getAccepted()
			+", effectiveStartDate=" + getEffectiveStartDate()
			+", effectiveEndDate=" + getEffectiveEndDate()
			+", JSONRepresentation=" + getJSONRepresentation()
			+ "]";
	}


	public enum SchedulingInterval {
        MONTHLY,
        WEEKLY,
        DAILY,
        HOURLY,
        /**
         * Special value indicating anything is allowed
         */
        ANY,
		/**
		 * Initial value
		 */
        NONE
	}
        
  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema schema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("report");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field(REPORT_NAME, Schema.STRING_SCHEMA);
    schemaBuilder.field(REPORT_DISPLAY_NAME, Schema.STRING_SCHEMA);
    schemaBuilder.field(REPORT_DESCRIPTION, Schema.STRING_SCHEMA);
    schemaBuilder.field(EFFECTIVE_SCHEDULING, Schema.STRING_SCHEMA); // String representation of SchedulingInterval
    schemaBuilder.field(AVAILABLE_SCHEDULING, Schema.STRING_SCHEMA); // String representation of SchedulingInterval
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<Report> serde = new ConnectSerde<Report>(schema, false, Report.class, Report::pack, Report::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<Report> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String effectiveScheduling = SchedulingInterval.NONE.name();
  private String possibleScheduling = SchedulingInterval.ANY.name(); // String separated with "|"
  private String name, displayName = "", description = "";

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //

  public String getReportID() { return getGUIManagedObjectID(); }
  public String getName() { return name; }
  public String getDisplayName() { return displayName; }
  public String getDescription() { return description; }
  public String getEffectiveScheduling() { return effectiveScheduling; }
  public String getPossibleScheduling() { return possibleScheduling; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Report(SchemaAndValue schemaAndValue, String name, String displayName, String description, String effectiveScheduling, String possibleScheduling)
  {
    super(schemaAndValue);
    this.name = name;
    this.displayName = displayName;
    this.description = description;
    this.effectiveScheduling = effectiveScheduling;
    this.possibleScheduling = possibleScheduling;
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public Report(JSONObject jsonRoot, long epoch, GUIManagedObject existingReportUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingReportUnchecked != null) ? existingReportUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingReport
    *
    *****************************************/

    Report existingReport = (existingReportUnchecked != null && existingReportUnchecked instanceof Report) ? (Report) existingReportUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/
    
    this.name = JSONUtilities.decodeString(jsonRoot, REPORT_NAME, true);
    this.displayName = JSONUtilities.decodeString(jsonRoot, REPORT_DISPLAY_NAME, false);
    this.description = JSONUtilities.decodeString(jsonRoot, REPORT_DESCRIPTION, false);
    this.effectiveScheduling = JSONUtilities.decodeString(jsonRoot, EFFECTIVE_SCHEDULING, false);
    this.possibleScheduling = JSONUtilities.decodeString(jsonRoot, AVAILABLE_SCHEDULING, false);

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingReport))
      {
        this.setEpoch(epoch);
      }
  }
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    Report report = (Report) value;
    Struct struct = new Struct(schema);
    packCommon(struct, report);
    struct.put(REPORT_NAME, report.getName());
    struct.put(REPORT_DISPLAY_NAME, report.getDisplayName());
    struct.put(REPORT_DESCRIPTION, report.getDescription());
    struct.put(EFFECTIVE_SCHEDULING, report.getEffectiveScheduling());
    struct.put(AVAILABLE_SCHEDULING, report.getPossibleScheduling());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static Report unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String name = valueStruct.getString(REPORT_NAME);
    String displayName = valueStruct.getString(REPORT_DISPLAY_NAME);
    String description = valueStruct.getString(REPORT_DESCRIPTION);
    String effectiveScheduling = valueStruct.getString(EFFECTIVE_SCHEDULING);
    String possibleScheduling = valueStruct.getString(AVAILABLE_SCHEDULING);

    //
    //  return
    //

    return new Report(schemaAndValue, name, displayName, description, effectiveScheduling, possibleScheduling);
  }


  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(Report existingReport)
  {
    if (existingReport != null && existingReport.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingReport.getGUIManagedObjectID());
        epochChanged = epochChanged || ! (name.equals(existingReport.getName()));
        epochChanged = epochChanged || ! (displayName.equals(existingReport.getDisplayName()));
        epochChanged = epochChanged || ! (description.equals(existingReport.getDescription()));
        epochChanged = epochChanged || ! (effectiveScheduling == existingReport.getEffectiveScheduling());
        epochChanged = epochChanged || ! (possibleScheduling == existingReport.getPossibleScheduling());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }

}
