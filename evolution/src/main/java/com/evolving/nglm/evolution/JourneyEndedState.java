package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.DeploymentCommon;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SchemaUtilities;
import org.apache.kafka.connect.data.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Date;

public class JourneyEndedState
{

  private static final Logger log = LoggerFactory.getLogger(JourneyEndedState.class);

  private static Schema schema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("journey_ended_state");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("journeyInstanceID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyExitDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("journeyMetricsPrior", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).name("journeystate_metrics_prior").schema());
    schemaBuilder.field("journeyMetricsDuring", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).name("journeystate_metrics_during").schema());
    schema = schemaBuilder.build();
  }

  private static ConnectSerde<JourneyEndedState> serde = new ConnectSerde<JourneyEndedState>(schema, false, JourneyEndedState.class, JourneyEndedState::pack, JourneyEndedState::unpack);
  public static Schema schema() { return schema; }
  public static ConnectSerde<JourneyEndedState> serde() { return serde; }

  //stored
  private String journeyInstanceID;
  private String journeyID;
  private Date journeyExitDate;
  private Map<String,Long> journeyMetricsPrior;
  private Map<String,Long> journeyMetricsDuring;
  //not stored
  private Map<String,Long> journeyMetricsPost;

  //stored
  public String getJourneyInstanceID() { return journeyInstanceID; }
  public String getJourneyID() { return journeyID; }
  public Date getJourneyExitDate() { return journeyExitDate; }
  public Map<String,Long> getJourneyMetricsPrior() { return journeyMetricsPrior; }
  public Map<String,Long> getJourneyMetricsDuring() { return journeyMetricsDuring; }
  //no stored
  public Map<String,Long> getJourneyMetricsPost() { return journeyMetricsPost; }

  // for old version unpack
  public JourneyEndedState(String journeyInstanceID, String journeyID, Date journeyExitDate, Map<String,Long> journeyMetricsPrior, Map<String,Long> journeyMetricsDuring, Map<String,Long> journeyMetricsPost)
  {
    this.journeyInstanceID = journeyInstanceID;
    this.journeyID = journeyID;
    this.journeyExitDate = journeyExitDate;
    this.journeyMetricsPrior = journeyMetricsPrior;
    this.journeyMetricsDuring = journeyMetricsDuring;
    this.journeyMetricsPost = journeyMetricsPost;
  }
  // for new unpack
  public JourneyEndedState(String journeyInstanceID, String journeyID, Date journeyExitDate, Map<String,Long> journeyMetricsPrior, Map<String,Long> journeyMetricsDuring)
  {
    this(journeyInstanceID,journeyID,journeyExitDate,journeyMetricsPrior,journeyMetricsDuring,new HashMap<>());
  }
  // for new instance
  public JourneyEndedState(String journeyInstanceID, String journeyID)
  {
    this(journeyInstanceID,journeyID,(Date)null,new HashMap<>(),new HashMap<>());
  }

  public static Object pack(Object value)
  {
    JourneyEndedState journeyEndedState = (JourneyEndedState) value;
    Struct struct = new Struct(schema);
    struct.put("journeyInstanceID", journeyEndedState.getJourneyInstanceID());
    struct.put("journeyID", journeyEndedState.getJourneyID());
    struct.put("journeyExitDate", journeyEndedState.getJourneyExitDate());
    struct.put("journeyMetricsPrior", journeyEndedState.getJourneyMetricsPrior());
    struct.put("journeyMetricsDuring", journeyEndedState.getJourneyMetricsDuring());
    return struct;
  }

  public static JourneyEndedState unpack(SchemaAndValue schemaAndValue)
  {

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;
    Struct valueStruct = (Struct) value;

    String journeyInstanceID = valueStruct.getString("journeyInstanceID");
    String journeyID = valueStruct.getString("journeyID");
    Date journeyExitDate = (Date) valueStruct.get("journeyExitDate");
    Map<String,Long> journeyMetricsPrior = (Map<String,Long>) valueStruct.get("journeyMetricsPrior");
    Map<String,Long> journeyMetricsDuring = (Map<String,Long>) valueStruct.get("journeyMetricsDuring");

    return new JourneyEndedState(journeyInstanceID, journeyID, journeyExitDate, journeyMetricsPrior, journeyMetricsDuring);

  }

  /**
   * @return true if subscriber state has been updated
   */
  public boolean setJourneyExitDate(Date journeyExitDate, SubscriberState subscriberState, Journey journey, EvolutionEngine.EvolutionEventContext context)
  {
    this.journeyExitDate = journeyExitDate;

    //
    // check if JourneyMetrics enabled: Metrics should be generated for campaigns only (not journeys nor bulk campaigns)
    //

    if(journey == null) {
      return false;
    }
    else if (journey.journeyMetricsNeeded()) {
      boolean statusUpdated = this.populateMetricsDuring(subscriberState);

      // Create a JourneyMetric to be added to JourneyStatistic from journeyState
      subscriberState.getJourneyMetrics().add(new JourneyMetric(context, subscriberState.getSubscriberID(), this));

      return statusUpdated;
    }
    else {
      return false;
    }
  }

  /**
   * populate journeyMetrics (prior and "during")
   * @return true if subscriber state has been updated
   */
  public boolean populateMetricsPrior(SubscriberState subscriberState, Date journeyEntryDate, int tenantID)
  {
    if(!DeploymentCommon.getJourneyMetricConfiguration().isEnabled()) {
      return false;
    }

    boolean subscriberStateUpdated = false;
    Date journeyEntryDay = RLMDateUtils.truncate(journeyEntryDate, Calendar.DATE, Deployment.getDeployment(tenantID).getTimeZone());
    Date metricStartDay = RLMDateUtils.addDays(journeyEntryDay, -1 * Deployment.getJourneyMetricConfiguration().getPriorPeriodDays(), Deployment.getDeployment(tenantID).getTimeZone());
    Date metricEndDay = RLMDateUtils.addDays(journeyEntryDay, -1, Deployment.getDeployment(tenantID).getTimeZone());

    for (JourneyMetricDeclaration journeyMetricDeclaration : Deployment.getJourneyMetricConfiguration().getMetrics().values()) {
      MetricHistory metricHistory = journeyMetricDeclaration.getMetricHistory(subscriberState.getSubscriberProfile());
      long priorMetricValue = metricHistory.getValue(metricStartDay, metricEndDay);

      this.getJourneyMetricsPrior().put(journeyMetricDeclaration.getID(), priorMetricValue);

      //
      //  during (note:  at entry these are set to the "all-time-total" and will be fixed up when the journey ends
      //
      Long startMetricValue = metricHistory.getAllTimeBucket();
      this.getJourneyMetricsDuring().put(journeyMetricDeclaration.getID(), startMetricValue);
      subscriberStateUpdated = true;
    }

    return subscriberStateUpdated;
  }

  /**
   * populate journeyMetrics (during)
   * @return true if subscriber state has been updated
   */
  public boolean populateMetricsDuring(SubscriberState subscriberState)
  {
    if(!Deployment.getJourneyMetricConfiguration().isEnabled()) {
      return false;
    }

    boolean subscriberStateUpdated = false;
    for (JourneyMetricDeclaration journeyMetricDeclaration : Deployment.getJourneyMetricConfiguration().getMetrics().values()) {
      MetricHistory metricHistory = journeyMetricDeclaration.getMetricHistory(subscriberState.getSubscriberProfile());

      // Check for journey metrics added while the journey was running
      if(! this.getJourneyMetricsDuring().containsKey(journeyMetricDeclaration.getID())) {
        continue;
      }
      long startMetricValue = this.getJourneyMetricsDuring().get(journeyMetricDeclaration.getID());
      long endMetricValue = metricHistory.getAllTimeBucket();
      long duringMetricValue = endMetricValue - startMetricValue;
      this.getJourneyMetricsDuring().put(journeyMetricDeclaration.getID(), duringMetricValue);
      subscriberStateUpdated = true;
    }

    return subscriberStateUpdated;
  }

  /**
   * populate journeyMetrics (post)
   * @return true if subscriber state has been updated
   *         (WARNING: here we return true if the post date is in the past, even if there is no journeyMetric (and therefore
   *         no modification of SubscriberState), because it is needed by the caller)
   */
  public boolean populateMetricsPost(SubscriberState subscriberState, Date now, int tenantID)
  {
    if(!Deployment.getJourneyMetricConfiguration().isEnabled()) {
      return true; // Special case, because we want to close the journey
    }

    boolean subscriberStateUpdated = false;
    Date journeyExitDay = RLMDateUtils.truncate(this.getJourneyExitDate(), Calendar.DATE, Deployment.getDeployment(tenantID).getTimeZone());
    Date metricStartDay = RLMDateUtils.addDays(journeyExitDay, 1, Deployment.getDeployment(tenantID).getTimeZone());
    Date metricEndDay = RLMDateUtils.addDays(journeyExitDay, Deployment.getDeployment(tenantID).getJourneyMetricConfiguration().getPostPeriodDays(), Deployment.getDeployment(tenantID).getTimeZone());

    if (now.after(RLMDateUtils.addDays(metricEndDay, 1, Deployment.getDeployment(tenantID).getTimeZone()))) {
      subscriberStateUpdated = true;

      for (JourneyMetricDeclaration journeyMetricDeclaration : Deployment.getDeployment(tenantID).getJourneyMetricConfiguration().getMetrics().values()) {
        MetricHistory metricHistory = journeyMetricDeclaration.getMetricHistory(subscriberState.getSubscriberProfile());
        long postMetricValue = metricHistory.getValue(metricStartDay, metricEndDay);
        this.getJourneyMetricsPost().put(journeyMetricDeclaration.getID(), postMetricValue);
      }
    }

    return subscriberStateUpdated;
  }

  @Override
  public String toString()
  {
    return "JourneyEndedState [journeyInstanceID=" + getJourneyInstanceID() + ", journeyID=" + getJourneyID() + ", journeyExitDate=" + getJourneyExitDate() + ", journeyMetricsPrior=" + getJourneyMetricsPrior() + ", journeyMetricsDuring=" + getJourneyMetricsDuring() + ", journeyMetricsPost=" + getJourneyMetricsPost() + "]";
  }
  
}
