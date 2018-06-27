/****************************************************************************
*
*  ExternalAggregatesFileSourceConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.ExternalAggregates.SubscriberStatus;

import com.evolving.nglm.core.FileSourceConnector;
import com.evolving.nglm.core.FileSourceTask;
import com.evolving.nglm.core.FileSourceTask.FileSourceTaskException;
import com.evolving.nglm.core.FileSourceTask.KeyValue;
import com.evolving.nglm.core.SubscriberIDService;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.DecimalFormatSymbols;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ExternalAggregatesFileSourceConnector extends FileSourceConnector
{
  /****************************************
  *
  *  attributes
  *
  ****************************************/

  private static SubscriberIDService subscriberIDService = null;
  
  /****************************************
  *
  *  taskClass
  *
  ****************************************/

  @Override public Class<? extends Task> taskClass()
  {
    return ExternalAggregatesFileSourceTask.class;
  }

  /*****************************************
  *
  *  class ExternalAggregatesFileSourceTask
  *
  *****************************************/

  public static class ExternalAggregatesFileSourceTask extends FileSourceTask
  {
    /*****************************************
    *
    *  config
    *
    *****************************************/

    //
    //  logger
    //

    private static final Logger log = LoggerFactory.getLogger(ExternalAggregatesFileSourceConnector.class);

    /*****************************************
    *
    *  start
    *
    *****************************************/

    @Override public void start(Map<String, String> properties)
    {
      //
      //  super
      //
      
      super.start(properties);

      //
      //  subscriberIDService
      //

      subscriberIDService = new SubscriberIDService(Deployment.getRedisSentinels());
    }

    /*****************************************
    *
    *  stop
    *
    *****************************************/

    @Override public void stop()
    {
      //
      //  subscriberIDService
      //
      
      subscriberIDService.close();
      
      //
      //  super
      //

      super.stop();
    }
    
    /*****************************************
    *
    *  processRecord
    *
    *****************************************/

    @Override protected List<KeyValue> processRecord(String record) throws FileSourceTaskException
    {
      try
        {
          /*****************************************
          *
          *  parseRecord
          *
          *****************************************/

          //
          //  parse
          //

          String[] tokens = record.split("[|]");

          //
          //  verify minimum number of fields
          //

          if (tokens.length < 117)
            {
              if (record.trim().equals("HEADER_STR"))
                {
                  return Collections.<KeyValue>emptyList();
                }
              else
                {
                  log.info("processRecord unknown record format: {}", record);
                  throw new FileSourceTaskException("bad fields in record");
                }
            }

          //
          //  msisdn
          //

          String msisdn = readString(tokens[1], record);
          if (msisdn == null)
            {
              log.info("processRecord empty msisdn: {}", record);
              throw new FileSourceTaskException("empty msisdn");
            }

          //
          //  eventDate
          //

          Date eventDate = readDate(tokens[0], record, "dd-MMM-yy");
          if (eventDate == null)
            {
              log.info("processRecord empty eventDate: {}", record);
              throw new FileSourceTaskException("empty eventDate");
            }

          //
          //  accountType
          //

          String accountType = readString(tokens[3], record);
          if (accountType == null)
            {
              log.info("processRecord empty accountType: {}", record);
              throw new FileSourceTaskException("empty accountType");
            }

          //
          //  primaryTariffPlan
          //

          String primaryTariffPlan = readString(tokens[4], record);
          if (primaryTariffPlan == null)
            {
              log.info("processRecord empty primaryTariffPlan: {}", record);
              throw new FileSourceTaskException("empty primaryTariffPlan");
            }

          //
          //  activationDate
          //

          Date activationDate = readDate(tokens[5], record, "dd-MMM-yy");
          if (activationDate == null)
            {
              log.info("processRecord empty activationDate: {}", record);
              throw new FileSourceTaskException("empty activationDate");
            }

          //
          //  subscriberStatus
          //

          String subscriberStatusString = readString(tokens[6], record);
          SubscriberStatus subscriberStatus = (subscriberStatusString != null) ? SubscriberStatus.fromExternalRepresentation(subscriberStatusString) : null;
          if (subscriberStatus == null)
            {
              log.info("processRecord empty subscriberStatus: {}", record);
              throw new FileSourceTaskException("empty subscriberStatus");
            }
          if (subscriberStatus == SubscriberStatus.Unknown)
            {
              log.info("processRecord unknown subscriberStatus {}: {}", subscriberStatusString, record);
              throw new FileSourceTaskException("bad subscriberStatus " + subscriberStatusString);
            }

          //
          //  daysInCurrentStatus
          //

          Integer daysInCurrentStatus = readInteger(tokens[7], record);
          if (daysInCurrentStatus == null)
            {
              log.info("processRecord empty daysInCurrentStatus: {}", record);
              throw new FileSourceTaskException("empty daysInCurrentStatus");
            }

          //
          //  previousSubscriberStatus
          //

          String previousSubscriberStatusString = readString(tokens[8], record);
          SubscriberStatus previousSubscriberStatus = (previousSubscriberStatusString != null) ? SubscriberStatus.fromExternalRepresentation(previousSubscriberStatusString) : null;
          if (previousSubscriberStatus == SubscriberStatus.Unknown)
            {
              log.info("processRecord unknown previousSubscriberStatus {}: {}", previousSubscriberStatusString, record);
              throw new FileSourceTaskException("bad previousSubscriberStatus " + previousSubscriberStatusString);
            }
          
          //
          //  lastRechargeDate
          //

          Date lastRechargeDate = readDate(tokens[11], record, "dd-MMM-yy");

          //
          //  tariffPlanChangeDate
          //

          Date tariffPlanChangeDate = readDate(tokens[13], record, "dd-MMM-yy");

          //
          //  mainBalanceValue
          //

          Long mainBalanceValue = readCurrency(tokens[14], record, 0L);
          
          //
          //  totalCharge
          //

          Long totalCharge = readCurrency(tokens[21], record, 0L);
          
          //
          //  dataCharge
          //

          Long dataCharge = readCurrency(tokens[22], record, 0L);
          
          //
          //  callsCharge
          //

          Long callsCharge = readCurrency(tokens[23], record, 0L);
          
          //
          //  rechargeCharge
          //

          Long rechargeCharge = readCurrency(tokens[24], record, 0L);
          
          //
          //  rechargeCount
          //

          Integer rechargeCount = readInteger(tokens[25], record, 0);
          
          //
          //  moCallsCharge
          //

          Long moCallsCharge = readCurrency(tokens[26], record, 0L);

          //
          //  moCallsCount
          //

          Integer moCallsCount = readInteger(tokens[27], record, 0);

          //
          //  moCallsDuration
          //

          Integer moCallsDuration = readInteger(tokens[28], record, 0);

          //
          //  mtCallsCount
          //

          Integer mtCallsCount = readInteger(tokens[38], record, 0);

          //
          //  mtCallsDuration
          //

          Integer mtCallsDuration = readInteger(tokens[42], record, 0);

          //
          //  mtCallsIntCount
          //

          Integer mtCallsIntCount = readInteger(tokens[46], record, 0);

          //
          //  mtCallsIntDuration
          //

          Integer mtCallsIntDuration = readInteger(tokens[47], record, 0);

          //
          //  moCallsIntCharge
          //

          Long moCallsIntCharge = readCurrency(tokens[62], record, 0L);

          //
          //  moCallsIntCount
          //

          Integer moCallsIntCount = readInteger(tokens[63], record, 0);

          //
          //  moCallsIntDuration
          //

          Integer moCallsIntDuration = readInteger(tokens[64], record, 0);

          //
          //  moSMSCharge
          //

          Long moSMSCharge = readCurrency(tokens[95], record, 0L);

          //
          //  moSMSCount
          //

          Integer moSMSCount = readInteger(tokens[96], record, 0);

          //
          //  dataVolume
          //

          Long dataVolume = readDecimalTruncateToLong(tokens[103], record, 0L);

          //
          //  dataBundleCharge
          //

          Long dataBundleCharge = readCurrency(tokens[111], record, 0L);

          //
          //  subscriberRegion
          //

          String subscriberRegion = readString(tokens[116], record);
          
          /*****************************************
          *
          *  look up subscriberID
          *
          *****************************************/

          String subscriberID = resolveSubscriberID(msisdn);
          
          /*****************************************
          *
          *  externalAggregates
          *
          *****************************************/

          ExternalAggregates externalAggregates = new ExternalAggregates((subscriberID != null ? subscriberID : msisdn), eventDate, accountType, primaryTariffPlan, activationDate, subscriberStatus, daysInCurrentStatus, previousSubscriberStatus, lastRechargeDate, tariffPlanChangeDate, mainBalanceValue, totalCharge, dataCharge, callsCharge, rechargeCharge, rechargeCount, moCallsCharge, moCallsCount, moCallsDuration, mtCallsCount, mtCallsDuration, mtCallsIntCount, mtCallsIntDuration, moCallsIntCharge, moCallsIntCount, moCallsIntDuration, moSMSCharge, moSMSCount, dataVolume, dataBundleCharge, subscriberRegion);

          /*****************************************
          *
          *  sourceRecord
          *
          *****************************************/

          if (!getStopRequested())
            return Collections.<KeyValue>singletonList(new KeyValue((subscriberID != null ? Deployment.getExternalAggregatesTopic() : Deployment.getExternalAggregatesAssignSubscriberIDTopic()), Schema.STRING_SCHEMA, (subscriberID != null ? subscriberID : msisdn), ExternalAggregates.schema(), ExternalAggregates.pack(externalAggregates)));
          else
            return Collections.<KeyValue>emptyList();
        }
      catch (RuntimeException e)
        {
          StringWriter stackTraceWriter = new StringWriter();
          e.printStackTrace(new PrintWriter(stackTraceWriter, true));
          log.error("Unexpected exception processing record: {}", record);
          log.error("External Aggregates error: {}", stackTraceWriter.toString());
          throw new FileSourceTaskException("UnexpectedException: " + e.getMessage(), e);
        }
    }

    /****************************************
    *
    *  resolveSubscriberID
    *
    ****************************************/

    private String resolveSubscriberID(String msisdn)
    {
      String result = null;
      while (!getStopRequested())
        {
          try
            {
              result = subscriberIDService.getSubscriberID("msisdn", msisdn);
              break;
            }
          catch (SubscriberIDService.SubscriberIDServiceException e)
            {
              //
              // sleep before retry
              //

              synchronized (this)
                {
                  if (! getStopRequested())
                    {
                      try
                        {
                          this.wait(10*1000L);
                        }
                      catch (InterruptedException e1)
                        {
                          // ignore
                        }
                    }
                }
            }
        }
      return result;
    }
    
    /****************************************
    *
    *  read utilities
    *
    ****************************************/

    //
    //  readDecimalTruncateToLong
    //

    final DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols(Locale.US);
    private Long readDecimalTruncateToLong(String token, String record, Long defaultValue) throws IllegalArgumentException
    {
      Long result = defaultValue;
      if (token != null && token.trim().length() > 0)
        {
          try
            {
              DecimalFormat decimalFormat = new DecimalFormat();
              decimalFormat.setDecimalFormatSymbols(decimalFormatSymbols);
              result = decimalFormat.parse(token.trim().replace(',','.')).longValue();
            }
          catch (ParseException|ClassCastException e)
            {
              log.info("processRecord unparsable decimal field {} in {}", token, record);
              throw new IllegalArgumentException(e);
            }
        }
      return result;
    }

    //
    //  readCurrency
    //

    private Long readCurrency(String token, String record, Long defaultValue) throws IllegalArgumentException
    {
      Long result = defaultValue;
      if (token != null && token.trim().length() > 0)
        {
          try
            {
              DecimalFormat decimalFormat = new DecimalFormat();
              decimalFormat.setDecimalFormatSymbols(decimalFormatSymbols);
              result = Math.round(decimalFormat.parse(token.trim().replace(',','.')).doubleValue() * 100.0);
            }
          catch (ParseException|ClassCastException e)
            {
              log.info("processRecord unparsable currency field {} in {}", token, record);
              throw new IllegalArgumentException(e);
            }
        }
      return result;
    }
  }
}
