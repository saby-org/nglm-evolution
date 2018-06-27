/****************************************************************************
*
*  AcceptanceLogSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.SimpleJDBCSinkConnector;
import com.evolving.nglm.core.SimpleJDBCSinkTask;

import com.rii.utilities.DatabaseUtilities;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TimeZone;

public class AcceptanceLogSinkConnector extends SimpleJDBCSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return AcceptanceLogSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class AcceptanceLogSinkTask extends SimpleJDBCSinkTask
  {
    //
    //  attributes
    //

    SimpleDateFormat dateTimeOffsetFormat;

    //
    //  start
    //
    
    @Override public void start(Map<String, String> taskConfig)
    {
      super.start(taskConfig);

      //
      // set up date format for datetimeoffset fields
      //

      dateTimeOffsetFormat = new SimpleDateFormat(Deployment.getSqlServerDateTimeOffsetFormat());
      dateTimeOffsetFormat.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
    }

    //
    //  prepareStatement
    //
    
    @Override public void prepareStatement(PreparedStatement preparedStatement, SinkRecord sinkRecord)
    {
      /****************************************
      *
      *  extract AcceptanceLog
      *
      ****************************************/

      Object acceptanceLogValue = sinkRecord.value();
      Schema acceptanceLogValueSchema = sinkRecord.valueSchema();
      AcceptanceLog acceptanceLog = AcceptanceLog.unpack(new SchemaAndValue(acceptanceLogValueSchema, acceptanceLogValue));
          
      /****************************************
      *
      *  prepare statement
      *
      ****************************************/

      try
        {
          preparedStatement.clearParameters();
          DatabaseUtilities.setLong(preparedStatement, 1, Long.parseLong(acceptanceLog.getTransactionID()));
          DatabaseUtilities.setLong(preparedStatement, 2, Long.parseLong(acceptanceLog.getSubscriberID()));
          DatabaseUtilities.setInteger(preparedStatement, 3, Integer.parseInt(acceptanceLog.getChannelID()));
          DatabaseUtilities.setLong(preparedStatement, 4, Long.parseLong(acceptanceLog.getOfferID()));
          DatabaseUtilities.setLong(preparedStatement, 5, (acceptanceLog.getUserID() != null) ? Long.parseLong(acceptanceLog.getUserID()) : null);
          DatabaseUtilities.setString(preparedStatement, 6, dateTimeOffsetFormat.format(acceptanceLog.getEventDate()));
          DatabaseUtilities.setString(preparedStatement, 7, (acceptanceLog.getFulfilledDate() != null) ? dateTimeOffsetFormat.format(acceptanceLog.getFulfilledDate()) : null);
          DatabaseUtilities.setInteger(preparedStatement, 8, acceptanceLog.getPosition());
          preparedStatement.addBatch();
        }
      catch (SQLException e)
        {
          log.error("Error processing sinkRecord: {}", sinkRecord);
          log.error(e.toString());
          throw new ConnectException(e);
        }
    }
  }
}
