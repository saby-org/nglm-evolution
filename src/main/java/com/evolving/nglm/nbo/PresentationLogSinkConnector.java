/****************************************************************************
*
*  PresentationLogSinkConnector.java
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

public class PresentationLogSinkConnector extends SimpleJDBCSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return PresentationLogSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class PresentationLogSinkTask extends SimpleJDBCSinkTask
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
      *  extract PresentationLog
      *
      ****************************************/

      Object presentationLogValue = sinkRecord.value();
      Schema presentationLogValueSchema = sinkRecord.valueSchema();
      PresentationLog presentationLog = PresentationLog.unpack(new SchemaAndValue(presentationLogValueSchema, presentationLogValue));
          
      /****************************************
      *
      *  prepare statement
      *
      ****************************************/

      try
        {
          preparedStatement.clearParameters();
          DatabaseUtilities.setLong(preparedStatement, 1, Long.parseLong(presentationLog.getSubscriberID()));
          DatabaseUtilities.setLong(preparedStatement, 2, Long.parseLong(presentationLog.getOfferID()));
          DatabaseUtilities.setInteger(preparedStatement, 3, Integer.parseInt(presentationLog.getChannelID()));
          DatabaseUtilities.setLong(preparedStatement, 4, (presentationLog.getUserID() != null) ? Long.parseLong(presentationLog.getUserID()) : null);
          DatabaseUtilities.setString(preparedStatement, 5, dateTimeOffsetFormat.format(presentationLog.getEventDate()));
          DatabaseUtilities.setInteger(preparedStatement, 6, presentationLog.getPosition());
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
