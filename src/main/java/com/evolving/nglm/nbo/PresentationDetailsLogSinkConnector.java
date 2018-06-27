/****************************************************************************
*
*  PresentationDetailsLogSinkConnector.java
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

public class PresentationDetailsLogSinkConnector extends SimpleJDBCSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return PresentationDetailsLogSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class PresentationDetailsLogSinkTask extends SimpleJDBCSinkTask
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
      *  extract PresentationDetailsLog
      *
      ****************************************/

      Object presentationDetailsLogValue = sinkRecord.value();
      Schema presentationDetailsLogValueSchema = sinkRecord.valueSchema();
      PresentationDetailsLog presentationDetailsLog = PresentationDetailsLog.unpack(new SchemaAndValue(presentationDetailsLogValueSchema, presentationDetailsLogValue));
          
      /****************************************
      *
      *  prepare statement
      *
      ****************************************/

      try
        {
          preparedStatement.clearParameters();
          DatabaseUtilities.setLong(preparedStatement, 1, Long.parseLong(presentationDetailsLog.getSubscriberID()));
          DatabaseUtilities.setInteger(preparedStatement, 2, Integer.parseInt(presentationDetailsLog.getChannelID()));
          DatabaseUtilities.setLong(preparedStatement, 3, Long.parseLong(presentationDetailsLog.getOfferID()));
          DatabaseUtilities.setLong(preparedStatement, 4, (presentationDetailsLog.getUserID() != null) ? Long.parseLong(presentationDetailsLog.getUserID()) : null);
          DatabaseUtilities.setString(preparedStatement, 5, dateTimeOffsetFormat.format(presentationDetailsLog.getEventDate()));
          DatabaseUtilities.setInteger(preparedStatement, 6, presentationDetailsLog.getPosition());
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
