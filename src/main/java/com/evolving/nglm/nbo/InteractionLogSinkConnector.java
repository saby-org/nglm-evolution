/****************************************************************************
*
*  InteractionLogSinkConnector.java
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

public class InteractionLogSinkConnector extends SimpleJDBCSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return InteractionLogSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class InteractionLogSinkTask extends SimpleJDBCSinkTask
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
      *  extract InteractionLog
      *
      ****************************************/

      Object interactionLogValue = sinkRecord.value();
      Schema interactionLogValueSchema = sinkRecord.valueSchema();
      InteractionLog interactionLog = InteractionLog.unpack(new SchemaAndValue(interactionLogValueSchema, interactionLogValue));
          
      /****************************************
      *
      *  prepare statement
      *
      ****************************************/

      try
        {
          preparedStatement.clearParameters();
          DatabaseUtilities.setLong(preparedStatement, 1, Long.parseLong(interactionLog.getSubscriberID()));
          DatabaseUtilities.setInteger(preparedStatement, 2, Integer.parseInt(interactionLog.getChannelID()));
          DatabaseUtilities.setLong(preparedStatement, 3, (interactionLog.getUserID() != null) ? Long.parseLong(interactionLog.getUserID()) : null);
          DatabaseUtilities.setString(preparedStatement, 4, dateTimeOffsetFormat.format(interactionLog.getEventDate()));
          DatabaseUtilities.setBoolean(preparedStatement, 5, interactionLog.getIsAvailable());
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
