/*****************************************************************************
*
*  MySQLDatabaseProperties.java
*
*****************************************************************************/

package com.evolving.nglm.core.utilities;

import com.evolving.nglm.core.utilities.ConnectionPool.DBException;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class MySQLDatabaseProperties extends DatabaseProperties
{
  /*****************************************
  *
  *  singleton
  *
  *****************************************/
  
  //
  //  singleton
  //

  private static DatabaseProperties instance = null;

  //
  //  getInstance
  //

  public static DatabaseProperties getInstance()
  {
    if (instance == null) instance = new MySQLDatabaseProperties();
    return instance;
  }

  //
  //  constructor
  //

  protected MySQLDatabaseProperties()
  {
    super();
  }
  
  /****************************************
  *
  *  classifyDatabaseSpecificSQLException
  *
  ****************************************/

  private static Map<Integer,DBException> mySqlExceptionMap = new HashMap<Integer,DBException>();
  static
  {
    mySqlExceptionMap.put(new Integer(1062),DBException.DuplicateInsert);
    mySqlExceptionMap.put(new Integer(1205),DBException.Busy);
    mySqlExceptionMap.put(new Integer(1040),DBException.ConnectionFailed);
    mySqlExceptionMap.put(new Integer(1049),DBException.ConnectionFailed);
    mySqlExceptionMap.put(new Integer(1129),DBException.ConnectionFailed);
    mySqlExceptionMap.put(new Integer(1152),DBException.ConnectionFailed);
    mySqlExceptionMap.put(new Integer(1184),DBException.ConnectionFailed);
    mySqlExceptionMap.put(new Integer(1203),DBException.ConnectionFailed);
    mySqlExceptionMap.put(new Integer(2002),DBException.ConnectionFailed);
    mySqlExceptionMap.put(new Integer(2013),DBException.ConnectionFailed);
    mySqlExceptionMap.put(new Integer(2048),DBException.ConnectionFailed);
    mySqlExceptionMap.put(new Integer(1114),DBException.TablespaceFull);
    mySqlExceptionMap.put(new Integer(1213),DBException.Deadlock);
  }

  @Override public DBException classifyDatabaseSpecificSQLException(SQLException e)
  {
    DBException result = null;

    //
    //  use error code first - iterate through all SQLExceptions
    //

    SQLException currentException = e;
    while (result == null && currentException != null)
      {
        result = mySqlExceptionMap.get(currentException.getErrorCode());
        currentException = currentException.getNextException();
      }
    
    //
    //  next use error state
    //

    currentException = e;
    while (result == null && currentException != null)
      {
        //
        // 08S01 - communications error
        //

        if (currentException.getSQLState().equals("08S01"))
          {
            result = DBException.ConnectionFailed;
          }

        currentException = currentException.getNextException();
      }

    return result;
  }

  /****************************************
  *
  *  sqlToDatabaseSpecificTypeMapping
  *
  ****************************************/

  @Override public int sqlToDatabaseSpecificTypeMapping(int sqlDataType)
  {
    switch (sqlDataType)
      {
        case java.sql.Types.VARCHAR:   return java.sql.Types.VARCHAR;
        case java.sql.Types.INTEGER:   return java.sql.Types.INTEGER;
        case java.sql.Types.BIT:       return java.sql.Types.BIT;
        case java.sql.Types.DECIMAL:   return java.sql.Types.DECIMAL;
        case java.sql.Types.TIMESTAMP: return java.sql.Types.TIMESTAMP;
        case java.sql.Types.BLOB:      return java.sql.Types.BLOB;
        case java.sql.Types.CHAR:      return java.sql.Types.CHAR;
                                       
        default:
          throw new RuntimeException();
      }
  }

  /****************************************
  *
  *  superclass
  *
  ****************************************/

  @Override public String getDatabaseDriverClassname() { return "com.mysql.jdbc.Driver"; }
  @Override public boolean supportsDBSpecificBatching() { return false; }
  @Override public boolean implementsBatchUpdateException() { return false; }
  @Override public boolean supportsResultSetCallableStatement() { return false; }
  @Override public boolean callableStatementRequiresOutParameter() { return false; }
  @Override public boolean requiresCommitAfterQuery() { return true; }
}
