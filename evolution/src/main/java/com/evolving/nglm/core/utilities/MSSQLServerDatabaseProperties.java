/*****************************************************************************
*
*  MSSQLServerDatabaseProperties.java
*
*****************************************************************************/

package com.evolving.nglm.core.utilities;

import com.evolving.nglm.core.utilities.ConnectionPool.DBException;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class MSSQLServerDatabaseProperties extends DatabaseProperties
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
    if (instance == null) instance = new MSSQLServerDatabaseProperties();
    return instance;
  }

  //
  //  constructor
  //

  protected MSSQLServerDatabaseProperties()
  {
    super();
  }
  
  /****************************************
  *
  *  classifyDatabaseSpecificSQLException
  *
  ****************************************/

  private static Map<Integer,DBException> msSqlServerExceptionMap = new HashMap<Integer,DBException>();
  static
  {
    msSqlServerExceptionMap.put(new Integer(2627),  DBException.DuplicateInsert);
    msSqlServerExceptionMap.put(new Integer(14355), DBException.Busy);
    msSqlServerExceptionMap.put(new Integer(1205),  DBException.Deadlock);
    msSqlServerExceptionMap.put(new Integer(208),   DBException.TableDoesNotExist);
    msSqlServerExceptionMap.put(new Integer(18450), DBException.LoginDenied);
    msSqlServerExceptionMap.put(new Integer(18451), DBException.LoginDenied);
    msSqlServerExceptionMap.put(new Integer(18452), DBException.LoginDenied);
    msSqlServerExceptionMap.put(new Integer(18456), DBException.LoginDenied);
    msSqlServerExceptionMap.put(new Integer(18457), DBException.LoginDenied);
    msSqlServerExceptionMap.put(new Integer(18458), DBException.LoginDenied);
    msSqlServerExceptionMap.put(new Integer(18459), DBException.LoginDenied);
    msSqlServerExceptionMap.put(new Integer(18460), DBException.LoginDenied);
    msSqlServerExceptionMap.put(new Integer(18461), DBException.LoginDenied);
    msSqlServerExceptionMap.put(new Integer(17302), DBException.ConnectionFailed);
    msSqlServerExceptionMap.put(new Integer(2603),  DBException.TablespaceFull);
  }

  @Override public DBException classifyDatabaseSpecificSQLException(SQLException e)
  {
    DBException result = null;
    SQLException currentException = e;
    while (result == null && currentException != null)
      {
        result = msSqlServerExceptionMap.get(currentException.getErrorCode());
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
        case java.sql.Types.VARCHAR: return java.sql.Types.VARCHAR;
        case java.sql.Types.INTEGER: return java.sql.Types.DECIMAL;
        case java.sql.Types.BIT:     return java.sql.Types.DECIMAL;
        case java.sql.Types.DECIMAL: return java.sql.Types.DECIMAL;
        case java.sql.Types.TIMESTAMP: return java.sql.Types.TIMESTAMP;
        case java.sql.Types.BLOB:    return java.sql.Types.VARBINARY;
        case java.sql.Types.CHAR:    return java.sql.Types.CHAR;
                                       
        default:
          throw new RuntimeException();
      }
  }

  /****************************************
  *
  *  superclass
  *
  ****************************************/

  @Override public String getDatabaseDriverClassname() { return "com.microsoft.sqlserver.jdbc.SQLServerDriver"; }
  @Override public boolean supportsDBSpecificBatching() { return false; }
  @Override public boolean implementsBatchUpdateException() { return false; }
  @Override public boolean supportsResultSetCallableStatement() { return true; }
  @Override public boolean callableStatementRequiresOutParameter() { return false; }
  @Override public boolean requiresCommitAfterQuery() { return false; }
}
