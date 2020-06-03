/*****************************************************************************
*
*  DatabaseProperties.java
*
*****************************************************************************/

package com.evolving.nglm.core.utilities;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public abstract class DatabaseProperties
{
  /*****************************************
  *
  *  getDatabaseProperties
  *
  *****************************************/

  //
  //  class methods
  //

  private static Map<String,DatabaseProperties> databasePropertiesMap = new HashMap<String,DatabaseProperties>();
  static
  {
    databasePropertiesMap.put("MySQL",       MySQLDatabaseProperties.getInstance());
    databasePropertiesMap.put("MSSQLServer", MSSQLServerDatabaseProperties.getInstance());
  }
  
  //
  //  getDatabaseProperties
  //

  public static DatabaseProperties getDatabaseProperties(String databaseProduct)
  {
    return databasePropertiesMap.get(databaseProduct);
  }

  /*****************************************
  *
  *  registerDatabaseProperties
  *
  *****************************************/

  public static void registerDatabaseProperties(String databaseProduct, DatabaseProperties databaseProperties)
  {
    databasePropertiesMap.put(databaseProduct, databaseProperties);
  }
  
  /*****************************************
  *
  *  classifySQLException
  *
  *****************************************/

  public final ConnectionPool.DBException classifySQLException(SQLException e)
  {
    ConnectionPool.DBException result = classifyDatabaseSpecificSQLException(e);
    return (result != null ? result : ConnectionPool.DBException.Unknown);
  }

  /*****************************************
  *
  *  abstract/overridable methods
  *
  *****************************************/

  public abstract String getDatabaseDriverClassname();
  public abstract ConnectionPool.DBException classifyDatabaseSpecificSQLException(SQLException e);
  public int sqlToDatabaseSpecificTypeMapping(int sqlDataType) { throw new UnsupportedOperationException("sqlToDatabaseSpecificTypeMapping not supported"); }
  public boolean supportsDBSpecificBatching() { return false; }
  public void setBatchSize(PreparedStatement preparedStatement, int batchSize) throws SQLException, UtilitiesException { throw new UnsupportedOperationException("setBatchSize not supported"); }
  public boolean implementsBatchUpdateException() { return false; }
  public boolean supportsResultSetCallableStatement() { return false; }
  public boolean callableStatementRequiresOutParameter() { return false; }
  public int getResultSetType() throws UtilitiesException { throw new UnsupportedOperationException("CallableStatement ResultSets not supported"); }
  public boolean requiresCommitAfterQuery() { return false; }
}
