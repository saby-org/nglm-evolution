/*****************************************************************************
*
*  ConnectionPool
*
*****************************************************************************/

package com.evolving.nglm.core.utilities;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ConnectionPool
{
  /*****************************************
  *
  *  enum -- DBException
  *
  *****************************************/

  public enum DBException
  {
    Unknown("unknown", null),
    DuplicateInsert("duplicateInsert", Boolean.FALSE),
    TablespaceFull("tablespaceFull", Boolean.FALSE),
    TableDoesNotExist("tableDoesNotExist", Boolean.FALSE),
    LoginDenied("loginDenied", Boolean.FALSE),
    ExhaustedResultSet("exhaustedResultSet", Boolean.FALSE),
    ConnectionFailed("connectionFailed", Boolean.TRUE),
    Busy("busy", Boolean.TRUE),
    Deadlock("deadlock", Boolean.TRUE);
    private String externalRepresentation;
    private Boolean transientError;
    private DBException(String externalRepresentation, Boolean transientError) { this.externalRepresentation = externalRepresentation; this.transientError = transientError; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public Boolean getTransientError() { return transientError; }
  };

  //
  //  drivers
  //

  private static Set<String> loadedJDBCDrivers = new HashSet<String>();

  //
  //  configuration
  //
  
  private DatabaseProperties databaseProperties;
  private String connectionUrl;
  private java.util.Properties sqlProperties;
  private int numberOfConnections;
  private boolean recyclingConnections;
  private int recycleIntervalMinutes;
  private boolean useJDBCBatching;
  
  //
  //  connections
  //

  private LinkedList<Connection> unallocated = new LinkedList<Connection>();
  private Set<Connection> allocated = new HashSet<Connection>();
  private Set<Connection> failed = new HashSet<Connection>();
  protected Map<Connection,Date> connectionExpirations = new HashMap<Connection,Date>();
  
  //
  //  state
  //

  private boolean closeRequested = false;
  
  //
  //  prepared statements
  //

  private Map<Connection,Map<String,PreparedStatement>> statements = new HashMap<Connection,Map<String,PreparedStatement>>();

  /*****************************************
  *
  *  constructor - full
  *
  *****************************************/

  public ConnectionPool(String databaseProduct, String connectionUrl, java.util.Properties sqlProperties, int numberOfConnections, Integer recycleIntervalMinutes, boolean useJDBCBatching) throws UtilitiesException
  {
    //
    //  arguments
    //

    this.databaseProperties = DatabaseProperties.getDatabaseProperties(databaseProduct);
    this.connectionUrl = connectionUrl;
    this.sqlProperties = sqlProperties;
    this.numberOfConnections = numberOfConnections;
    this.recyclingConnections = (recycleIntervalMinutes != null);
    this.recycleIntervalMinutes = (recycleIntervalMinutes != null) ? recycleIntervalMinutes : 60;
    this.useJDBCBatching = useJDBCBatching;
    
    //
    //  validate
    //

    if (databaseProperties == null)
      {
        throw new UtilitiesException("unknown database product: " + databaseProduct);
      }

    //
    //  load the appropriate database driver (if necessary)
    //

    try
      {
        String jdbcDriverClassName = databaseProperties.getDatabaseDriverClassname();
        synchronized (loadedJDBCDrivers)
          {
            if (! loadedJDBCDrivers.contains(jdbcDriverClassName))
              {
                Class jdbcDriverClass = Class.forName(jdbcDriverClassName);
                Driver driver = (Driver) jdbcDriverClass.newInstance();
                DriverManager.registerDriver(driver);
                loadedJDBCDrivers.add(jdbcDriverClassName);
              }
          }
      }
    catch (Throwable e)
      {
        throw new UtilitiesException("failed to load database driver", e);
      }

    //
    //  initial pool of connections
    //

    addConnections(this.numberOfConnections);
  }

  /*****************************************
  *
  *  constructor - basic
  *
  *****************************************/

  public ConnectionPool(String databaseProduct, String connectionUrl, java.util.Properties sqlProperties, int numberOfConnections) throws UtilitiesException
  {
    this(databaseProduct, connectionUrl, sqlProperties, numberOfConnections, null, false);
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public DatabaseProperties getDatabaseProperties() { return databaseProperties; }
  public DBException classifySQLException(SQLException e) { return databaseProperties.classifySQLException(e); }
  public String getConnectionUrl() { return connectionUrl; }
  public Properties getSQLProperties() { return sqlProperties; }

  /*****************************************
  *
  *  addConnections
  *
  *****************************************/

  private void addConnections(int connectionsToAdd) throws UtilitiesException
  {
    List<Connection> pool = new ArrayList<Connection>();
    boolean success = false;
    try
      {
        for (int i=0; i<connectionsToAdd; i++)
          {
            Connection connection = DriverManager.getConnection(connectionUrl, sqlProperties);
            connection.setAutoCommit(false);
            pool.add(connection);
          }

        //
        //  calculate connection expiration time
        //

        Date connectionExpiration = recyclingConnections ? new Date(com.evolving.nglm.core.SystemTime.getCurrentTime().getTime() + recycleIntervalMinutes*1000*60) : null;

        //
        //  add connections to the unallocated pool
        //

        synchronized (this)
          {
            for (Connection connection : pool)
              {
                unallocated.addLast(connection);
                if (recyclingConnections) connectionExpirations.put(connection, connectionExpiration);
              }
            notifyAll();
          }
        success = true;
      }
    catch (SQLException e)
      {
        //
        // we could not add connections to the database
        //

        throw new UtilitiesException(e);
      }
    finally
      {
        if (! success)
          {
            for (Connection connection : pool)
              {
                closeConnection(connection);
              }
          }
      }
  }

  /*****************************************
  *
  *  getConnection
  *
  *****************************************/

  public Connection getConnection()
  {
    boolean obtainedConnection = false;
    Connection result = null;
    
    while (!obtainedConnection)
      {
        synchronized (this)
          {
            if (unallocated.size() > 0 && !closeRequested)
              {
                result = unallocated.removeFirst();
                allocated.add(result);
                obtainedConnection = true;
              }
            else
              {
                try
                  {
                    wait();
                  }
                catch (InterruptedException e)
                  {
                  }
              }
          }
      }
    return result;
  }
    
  /*****************************************
  *
  *  closeConnection
  *
  *****************************************/

  private void closeConnection(Connection connection)
  {
    //
    // close associated PreparedStatements
    //

    Map<String,PreparedStatement> statementMap = null;
    synchronized (statements)
      {
        statementMap = statements.remove(connection);
      }
    if (statementMap != null)
      {
        for (PreparedStatement preparedStatement : statementMap.values())
          {
            try
              {
                preparedStatement.close();
              }
            catch (SQLException e)
              {
                //  ignore
              }
          }
      }

    //
    // close the connection
    //

    try
      {
        connection.close();
      }
    catch (SQLException e)
      {
        // ignore
      }
  }

  /*****************************************
  *
  *  waitUntilNoConnectionsAllocated
  *
  ******************************************/

  private void waitUntilNoConnectionsAllocated()
  {
    boolean success = false;

    //
    //  this loop terminates when all connections are returned (or failed)
    //  NOTE: this method should only be called when in a mode where connections are not being allocated
    //        (i.e., when stopping the connection pool)
    //
    
    while (!success)
      {
        if (allocated.size() == 0)
          {
            success = true;
          }
        else
          {
            try
              {
                wait();
              }
            catch (InterruptedException e)
              {
              }
          }
      }
  }

  /*****************************************
  *
  *  closeAllActiveConnections
  *
  *****************************************/
  
  private void closeAllActiveConnections()
  {
    waitUntilNoConnectionsAllocated();
    for (Connection connection : unallocated)
      {
        closeConnection(connection);
      }
    unallocated.clear();
    connectionExpirations.clear();
  }

  /*****************************************
  *
  *  closeAllFailedConnections
  *
  *****************************************/
  
  private void closeAllFailedConnections()
  {
    for (Connection connection : failed)
      {
        closeConnection(connection);
      }
    failed.clear();
  }

  /*****************************************
  *
  *  close
  *
  *****************************************/
  
  public synchronized void close()
  {
    closeRequested = true;
    closeAllActiveConnections();
    closeAllFailedConnections();
  }
  
  /*****************************************
  *
  *  releaseConnection
  *
  *****************************************/

  public synchronized void releaseConnection(Connection connection) throws UtilitiesException
  {
    if (! allocated.contains(connection)) throw new UtilitiesException("invariant violation: releaseConnection");

    //
    //  remove from allocated
    //
    
    allocated.remove(connection);

    //
    //  recycle connection (if necessary) or put original connection back into unallocated
    //

    if (recyclingConnections)
      {
        Date connectionExpiration = connectionExpirations.get(connection);
        if (connectionExpiration == null) throw new UtilitiesException("invariant violation: releaseConnection");
        if (connectionExpiration.before(com.evolving.nglm.core.SystemTime.getCurrentTime()))
          {
            closeConnection(connection);
            connectionExpirations.remove(connection);
            addConnections(1);
          }
        else
          {
            unallocated.addLast(connection);
          }
      }
    else
      {
        unallocated.addLast(connection);
      }

    //
    //  notify
    //
    
    notifyAll();
  }

  /*****************************************
  *
  *  failConnection
  *
  *****************************************/

  public void failConnection(Connection connection) throws UtilitiesException
  {
    boolean recoverThisConnection = false;
    synchronized (this)
      {
        //
        //  remove existing connection
        //

        if (! allocated.remove(connection)) throw new UtilitiesException("invariant violation: failConnection");
        connectionExpirations.remove(connection);

        //
        //  determine if we should try to recover the connection
        //  (i.e., do not attempt if shutting down)
        //

        if (!(closeRequested))
          {
            recoverThisConnection = true;
          }                
        else
          {
            failed.add(connection);
            notifyAll();
          }
      }

    //
    //  recover connection (if necessary)
    //

    if (recoverThisConnection)
      {
        //
        //  close the connection and associated prepared statements
        //
        closeConnection(connection);
        addConnections(1);
      }
  }

  /*****************************************
  *
  *  handleException
  *
  *****************************************/

  public void handleException(Connection connection, SQLException e) throws UtilitiesException, SQLException
  {
    try { connection.rollback(); } catch (SQLException e1) { }
    DBException dbException = classifySQLException(e);
    switch (dbException)
      {
        case DuplicateInsert:
        case TablespaceFull:
          releaseConnection(connection);
          throw new UtilitiesException("database error - " + dbException.getExternalRepresentation(), e);

        case ConnectionFailed:
          failConnection(connection);
          break;

        case Busy:
        case Deadlock:
          releaseConnection(connection);
          break;
          
        default:
          failConnection(connection);
          throw e;
      }
  }

  /*****************************************
  *
  *  statement
  *
  *****************************************/

  //
  //  statement
  //

  public PreparedStatement statement(Connection connection, String statementString) throws UtilitiesException
  {
    return statement(connection, statementString, 1, false);
  }
  
  //
  //  statement
  //

  public PreparedStatement statement(Connection connection, String statementString, int batchSize) throws UtilitiesException
  {
    return statement(connection, statementString, batchSize, false);
  }
  
  //
  //  statement
  //

  public PreparedStatement statement(Connection connection, String statementString, boolean isCallableStatement) throws UtilitiesException
  {
    return statement(connection, statementString, 1, isCallableStatement);
  }
  
  //
  //  statement
  //

  public PreparedStatement statement(Connection connection, String statementString, int batchSize, boolean isCallableStatement) throws UtilitiesException
  {
    //
    //  verify that this connection is in this pool
    //

    if (! allocated.contains(connection)) throw new UtilitiesException("invariant violation: statement");

    //
    //  get the inner map (create if necessary)
    //

    Map<String,PreparedStatement> innerMap = null;
    synchronized (statements)
      {
        innerMap = statements.get(connection);
        //
        //  we are assuming that there is only one thread accessing a connection,
        //  and thus we do not have to synchronize when manipulating the innerMap 
        //
        if (innerMap == null)
          {
            innerMap = new HashMap<String,PreparedStatement>();
            statements.put(connection,innerMap);
          }
      }

    //
    //  get the PreparedStatement (create if necessary)
    //

    PreparedStatement result = innerMap.get(statementString);
    if (result == null)
      {
        try
          {
            if (isCallableStatement)
              {
                result = connection.prepareCall(statementString);
                if (databaseProperties.callableStatementRequiresOutParameter())
                  {
                    ((CallableStatement) result).registerOutParameter(1,databaseProperties.getResultSetType());
                  }
              }
            else
              {
                result = connection.prepareStatement(statementString);
              }

            //
            // set batch size if appropriate and supported
            //

            if ((batchSize > 1) && databaseProperties.supportsDBSpecificBatching())
              {
                databaseProperties.setBatchSize(result, batchSize);
              }
          }
        catch (SQLException e)
          {
            throw new UtilitiesException(statementString,e);
          }
        innerMap.put(statementString,result);
      }

    //
    //  return
    //

    return result;
  }
}
