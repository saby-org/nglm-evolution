/****************************************************************************
*
*  DatabaseUtilities.java
*
****************************************************************************/

package com.evolving.nglm.core.utilities;

import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;

public class DatabaseUtilities
{
  /*****************************************
  *
  *  getString
  *
  *****************************************/

  public static String getString(ResultSet resultSet, String column, boolean required, String defaultValue) throws UtilitiesException, SQLException
  {
    String result = resultSet.getString(column);
    if (required && result == null) throw new UtilitiesException("required column '" + column + "' was null");
    if (result == null) result = defaultValue;
    return result;
  }

  public static String getString(ResultSet resultSet, String column, boolean required) throws UtilitiesException, SQLException
  {
    return getString(resultSet, column, required, null);
  }

  public static String getString(ResultSet resultSet, String column) throws UtilitiesException, SQLException
  {
    return getString(resultSet, column, false, null);
  }

  /*****************************************
  *
  *  setString
  *   - takes four parameters (truncates value string to length of maxLength)
  *
  *****************************************/

  public static void setString(PreparedStatement statement, int index, String value, int maxLength) throws SQLException
  {
    if (value != null)
      {
        if (value.length() > maxLength)
          statement.setString(index, value.substring(0, maxLength));
        else
          statement.setString(index, value);
      }
    else
      {
        statement.setNull(index, java.sql.Types.VARCHAR);
      }
  }

  /*****************************************
  *
  *  updateString
  *   - takes four parameters (truncates value string to length of maxLength)
  *
  *****************************************/

  public static void updateString(ResultSet resultSet, String columnName, String value, int maxLength) throws SQLException
  {
    if (value != null)
      {
        if (value.length() > maxLength)
          resultSet.updateString(columnName, value.substring(0, maxLength));
        else
          resultSet.updateString(columnName, value);
      }
    else
      {
        resultSet.updateNull(columnName);
      }
  }

  /*****************************************
  *
  *  setString
  *   - takes 3 parameters (allows value string to be any length)
  *
  *****************************************/

  public static void setString(PreparedStatement statement, int index, String value) throws SQLException
  {
    if (value != null)
      statement.setString(index, value);
    else
      statement.setNull(index, java.sql.Types.VARCHAR);
  }
  
  /*****************************************
  *
  *  updateString
  *   - takes 3 parameters (allows value string to be any length)
  *
  *****************************************/

  public static void updateString(ResultSet resultSet, String columnName, String value) throws SQLException
  {
    if (value != null)
      resultSet.updateString(columnName, value);
    else
      resultSet.updateNull(columnName);
  }
  
  /*****************************************
  *
  *  getNString
  *
  *****************************************/

  public static String getNString(ResultSet resultSet, String column, boolean required, String defaultValue) throws UtilitiesException, SQLException
  {
    String result = resultSet.getNString(column);
    if (required && result == null) throw new UtilitiesException("required column '" + column + "' was null");
    if (result == null) result = defaultValue;
    return result;
  }

  public static String getNString(ResultSet resultSet, String column, boolean required) throws UtilitiesException, SQLException
  {
    return getNString(resultSet, column, required, null);
  }

  public static String getNString(ResultSet resultSet, String column) throws UtilitiesException, SQLException
  {
    return getNString(resultSet, column, false, null);
  }

  /*****************************************
  *
  *  setNString
  *   - takes four parameters (truncates value string to length of maxLength)
  *
  *****************************************/

  public static void setNString(PreparedStatement statement, int index, String value, int maxLength) throws SQLException
  {
    if (value != null)
      {
        if (value.length() > maxLength)
          statement.setNString(index, value.substring(0, maxLength));
        else
          statement.setNString(index, value);
      }
    else
      {
        statement.setNull(index, java.sql.Types.NVARCHAR);
      }
  }

  /*****************************************
  *
  *  updateNString
  *   - takes four parameters (truncates value string to length of maxLength)
  *
  *****************************************/

  public static void updateNString(ResultSet resultSet, String columnName, String value, int maxLength) throws SQLException
  {
    if (value != null)
      {
        if (value.length() > maxLength)
          resultSet.updateNString(columnName, value.substring(0, maxLength));
        else
          resultSet.updateNString(columnName, value);
      }
    else
      {
        resultSet.updateNull(columnName);
      }
  }

  /*****************************************
  *
  *  setNString
  *   - takes 3 parameters (allows value string to be any length)
  *
  *****************************************/

  public static void setNString(PreparedStatement statement, int index, String value) throws SQLException
  {
    if (value != null)
      statement.setNString(index, value);
    else
      statement.setNull(index, java.sql.Types.NVARCHAR);
  }

  /*****************************************
  *
  *  updateNString
  *   - takes 3 parameters (allows value string to be any length)
  *
  *****************************************/

  public static void updateNString(ResultSet resultSet, String columnName, String value) throws SQLException
  {
    if (value != null)
      resultSet.updateNString(columnName, value);
    else
      resultSet.updateNull(columnName);
  }

  /*****************************************
  *
  *  getInteger
  *
  *****************************************/

  public static Integer getInteger(ResultSet resultSet, String column, boolean required, Integer defaultValue) throws UtilitiesException, SQLException
  {
    int value = resultSet.getInt(column);
    Integer result = (resultSet.wasNull() ? null : new Integer(value));
    if (required && result == null) throw new UtilitiesException("required column '" + column + "' was null");
    if (result == null) result = defaultValue;
    return result;
  }

  public static Integer getInteger(ResultSet resultSet, String column, boolean required) throws UtilitiesException, SQLException
  {
    return getInteger(resultSet, column, required, null);
  }

  public static Integer getInteger(ResultSet resultSet, String column) throws UtilitiesException, SQLException
  {
    return getInteger(resultSet, column, false, null);
  }

  /*****************************************
  *
  *  setInteger
  *
  *****************************************/

  public static void setInteger(PreparedStatement statement, int index, Integer value) throws SQLException
  {
    if (value != null)
      statement.setInt(index,value.intValue());
    else
      statement.setNull(index,java.sql.Types.INTEGER);
  }

  public static void setInteger(PreparedStatement statement, int index, int value) throws SQLException
  {
    statement.setInt(index,value);
  }

  /*****************************************
  *
  *  updateInteger
  *
  *****************************************/

  public static void updateInteger(ResultSet resultSet, String columnName, Integer value) throws SQLException
  {
    if (value != null)
      resultSet.updateInt(columnName,value.intValue());
    else
      resultSet.updateNull(columnName);
  }

  public static void updateInteger(ResultSet resultSet, String columnName, int value) throws SQLException
  {
    resultSet.updateInt(columnName,value);
  }

  /*****************************************
  *
  *  getShort
  *
  *****************************************/

  public static Short getShort(ResultSet resultSet, String column, boolean required, Short defaultValue) throws UtilitiesException, SQLException
  {
    short value = resultSet.getShort(column);
    Short result = (resultSet.wasNull() ? null : new Short(value));
    if (required && result == null) throw new UtilitiesException("required column '" + column + "' was null");
    if (result == null) result = defaultValue;
    return result;
  }

  public static Short getShort(ResultSet resultSet, String column, boolean required) throws UtilitiesException, SQLException
  {
    return getShort(resultSet, column, required, null);
  }

  public static Short getShort(ResultSet resultSet, String column) throws UtilitiesException, SQLException
  {
    return getShort(resultSet, column, false, null);
  }

  /*****************************************
  *
  *  setShort
  *
  *****************************************/

  public static void setShort(PreparedStatement statement, int index, Short value) throws SQLException
  {
    if (value != null)
      statement.setShort(index,value.shortValue());
    else
      statement.setNull(index,java.sql.Types.INTEGER);
  }

  public static void setShort(PreparedStatement statement, int index, short value) throws SQLException
  {
    statement.setShort(index,value);
  }

  /*****************************************
  *
  *  updateShort
  *
  *****************************************/

  public static void updateShort(ResultSet resultSet, String columnName, Short value) throws SQLException
  {
    if (value != null)
      resultSet.updateShort(columnName,value.shortValue());
    else
      resultSet.updateNull(columnName);
  }

  public static void updateShort(ResultSet resultSet, String columnName, short value) throws SQLException
  {
    resultSet.updateShort(columnName,value);
  }

  /*****************************************
  *
  *  getBoolean
  *
  *****************************************/

  public static Boolean getBoolean(ResultSet resultSet, String column, boolean required,  Boolean defaultValue) throws UtilitiesException, SQLException
  {
    boolean value = resultSet.getBoolean(column);
    Boolean result = (resultSet.wasNull() ? null : new Boolean(value));
    if (required && result == null) throw new UtilitiesException("required column '" + column + "' was null");
    if (result == null) result = defaultValue;
    return result;
  }

  public static Boolean getBoolean(ResultSet resultSet, String column, boolean required) throws UtilitiesException, SQLException
  {
    return getBoolean(resultSet, column, required, null);
  }

  /*****************************************
  *
  *  setBoolean
  *
  *****************************************/

  public static void setBoolean(PreparedStatement statement, int index, Boolean value) throws SQLException
  {
    if (value != null)
      statement.setBoolean(index,value.booleanValue());
    else
      statement.setNull(index,java.sql.Types.BIT);
  }

  public static void setBoolean(PreparedStatement statement, int index, boolean value) throws SQLException
  {
    statement.setBoolean(index,value);
  }

  /*****************************************
  *
  *  updateBoolean
  *
  *****************************************/

  public static void updateBoolean(ResultSet resultSet, String columnName, Boolean value) throws SQLException
  {
    if (value != null)
      resultSet.updateBoolean(columnName,value.booleanValue());
    else
      resultSet.updateNull(columnName);
  }

  public static void updateBoolean(ResultSet resultSet, String columnName, boolean value) throws SQLException
  {
    resultSet.updateBoolean(columnName,value);
  }

  /*****************************************
  *
  *  getDate
  *
  *****************************************/
  
  public static Date getDate(ResultSet resultSet, String column, boolean required,  Date defaultValue) throws UtilitiesException, SQLException
  {
    java.sql.Timestamp value = resultSet.getTimestamp(column);
    Date result = (resultSet.wasNull() ? null : new java.util.Date(value.getTime()));
    if (required && result == null) throw new UtilitiesException("required column '" + column + "' was null");
    if (result == null) result = defaultValue;
    return result;
  }

  public static Date getDate(ResultSet resultSet, String column, boolean required) throws UtilitiesException, SQLException
  {
    return getDate(resultSet, column, required, null);
  }
  
  /*****************************************
  *
  *  setDate
  *
  *****************************************/
  
  public static void setDate(PreparedStatement statement, int index, Date value) throws SQLException
  {
    if (value != null)
      statement.setTimestamp(index,new java.sql.Timestamp(value.getTime()));
    else
      statement.setNull(index,java.sql.Types.TIMESTAMP);
  }

  /*****************************************
  *
  *  updateDate
  *
  *****************************************/
  
  public static void updateDate(ResultSet resultSet, String columnName, Date value) throws SQLException
  {
    if (value != null)
      resultSet.updateTimestamp(columnName,new java.sql.Timestamp(value.getTime()));
    else
      resultSet.updateNull(columnName);
  }

  /*****************************************
  *
  *  getBigDecimal
  *
  *****************************************/

  public static BigDecimal getBigDecimal(ResultSet resultSet, String column, boolean required, BigDecimal defaultValue) throws UtilitiesException, SQLException
  {
    BigDecimal value = resultSet.getBigDecimal(column);
    BigDecimal result = (resultSet.wasNull() ? null : value);
    if (required && result == null) throw new UtilitiesException("required column '" + column + "' was null");
    if (result == null) result = defaultValue;
    return result;
  }

  public static BigDecimal getBigDecimal(ResultSet resultSet, String column, boolean required) throws UtilitiesException, SQLException
  {
    return getBigDecimal(resultSet, column, required, null);
  }

  public static BigDecimal getBigDecimal(ResultSet resultSet, String column) throws UtilitiesException, SQLException
  {
    return getBigDecimal(resultSet, column, false, null);
  }
  
  /*****************************************
  *
  *  setBigDecimal
  *
  *****************************************/

  public static void setBigDecimal(PreparedStatement statement, int index, BigDecimal value) throws SQLException
  {
    if (value != null)
      statement.setBigDecimal(index,value);
    else
      statement.setNull(index,java.sql.Types.INTEGER);
  }

  /*****************************************
  *
  *  updateBigDecimal
  *
  *****************************************/

  public static void updateBigDecimal(ResultSet resultSet, String columnName, BigDecimal value) throws SQLException
  {
    if (value != null)
      resultSet.updateBigDecimal(columnName,value);
    else
      resultSet.updateNull(columnName);
  }

  /*****************************************
  *
  *  getLong
  *
  *****************************************/

  public static Long getLong(ResultSet resultSet, String column, boolean required, Long defaultValue) throws UtilitiesException, SQLException
  {
    long value = resultSet.getLong(column);
    Long result = (resultSet.wasNull() ? null : new Long(value));
    if (required && result == null) throw new UtilitiesException("required column '" + column + "' was null");
    if (result == null) result = defaultValue;
    return result;
  }

  public static Long getLong(ResultSet resultSet, String column, boolean required) throws UtilitiesException, SQLException
  {
    return getLong(resultSet, column, required, null);
  }

  public static Long getLong(ResultSet resultSet, String column) throws UtilitiesException, SQLException
  {
    return getLong(resultSet, column, false, null);
  }

  /*****************************************
  *
  *  setLong
  *
  *****************************************/

  public static void setLong(PreparedStatement statement, int index, Long value) throws SQLException
  {
    if (value != null)
      statement.setLong(index,value.longValue());
    else
      statement.setNull(index,java.sql.Types.INTEGER);
  }

  public static void setLong(PreparedStatement statement, int index, long value) throws SQLException
  {
    statement.setLong(index,value);
  }

  /*****************************************
  *
  *  updateLong
  *
  *****************************************/

  public static void updateLong(ResultSet resultSet, String columnName, Long value) throws SQLException
  {
    if (value != null)
      resultSet.updateLong(columnName,value.longValue());
    else
      resultSet.updateNull(columnName);
  }

  public static void updateLong(ResultSet resultSet, String columnName, long value) throws SQLException
  {
    resultSet.updateLong(columnName,value);
  }

  /*****************************************
  *
  *  getDouble
  *
  *****************************************/

  public static Double getDouble(ResultSet resultSet, String column, boolean required, Double defaultValue) throws UtilitiesException, SQLException
  {
    double value = resultSet.getDouble(column);
    Double result = (resultSet.wasNull() ? null : new Double(value));
    if (required && result == null) throw new UtilitiesException("required column '" + column + "' was null");
    if (result == null) result = defaultValue;
    return result;
  }

  public static Double getDouble(ResultSet resultSet, String column, boolean required) throws UtilitiesException, SQLException
  {
    return getDouble(resultSet, column, required, null);
  }

  public static Double getDouble(ResultSet resultSet, String column) throws UtilitiesException, SQLException
  {
    return getDouble(resultSet, column, false, null);
  }

  /*****************************************
  *
  *  setDouble
  *
  *****************************************/

  public static void setDouble(PreparedStatement statement, int index, Double value) throws SQLException
  {
    if (value != null)
      statement.setDouble(index,value.doubleValue());
    else
      statement.setNull(index,java.sql.Types.DOUBLE);
  }

  public static void setDouble(PreparedStatement statement, int index, double value) throws SQLException
  {
    statement.setDouble(index,value);
  }
  
  /*****************************************
  *
  *  updateDouble
  *
  *****************************************/

  public static void updateDouble(ResultSet resultSet, String columnName, Double value) throws SQLException
  {
    if (value != null)
      resultSet.updateDouble(columnName,value.doubleValue());
    else
      resultSet.updateNull(columnName);
  }

  public static void updateDouble(ResultSet resultSet, String columnName, double value) throws SQLException
  {
    resultSet.updateDouble(columnName,value);
  }
  
  /*****************************************
  *
  *  getBytes
  *
  *****************************************/

  public static byte[] getBytes(ResultSet resultSet, String column, boolean required, byte[] defaultValue) throws UtilitiesException, SQLException
  {
    java.sql.Blob blob = resultSet.getBlob(column); 
    if (required && blob == null) throw new UtilitiesException("required column '" + column + "' was null");
    byte[] result = resultSet.wasNull() ? null : blob.getBytes(1, (int) blob.length());
    if (result == null) result = defaultValue;
    return result;
  }

  public static byte[] getBytes(ResultSet resultSet, String column, boolean required) throws UtilitiesException, SQLException
  {
    return getBytes(resultSet, column, required, null);
  }

  public static byte[] getBytes(ResultSet resultSet, String column) throws UtilitiesException, SQLException
  {
    return getBytes(resultSet, column, false, null);
  }

  /*****************************************
  *
  *  setBytes
  *
  *****************************************/

  public static void setBytes(PreparedStatement statement, int index, byte[] value, DatabaseProperties databaseProperties) throws SQLException
  {
    if (value != null)
      {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(value);
        statement.setBinaryStream(index,inputStream,value.length);
      }
    else
      {
        statement.setNull(index,databaseProperties.sqlToDatabaseSpecificTypeMapping(java.sql.Types.BLOB));
      }
  }
  
  /*****************************************
  *
  *  updateBytes
  *
  *****************************************/

  public static void updateBytes(ResultSet resultSet, String columnName, byte[] value, DatabaseProperties databaseProperties) throws SQLException
  {
    if (value != null)
      {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(value);
        resultSet.updateBinaryStream(columnName,inputStream,value.length);
      }
    else
      {
        resultSet.updateNull(columnName);
      }
  }
  
  /*****************************************
  *
  *  appendBytes
  *
  *****************************************/

  public static void appendBytes(ResultSet resultSet, String columnName, byte[] value, DatabaseProperties databaseProperties) throws SQLException, IOException
  {
    if (value != null)
      {
        Blob blob = resultSet.getBlob(columnName);
        if (blob != null)
          {
            OutputStream os = blob.setBinaryStream(blob.length()+1);
            os.write(value);
            os.flush();
            os.close();
          }
        else
          {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(value);
            resultSet.updateBinaryStream(columnName,inputStream,value.length);
          }
      }
    else
      {
        // ignore null value
      }
  }
  
  /*****************************************
  *
  *  getBlobInputStream
  *
  *****************************************/

  public static InputStream getBlobInputStream(ResultSet resultSet, String column, boolean required) throws SQLException, UtilitiesException
  {
    InputStream result;
    java.sql.Blob blob = resultSet.getBlob(column); 
    if (required && blob == null) throw new UtilitiesException("required column '" + column + "' was null");
    if (! resultSet.wasNull())
      {
        result = blob.getBinaryStream();
      }
    else
      {
        result = new ByteArrayInputStream(new byte[0]);
      }
    return result;
  }


  public static InputStream getBlobInputStream(ResultSet resultSet, String column) throws SQLException, UtilitiesException
  {
    return getBlobInputStream(resultSet, column, false);
  }  
}
