package com.evolving.nglm.evolution.reports;

public class FilterObject {
  String columnName;
  ColumnType columnType;
  Object[] values;

  public FilterObject(String columnName, ColumnType columnType, Object[] values) {
    super();
    this.columnName = columnName;
    this.columnType = columnType;
    this.values = values;
  }
  public String getColumnName() {
    return columnName;
  }
  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }
  public ColumnType getColumnType() {
    return columnType;
  }
  public void setColumnType(ColumnType columnType) {
    this.columnType = columnType;
  }
  public Object[] getValues() {
    return values;
  }
  public void setValues(Object[] values) {
    this.values = values;
  }
}
