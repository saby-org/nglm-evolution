package com.evolving.nglm.evolution.reports;

public class FilterObject {
	String columnName;

	public FilterObject(String columnName) {
		super();
		this.columnName = columnName;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
}
