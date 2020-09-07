package com.evolving.nglm.evolution.reports;

public enum ColumnType
{
	STRING("string"),
	MULTIPLE_STRING("multiple.string");
    private String externalRepresentation;
    
    public String getExternalRepresentation() {
		return externalRepresentation;
	}

	private ColumnType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
}
