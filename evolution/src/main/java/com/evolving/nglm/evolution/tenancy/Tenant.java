package com.evolving.nglm.evolution.tenancy;

public class Tenant 
{
  private int tenantID;
  private String name;
  private String description;
  private String display;
  private String languageID;
  private boolean isDefault;
  private boolean active = true;
  private boolean readonly = true;
  
  public int getTenantID() {
    return tenantID;
  }
	public String getName() {
		return name;
	}
	public String getDescription() {
		return description;
	}
	public String getDisplay() {
		return display;
	}
	public String getLanguageID() {
		return languageID;
	}
	public boolean isDefault() {
		return isDefault;
	}
	public boolean isActive() {
		return active;
	}
	public boolean isReadonly() {
		return readonly;
	}
	public Tenant(int tenantID, String name, String description, String display, String languageID, boolean isDefault) {
		super();
		this.tenantID = tenantID;
		this.name = name;
		this.description = description;
		this.display = display;
		this.languageID = languageID;
		this.isDefault = isDefault;
	}	
}
