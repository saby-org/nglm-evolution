package com.evolving.nglm.evolution.toolbox;

public class AvailableValueStaticStringBuilder extends AvailableValueStaticBuilder
{
  
  private String id = null;
  private String display = null;
  
  public AvailableValueStaticStringBuilder(String id, String display) {
    this.id = id;
    this.display = display;
  }
  
  public String build(Integer indentationTabNumber) {
    return ToolBoxUtils.getIndentation(indentationTabNumber + 1) + "{ \"id\" : \"" + id + "\", \"display\" : \"" + display + "\" }"; 
  }  
  // { "id" : "callToAction",  "display" : "Advertising" }

}
