package com.evolving.nglm.evolution.toolbox;

public class AvailableValueStaticBooleanAndNumberBuilder extends AvailableValueStaticBuilder
{
  
  private String value;
  
  public AvailableValueStaticBooleanAndNumberBuilder(Object value) {
    if(value == null) {
      this.value = "false";
    }
    else {
      this.value = value.toString();
    }
  }
  
  public String build(Integer indentationTabNumber) {
    return ToolBoxUtils.getIndentation(indentationTabNumber + 1) + "{ \"id\" : " + this.value + ", \"display\" : \"" + this.value + "\" }"; 
  }  
  // { "id" : true,  "display" : "true" }

}
