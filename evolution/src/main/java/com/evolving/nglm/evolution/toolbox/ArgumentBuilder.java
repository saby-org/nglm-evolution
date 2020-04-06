package com.evolving.nglm.evolution.toolbox;

import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;

public class ArgumentBuilder implements ToolBoxBuilderInterface
{

  private String expression = null;
  private TimeUnit timeUnit = null;
  
  public ArgumentBuilder(String expression) {
    this.expression = expression;
  }
  
  public ArgumentBuilder setTimeUnit(TimeUnit timeUnit) {
    this.timeUnit = timeUnit;
    return this;
  }
  
  public String build(Integer indentationTabNumber) {
    // { "timeUnit" : "instant", "expression" : "dateAdd(node.entryDate, 1, 'minute')" }
    StringBuilder sb = new StringBuilder();
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber) + "{ ");
    if(timeUnit != null) {
      sb.append("\"timeUnit\" : \"" + timeUnit.getExternalRepresentation() + "\", ");      
    }
    sb.append("\"expression\" : \"" + expression + "\"");
    sb.append(" }");
    
    return sb.toString();
  }
  
}
