package com.evolving.nglm.evolution.toolbox;

import java.util.ArrayList;
import java.util.Iterator;

import com.evolving.nglm.core.Pair;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;

public class ParameterBuilder
{
  
//  { 
//    "id" : "node.parameter.dialog_template",
//    "display" : "Message Template",
//    "dataType" : "string",
//    "multiple" : false,
//    "mandatory" : true,
//    "availableValues" : [ "#dialog_template_2#" ],
//    "defaultValue" : null
//  }
  
  private String id = null;
  private String display = null;
  private CriterionDataType dataType = null;
  private boolean multiple = false;
  private boolean mandatory = false;
  private ArrayList<AvailableValueBuilder> availableValues = null;
  private String defaultValue = null; 
  private ArrayList<Pair<String, String>> flatStringFields = new ArrayList<>();
  
  public ParameterBuilder(String id, String display, CriterionDataType dataType, boolean multiple, boolean mandatory, Object defaultValue) {
    this.id = id;
    this.display = display;
    this.dataType = dataType;
    this.multiple = multiple;
    this.mandatory = mandatory;
    this.setDefaultValue(defaultValue);
  }
  
  public ParameterBuilder addAvailableValue(AvailableValueBuilder availableValue) {
    if(availableValues == null) {
      availableValues = new ArrayList<>();      
    }
    availableValues.add(availableValue);
    return this;
  }
  
  public ParameterBuilder addFlatStringField(String key, String value) {
    this.flatStringFields.add(new Pair(key, value));
    return this;
  }
  
  private ParameterBuilder setDefaultValue(Object defaultValue) {
    if(defaultValue == null){
      this.defaultValue = "null";      
    }
    else if(defaultValue instanceof String) {
      this.defaultValue = "\"" + defaultValue + "\"";
    }
    else if(defaultValue instanceof Boolean) {
      this.defaultValue = ((Boolean)defaultValue).toString();
    }
    else if(defaultValue instanceof Number) {
      this.defaultValue = ((Number)defaultValue).toString();
    }
    return this;
  }
  
  public String build(Integer indentationTabNumber) {
    StringBuilder sb = new StringBuilder();
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 1) + "{\n");
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 2) + "\"id\" : \"" + id + "\",\n");
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 2) + "\"display\" : " + (display != null ? "\"" + display + "\"" : display) + ",\n");
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 2) + "\"dataType\" : \"" + dataType.getExternalRepresentation() + "\",\n");
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 2) + "\"multiple\" : " + multiple + ",\n");
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 2) + "\"mandatory\" : " + mandatory + ",\n");
    for(Pair<String, String> flatField : flatStringFields) {
      sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 2) + "\"" + flatField.getFirstElement() + "\" : \"" + flatField.getSecondElement() + "\",\n");
    }
    
    
    if(availableValues != null) {
      sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 2) + "\"availableValues\" : [\n");
      for(Iterator<AvailableValueBuilder> it = availableValues.iterator(); it.hasNext();) {
        AvailableValueBuilder current = it.next();
        sb.append(current.build(indentationTabNumber + 2));
        if(it.hasNext()) {
          sb.append(",\n");
        }
        else {
          sb.append("\n");
        }
      }
      sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 2) + "],\n");
    }
    else {
      sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 2) + "\"availableValues\" : null,");
    }
    
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 2) + "\"defaultValue\" : " + defaultValue + "\n");
    
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 1) + "}");
    return sb.toString();    
  }

}
