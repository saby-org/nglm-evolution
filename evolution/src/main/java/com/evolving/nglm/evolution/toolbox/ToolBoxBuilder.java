package com.evolving.nglm.evolution.toolbox;

import java.util.ArrayList;
import java.util.Iterator;

import com.evolving.nglm.core.Pair;
import com.evolving.nglm.evolution.NodeType.OutputType;

public class ToolBoxBuilder implements ToolBoxBuilderInterface
{  
  private String id = null;
  private String name = null;
  private String display = null;
  private String icon = null;
  private int height;
  private int width;
  private OutputType outputType = null;
  private ArrayList<OutputConnectorBuilder> outputConnectors = new ArrayList<>();
  private ArrayList<ParameterBuilder> parameters = new ArrayList<>();
  private ActionBuilder action = null;
  private ArrayList<Pair<String,String>> flatStringFields = new ArrayList<>();
      
  public ToolBoxBuilder(String id, String name, String display, String icon, int height, int width, OutputType outputType) {
    this.id = id;
    this.name = name;
    this.display = display;
    this.icon = icon;
    this.height = height;
    this.width = width;
    this.outputType = outputType;    
  }
  
  public ToolBoxBuilder addOutputConnector(OutputConnectorBuilder outputConnector) {
    this.outputConnectors.add(outputConnector);
    return this;
  }
  
  public ToolBoxBuilder addParameter(ParameterBuilder parameter) {
    this.parameters.add(parameter);
    return this;
  }
  
  public ToolBoxBuilder setAction(ActionBuilder action) {
    this.action = action;
    return this;
  }
  
  public ToolBoxBuilder addFlatStringField(String key, String value) {
    this.flatStringFields.add(new Pair(key, value));
    return this;
  }

  @Override
  public String build(Integer indentationTabNumber)
  {
    StringBuilder sb = new StringBuilder();
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber) + "{\n");
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 1) + "\"id\" : \"" + id + "\",\n");
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 1) + "\"name\" : \"" + name + "\",\n");
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 1) + "\"display\" : \"" + display + "\",\n");
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 1) + "\"icon\" : \"" + icon + "\",\n");
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 1) + "\"height\" : " + height + ",\n");
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 1) + "\"width\" : " + width + ",\n");
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 1) + "\"outputType\" : \"" + outputType.getExternalRepresentation() + "\",\n");

    for(Pair<String, String> flatField : flatStringFields) {
      sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 1) + "\"" + flatField.getFirstElement() + "\" : \"" + flatField.getSecondElement() + "\",\n");
    }
    
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 1) + "\"outputConnectors\" : [\n");
    for(Iterator<OutputConnectorBuilder> it = outputConnectors.iterator(); it.hasNext();) {
      OutputConnectorBuilder current = it.next();
      sb.append(current.build(indentationTabNumber + 2));
      if(it.hasNext()) {
        sb.append(",\n");
      }
      else {
        sb.append("\n");
      }
    }
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 1) + "],\n");
    
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 1) + "\"parameters\" : [\n");
    for(Iterator<ParameterBuilder> it = parameters.iterator(); it.hasNext();) {
      ParameterBuilder current = it.next();
      sb.append(current.build(indentationTabNumber + 2));
      if(it.hasNext()) {
        sb.append(",\n");
      }
      else {
        sb.append("\n");
      }
    }
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 1) + "]");
    if(action != null) {
      sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 1) + ",\n");
      sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 1) + "\"action\" : \n" + action.build(indentationTabNumber + 1) + "\n");
    }  
    
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber) + "}");
    
    
    return sb.toString();   
  }
}