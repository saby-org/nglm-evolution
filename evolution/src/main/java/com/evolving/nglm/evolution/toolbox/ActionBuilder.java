package com.evolving.nglm.evolution.toolbox;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class ActionBuilder implements ToolBoxBuilderInterface
{

//  {
//    "actionManagerClass" : "com.evolving.nglm.evolution.CommodityDeliveryManager$ActionManager",
//    "deliveryType" : "inFulfillmentFakeA",
//    "moduleID" : "1",                           => C'est de l'extra action configuration
//    "operation" : "credit"                      => C'est de l'extra action configuration
//  }
  
  private String actionManagerClass = null;
  private HashMap<String, String> managerClassConfiguration = new HashMap<>();

  public ActionBuilder(String actionManagerClass) {
    this.actionManagerClass = actionManagerClass;
  }

  public ActionBuilder addManagerClassConfigurationField(String name, Object value) {
    if(value == null) {
      managerClassConfiguration.put(name, null);
    }
    else if(value instanceof String) {
      managerClassConfiguration.put(name, "\"" + value + "\"" );
    }
    else if(value instanceof Boolean) {
      managerClassConfiguration.put(name, ((Boolean)value).toString());
    }
    else if(value instanceof Number){
      managerClassConfiguration.put(name, ((Number)value).toString());
    }
    return this;
  }
  
  public String build(Integer indentationTabNumber) {
    // { "timeUnit" : "instant", "expression" : "dateAdd(node.entryDate, 1, 'minute')" }
    StringBuilder sb = new StringBuilder();
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber+1) + "{\n");
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber+2) + "\"actionManagerClass\" : \"" + actionManagerClass + "\",\n");
    if(managerClassConfiguration.size() > 0) {
      sb.append(",\n");
      for(Iterator<Map.Entry<String, String>> it = managerClassConfiguration.entrySet().iterator(); it.hasNext();) {
        Map.Entry<String, String> current = it.next();
        sb.append(ToolBoxUtils.getIndentation(indentationTabNumber+2) + "\"" + current.getKey()+ "\" : " + current.getValue());
        if(it.hasNext()) {
          sb.append(",\n");
        }
        else {
          sb.append("\n");
        }
      }
    }
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber+1) + "} ");
    
    return sb.toString();
  }
  
}
