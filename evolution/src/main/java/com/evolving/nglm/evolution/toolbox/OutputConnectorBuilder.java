package com.evolving.nglm.evolution.toolbox;

import java.util.ArrayList;
import java.util.Iterator;

import com.evolving.nglm.evolution.Journey.EvaluationPriority;

public class OutputConnectorBuilder
{
  private String name = null;
  private String display = null;
  private EvaluationPriority evaluationPriority = null;
  private ArrayList<TransitionCriteriaBuilder> transitionCriteria = new ArrayList<>();
  
//  { 
//    "name" : "delivered", 
//    "display" : "Delivered/Sent",
//    "transitionCriteria" : [ 
//       { 
//         "criterionField" : "node.action.deliverystatus", 
//         "criterionOperator" : "is in set", 
//         "argument" : 
//           { 
//             "expression" : "[ 'delivered', 'acknowledged' ]" 
//           } 
//        } 
//     ] 
//   }
  
  public OutputConnectorBuilder(String name, String display) {
    this.name = name;
    this.display = display;
  }
  
  public OutputConnectorBuilder addTransitionCriteria(TransitionCriteriaBuilder transitionCriteriaBuilder) {
    if(transitionCriteriaBuilder != null) {
      this.transitionCriteria.add(transitionCriteriaBuilder);
    }
    return this;
  }
  
  public String build(Integer indentationTabNumber) {
    StringBuilder sb = new StringBuilder();
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 1) + "{\n");
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 2) + "\"name\" : \"" + name + "\",\n");
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 2) + "\"display\" : \"" + display + "\",\n");
    if(evaluationPriority != null) {
      sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 2) + "\"evaluationPriority\" : \"" + evaluationPriority + "\",\n");
    }
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 2) + "\"transitionCriteria\" : [\n");
    for(Iterator<TransitionCriteriaBuilder> it = transitionCriteria.iterator(); it.hasNext();) {
      TransitionCriteriaBuilder current = it.next();
      sb.append(current.build(indentationTabNumber + 3));
      if(it.hasNext()) {
        sb.append(",\n");
      }
      else {
        sb.append("\n");
      }
    }
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 2) + "]\n");
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 1) + "}");
    return sb.toString();    
  }
  
}
