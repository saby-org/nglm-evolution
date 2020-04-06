package com.evolving.nglm.evolution.toolbox;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionOperator;

public class TransitionCriteriaBuilder implements ToolBoxBuilderInterface
{
  private String criterionField = null;
  private CriterionOperator criterionOperator = null;
  private ArgumentBuilder argument = null; // can be null
  
//  {
//    "criterionField": "node.action.deliverystatus",
//    "criterionOperator": "is in set",
//    "argument": {
//      "expression": "[ 'delivered' ]"
//    }
//  }

  public TransitionCriteriaBuilder(String criterionField, CriterionOperator criterionOperator, ArgumentBuilder argument) {
    this.criterionField = criterionField;
    this.criterionOperator = criterionOperator;
    this.argument = argument;
  }
  
  @Override
  public String build(Integer indentationTabNumber)
  {
    StringBuilder sb = new StringBuilder();
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 1) + "{\n");
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 2) + "\"criterionField\" : \"" + criterionField + "\",\n");
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 2) + "\"criterionOperator\" : \"" + criterionOperator.getExternalRepresentation() + "\"");
    if(argument != null) {
      sb.append(",\n");
      sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 2) + "\"argument\" : \n");
      sb.append(argument.build(indentationTabNumber + 3));
      sb.append("\n");
    }
    else {
      sb.append("\n");
    }
    sb.append(ToolBoxUtils.getIndentation(indentationTabNumber + 1) + "}");
    return sb.toString();
    
  }
}
