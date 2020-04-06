package com.evolving.nglm.evolution.toolbox;

public class AvailableValueDynamicBuilder extends AvailableValueBuilder
{
  private String sharpPlaceHolder = null;
  
  public AvailableValueDynamicBuilder(String sharpPlaceHolder) {
    // by example "#callableCampaigns#"
    this.sharpPlaceHolder = sharpPlaceHolder;
  }
  
  public String build(Integer indentationTabNumber) {
    return ToolBoxUtils.getIndentation(indentationTabNumber+1) + "\"" + sharpPlaceHolder + "\"";
  }

}
