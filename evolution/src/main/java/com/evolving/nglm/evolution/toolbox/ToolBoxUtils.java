package com.evolving.nglm.evolution.toolbox;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionOperator;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.NodeType.OutputType;

public class ToolBoxUtils
{
  public static String getIndentation(Integer tabNumber) {
    StringBuilder sb = new StringBuilder();
    for(int i = 0; i<tabNumber; i++) {
      sb.append("  ");
    }
    return sb.toString();
  }
  
  public static void main(String[] args)
  {
    
//    {
//      "id"                     : "141",
//      "name"                   : "sms",
//      "display"                : "SMS",
//      "icon"                   : "jmr_components/styles/images/objects/sms.png",
//      "height"                 : 70,
//      "width"                  : 70,
//      "outputType"             : "static",
//      "outputConnectors"       : 
//        [ 
//          { "name" : "delivered", "display" : "Delivered/Sent","transitionCriteria" : [ { "criterionField" : "node.action.deliverystatus", "criterionOperator" : "is in set", "argument" : { "expression" : "[ 'delivered', 'acknowledged' ]" } } ] },
//          { "name" : "failed",    "display" : "Failed",        "transitionCriteria" : [ { "criterionField" : "node.action.deliverystatus", "criterionOperator" : "is in set", "argument" : { "expression" : "[ 'failed', 'indeterminate', 'failedTimeout' ]" } } ] },
//          { "name" : "timeout",   "display" : "Timeout",       "transitionCriteria" : [ { "criterionField" : "evaluation.date", "criterionOperator" : ">=", "argument" : { "timeUnit" : "instant", "expression" : "dateAdd(node.entryDate, 2, 'day')" } } ] },
//          { "name" : "unknown",   "display" : "UnknownMSISDN", "transitionCriteria" : [ { "criterionField" : "subscriber.msisdn", "criterionOperator" : "is null" } ] }
//        ],
//      "parameters" :
//        [
//          { 
//            "id" : "node.parameter.contacttype",
//            "display" : "Contact Type",
//            "dataType" : "string",
//            "multiple" : false,
//            "mandatory" : true,
//            "availableValues" : 
//              [ 
//                { "id" : "callToAction",  "display" : "Advertising" },
//                { "id" : "response", "display" : "Bonus Message" },
//                { "id" : "reminder", "display" : "Reminder" },
//                { "id" : "announcement", "display" : "Announcement" },
//                { "id" : "actionNotification", "display" : "Action Notification" }
//              ],
//            "defaultValue" : null
//          },    
//          { 
//            "id" : "node.parameter.message",
//            "display" : "Message Text",
//            "dataType" : "smsMessage",
//            "multiple" : false,
//            "mandatory" : true,
//            "availableValues" : null,
//            "defaultValue" : null
//          },    
//          { 
//            "id" : "node.parameter.source",
//            "display" : "Short Code",
//            "dataType" : "string",
//            "multiple" : false,
//            "mandatory" : true,
//            "availableValues" : [ "#sourceAddressesSMS#" ],
//            "defaultValue" : null
//          },
    
    
//          {
//            "id" : "node.parameter.confirmationexpected",
//            "display" : "Delivery Confirmation Expected",
//            "dataType" : "boolean",
//            "multiple" : false,
//            "mandatory" : true,
//            "availableValues" : [true, false],
//            "defaultValue" : false
//          }
//        ],
//      "action" : 
//        {
//          "actionManagerClass" : "com.evolving.nglm.evolution.SMSNotificationManager$ActionManager",
//          "deliveryType" : "notificationmanagersms",
//          "isFlashSMS" : false,
//          "moduleID" : "1"    
//        } 
//     }

    ToolBoxBuilder b = new ToolBoxBuilder("141", "sms", "SMS", "jmr_components/styles/images/objects/sms.png", 70, 70, OutputType.Static);
    
    // { "name" : "delivered", "display" : "Delivered/Sent","transitionCriteria" : [ { "criterionField" : "node.action.deliverystatus", "criterionOperator" : "is in set", "argument" : { "expression" : "[ 'delivered', 'acknowledged' ]" } } ] },
    b.addOutputConnector(new OutputConnectorBuilder("delivered", "Delivered/Sent").addTransitionCriteria(new TransitionCriteriaBuilder("node.action.deliverystatus", CriterionOperator.IsInSetOperator, new ArgumentBuilder("[ 'delivered', 'acknowledged' ]"))));
    
    // { "name" : "failed",    "display" : "Failed",        "transitionCriteria" : [ { "criterionField" : "node.action.deliverystatus", "criterionOperator" : "is in set", "argument" : { "expression" : "[ 'failed', 'indeterminate', 'failedTimeout' ]" } } ] },
    b.addOutputConnector(new OutputConnectorBuilder("failed", "Failed").addTransitionCriteria(new TransitionCriteriaBuilder("node.action.deliverystatus", CriterionOperator.IsInSetOperator, new ArgumentBuilder("[ 'failed', 'indeterminate', 'failedTimeout' ]"))));
    
    // { "name" : "timeout",   "display" : "Timeout",       "transitionCriteria" : [ { "criterionField" : "evaluation.date", "criterionOperator" : ">=", "argument" : { "timeUnit" : "instant", "expression" : "dateAdd(node.entryDate, 2, 'day')" } } ] },
    b.addOutputConnector(new OutputConnectorBuilder("timeout", "Timeout").addTransitionCriteria(new TransitionCriteriaBuilder("evaluation.date", CriterionOperator.GreaterThanOrEqualOperator, new ArgumentBuilder("dateAdd(node.entryDate, 2, 'day')").setTimeUnit(TimeUnit.Instant))));
    
    // { "name" : "unknown",   "display" : "UnknownMSISDN", "transitionCriteria" : [ { "criterionField" : "subscriber.msisdn", "criterionOperator" : "is null" } ] }
    b.addOutputConnector(new OutputConnectorBuilder("unknown", "UnknownMSISDN").addTransitionCriteria(new TransitionCriteriaBuilder("subscriber.msisdn", CriterionOperator.IsNullOperator, null)));
    
    b.addParameter(new ParameterBuilder("node.parameter.contacttype", "Contact Type", CriterionDataType.StringCriterion, false, true, null)
         .addAvailableValue(new AvailableValueStaticStringBuilder("callToAction", "Advertising"))
         .addAvailableValue(new AvailableValueStaticStringBuilder("response", "Bonus Message"))
         .addAvailableValue(new AvailableValueStaticStringBuilder("reminder", "Reminder"))
         .addAvailableValue(new AvailableValueStaticStringBuilder("announcement", "Announcement"))
         .addAvailableValue(new AvailableValueStaticStringBuilder("actionNotification", "Action Notification")));
    
    b.addParameter(new ParameterBuilder("node.parameter.message", "Message Text", CriterionDataType.SMSMessageParameter, false, true, null));
    b.addParameter(new ParameterBuilder("node.parameter.source", "Short Code", CriterionDataType.StringCriterion, false, true, null)
        .addAvailableValue(new AvailableValueDynamicBuilder("#sourceAddressesSMS#")));

    b.addParameter(new ParameterBuilder("node.parameter.confirmationexpected", "Delivery Confirmation Expected", CriterionDataType.BooleanCriterion, false, true, Boolean.FALSE)
        .addAvailableValue(new AvailableValueStaticBooleanAndNumberBuilder(Boolean.TRUE))
        .addAvailableValue(new AvailableValueStaticBooleanAndNumberBuilder(Boolean.FALSE)));
    
    b.setAction(new ActionBuilder("com.evolving.nglm.evolution.SMSNotificationManager$ActionManager")
        .addManagerClassConfigurationField("deliveryType", "notificationmanagersms")
        .addManagerClassConfigurationField("isFlashSMS", Boolean.FALSE)
        .addManagerClassConfigurationField("moduleID", "1"));
    
    System.out.println(b.build(0));
      
         
  }
}
