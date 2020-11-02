/*****************************************
*
*  WorkflowParameter.java
*
*****************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.CriterionField.VariableType;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionException;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.Expression.ExpressionContext;
import com.evolving.nglm.evolution.Expression.ExpressionDataType;
import com.evolving.nglm.evolution.Expression.ExpressionEvaluationException;
import com.evolving.nglm.evolution.Expression.ExpressionParseException;
import com.evolving.nglm.evolution.Expression.ExpressionReader;
import com.evolving.nglm.evolution.Expression.ExpressionTypeCheckException;
import com.evolving.nglm.evolution.Expression.ReferenceExpression;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class WorkflowParameter
{
  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema schema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("workflow_parameter");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("workflowID", Schema.STRING_SCHEMA);
    schemaBuilder.field("workflowParameters", SimpleParameterMap.schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<WorkflowParameter> serde = new ConnectSerde<WorkflowParameter>(schema, false, WorkflowParameter.class, WorkflowParameter::pack, WorkflowParameter::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<WorkflowParameter> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String workflowID;
  private SimpleParameterMap workflowParameters;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private WorkflowParameter(String workflowID, SimpleParameterMap workflowParameters)
  {
    this.workflowID = workflowID;
    this.workflowParameters = workflowParameters;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  public WorkflowParameter(JSONObject jsonRoot, JourneyService journeyService, CriterionContext criterionContext) throws GUIManagerException
  {
    //
    //  workflow
    //

    this.workflowID = JSONUtilities.decodeString(jsonRoot, "workflowID", true);
    Journey workflow = journeyService.getActiveJourney(workflowID, SystemTime.getCurrentTime());
    workflow = (workflow.getGUIManagedObjectType() == GUIManagedObjectType.Workflow || workflow.getGUIManagedObjectType() == GUIManagedObjectType.LoyaltyWorkflow) ? workflow : null;
    if (workflow == null)
      {
        throw new GUIManagerException("unknown workflow", workflowID);
      }

    //
    //  workflow parameters
    //

    this.workflowParameters = decodeWorkflowParameters(JSONUtilities.decodeJSONArray(jsonRoot, "parameters", new JSONArray()), workflow, criterionContext);
  }

  /*****************************************
  *
  *  decodeWorkflowParameters
  *
  *****************************************/

  private SimpleParameterMap decodeWorkflowParameters(JSONArray jsonArray, Journey workflow, CriterionContext criterionContext) throws GUIManagerException
  {
    /*****************************************
    *
    *  decode
    *
    *****************************************/

    SimpleParameterMap workflowParameters = new SimpleParameterMap();
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject parameterJSON = (JSONObject) jsonArray.get(i);
        String parameterID = JSONUtilities.decodeString(parameterJSON, "name", true);
        CriterionField parameter = workflow.getJourneyParameters().get(parameterID);
        if (parameter == null) throw new GUIManagerException("unknown workflow parameter", parameterID);
        if (! Journey.isExpressionValuedParameterValue(parameterJSON))
          {
            switch (parameter.getFieldDataType())
              {
                case IntegerCriterion:
                  workflowParameters.put(parameterID, JSONUtilities.decodeInteger(parameterJSON, "value", false));
                  break;

                case DoubleCriterion:
                  workflowParameters.put(parameterID, JSONUtilities.decodeDouble(parameterJSON, "value", false));
                  break;

                case StringCriterion:
                  workflowParameters.put(parameterID, JSONUtilities.decodeString(parameterJSON, "value", false));
                  break;

                case BooleanCriterion:
                  workflowParameters.put(parameterID, JSONUtilities.decodeBoolean(parameterJSON, "value", false));
                  break;

                case DateCriterion:
                  workflowParameters.put(parameterID, GUIManagedObject.parseDateField(JSONUtilities.decodeString(parameterJSON, "value", false)));
                  break;

                case StringSetCriterion:
                  Set<String> stringSetValue = new HashSet<String>();
                  JSONArray stringSetArray = JSONUtilities.decodeJSONArray(parameterJSON, "value", new JSONArray());
                  for (int j=0; j<stringSetArray.size(); j++)
                    {
                      stringSetValue.add((String) stringSetArray.get(j));
                    }
                  workflowParameters.put(parameterID, stringSetValue);
                  break;

                default:
                  throw new GUIManagerException("unsupported workflow parameter type", parameterID);
              }
          }
        else
          {
            ParameterExpression parameterExpressionValue = new ParameterExpression(JSONUtilities.decodeJSONObject(parameterJSON, "value", true), criterionContext);
            workflowParameters.put(parameterID, parameterExpressionValue);
            switch (parameterExpressionValue.getType())
              {
                case IntegerExpression:
                case DoubleExpression:
                case StringExpression:
                case BooleanExpression:
                case DateExpression:
                case StringSetExpression:
                  break;

                default:
                  throw new GUIManagerException("unsupported workflow parameter expression type", parameterID);
              }
          }
      }

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return workflowParameters;
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getWorkflowID() { return workflowID; }
  public SimpleParameterMap getWorkflowParameters() { return workflowParameters; }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    WorkflowParameter workflowParameter = (WorkflowParameter) value;
    Struct struct = new Struct(schema);
    struct.put("workflowID", workflowParameter.getWorkflowID());
    struct.put("workflowParameters", SimpleParameterMap.pack(workflowParameter.getWorkflowParameters()));
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static WorkflowParameter unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack all but expression
    //

    Struct valueStruct = (Struct) value;
    String workflowID = valueStruct.getString("workflowID");
    SimpleParameterMap workflowParameters = SimpleParameterMap.unpack(new SchemaAndValue(schema.field("workflowParameters").schema(), valueStruct.get("workflowParameters")));    

    //
    //  construct 
    //

    WorkflowParameter result = new WorkflowParameter(workflowID, workflowParameters);

    //
    //  return
    //

    return result;
  }
}
