package com.evolving.nglm.evolution.extracts;

import com.evolving.nglm.evolution.CriterionContext;
import com.evolving.nglm.evolution.EvaluationCriterion;
import com.evolving.nglm.evolution.GUIManager;
import com.rii.utilities.JSONUtilities;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExtractItem
{

  /****************************************
   *
   *  data
   *
   ****************************************/

  private String extractName;
  private List<String> returnFields;
  private List<EvaluationCriterion> evaluationCriterionList;
  private Integer returnNoOfRecords;
  private String userID;

  /****************************************
   *
   *  contructor from JSON object
   *
   ****************************************/

  public ExtractItem(JSONObject jsonRoot) throws GUIManager.GUIManagerException
  {
    this.extractName = JSONUtilities.decodeString(jsonRoot, "extractName", true);
    this.returnFields = JSONUtilities.decodeJSONArray(jsonRoot, "returnFileds", false);
    //this is used when the information is transfered in json between gui manager and extract manager
    //when is set from gui manager user id is comming as param at processing
    //when is taken by service this have to be in JSON because is used to compose the file. (Multiple users can generate extract with the same name)
    this.userID = JSONUtilities.decodeString(jsonRoot, "userID", false);
    this.returnNoOfRecords = JSONUtilities.decodeInteger(jsonRoot, "returnNoOfRecords", false);
    evaluationCriterionList = new ArrayList<EvaluationCriterion>();
    ArrayList<JSONObject> evaluationCritetionListJSON = JSONUtilities.decodeJSONArray(jsonRoot, "evaluationCriterionList", true);
    if (evaluationCritetionListJSON != null)
    {
      for (int i = 0; i < evaluationCritetionListJSON.size(); i++)
      {
        evaluationCriterionList.add(new EvaluationCriterion((JSONObject) evaluationCritetionListJSON.get(i), CriterionContext.DynamicProfile));
      }
    }
  }

  /****************************************
   *
   *  contructor
   *
   ****************************************/

  public ExtractItem(String extractName, List<EvaluationCriterion> evaluationCriterionList, List<String> returnFields, Integer returnNoOfRecords, String userID)
  {
    this.extractName = extractName;
    this.evaluationCriterionList = evaluationCriterionList;
    this.returnFields = returnFields;
    this.returnNoOfRecords = returnNoOfRecords;
    this.userID = userID;
  }

  /****************************************
   *
   *  accessors
   *
   ****************************************/

  public String getExtractName()
  {
    return extractName;
  }

  public List<String> getReturnFields()
  {
    return returnFields;
  }

  public List<EvaluationCriterion> getEvaluationCriterionList()
  {
    return evaluationCriterionList;
  }

  public int getReturnNoOfRecords()
  {
    return returnNoOfRecords == null ? 0 : returnNoOfRecords.intValue();
  }

  public String getUserId()
  {
    return userID;
  }

  /****************************************
   *
   *  getJSONObject
   * @return String that represend object serialized as string
   *
   ****************************************/
  public String getJSONObjectAsString()
  {
    Map extractItemMap = new HashMap();
    extractItemMap.put("extractName", this.extractName);
    extractItemMap.put("evaluationCriterionList", packCriteria());
    extractItemMap.put("returnFields", returnFields);
    extractItemMap.put("userID", userID);
    extractItemMap.put("returnNoOfRecords", returnNoOfRecords);
    JSONObject returnObject = JSONUtilities.encodeObject(extractItemMap);
    return returnObject.toJSONString();
  }

  private List<Object> packCriteria()
  {
    List<Object> result = new ArrayList<Object>();
    if (evaluationCriterionList != null)
    {
      for (EvaluationCriterion criterion : evaluationCriterionList)
      {
        result.add(packJSONCriteria(criterion));
      }
    }
    return result;
  }

  private JSONObject packJSONCriteria(EvaluationCriterion criterion)
  {
    Map criterionMap = new HashMap();
    criterionMap.put("criterionField", criterion.getCriterionField().getID());
    criterionMap.put("criterionOperator", criterion.getCriterionOperator().getExternalRepresentation());
    Map argument = new HashMap();
    if(criterion.getArgument() != null)
    {
      argument.put("expression", criterion.getArgumentExpression());
      argument.put("timeUnit", criterion.getArgumentBaseTimeUnit().getExternalRepresentation());
    }
    criterionMap.put("argument", argument);
    criterionMap.put("storyReference", criterion.getStoryReference());
    criterionMap.put("criterionDefault", criterion.getCriterionDefault());
    return JSONUtilities.encodeObject(criterionMap);
  }
}
