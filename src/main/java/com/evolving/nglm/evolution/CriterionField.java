/*****************************************************************************
*
*  CriterionField.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;

import com.evolving.nglm.core.DeploymentManagedObject;
import com.evolving.nglm.core.ServerRuntimeException;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
import com.rii.utilities.SystemTime;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.util.Objects;

public class CriterionField extends DeploymentManagedObject
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private CriterionDataType fieldDataType;
  private String esField;
  private String criterionFieldRetriever;

  //
  //  calculated
  //

  private MethodHandle retriever = null;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getCriterionFieldName() { return getName(); }
  public CriterionDataType getFieldDataType() { return fieldDataType; }
  public String getESField() { return esField; }
  public String getCriterionFieldRetriever() { return criterionFieldRetriever; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public CriterionField(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    //
    //  super
    //

    super(jsonRoot);

    //
    //  data
    //

    this.fieldDataType = CriterionDataType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "dataType", true));
    this.esField = JSONUtilities.decodeString(jsonRoot, "esField", false);
    this.criterionFieldRetriever = JSONUtilities.decodeString(jsonRoot, "retriever", false);

    //
    //  retriever
    //

    if (this.criterionFieldRetriever != null)
      {
        MethodType methodType = MethodType.methodType(Object.class, SubscriberEvaluationRequest.class);
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        retriever = lookup.findStatic(Deployment.class, criterionFieldRetriever, methodType);
      }
  }

  /*****************************************
  *
  *  retrieve
  *
  *****************************************/

  public Object retrieve(SubscriberEvaluationRequest evaluationRequest)
  {
    if (retriever != null)
      {
        try
          {
            return retriever.invokeExact(evaluationRequest);
          }
        catch (RuntimeException | Error e)
          {
            throw e;
          }
        catch (Throwable e)
          {
            throw new ServerRuntimeException(e);
          }
      }
    else
      {
        throw new UnsupportedOperationException();
      }
  }
}

