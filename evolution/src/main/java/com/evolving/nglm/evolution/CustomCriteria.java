/*****************************************************************************
*
*  CustomCriteria.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.Expression.ExpressionContext;
import com.evolving.nglm.evolution.Expression.ExpressionReader;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@GUIDependencyDef(objectType = "customcriteria", serviceClass = CustomCriteriaService.class, dependencies = {})
public class CustomCriteria extends GUIManagedObject
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
    schemaBuilder.name("custom_criteria");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("formula", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("dataType", Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<CustomCriteria> serde = new ConnectSerde<CustomCriteria>(schema, false, CustomCriteria.class, CustomCriteria::pack, CustomCriteria::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<CustomCriteria> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/
  
  private String formula;
  private String dataType;
  private Expression expression;
  
  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getCustomCriteriaID() { return getGUIManagedObjectID(); }
  public String getCustomCriteriaName() { return getGUIManagedObjectName(); }
  public String getFormula(){ return formula; }
  public String getDataType() { return dataType; }
  public Expression getExpression() { return expression; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public CustomCriteria(SchemaAndValue schemaAndValue, String formula, String dataType, Expression expression)
  {
    super(schemaAndValue);
    this.formula = formula;
    this.dataType = dataType;
    this.expression = expression;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    CustomCriteria customCriteria = (CustomCriteria) value;
    Struct struct = new Struct(schema);
    packCommon(struct, customCriteria);
    struct.put("formula", customCriteria.getFormula());
    struct.put("dataType", customCriteria.getDataType());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static CustomCriteria unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String formula = valueStruct.getString("formula");
    String dataType = valueStruct.getString("dataType");
    
    int tenantID = schema.field("tenantID") != null ? valueStruct.getInt16("tenantID") : 1; // by default tenantID = 1
    Expression expression = computeExpression(formula, tenantID);
    
    //
    //  return
    //

    return new CustomCriteria(schemaAndValue, formula, dataType, expression);
  }
  
  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public CustomCriteria(JSONObject jsonRoot, long epoch, GUIManagedObject existingCustomCriteriaUnchecked, int tenantID) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingCustomCriteriaUnchecked != null) ? existingCustomCriteriaUnchecked.getEpoch() : epoch, tenantID);

    /*****************************************
    *
    *  existingCustomCriteria
    *
    *****************************************/

    CustomCriteria existingCustomCriteria = (existingCustomCriteriaUnchecked != null && existingCustomCriteriaUnchecked instanceof CustomCriteria) ? (CustomCriteria) existingCustomCriteriaUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/
    
    this.formula = JSONUtilities.decodeString(jsonRoot, "formula", false);
    log.debug("decode formula : " + formula);
    expression = computeExpression(formula, tenantID);
    switch (expression.getType()) {
      case IntegerExpression:
        this.dataType = CriterionDataType.IntegerCriterion.getExternalRepresentation();
        break;
      case DoubleExpression:
        this.dataType = CriterionDataType.DoubleCriterion.getExternalRepresentation();
        break;
      default:
        throw new GUIManagerException("Type of " + formula + " must be numeric, not " + expression.getType(), formula);
    }


    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingCustomCriteria))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(CustomCriteria existingCustomCriteria)
  {
    if (existingCustomCriteria != null && existingCustomCriteria.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingCustomCriteria.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(getFormula(), existingCustomCriteria.getFormula());
        epochChanged = epochChanged || ! Objects.equals(getDataType(), existingCustomCriteria.getDataType());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
  
  private static Expression computeExpression(String formula, int tenantID) {
    Expression res = null;
    ExpressionReader expressionReader = new ExpressionReader(CriterionContext.FullDynamicProfile(tenantID), formula, null, tenantID);
    log.debug("decode expressionReader : " + expressionReader);
    res = expressionReader.parse(ExpressionContext.Criterion, tenantID);
    log.debug("decode type : " + ((res==null)?"null":res.getType()));
    return res;
  }
  
}
