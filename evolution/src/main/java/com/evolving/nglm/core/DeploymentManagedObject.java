/*****************************************************************************
*
*  DeploymentManagedObject.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import org.json.simple.JSONObject;

import com.evolving.nglm.evolution.BillingMode;
import com.evolving.nglm.evolution.CallingChannelProperty;
import com.evolving.nglm.evolution.CatalogCharacteristicUnit;
import com.evolving.nglm.evolution.CriterionField;
import com.evolving.nglm.evolution.DNBOMatrixVariable;
import com.evolving.nglm.evolution.ExternalAPITopic;
import com.evolving.nglm.evolution.JourneyMetricDeclaration;
import com.evolving.nglm.evolution.NodeType;
import com.evolving.nglm.evolution.OfferCategory;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm;
import com.evolving.nglm.evolution.OfferProperty;
import com.evolving.nglm.evolution.PartnerType;
import com.evolving.nglm.evolution.ProgramType;
import com.evolving.nglm.evolution.ScoringEngine;
import com.evolving.nglm.evolution.ScoringType;
import com.evolving.nglm.evolution.ServiceType;
import com.evolving.nglm.evolution.SupportedCurrency;
import com.evolving.nglm.evolution.SupportedDataType;
import com.evolving.nglm.evolution.SupportedLanguage;
import com.evolving.nglm.evolution.SupportedOperator;
import com.evolving.nglm.evolution.SupportedRelationship;
import com.evolving.nglm.evolution.SupportedTimeUnit;
import com.evolving.nglm.evolution.SupportedTokenCodesFormat;
import com.evolving.nglm.evolution.SupportedVoucherCodePattern;
import com.evolving.nglm.evolution.ToolboxSection;
import com.evolving.nglm.evolution.datacubes.SubscriberProfileDatacubeMetric;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import java.util.Objects;

public abstract class DeploymentManagedObject
{
  /*****************************************
  *
  * DeploymentManagedObject classes factory
  *
  *****************************************/
  public static DeploymentManagedObject create(Class<? extends DeploymentManagedObject> Tclass, JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException, ClassNotFoundException, GUIManagerException {
    if(Tclass.equals(AlternateID.class))                           { return new AlternateID(jsonRoot); }
    else if(Tclass.equals(AutoProvisionEvent.class))               { return new AutoProvisionEvent(jsonRoot); }
    else if(Tclass.equals(BillingMode.class))                      { return new BillingMode(jsonRoot); }
    else if(Tclass.equals(CallingChannelProperty.class))           { return new CallingChannelProperty(jsonRoot); }
    else if(Tclass.equals(CatalogCharacteristicUnit.class))        { return new CatalogCharacteristicUnit(jsonRoot); }
    else if(Tclass.equals(CriterionField.class))                   { return new CriterionField(jsonRoot); }
    else if(Tclass.equals(DNBOMatrixVariable.class))               { return new DNBOMatrixVariable(jsonRoot); }
    else if(Tclass.equals(ExternalAPITopic.class))                 { return new ExternalAPITopic(jsonRoot); }
    else if(Tclass.equals(JourneyMetricDeclaration.class))         { return new JourneyMetricDeclaration(jsonRoot); }
    else if(Tclass.equals(NodeType.class))                         { return new NodeType(jsonRoot); }
    else if(Tclass.equals(OfferCategory.class))                    { return new OfferCategory(jsonRoot); }
    else if(Tclass.equals(OfferOptimizationAlgorithm.class))       { return new OfferOptimizationAlgorithm(jsonRoot); }
    else if(Tclass.equals(OfferProperty.class))                    { return new OfferProperty(jsonRoot); }
    else if(Tclass.equals(PartnerType.class))                      { return new PartnerType(jsonRoot); }
    else if(Tclass.equals(ProgramType.class))                      { return new ProgramType(jsonRoot); }
    else if(Tclass.equals(ScoringEngine.class))                    { return new ScoringEngine(jsonRoot); }
    else if(Tclass.equals(ScoringType.class))                      { return new ScoringType(jsonRoot); }
    else if(Tclass.equals(ServiceType.class))                      { return new ServiceType(jsonRoot); }
    else if(Tclass.equals(SubscriberProfileDatacubeMetric.class))  { return new SubscriberProfileDatacubeMetric(jsonRoot); }
    else if(Tclass.equals(SupportedCurrency.class))                { return new SupportedCurrency(jsonRoot); }
    else if(Tclass.equals(SupportedDataType.class))                { return new SupportedDataType(jsonRoot); }
    else if(Tclass.equals(SupportedLanguage.class))                { return new SupportedLanguage(jsonRoot); }
    else if(Tclass.equals(SupportedOperator.class))                { return new SupportedOperator(jsonRoot); }
    else if(Tclass.equals(SupportedRelationship.class))            { return new SupportedRelationship(jsonRoot); }
    else if(Tclass.equals(SupportedTimeUnit.class))                { return new SupportedTimeUnit(jsonRoot); }
    else if(Tclass.equals(SupportedTokenCodesFormat.class))        { return new SupportedTokenCodesFormat(jsonRoot); }
    else if(Tclass.equals(SupportedVoucherCodePattern.class))      { return new SupportedVoucherCodePattern(jsonRoot); }
    else if(Tclass.equals(ToolboxSection.class))                   { return new ToolboxSection(jsonRoot); }
    else {
      throw new ServerRuntimeException("Unprovided constructor for class "+ Tclass.getName() + ". Please add it to the DeploymentManagedObject factory.");
    }
  }
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private JSONObject jsonRepresentation;
  private String id;
  private String name;
  private String display;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public JSONObject getJSONRepresentation() { return jsonRepresentation; }
  public String getID() { return id; }
  public String getName() { return name; }
  public String getDisplay() { return display; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public DeploymentManagedObject(JSONObject jsonRoot)
  {
    this.jsonRepresentation = jsonRoot;
    this.id = JSONUtilities.decodeString(jsonRoot, "id", true);
    this.name = JSONUtilities.decodeString(jsonRoot, "name", false);
    this.display = JSONUtilities.decodeString(jsonRoot, "display", false);
    if (this.name == null)
      {
        this.name = this.id;
        this.jsonRepresentation.put("name", this.name);
      }
  }

  /*****************************************
  *
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof DeploymentManagedObject)
      {
        DeploymentManagedObject deploymentManagedObject = (DeploymentManagedObject) obj;
        result = true;
        result = result && Objects.equals(jsonRepresentation, deploymentManagedObject.getJSONRepresentation());
      }
    return result;
  }

  /*****************************************
  *
  *  hashCode
  *
  *****************************************/

  public int hashCode()
  {
    return jsonRepresentation.hashCode();
  }
}
