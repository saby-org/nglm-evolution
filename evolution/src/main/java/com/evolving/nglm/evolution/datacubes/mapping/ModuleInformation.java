package com.evolving.nglm.evolution.datacubes.mapping;

public class ModuleInformation
{
  public enum ModuleFeature
  {
    JourneyID("journeyID"),
    LoyaltyProgramID("loyaltyProgramID"),
    OfferID("offerID"),
    DeliverableID("deliverableID"),
    None("none");
    private String externalRepresentation;
    private ModuleFeature(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static ModuleFeature fromExternalRepresentation(String externalRepresentation) { for (ModuleFeature enumeratedValue : ModuleFeature.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return None; }
  }
  
  public String moduleDisplay;
  public ModuleFeature moduleFeature;
  
  public ModuleInformation(String moduleDisplay, String moduleFeature)
  {
    this.moduleDisplay = moduleDisplay;
    this.moduleFeature = ModuleFeature.fromExternalRepresentation(moduleFeature);
  }
}
