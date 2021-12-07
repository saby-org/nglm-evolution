package com.evolving.nglm.evolution.event;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.evolution.SupportedRelationship;

public class Parent {

	private SupportedRelationship relationship;
	private Pair<AlternateID,String> parent;

	protected Parent(SupportedRelationship relationship, AlternateID alternateID, String alternateIDValue){
		this.relationship = relationship;
		this.parent = new Pair<>(alternateID,alternateIDValue);
	}

	protected Pair<AlternateID,String> getParent(){return this.parent;}
	protected SupportedRelationship getRelationship(){return this.relationship;}

}
