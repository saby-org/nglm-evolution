package com.evolving.nglm.evolution;

public interface LoyaltyProgramPointsEvent
{
  default public int getUnit() { return 0; };
  default public int getScoreUnit() { return 0; } // 0 means it is disabled
}
