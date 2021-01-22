package com.evolving.nglm.evolution.preprocessor;

import com.evolving.nglm.evolution.EvolutionEngineEvent;

import java.util.Collection;

public interface PreprocessorEvent {
  void preprocessEvent(PreprocessorContext context);
  Collection<EvolutionEngineEvent> getPreprocessedEvents();
}
