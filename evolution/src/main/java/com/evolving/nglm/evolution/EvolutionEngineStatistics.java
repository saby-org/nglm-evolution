package com.evolving.nglm.evolution;

public class EvolutionEngineStatistics
{

  Histogram subscriberStateSize;
  Histogram subscriberHistorySize;
  Histogram extendedProfileSize;
  public Histogram getSubscriberStateSize() { return subscriberStateSize; }
  public Histogram getSubscriberHistorySize() { return subscriberHistorySize; }
  public Histogram getExtendedProfileSize() { return extendedProfileSize; }

  public EvolutionEngineStatistics(String name)
  {
    this.subscriberStateSize = new Histogram("subscriberStateSize", 10, 20, 1000, "KB");
    this.subscriberHistorySize = new Histogram("subscriberHistorySize", 10, 20, 1000, "KB");
    this.extendedProfileSize = new Histogram("extendedProfileSize", 10, 20, 1000, "KB");
  }

  void updateSubscriberStateSize(byte[] kafkaRepresentation)
  {
    subscriberStateSize.logData(kafkaRepresentation.length);
  }

  void updateSubscriberHistorySize(byte[] kafkaRepresentation)
  {
    subscriberHistorySize.logData(kafkaRepresentation.length);
  }

  void updateExtendedProfileSize(byte[] kafkaRepresentation)
  {
    extendedProfileSize.logData(kafkaRepresentation.length);
  }

}
