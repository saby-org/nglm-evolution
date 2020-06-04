package com.evolving.nglm.evolution.elasticsearch;

public class ElasticsearchClientException extends Exception
{
  private static final long serialVersionUID = 1L;

  public ElasticsearchClientException(String message) {
    super(message);
  }
}
