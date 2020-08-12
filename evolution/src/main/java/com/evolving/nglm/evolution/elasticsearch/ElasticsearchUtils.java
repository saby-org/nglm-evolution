package com.evolving.nglm.evolution.elasticsearch;

import java.util.Set;

public class ElasticsearchUtils
{
  public static String displayList(Set<String> items) 
  {
    String result = null;
    for(String item: items) {
      result = (result == null)? item : (result + ", " + item);
    }
    
    return (result == null)? "" : result;
  }
}
