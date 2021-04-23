package com.evolving.nglm.evolution.elasticsearch;

import java.util.Map;

import com.evolving.nglm.core.Deployment;

public class ElasticsearchUpgrade
{
  /**
   * HACKY (only used to update from version < 2.0.0)
   * 
   * In case no _meta data can be found.
   * Either, this index is not referenced, or is in version 0 (before _meta implementation < 2.0.0) 
   * Try to match the index name with a template in version 0
   * 
   * A bit hacky because template "id" from Deployment.getElasticsearchTemplatesVersion should not change 
   * and template pattern in version 0 matches THIS template "id".
   * 
   * We try to find the best match (comparing size) because for instance: 
   *  - datacube_subscriberprofile (index) matches subscriberprofile (template)
   *  - datacube_subscriberprofile (index) matches datacube_subscriberprofile (template)
   * 
   * @return matching template (in version 0)
   */
  public static String recoverTemplateVersion0(String index) {
    Map<String, Long> templatesVersion = Deployment.getElasticsearchTemplatesVersion();
    String bestMatch = null;
    int bestMatchLength = -1;
    for(String template: templatesVersion.keySet()) {
      if(index.contains(template)) {
        if(template.length() > bestMatchLength) {
          bestMatch = template;
          bestMatchLength = template.length();
        }
      }
    }
    return bestMatch;
  }
}
