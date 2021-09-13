package com.evolving.nglm.evolution.elasticsearch;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;

/**
 * About Elasticsearch template versions
 * 
 * Template versions were implemented in Evolution 2.0.0.
 * From that moment, every template has an explicit version. This version is define in deployment variables and 
 * directly written in the metadata of the template. Meaning that, this version can be retrieve when looking at 
 * any Elasticsearch index in production.
 * 
 * The goal of this explicit version is to be able to retrieve the state of an index, and therefore upgrade it 
 * to the current version.
 * 
 * All templates start in Evolution 2.0.0 with a version = 2. 
 * 
 * Template version meaning:
 * - version 0:            undetermined version of the template, created < Evolution 1.5.2
 * - version 1:            version of the template in Evolution 1.5.2 (cannot be retrieve)
 * - version 2 or above:   version of the template in Evolution 2.0.0 or above, explicitly written in the metadata.
 * 
 * Version 0 of a template is not really a "version" because it contains plenty of potential different versions of 
 * it. That's why it is impossible to upgrade an index from version 0.
 * 
 * Version 1 correspond to the version of the template in Evolution 1.5.2. But, because this mechanism did not exist 
 * in that time, this version need to be "manually set" by using the "elasticsearchIndexByPassVersion" debug variable. 
 * Beware that, if one want to force an index in version 1, they need to perform every checks to ensure the index is 
 * in a 1.5.2 format.
 */

/**
 * About the debug variable elasticsearchIndexByPassVersion 
 * 
 * The goal of this variable is to "fast forward" any specified template that is in an older version than the defined one 
 * to the defined one. The meaning of "fast forward" here is to assume a "new version" of it without performing any upgrade.
 * 
 * Let's take the following example of deployment.json configuration:
 * "elasticsearchTemplatesVersion"   : { "datacube_odr": 8 }
 * "elasticsearchIndexByPassVersion" : { "datacube_odr": 6 }
 * 
 * Here, any indexes of "datacube_odr" in version 5 or below, will be assumed as a version 6 of it, and therefore the *setup* 
 * process will try to upgrade them from 6 to 8.
 * 
 * The main goal of the elasticsearchIndexByPassVersion variable, is, in fact, to skip entirely the process of an upgrade on 
 * a specified template in order to **discard** all warnings that prevent the upgrade when performing SOME DEBUGGING. 
 * Do not do that in production. In production, this variable is provided as 
 * 
 * In the previous example, we could skip this process by setting this:
 * "elasticsearchTemplatesVersion"   : { "datacube_odr": 8 }
 * "elasticsearchIndexByPassVersion" : { "datacube_odr": 8 }
 * 
 * Something very important here, is that, compared to a simple enable/disable button, the debug state/mode is not "saved".
 * If one set this variable in production, next time the "datacube_odr" is upgraded to version 9, the upgrade from 8 to 9
 * will NOT be by passed, even if they forgot to reset the elasticsearchIndexByPassVersion variable.
 * It is still highly recommended to reset elasticsearchIndexByPassVersion to {} every time the system is upgraded.
 * 
 * Another goal of this variable, is, for every upgrade from Evolution < 2.0.0, to manually specify if a template can be
 * assumed as a version 1 (from Evolution 1.5.2).
 */
public class ElasticsearchUpgrade
{
  private static final Logger log = LoggerFactory.getLogger(ElasticsearchUpgrade.class);
  
  
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
  
  /**
   * PATCHES
   * 
   * Map(TemplateName -> Map(FromVersion -> Patch))
   * 
   * All patches inside this map do upgrade to the current version of TemplateName.
   * All others patches (that upgrade to a previous version) are not stored in this map and are just deprecated.
   * 
   * @rl For the moment we only support upgrade from X->current, meaning that, event if 
   * BOTH patches (X->Y) and (Y->current) exist, we will need to write down the patch (X->current)
   * here, because it CANNOT be deduce from those two patches.
   * The main reason is because we do not store templates for old versions. We only have access 
   * to the template of the current version in `evolution-setup-elasticsearch.sh` file. 
   * However, applying two successive patches (X->Y) & (Y->current) mean to _reindex the ES index in a 
   * temporary index with the template version of Y... And we do not know what this template looks like.
   */
  public static final Map<String, Map<Long, IndexPatch>> PATCHES = new LinkedHashMap<String, Map<Long, IndexPatch>>();
  
  /**
   * Interface for lambda function. Goal here is to retrieve the destination index name from the source index name.
   */
  public static interface IndexNameChange { public String get(String sourceIndex); }
  
  /**
   * PATCH
   * Special meaning 
   * - script==null    means that there is no way to upgrade it automatically
   * - script==""      means that the upgrade do not need any transform script (default value directly filled by Elasticsearch)
   */
  public static class IndexPatch {
    public String tmpIndex;
    public IndexNameChange transform; // compute destination index name from source index name
    public String script;
    
    public IndexPatch(String tmpIndex, IndexNameChange transform, String script) {
      this.tmpIndex = tmpIndex;
      this.transform = transform;
      this.script = script;
    }
  }
  
  private static void loadPatch(String template, Long fromVersion, Long toVersion, String tmpIndexPattern, IndexNameChange transform, String script) {
    Long currentVersion = Deployment.getElasticsearchTemplatesVersion().get(template);
    
    if(fromVersion < 1) { // Forbidden patch
      throw new RuntimeException("Patches for version before 1 do not make sense. Version 0 of a template is an undetermined version.");
    }
    
    if(toVersion != currentVersion) { // Deprecated patch
      log.warn("Trying to load a deprecated ES index patch, will be discarded.");
      return;
    }
    
    Map<Long, IndexPatch> templatePatches = PATCHES.get(template);
    if(templatePatches == null) {
      templatePatches = new LinkedHashMap<Long, IndexPatch>();
      PATCHES.put(template, templatePatches);
    }
    if(templatePatches.get(fromVersion) != null) {
      throw new RuntimeException("Trying to load an ES index patch for template "+template+" for version "+ fromVersion +" but one is already loaded.");
    }
    templatePatches.put(fromVersion, new IndexPatch(tmpIndexPattern, transform, script));
  }
  
  private static void loadPatch(String template, int fromVersion, int toVersion, String tmpIndexPattern, IndexNameChange transform, String script) {
    loadPatch(template, (long) fromVersion, (long) toVersion, tmpIndexPattern, transform, script);
  }

  /*****************************************
  *
  * ES Index Patches
  *
  *****************************************/
  // /!\ tmpIndex must match the index_pattern in `evolution-setup-elasticsearch.sh` in the current version,
  // in order to be pushed in a ES index with the current template version !
  static {
    /*****************************************
    *
    * root template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1): No change
    // - from 1.5.2 (1) to 2.0.0 (2): No change (version setup)
    loadPatch("root"                              , 1, 2, null, null, null); // Root template management - Not Implemented Yet

    /*****************************************
    *
    * subscriberprofile template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1): No change
    // - from 1.5.2 (1) to 2.0.0 (2):
    //   - tenantID                                        (new)
    //   - evaluationDate                                  (date format change) 
    //   - evolutionSubscriberStatusChangeDate             (date format change)
    //   - vouchers.voucherExpiryDate                      (date format change)
    //   - vouchers.voucherDeliveryDate                    (date format change)
    //   - loyaltyPrograms.loyaltyProgramEnrollmentDate    (date format change)
    //   - loyaltyPrograms.loyaltyProgramExitDate          (date format change)
    //   - loyaltyPrograms.tierUpdateDate                  (date format change)
    //   - pointBalances.earliestExpirationDate            (date format change)
    //   - pointBalances.expirationDates.date              (date format change)
    // - from 2.0.0 (2) to 2.0.0 (5):
    //   - complexFields                                   (new)
    loadPatch("subscriberprofile"                 , 1, 3, "subscriberprofile_tmp", null, null); // Special - do not try to upgrade this index, remove it (or keep it, for snapshots) !
    loadPatch("subscriberprofile"                 , 2, 3, "subscriberprofile_tmp", null, null);
    
    /*****************************************
    *
    * detailedrecords_bonuses template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1):
    //   - creationDate                                    (new)
    // - from 1.5.2 (1) to 2.0.0 (2):
    //   - tenantID                                        (new)
    //   - deliverableExpirationDate                       (rename & date format change)
    // - from to 2.0.0 (2) to 2.0.0_2 (3) :
    //   - origin                                          (was not indexed: index config was set to false)
    // - from to 2.0.0_2 (3) to 2.0.0_2 (4) :
    //   - returnCode                                      (was a keyword, is now a integer)
    loadPatch("detailedrecords_bonuses"           , 1, 4, "detailedrecords_bonuses-_tmp", (s) -> s,
        "ctx._source.tenantID = 1;"
      + "def dateString = ctx._source.remove(\\\"deliverableExpirationDate\\\");" 
      + "if (dateString == null) {"
      +   "dateString = \\\"2020-01-01T00:00:00.000Z\\\";"
      + "}"
      + "DateTimeFormatter inputFormat = DateTimeFormatter.ofPattern(\\\"yyyy-MM-dd'T'HH:mm:ss.SSSVV\\\");"
      + "DateTimeFormatter outputFormat = DateTimeFormatter.ofPattern(\\\"yyyy-MM-dd HH:mm:ss.SSSZZ\\\");"
      + "ZonedDateTime zdt = ZonedDateTime.parse(dateString, inputFormat);"
      + "ctx._source.deliverableExpirationDate = zdt.format(outputFormat);");
    loadPatch("detailedrecords_bonuses"           , 2, 4, "detailedrecords_bonuses-_tmp", (s) -> s, "");
    loadPatch("detailedrecords_bonuses"           , 3, 4, "detailedrecords_bonuses-_tmp", (s) -> s, "");
    
    /*****************************************
    *
    * detailedrecords_tokens template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1): No change
    // - from 1.5.2 (1) to 2.0.0 (2):
    //   - tenantID                                        (new)
    // - from to 2.0.0 (2) to 2.0.0_2 (3) :
    //   - origin                                          (was not indexed: index config was set to false)
    loadPatch("detailedrecords_tokens"            , 1, 3, "detailedrecords_tokens-_tmp", (s) -> s,
        "ctx._source.tenantID = 1;");
    loadPatch("detailedrecords_tokens"            , 2, 3, "detailedrecords_tokens-_tmp", (s) -> s, "");
    
    /*****************************************
    *
    * detailedrecords_offers template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1):
    //   - creationDate                                    (new)
    // - from 1.5.2 (1) to 2.0.0 (2):
    //   - tenantID                                        (new)
    // - from to 2.0.0 (2) to 2.0.0_2 (3) :
    //   - origin                                          (was not indexed: index config was set to false)
    // - from to 2.0.0_2 (3) to 2.0.0_2 (4) :
    //   - returnCode                                      (was a keyword, is now a integer)
    loadPatch("detailedrecords_offers"            , 1, 4, "detailedrecords_offers-_tmp", (s) -> s,
        "ctx._source.tenantID = 1;");
    loadPatch("detailedrecords_offers"            , 2, 4, "detailedrecords_offers-_tmp", (s) -> s, "");
    loadPatch("detailedrecords_offers"            , 3, 4, "detailedrecords_offers-_tmp", (s) -> s, "");
    
    /*****************************************
    *
    * detailedrecords_vouchers template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1): No change
    // - from 1.5.2 (1) to 2.0.0 (2):
    //   - tenantID                                        (new)
    //   - returnCode                                      (definition)
    // - from to 2.0.0 (2) to 2.0.0_2 (3) :
    //   - origin                                          (was not indexed: index config was set to false)
    // - from to 2.0.0_2 (3) to 2.0.0_2 (4) :
    //   - returnCode                                      (was a keyword, is now a integer)
    loadPatch("detailedrecords_vouchers"          , 1, 4, "detailedrecords_vouchers-_tmp", (s) -> s,
        "ctx._source.tenantID = 1;");
    loadPatch("detailedrecords_vouchers"          , 2, 4, "detailedrecords_vouchers-_tmp", (s) -> s, "");
    loadPatch("detailedrecords_vouchers"          , 3, 4, "detailedrecords_vouchers-_tmp", (s) -> s, "");

    /*****************************************
    *
    * detailedrecords_messages template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1):
    //   - destination                                     (definition)
    //   - contactType                                     (definition)
    // - from 1.5.2 (1) to 2.0.0 (2):
    //   - tenantID                                        (new)
    // - from to 2.0.0 (2) to 2.0.0_2 (3) :
    //   - origin                                          (was not indexed: index config was set to false)
    loadPatch("detailedrecords_messages"          , 1, 3, "detailedrecords_messages-_tmp", (s) -> s,
        "ctx._source.tenantID = 1;");
    loadPatch("detailedrecords_messages"          , 2, 3, "detailedrecords_messages-_tmp", (s) -> s, "");

    /*****************************************
    *
    * journeystatistic template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1):
    //   - journeyExitDate                                 (date format change)
    // - from 1.5.2 (1) to 2.0.0 (2):
    //   - tenantID                                        (new)
    //   - transitionDate                                  (date format change)
    loadPatch("journeystatistic"                  , 1, 2, "journeystatistic_tmp", (s) -> s,
        "ctx._source.tenantID = 1;"
      + "def dateString = ctx._source.remove(\\\"transitionDate\\\");" 
      + "if (dateString == null) {"
      +   "dateString = \\\"2020-01-01T00:00:00.000Z\\\";"
      + "}"
      + "DateTimeFormatter inputFormat = DateTimeFormatter.ofPattern(\\\"yyyy-MM-dd'T'HH:mm:ss.SSSVV\\\");"
      + "DateTimeFormatter outputFormat = DateTimeFormatter.ofPattern(\\\"yyyy-MM-dd HH:mm:ss.SSSZZ\\\");"
      + "ZonedDateTime zdt = ZonedDateTime.parse(dateString, inputFormat);"
      + "ctx._source.transitionDate = zdt.format(outputFormat);");

    /*****************************************
    *
    * datacube_subscriberprofile template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1): No change
    // - from 1.5.2 (1) to 2.0.0 (2):
    //   - INDEX NAME CHANGE                               (tX_) 
    //   - filter.tenantID                                 (new)
    loadPatch("datacube_subscriberprofile"        , 1, 2, "t_tmp_datacube_subscriberprofile", (s) -> "t1_"+s, // Index name change !
        "ctx._source.tenantID = 1;");

    /*****************************************
    *
    * datacube_loyaltyprogramshistory template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1):
    //   - metric.rewards.redemptions                      (new)
    // - from 1.5.2 (1) to 2.0.0 (2):
    //   - INDEX NAME CHANGE                               (tX_)
    //   - filter.tenantID                                 (new)
    loadPatch("datacube_loyaltyprogramshistory"   , 1, 2, "t_tmp_datacube_loyaltyprogramshistory", (s) -> "t1_"+s, // Index name change !
        "ctx._source.tenantID = 1;");

    /*****************************************
    *
    * datacube_loyaltyprogramschanges template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1): No change
    // - from 1.5.2 (1) to 2.0.0 (2):
    //   - INDEX NAME CHANGE                               (tX_)
    //   - filter.tenantID                                 (new)
    loadPatch("datacube_loyaltyprogramschanges"   , 1, 2, "t_tmp_datacube_loyaltyprogramschanges", (s) -> "t1_"+s, // Index name change !
        "ctx._source.tenantID = 1;");

    /*****************************************
    *
    * datacube_journeytraffic template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1):
    //   - INDEX NAME CHANGE                               (from yyyy-MM-dd to YYYY-'w'ww)
    // - from 1.5.2 (1) to 2.0.0 (2):
    //   - INDEX NAME CHANGE                               (tX_)
    //   - filter.tenantID                                 (new)
    loadPatch("datacube_journeytraffic"           , 1, 2, "t_tmp_datacube_journeytraffic-_tmp", (s) -> "t1_"+s, // Index name change !
        "ctx._source.tenantID = 1;");

    /*****************************************
    *
    * datacube_journeyrewards template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1):
    //   - INDEX NAME CHANGE                               (from yyyy-MM-dd to YYYY-'w'ww)
    // - from 1.5.2 (1) to 2.0.0 (2):
    //   - INDEX NAME CHANGE                               (tX_)
    //   - filter.tenantID                                 (new)
    loadPatch("datacube_journeyrewards"           , 1, 2, "t_tmp_datacube_journeyrewards-_tmp", (s) -> "t1_"+s, // Index name change !
        "ctx._source.tenantID = 1;");

    /*****************************************
    *
    * datacube_odr template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1): No change
    // - from 1.5.2 (1) to 2.0.0 (2):
    //   - INDEX NAME CHANGE                               (tX_)
    //   - filter.tenantID                                 (new)
    // - from to 2.0.0 (2) to 2.0.0_2 (3) :
    //   - filter.origin                                   (new)
    loadPatch("datacube_odr"                      , 1, 3, "t_tmp_datacube_odr", (s) -> "t1_"+s, // Index name change !
        "ctx._source.tenantID = 1;");
    loadPatch("datacube_odr"                      , 2, 3, "t_tmp_datacube_odr", (s) -> s, "");

    /*****************************************
    *
    * datacube_bdr template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1): No change
    // - from 1.5.2 (1) to 2.0.0 (2):
    //   - INDEX NAME CHANGE                               (tX_)
    //   - filter.tenantID                                 (new)
    // - from to 2.0.0 (2) to 2.0.0_2 (3) :
    //   - filter.origin                                   (new)
    loadPatch("datacube_bdr"                      , 1, 3, "t_tmp_datacube_bdr", (s) -> "t1_"+s, // Index name change !
        "ctx._source.tenantID = 1;");
    loadPatch("datacube_bdr"                      , 2, 3, "t_tmp_datacube_bdr", (s) -> s, "");

    /*****************************************
    *
    * datacube_messages template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1):
    //   - filter.contactType                              (definition)
    // - from 1.5.2 (1) to 2.0.0 (2):
    //   - INDEX NAME CHANGE                               (tX_)
    //   - filter.tenantID                                 (new)
    loadPatch("datacube_messages"                 , 1, 2, "t_tmp_datacube_messages", (s) -> "t1_"+s, // Index name change !
        "ctx._source.tenantID = 1;");

    /*****************************************
    *
    * datacube_vdr template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.2 (1) to 2.0.0 (2): New index (version setup)
    loadPatch("datacube_vdr"                      , 1, 2, "t_tmp_datacube_vdr", (s) -> "t1_"+s, "");

    /*****************************************
    *
    * mapping_modules template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1): No change
    // - from 1.5.2 (1) to 2.0.0 (2): No change (version setup)
    loadPatch("mapping_modules"                   , 1, 2, "mapping_modules_tmp", (s) -> s, "");

    /*****************************************
    *
    * mapping_journeys template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1): No change
    // - from 1.5.2 (1) to 2.0.0 (2):
    //   - tenantID                                        (new)
    loadPatch("mapping_journeys"                  , 1, 2, "mapping_journeys_tmp", (s) -> s,
        "ctx._source.tenantID = 1;");

    /*****************************************
    *
    * mapping_journeyrewards template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1): No change
    // - from 1.5.2 (1) to 2.0.0 (2): No change (version setup)
    loadPatch("mapping_journeyrewards"            , 1, 2, "mapping_journeyrewards_tmp", (s) -> s, "");

    /*****************************************
    *
    * mapping_deliverables template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1): No change
    // - from 1.5.2 (1) to 2.0.0 (2): No change (version setup)
    loadPatch("mapping_deliverables"              , 1, 2, "mapping_deliverables_tmp", (s) -> s, "");

    /*****************************************
    *
    * mapping_basemanagement template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1): No change
    // - from 1.5.2 (1) to 2.0.0 (2):
    //   - createdDate                                     (date format change)
    loadPatch("mapping_basemanagement"            , 1, 2, "mapping_basemanagement_tmp", (s) -> s,
        "def dateString = ctx._source.remove(\\\"createdDate\\\");" 
      + "if (dateString == null) {"
      +   "dateString = \\\"2020-01-01T00:00:00.000Z\\\";"
      + "}"
      + "DateTimeFormatter inputFormat = DateTimeFormatter.ofPattern(\\\"yyyy-MM-dd'T'HH:mm:ss.SSSVV\\\");"
      + "DateTimeFormatter outputFormat = DateTimeFormatter.ofPattern(\\\"yyyy-MM-dd HH:mm:ss.SSSZZ\\\");"
      + "ZonedDateTime zdt = ZonedDateTime.parse(dateString, inputFormat);"
      + "ctx._source.createdDate = zdt.format(outputFormat);");

    /*****************************************
    *
    * mapping_journeyobjective template
    *
    *****************************************/
    // Changes: 
    // - from 1.5.0 (<1) to 1.5.2 (1): No change
    // - from 1.5.2 (1) to 2.0.0 (2): No change (version setup)
    loadPatch("mapping_journeyobjective"          , 1, 2, "mapping_journeyobjective_tmp", (s) -> s, "");
  }
  
}
