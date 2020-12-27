package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.SalesChannel;
import com.evolving.nglm.evolution.SalesChannelService;
import com.evolving.nglm.evolution.SubscriberMessageTemplate;
import com.evolving.nglm.evolution.SubscriberMessageTemplateService;

public class SubscriberMessageTemplatesMap extends GUIManagedObjectMap<SubscriberMessageTemplate>
{
  protected static final Logger log = LoggerFactory.getLogger(SubscriberMessageTemplatesMap.class);

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private SubscriberMessageTemplateService service;
  
  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  
  public SubscriberMessageTemplatesMap(SubscriberMessageTemplateService service) {
    super(SubscriberMessageTemplate.class);
    this.service = service;
  }
  
  /*****************************************
  *
  * GUIManagedObjectMap implementation
  *
  *****************************************/
  // TODO: for the moment, we also retrieve archived objects
  protected Collection<GUIManagedObject> getCollection(int tenantID) { return this.service.getStoredSubscriberMessageTemplates(true, tenantID); }
}