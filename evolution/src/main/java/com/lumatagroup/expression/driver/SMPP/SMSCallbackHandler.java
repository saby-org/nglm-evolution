package com.lumatagroup.expression.driver.SMPP;

import java.lang.reflect.Method;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SMSCallbackHandler
{
  private static final Logger log = LoggerFactory.getLogger(SMSCallbackHandler.class);
  private Method setCorrelator = null;
  private Method updateDeliveryRequest = null;
  private Method completeDeliveryRequest = null;
  private Method setDeliveryStatus = null;
  private Method setDeliveryDate = null;
  private Object deliveryManager = null;
  
  public SMSCallbackHandler(Object deliveryManager){
    this.deliveryManager = deliveryManager;
  }
  
  public void invokeSetCorrelator(Object deliveryRequest, String correlator){
    if(setCorrelator == null){
      try
        {
          setCorrelator = deliveryRequest.getClass().getMethod("setCorrelator", new Class[]{String.class});
        } catch (Exception e)
        {
          log.warn("SMSCallbackHandler.invokeSetCorrelator(problem loading delivery request", e);
        }
    }
    try
      {
        setCorrelator.invoke(deliveryRequest, new Object[]{correlator});
      } catch (Exception e)
      {
        log.warn("SMSCallbackHandler.invokeSetCorrelator(exception "+ e.getClass().getName() + " while invoking setCorrelator ", e);
      }
  }
  
  public void invokeUpdateDeliveryRequest(Object deliveryRequest){
    if(updateDeliveryRequest == null){
      try
        {
          updateDeliveryRequest = deliveryRequest.getClass().getMethod("updateRequest", new Class[]{Class.forName("com.evolving.nglm.evolution.DeliveryRequest")});
        } catch (Exception e)
        {
          log.warn("SMSCallbackHandler.invokeSetCorrelator(problem loading delivery request", e);
        }
    }
    try
      {
        updateDeliveryRequest.invoke(deliveryManager, new Object[]{deliveryRequest});
      } catch (Exception e)
      {
        log.warn("SMSCallbackHandler.invokeSetCorrelator(exception "+ e.getClass().getName() + " while invoking setCorrelator ", e);
      }
  }
  
  public void invokeCompleteDeliveryRequest(Object deliveryRequest, Object deliveryStatus, Date date){
    if(setDeliveryStatus == null){
      try
        {
          setDeliveryStatus = deliveryRequest.getClass().getMethod("setDeliveryStatus", new Class[]{Class.forName("com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus")});
        } catch (Exception e)
        {
          log.warn("SMSCallbackHandler.invokeSetCorrelator(problem loading delivery request", e);
        }
    }
    try
      {
        setDeliveryStatus.invoke(deliveryRequest, new Object[]{deliveryStatus});
      } catch (Exception e)
      {
        log.warn("SMSCallbackHandler.invokeSetCorrelator(exception "+ e.getClass().getName() + " while invoking setCorrelator ", e);
      }
    
    if(setDeliveryDate == null){
      try
        {
          setDeliveryDate = deliveryRequest.getClass().getMethod("setDeliveryDate", new Class[]{Date.class});
        } catch (Exception e)
        {
          log.warn("SMSCallbackHandler.invokeSetCorrelator(problem loading delivery request", e);
        }
    }
    try
      {
        setDeliveryStatus.invoke(deliveryRequest, new Object[]{date});
      } catch (Exception e)
      {
        log.warn("SMSCallbackHandler.invokeSetCorrelator(exception "+ e.getClass().getName() + " while invoking setCorrelator ", e);
      }
    
    
    if(completeDeliveryRequest == null){
      try
        {
          completeDeliveryRequest = deliveryRequest.getClass().getMethod("completeRequest", new Class[]{Class.forName("com.evolving.nglm.evolution.DeliveryRequest")});
        } catch (Exception e)
        {
          log.warn("SMSCallbackHandler.invokeSetCorrelator(problem loading delivery request", e);
        }
    }
    try
      {
        completeDeliveryRequest.invoke(deliveryManager, new Object[]{deliveryRequest});
      } catch (Exception e)
      {
        log.warn("SMSCallbackHandler.invokeSetCorrelator(exception "+ e.getClass().getName() + " while invoking setCorrelator ", e);
      }
    
    
  }
  
}
