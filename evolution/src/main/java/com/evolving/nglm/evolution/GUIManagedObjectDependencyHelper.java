package com.evolving.nglm.evolution;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.evolution.GUIManagedObject.ISContaining;
import com.evolving.nglm.evolution.GUIManagedObject.Provides;

/*****************************************
 * 
 *  GUIManagedObjectDependencyHelper
 *
 *****************************************/

public class GUIManagedObjectDependencyHelper
{
  //
  //  log
  //
  
  private static final Logger log = LoggerFactory.getLogger(GUIManagedObjectDependencyHelper.class);
  
  /****************************************
  *
  *  createDependencyTreeMAP
  *
  ****************************************/
  
  public static void createDependencyTreeMAP(Map<Class<? extends GUIManagedObject>, GUIDependencyModelTree> guiDependencyModelTreeMap, GUIDependencyModelTree guiDependencyModelTree, List<Class<? extends GUIManagedObject>>  dependencyList, String objectID, List<JSONObject> dependencyListOutput, boolean fiddleTest, List<GUIService> guiServiceList) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException
  {
    log.info("RAJ K createDependencyTreeMAP for {} and ID {} will look into {} types", guiDependencyModelTree.getGuiManagedObjectTypeID(), objectID, dependencyList);
    for (Class<? extends GUIManagedObject> dependency : dependencyList)
      {
        //
        // stored and container
        //
        
        Collection<GUIManagedObject> containerObjectList = new ArrayList<GUIManagedObject>();
        
        //
        // get storedObjectList
        //
        
        Class serviceClass = guiDependencyModelTreeMap.get(dependency).getServiceClass();
        Method retriver = getProvidesMethod(serviceClass, dependency);
        Object serviceObject  = getService(guiServiceList, serviceClass);
        Collection<GUIManagedObject> storedObjectList = (Collection<GUIManagedObject>) retriver.invoke(serviceObject, null);
        
        //
        //  containerObjectList
        //
        
        if (storedObjectList != null)
          {
            for (GUIManagedObject guiManagedObject : storedObjectList)
              {
                Class actualTypeClass = guiDependencyModelTreeMap.get(dependency).getGuiManagedObjectTypeClass() == Campaign.class ? Journey.class :  guiDependencyModelTreeMap.get(dependency).getGuiManagedObjectTypeClass();
                if (actualTypeClass == guiManagedObject.getClass())
                  {
                    Method containingRetriver = getContainsMethod(guiManagedObject.getClass(), guiDependencyModelTree.getGuiManagedObjectTypeClass());
                    boolean contains = (boolean) containingRetriver.invoke(guiManagedObject, objectID);
                    if (contains) containerObjectList.add(guiManagedObject);
                  }
              }
          }

        //
        // presentation/recursion
        //

        for (GUIManagedObject guiManagedObject : containerObjectList)
          {
            //
            // prepare recursion data 
            //
            
            GUIDependencyModelTree netxGUIDependencyModelTree = guiDependencyModelTreeMap.get(dependency);
            String nextObjectID = guiManagedObject.getGUIManagedObjectID();
            List<JSONObject> nextDependencyOutputList = new LinkedList<JSONObject>();
            
            //
            //  recursion
            //
            
            if (netxGUIDependencyModelTree!= null) createDependencyTreeMAP(guiDependencyModelTreeMap, netxGUIDependencyModelTree, netxGUIDependencyModelTree.getDependencyList(), nextObjectID, nextDependencyOutputList, fiddleTest, guiServiceList);
            
            //
            // dependentMap
            //
            
            Map<String, Object> dependentMap = new LinkedHashMap<String, Object>();
            if (fiddleTest)
              {
                dependentMap.put("name", guiManagedObject.getGUIManagedObjectDisplay());
                dependentMap.put("size", new Integer(500));
                dependentMap.put("children", JSONUtilities.encodeArray(nextDependencyOutputList));
              }
            else
              {
                dependentMap.put("id", guiManagedObject.getGUIManagedObjectID());
                dependentMap.put("name", guiManagedObject.getGUIManagedObjectName());
                dependentMap.put("display", guiManagedObject.getGUIManagedObjectDisplay());
                dependentMap.put("active", guiManagedObject.getActive());
                dependentMap.put("objectType", dependency);
                dependentMap.put("dependents", JSONUtilities.encodeArray(nextDependencyOutputList));
              }
            
            //
            // add
            //
            
            dependencyListOutput.add(JSONUtilities.encodeObject(dependentMap));
          }
      }
  }
  
  /***********************************
   * 
   * getContainsMethod
   * 
   ***********************************/
  
  private static Method getContainsMethod(Class serviceClass, final Class containsType)
  {
    log.info("RAJ K getContainsMethod call for {} in class {}", containsType, serviceClass.getName());
    Method result = null;
    Method[] methods = serviceClass.getMethods();
    for (Method m : methods)
      {
        ISContaining contains = m.getAnnotation(ISContaining.class);
        if (contains != null && containsType == contains.value())
          {
            result = m;
            break;
          }
      }
    if (result == null) throw new ServerRuntimeException(serviceClass.getName() + " does not have a ISContaining method for" + containsType);
    log.info("RAJ K getContainsMethod for {} in class {} is {}", containsType, serviceClass.getName(), result.getName());
    return result;
  }
  
  /***********************************
   * 
   * getProvidesMethod
   * 
   ***********************************/
  
  private static Method getProvidesMethod(Class serviceClass, final Class serviceRetriverType)
  {
    log.info("RAJ K getProvidesMethod call for {} in class {}", serviceRetriverType, serviceClass.getName());
    Method result = null;
    Method[] methods = serviceClass.getMethods();
    for (Method m : methods)
      {
        Provides provides = m.getAnnotation(Provides.class);
        if (provides != null && serviceRetriverType == provides.value())
          {
            result = m;
            break;
          }
      }
    if (result == null) throw new ServerRuntimeException(serviceClass.getName() + " does not have Provides method for " + serviceRetriverType);
    log.info("RAJ K getProvidesMethod for {} in class {} is {}", serviceRetriverType, serviceClass.getName(), result.getName());
    return result;
  }

  /***********************************
   * 
   * getService
   * 
   ***********************************/
  
  private static Object getService(List<GUIService> guiServiceList, final Class serviceClass)
  {
    log.info("RAJ K getService call for {}", serviceClass.getName());
    Object result = null;
    for (GUIService guiService : guiServiceList)
      {
        if (serviceClass == guiService.getClass())
          {
            result = guiService;
            break;
          }
      }
    if (result == null) throw new ServerRuntimeException(serviceClass.getName() + " not found in guiServiceList");
    log.info("RAJ K found service is {}", result.getClass().getName());
    return result;
  }
}
