package com.evolving.nglm.evolution.otp;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.core.UniqueKeyServer;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.GUIManager;
import com.evolving.nglm.evolution.RESTAPIGenericReturnCodes;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.GUIManagerGeneral;

public class GUIManagerOTP
{
  
  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(GUIManagerOTP.class);
  
  /*****************************************
  *
  *  processGetOTPTypeList
  *
  *****************************************/

  public static JSONObject processGetOTPTypeList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived, OTPTypeService otpTypeService, int tenantID)
  {
    /*****************************************
    *
    *  retrieve and convert otpTypes
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> otpTypes = new ArrayList<JSONObject>();    
    Collection <GUIManagedObject> otpTypeObjects = new ArrayList<GUIManagedObject>();
    
    if (jsonRoot.containsKey("ids"))
      {
        JSONArray otpTypeIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
        for (int i = 0; i < otpTypeIDs.size(); i++)
          {
            String otpTypeID = otpTypeIDs.get(i).toString();
            GUIManagedObject otpType = otpTypeService.getStoredOTPType(otpTypeID, includeArchived);
            if (otpType != null && otpType.getTenantID() == tenantID)
              {
                otpTypeObjects.add(otpType);
              }
          }
      }
    else
      {
        otpTypeObjects = otpTypeService.getStoredOTPTypes(includeArchived, tenantID);
      }
    for (GUIManagedObject otpType : otpTypeObjects)
      {
        otpTypes.add(otpTypeService.generateResponseJSON(otpType, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("otpTypes", JSONUtilities.encodeArray(otpTypes));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processOTPType
  *
  *****************************************/

  public static JSONObject processGetOTPType(String userID, JSONObject jsonRoot, boolean includeArchived, OTPTypeService otpTypeService, int tenantID)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String otpTypeID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate scoring strategy
    *
    *****************************************/

    GUIManagedObject otpType = otpTypeService.getStoredOTPType(otpTypeID, includeArchived);
    JSONObject otpTypeJSON = otpTypeService.generateResponseJSON(otpType, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (otpType != null) ? "ok" : "otpTypeNotFound");
    if (otpType != null) response.put("otpType", otpTypeJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutOTPType
  *
  *****************************************/

  public static JSONObject processPutOTPType(String userID, JSONObject jsonRoot, UniqueKeyServer epochServer, OTPTypeService otpTypeService, int tenantID)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();
    Boolean dryRun = false;
    

    /*****************************************
    *
    *  dryRun
    *
    *****************************************/
    if (jsonRoot.containsKey("dryRun")) {
      dryRun = JSONUtilities.decodeBoolean(jsonRoot, "dryRun", false);
    }


    /*****************************************
    *
    *  otpTypeID
    *
    *****************************************/

    String otpTypeID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (otpTypeID == null)
      {
        otpTypeID = otpTypeService.generateOTPTypeID();
        jsonRoot.put("id", otpTypeID);
      }

    /*****************************************
    *
    *  existing otpType
    *
    *****************************************/

    GUIManagedObject existingOTPType = otpTypeService.getStoredOTPType(otpTypeID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingOTPType != null && existingOTPType.getReadOnly())
      {
        response.put("id", existingOTPType.getGUIManagedObjectID());
        response.put("accepted", existingOTPType.getAccepted());
        response.put("valid", existingOTPType.getAccepted());
        response.put("processing", otpTypeService.isActiveOTPType(existingOTPType, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process otpType
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate otpType
        *
        ****************************************/

        OTPType otpType = new OTPType(jsonRoot, epoch, existingOTPType, tenantID);

        /*****************************************
        *
        *  store
        *
        *****************************************/
        if (!dryRun)
          {
            otpTypeService.putOTPType(otpType, (existingOTPType == null), userID, tenantID);
          }

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", otpType.getOTPTypeID());
        response.put("accepted", otpType.getAccepted());
        response.put("valid", otpType.getAccepted());
        response.put("processing", otpTypeService.isActiveOTPType(otpType, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch, tenantID);

        //
        //  store
        //
        if (!dryRun)
          {
            otpTypeService.putOTPType(incompleteObject, (existingOTPType == null), userID, tenantID);
          }

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "otpTypeNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }
  
  /*****************************************
  *
  * processRemoveSupplier
  *
  *****************************************/

 public static JSONObject processRemoveOTPType(String userID, JSONObject jsonRoot, GUIManagerGeneral guiManagerGeneral, OTPTypeService otpTypeService, int tenantID)
 {
   /****************************************
    *
    * response
    *
    ****************************************/

   HashMap<String, Object> response = new HashMap<String, Object>();
   JSONObject dependencyRequest = new JSONObject();

   /*****************************************
    *
    * now
    *
    *****************************************/

   Date now = SystemTime.getCurrentTime();

   String responseCode = "";
   String singleIDresponseCode = "";
   List<GUIManagedObject> otpTypes = new ArrayList<>();
   JSONArray otpTypeIDs = new JSONArray();
   List<String> validIDs = new ArrayList<>();

   /****************************************
    *
    * argument
    *
    ****************************************/
   boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

   //
   // remove single supplier
   //
   if (jsonRoot.containsKey("id"))
     {
       String supplierID = JSONUtilities.decodeString(jsonRoot, "id", false);
       otpTypeIDs.add(supplierID);
       GUIManagedObject supplier = otpTypeService.getStoredOTPType(supplierID);
       if (supplier != null && (force || !supplier.getReadOnly()))
         singleIDresponseCode = "ok";
       else if (supplier != null)
         singleIDresponseCode = "failedReadOnly";
       else singleIDresponseCode = "supplierNotFound";
     }
   //
   // multiple deletion
   //

   if (jsonRoot.containsKey("ids"))
     {
       otpTypeIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids", false);
     }

   for (int i = 0; i < otpTypeIDs.size(); i++)
     {
       String otpTypeID = otpTypeIDs.get(i).toString();
       GUIManagedObject otpType = otpTypeService.getStoredOTPType(otpTypeID);

       if (otpType != null && (force || !otpType.getReadOnly()))
         {
           dependencyRequest.put("apiVersion", 1);
           dependencyRequest.put("objectType", "otpType");
           dependencyRequest.put("id", otpType.getGUIManagedObjectID());

           JSONObject dependenciesObject = guiManagerGeneral.processGetDependencies(userID, dependencyRequest, tenantID);
           JSONArray dependencies = JSONUtilities.decodeJSONArray(dependenciesObject, "dependencies", new JSONArray());
           boolean parentDependency = false;
           if (dependencies.size() != 0)
             {
               for (int j = 0; j < dependencies.size(); j++)
                 {
                   JSONObject dependent = (JSONObject) dependencies.get(j);
                   String ojectType = JSONUtilities.decodeString(dependent, "objectType", false);
                   if (("supplier".equals(ojectType)) || ("product".equals("ojectType")))
                     {
                       parentDependency = true;
                       break;
                     }
                 }
             }
           if (!parentDependency) {
             otpTypes.add(otpType);
             validIDs.add(otpTypeID);
           }
           else
             {
               if (jsonRoot.containsKey("id")) {
               response.put("responseCode", RESTAPIGenericReturnCodes.DEPENDENCY_RESTRICTION.getGenericResponseCode());
               response.put("responseMessage",
                   RESTAPIGenericReturnCodes.DEPENDENCY_RESTRICTION.getGenericResponseMessage());
               return JSONUtilities.encodeObject(response);
               }
             }
         }
     }

   /*****************************************
    *
    * remove
    *
    *****************************************/
   for (GUIManagedObject otpType : otpTypes)
     {
       if (otpType != null && (force || !otpType.getReadOnly()))
         {
            otpTypeService.removeOTPType(otpType.getGUIManagedObjectID(), userID, tenantID);
         }
     }

   /*****************************************
    *
    * responseCode
    *
    *****************************************/

   if (jsonRoot.containsKey("id"))
     {
       response.put("responseCode", singleIDresponseCode);
       return JSONUtilities.encodeObject(response);
     }

   else
     {
       response.put("responseCode", "ok");
     }

   /*****************************************
    *
    * response
    *
    *****************************************/
   response.put("removedSupplierIDS", JSONUtilities.encodeArray(validIDs));

   return JSONUtilities.encodeObject(response);
 }



}
