package com.evolving.nglm.evolution;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.management.RuntimeErrorException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.Pair;

public class EDREventsESFieldsDetails
{
  //
  //  data
  //
  
  private String eventName;
  private List<String> esModelClasses;
  private List<ESField> fields;

  /***************************
   * 
   * EDREventsESFieldsDetails
   * 
   ***************************/

  public EDREventsESFieldsDetails(JSONObject jsonRoot)
  {
    this.eventName = JSONUtilities.decodeString(jsonRoot, "eventName", true).toUpperCase();
    String esModelClassesRaw = JSONUtilities.decodeString(jsonRoot, "esModelClasses", true);
    this.esModelClasses = Arrays.asList(esModelClassesRaw.split(",", -1));
    this.fields = decodeFields(jsonRoot);

  }

  /***************************
   * 
   * decodeFields
   * 
   ***************************/
  
  private List<ESField> decodeFields(JSONObject jsonRoot)
  {
    List<ESField> fields = new ArrayList<ESField>();
    JSONArray fieldsArray = JSONUtilities.decodeJSONArray(jsonRoot, "fields", true);
    for (int i = 0; i < fieldsArray.size(); i++)
      {
        JSONObject fieldJSON = (JSONObject) fieldsArray.get(i);
        ESField field = new ESField(fieldJSON);
        fields.add(field);

      }
    return fields;
  }
  
  //
  //  getters
  //
  
  public String getEventName() { return eventName; }
  public List<ESField> getFields() { return fields; }
  public List<String> getEsModelClasses() { return esModelClasses; }
  
  public class ESField
  {
    private String fieldName;
    private String retrieverName;
    
    public ESField(JSONObject fieldJSON)
    {
      this.fieldName = JSONUtilities.decodeString(fieldJSON, "name", true);
      this.retrieverName = JSONUtilities.decodeString(fieldJSON, "retriever", true);
    }
    
    public String getFieldName () { return fieldName; }
    public String getRetrieverName () { return retrieverName; }
    
  }

}
