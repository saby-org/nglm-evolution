package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.JSONUtilities;

public class DeliveryManagerAccount
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String providerID;
  private List<Account> accounts;
  
  //
  //  accessors
  //

  public String getProviderID() { return providerID; }
  public List<Account> getAccounts() { return accounts; }
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public DeliveryManagerAccount(String providerID, List<Account> accounts)
  {
    this.providerID = providerID;
    this.accounts = accounts;
  }

  /*****************************************
  *
  *  constructor -- external
  *
  *****************************************/

  public DeliveryManagerAccount(JSONObject jsonRoot)
  {
    this.providerID = JSONUtilities.decodeString(jsonRoot, "providerID", true);
    this.accounts = decodeAccounts(JSONUtilities.decodeJSONArray(jsonRoot, "accounts", true));
  }
  
  /*****************************************
  *
  *  decodeSegments
  *
  *****************************************/

  private List<Account> decodeAccounts(JSONArray jsonArray)
   {
    List<Account> result = new ArrayList<Account>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject accountJSON = (JSONObject) jsonArray.get(i);
        Account account = new Account(accountJSON);
        if (account != null)
          {
            result.add(account);
          }
      }
    return result;
  }

  /*****************************************
  *
  *  class Account
  *
  *****************************************/
  
  public class Account
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String externalAccountID;
    private String name;
    private boolean creditable;
    private boolean debitable;
    
    //
    //  accessors
    //

    public String getExternalAccountID() { return externalAccountID; }
    public String getName() { return name; }
    public boolean getCreditable() { return creditable; }
    public boolean getDebitable() { return debitable; }
    
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public Account(String externalAccountID, String name, boolean creditable, boolean debitable)
    {
      this.externalAccountID = externalAccountID;
      this.name = name;
      this.creditable = creditable;
      this.debitable = debitable;
    }

    /*****************************************
    *
    *  constructor -- external
    *
    *****************************************/

    public Account(JSONObject jsonRoot)
    {
      this.externalAccountID = JSONUtilities.decodeString(jsonRoot, "externalAccountID", true);
      this.name = JSONUtilities.decodeString(jsonRoot, "name", true);
      this.creditable = JSONUtilities.decodeBoolean(jsonRoot, "creditable", true);
      this.debitable = JSONUtilities.decodeBoolean(jsonRoot, "debitable", true);
    }
    
  }
}
