/*****************************************************************************
*
*  TokenTypeService.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Collection;
import java.util.Date;
import java.util.Objects;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.SystemTime;

public class TokenTypeService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(TokenTypeService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private TokenTypeListener tokenTypeListener = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public TokenTypeService(String bootstrapServers, String groupID, String tokenTypeTopic, boolean masterService, TokenTypeListener tokenTypeListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "TokenTypeService", groupID, tokenTypeTopic, masterService, getSuperListener(tokenTypeListener), "putTokenType", "removeTokenType", notifyOnSignificantChange);
  }
  
  //
  //  constructor
  //

  public TokenTypeService(String bootstrapServers, String groupID, String tokenTypeTopic, boolean masterService, TokenTypeListener tokenTypeListener)
  {
    this(bootstrapServers, groupID, tokenTypeTopic, masterService, tokenTypeListener, true);
  }

  //
  //  constructor
  //

  public TokenTypeService(String bootstrapServers, String groupID, String tokenTypeTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, tokenTypeTopic, masterService, (TokenTypeListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(TokenTypeListener tokenTypeListener)
  {
    GUIManagedObjectListener superListener = null;
    if (tokenTypeListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { tokenTypeListener.tokenTypeActivated((TokenType) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { tokenTypeListener.tokenTypeDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getTokenTypes
  *
  *****************************************/

  public String generateTokenTypeID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredTokenType(String tokenTypeID) { return getStoredGUIManagedObject(tokenTypeID); }
  public GUIManagedObject getStoredTokenType(String tokenTypeID, boolean includeArchived) { return getStoredGUIManagedObject(tokenTypeID, includeArchived); }
  public Collection<GUIManagedObject> getStoredTokenTypes(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredTokenTypes(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActiveTokenType(GUIManagedObject tokenTypeUnchecked, Date date) { return isActiveGUIManagedObject(tokenTypeUnchecked, date); }
  public TokenType getActiveTokenType(String tokenTypeID, Date date) { return (TokenType) getActiveGUIManagedObject(tokenTypeID, date); }
  public Collection<TokenType> getActiveTokenTypes(Date date, int tenantID) { return (Collection<TokenType>) getActiveGUIManagedObjects(date, tenantID); }

  /*****************************************
  *
  *  putTokenType
  *
  *****************************************/

  public void putTokenType(GUIManagedObject tokenType, boolean newObject, String userID) { putGUIManagedObject(tokenType, SystemTime.getCurrentTime(), newObject, userID); }

  /*****************************************
  *
  *  removeTokenType
  *
  *****************************************/

  public void removeTokenType(String tokenTypeID, String userID, int tenantID) { removeGUIManagedObject(tokenTypeID, SystemTime.getCurrentTime(), userID, tenantID); }

  /*****************************************
  *
  *  interface TokenTypeListener
  *
  *****************************************/

  public interface TokenTypeListener
  {
    public void tokenTypeActivated(TokenType tokenType);
    public void tokenTypeDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  tokenTypeListener
    //

    TokenTypeListener tokenTypeListener = new TokenTypeListener()
    {
      @Override public void tokenTypeActivated(TokenType tokenType) { log.trace("tokenType activated: " + tokenType.getTokenTypeID()); }
      @Override public void tokenTypeDeactivated(String guiManagedObjectID) { log.trace("tokenType deactivated: " + guiManagedObjectID); }
    };

    //
    //  tokenTypeService
    //

    TokenTypeService tokenTypeService = new TokenTypeService(Deployment.getBrokerServers(), "example-tokentypeservice-001", Deployment.getTokenTypeTopic(), false, tokenTypeListener);
    tokenTypeService.start();

    //
    //  sleep forever
    //

    while (true)
      {
        try
          {
            Thread.sleep(Long.MAX_VALUE);
          }
        catch (InterruptedException e)
          {
            //
            //  ignore
            //
          }
      }
  }
}
