/*****************************************************************************
*
*  DNBOUtils.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.Journey.ContextUpdate;
import com.evolving.nglm.evolution.Token.TokenStatus;
import com.evolving.nglm.evolution.offeroptimizer.DNBOMatrixAlgorithmParameters;
import com.evolving.nglm.evolution.offeroptimizer.GetOfferException;
import com.evolving.nglm.evolution.offeroptimizer.ProposedOfferDetails;

public class DNBOUtils
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(DNBOUtils.class);
  
  public static final int MAX_PRESENTED_OFFERS = 5;
  private static final int HOW_MANY_TIMES_TO_TRY_TO_GENERATE_A_TOKEN_CODE = 100;
  
  /*****************************************
  *
  *  class ActionManagerDNBO (superclass)
  *
  *****************************************/

  private static class ActionManagerDNBO extends com.evolving.nglm.evolution.ActionManager
  {
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ActionManagerDNBO(JSONObject configuration) throws GUIManagerException
    {
      super(configuration);
    }

    /*****************************************
    *
    *  handleToken
    *
    *****************************************/
        
    protected Object[] handleToken(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
    {
      log.info("ActionManagerDNBO.handleToken() method call");
    	
      /*****************************************
      *
      *  parameters
      *
      *****************************************/
      String scoringStrategyID = (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest, "node.parameter.strategy");
      String tokenTypeID = (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest, "node.parameter.tokentype"); 
      
      /*****************************************
      *
      *  scoring strategy
      *
      *****************************************/
      ScoringStrategy scoringStrategy = evolutionEventContext.getScoringStrategyService().getActiveScoringStrategy(scoringStrategyID, evolutionEventContext.now());
      if (scoringStrategy == null)
        {
          log.error("invalid scoring strategy {}", scoringStrategyID);
          return new Object[] {Collections.<Action>emptyList()};
        }

      log.info("ActionManagerDNBO.handleToken() scoringStrategy valid");

      /*****************************************
      *
      *  token type
      *
      *****************************************/
      TokenType tokenType = evolutionEventContext.getTokenTypeService().getActiveTokenType(tokenTypeID, evolutionEventContext.now());
      if (tokenType == null)
        {
          log.error("no token type {}", tokenTypeID);
          return new Object[] {Collections.<Action>emptyList()};
        }

      log.info("ActionManagerDNBO.handleToken() tokenType valid");

      /*****************************************
      *
      *  action -- generate new token code (different from others already associated with this subscriber)
      *
      *****************************************/
      List<String> currentTokens = evolutionEventContext.getSubscriberState().getSubscriberProfile().getTokens().stream().map(token->token.getTokenCode()).collect(Collectors.toList());
      String tokenCode = null;
      boolean newTokenGenerated = false;
      String codeFormat = tokenType.getCodeFormat();
      for (int i=0; i<HOW_MANY_TIMES_TO_TRY_TO_GENERATE_A_TOKEN_CODE; i++)
      {
        tokenCode = TokenUtils.generateFromRegex(codeFormat);
        if (!currentTokens.contains(tokenCode))
          {
            newTokenGenerated = true;
            break;
          }
      }
      if (!newTokenGenerated)
        {
          if (log.isTraceEnabled()) log.trace("After {} tries, unable to generate a new token code with pattern {}", HOW_MANY_TIMES_TO_TRY_TO_GENERATE_A_TOKEN_CODE, codeFormat);
          return new Object[] {Collections.<Action>emptyList()};
        }
      
      log.info("ActionManagerDNBO.handleToken() token code generated " + tokenCode);

      DNBOToken token = new DNBOToken(tokenCode, subscriberEvaluationRequest.getSubscriberProfile().getSubscriberID(), tokenType);
      token.setScoringStrategyIDs(Collections.<String>singletonList(scoringStrategy.getScoringStrategyID()));
      token.setCreationDate(evolutionEventContext.now());

      /*****************************************
      *
      *  action -- token code
      *
      *****************************************/
      ContextUpdate tokenUpdate = new ContextUpdate(ActionType.ActionManagerContextUpdate);
      tokenUpdate.getParameters().put("action.token.code", token.getTokenCode());

      /*****************************************
      *
      *  Action list
      *
      *****************************************/
      List<Action> actionList = new ArrayList<>();
      actionList.add(token);
      actionList.add(tokenUpdate);

      /*****************************************
      *
      *  return
      *
      *****************************************/
      return new Object[] {actionList, token, tokenUpdate, scoringStrategy, tokenType};
    }
    
    /*****************************************
    *
    *  handleAllocate
    *
    *****************************************/
        
    protected Collection<ProposedOfferDetails> handleAllocate(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest, ScoringStrategy scoringStrategy, DNBOToken token, TokenType tokenType, ContextUpdate tokenUpdate)
    {
      /*****************************************
      *
      *  maxNumberofPlays
      *
      *****************************************/
      int boundCount = token.getBoundCount();
      Integer maxNumberofPlaysInt = tokenType.getMaxNumberOfPlays();
      int maxNumberofPlays = (maxNumberofPlaysInt == null) ? Integer.MAX_VALUE : maxNumberofPlaysInt.intValue();
      if (boundCount >= maxNumberofPlays)
        {
            log.error("maxNumberofPlays has been reached {}", maxNumberofPlays);
            return Collections.<ProposedOfferDetails>emptyList();
        }
      token.setBoundCount(boundCount+1);
      
      /*****************************************
      *
      *  services
      *
      *****************************************/
      OfferService offerService = evolutionEventContext.getOfferService();
      ProductService productService = evolutionEventContext.getProductService();
      ProductTypeService productTypeService = evolutionEventContext.getProductTypeService();
      CatalogCharacteristicService catalogCharacteristicService = evolutionEventContext.getCatalogCharacteristicService();
      DNBOMatrixService dnboMatrixService = evolutionEventContext.getDnboMatrixService();
      SegmentationDimensionService segmentationDimensionService = evolutionEventContext.getSegmentationDimensionService();
      ReferenceDataReader<PropensityKey, PropensityState> propensityDataReader = evolutionEventContext.getPropensityDataReader();
      ReferenceDataReader<String, SubscriberGroupEpoch> subscriberGroupEpochReader = evolutionEventContext.getSubscriberGroupEpochReader();

      Date now = evolutionEventContext.now();
      String subscriberID = evolutionEventContext.getSubscriberState().getSubscriberID();

      StringBuffer returnedLog = new StringBuffer();
      SubscriberProfile subscriberProfile = evolutionEventContext.getSubscriberState().getSubscriberProfile();
      DNBOMatrixAlgorithmParameters dnboMatrixAlgorithmParameters = new DNBOMatrixAlgorithmParameters(dnboMatrixService, 0);

      /*****************************************
      *
      *  Score offers for this subscriber
      *
      *****************************************/
      Collection<ProposedOfferDetails> presentedOffers;
      try
        {
          presentedOffers = TokenUtils.getOffers(now , null, subscriberProfile, scoringStrategy, productService, productTypeService, catalogCharacteristicService, propensityDataReader, subscriberGroupEpochReader, segmentationDimensionService, dnboMatrixAlgorithmParameters, offerService, returnedLog, subscriberID);
        }
      catch (GetOfferException e)
        {
          log.error("unknown offer while scoring {}", e.getLocalizedMessage());
          return Collections.<ProposedOfferDetails>emptyList();
        }

      /*****************************************
      *
      *  transcode list of offers
      *
      *****************************************/
      
      List<String> presentedOfferIDs = new ArrayList<>();
      int index = 0;
      for (ProposedOfferDetails presentedOffer : presentedOffers)
        {
          String offerId = presentedOffer.getOfferId();
          presentedOfferIDs.add(offerId);
          Offer offer = offerService.getActiveOffer(offerId, now);
          if (offer == null)
            {
              log.error("invalid offer returned by scoring {}", offerId);
              return Collections.<ProposedOfferDetails>emptyList();
            }
          tokenUpdate.getParameters().put("action.presented.offer." + (index+1), offer.getDisplay());
          if (++index == MAX_PRESENTED_OFFERS)
            break;
        }
      for (int j=index; j<MAX_PRESENTED_OFFERS; j++)
        {
          tokenUpdate.getParameters().put("action.presented.offer." + (j+1), "");
        }
      
      token.setPresentedOfferIDs(presentedOfferIDs);
      token.setBoundDate(now);
      
      /*****************************************
      *
      *  return
      *
      *****************************************/
      return presentedOffers;
    }    
    
  }
  
  /*****************************************
  *
  *  class ActionManagerToken
  *
  *****************************************/

  public static class ActionManagerToken extends ActionManagerDNBO
  {
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ActionManagerToken(JSONObject configuration) throws GUIManagerException
    {
      super(configuration);
    }
        
    /*****************************************
    *
    *  execute
    *
    *****************************************/

    @Override public List<Action> executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
    {
      log.info("ActionManagerToken.executeOnEntry() method call");
      Object[] res = handleToken(evolutionEventContext, subscriberEvaluationRequest);
      @SuppressWarnings("unchecked")
      List<Action> result = (List<Action>) res[0];

      /*****************************************
      *
      *  return
      *
      *****************************************/
      return result;
    }
  }
  
  /*****************************************
  *
  *  class ActionManagerAllocate
  *
  *****************************************/

  public static class ActionManagerAllocate extends ActionManagerDNBO
  {
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ActionManagerAllocate(JSONObject configuration) throws GUIManagerException
    {
      super(configuration);
    }
        
    /*****************************************
    *
    *  execute
    *
    *****************************************/

    @Override public List<Action> executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
    {
      log.info("ActionManagerAllocate.executeOnEntry() method call");
      Object[] res = handleToken(evolutionEventContext, subscriberEvaluationRequest);
      @SuppressWarnings("unchecked")
      List<Action> result = (List<Action>) res[0];
      if (res.length == 1)
        {
          return result;
        }
      DNBOToken token = (DNBOToken) res[1];
      ContextUpdate tokenUpdate = (ContextUpdate) res[2];
      ScoringStrategy scoringStrategy = (ScoringStrategy) res[3];
      TokenType tokenType = (TokenType) res[4];
      
      /*****************************************
      *
      *  action -- score offers (ignore returned value)
      *
      *****************************************/
      
      handleAllocate(evolutionEventContext, subscriberEvaluationRequest, scoringStrategy, token, tokenType, tokenUpdate);
      token.setTokenStatus(TokenStatus.Bound);
      token.setAutoBounded(true);
      token.setAutoRedeemed(false);

      /*****************************************
      *
      *  return
      *
      *****************************************/
      return result;
    }
  }

  
  /*****************************************
  *
  *  class ActionManagerPurchase
  *
  *****************************************/

  public static class ActionManagerPurchase extends ActionManagerDNBO
  {
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ActionManagerPurchase(JSONObject configuration) throws GUIManagerException
    {
      super(configuration);
    }
        
    /*****************************************
    *
    *  execute
    *
    *****************************************/

    @Override public List<Action> executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
    {
      log.info("ActionManagerPurchase.executeOnEntry() method call");
      Object[] res = handleToken(evolutionEventContext, subscriberEvaluationRequest);
      @SuppressWarnings("unchecked")
      List<Action> result = (List<Action>) res[0];
      if (res.length == 1)
        {
          return result;
        }
      DNBOToken token = (DNBOToken) res[1];
      ContextUpdate tokenUpdate = (ContextUpdate) res[2];
      ScoringStrategy scoringStrategy = (ScoringStrategy) res[3];
      TokenType tokenType = (TokenType) res[4];

      /*****************************************
      *
      *  action -- score offers
      *
      *****************************************/
      
      Collection<ProposedOfferDetails> presentedOfferDetailsList = handleAllocate(evolutionEventContext, subscriberEvaluationRequest, scoringStrategy, token, tokenType, tokenUpdate);
      if (presentedOfferDetailsList.isEmpty())
        {
          log.error("cannot select first offer because list is empty");
          return Collections.<Action>emptyList();
        }

      //   select 1st offer of the list
      
      ProposedOfferDetails acceptedOfferDetail = presentedOfferDetailsList.iterator().next();
      String offerId = acceptedOfferDetail.getOfferId();
      token.setAcceptedOfferID(offerId);
      Offer offer = evolutionEventContext.getOfferService().getActiveOffer(offerId, evolutionEventContext.now());
      if (offer == null)
        {
          log.error("invalid offer returned by scoring {}", offerId);
          return Collections.<Action>emptyList();
        }
      tokenUpdate.getParameters().put("action.accepted.offer", offer.getDisplay());
      
      token.setTokenStatus(TokenStatus.Redeemed);
      token.setAutoBounded(true);
      token.setAutoRedeemed(true);
      token.setRedeemedDate(evolutionEventContext.now());

      /*****************************************
      *
      *  return
      *
      *****************************************/
      return result;
    }
  }

}
