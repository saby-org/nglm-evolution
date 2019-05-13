package com.evolving.nglm.evolution.offeroptimizer;


public class OfferList {
	
	private String msisdn;
	private String strategy;
	private ProposedOfferDetails[] offers;
	private String log;
	
	// ExternalOfferIf
	// SalesChannelId
	// Score double
	// algorithmValues [P, V]
	
	public String getMsisdn() {
		return msisdn;
	}
	public void setMsisdn(String msisdn) {
		this.msisdn = msisdn;
	}
	public String getStrategy() {
		return strategy;
	}
	public void setStrategy(String strategy) {
		this.strategy = strategy;
	}	
	public ProposedOfferDetails[] getOffers()
    {
      return offers;
    }
    public void setOffers(ProposedOfferDetails[] offers)
    {
      this.offers = offers;
    }
    public String getLog(){
    	return log;
    }
    
    public void setLog(String log){
    	this.log = log;
    }
    
  public String toString(){
		return "OfferList: msisdn=" + msisdn + ", strategy=" + strategy +", offerDetails=" + offers;
	}
	
//	public static void main(String[] args) throws JsonGenerationException, JsonMappingException, IOException
//    {
//      ObjectMapper objectMapper = new ObjectMapper();
//      OfferList ol = new OfferList();
//      ol.setMsisdn("336402152");
//      ol.setStrategy("DataStrategy");
//      ol.setTokenCode("13456");
//      
//      HashMap<String, String> algorithmValues1 = new HashMap<>();
//      algorithmValues1.put("P", "0.23");
//      algorithmValues1.put("V", "0.55");
//      ProposedOfferDetails offer1 = new ProposedOfferDetails("65", "2", "0.334", algorithmValues1);
//      
//      HashMap<String, String> algorithmValues2 = new HashMap<>();
//      algorithmValues2.put("V", "0.55");
//      ProposedOfferDetails offer2 = new ProposedOfferDetails("22", "2", "0.235", algorithmValues2);
//      
//      ProposedOfferDetails[] offers = new ProposedOfferDetails[]{offer1, offer2};
//      
//      ol.setOffers(offers);
//      
//      System.out.println(objectMapper.writeValueAsString(ol));
//    }
}
