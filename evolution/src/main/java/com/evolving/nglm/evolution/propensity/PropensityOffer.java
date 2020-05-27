package com.evolving.nglm.evolution.propensity;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.evolution.zookeeper.ZookeeperJSONObject;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PropensityOffer extends ZookeeperJSONObject<PropensityOffer> {

	// this will store the count per stratum, this is what is stored in zookeeper
	private volatile Map<List<String>,PropensityState> propensityStates;
	private static final String propensityStatesJsonKey="propensityStates"; // the object name containing all this offer propensity
	private static final String propensityStratumJsonKey="propensityStratum";

	protected PropensityOffer(){
		this.propensityStates = new ConcurrentHashMap<>();
	}

	protected PropensityOffer(List<String> stratum){
		this();
		this.propensityStates.put(stratum,new PropensityState());
	}

	public PropensityState getPropensityStateForStratum(List<String> stratum){
		PropensityState toRet = propensityStates.get(stratum);
		if(toRet==null){
			synchronized (propensityStates){
				if(propensityStates==null){
					toRet=new PropensityState();
					propensityStates.put(stratum,toRet);
				}
			}
		}
		return toRet;
	}

	private void addPropensityState(List<String> strat, PropensityState propensityState){
		propensityStates.put(strat,propensityState);
	}

	// called when we update propensity from zookeeper data
	public void updatePropensityFromZookeeper(PropensityOffer zookeeperPropensity){
		for(Map.Entry<List<String>,PropensityState> zookeeperEntry:zookeeperPropensity.propensityStates.entrySet()){

			// the one we got locally
			PropensityState cachedPropensityState = propensityStates.get(zookeeperEntry.getKey());
			if(cachedPropensityState==null){
				synchronized (propensityStates){
					cachedPropensityState = propensityStates.get(zookeeperEntry.getKey());
					if(cachedPropensityState==null){
						propensityStates.put(zookeeperEntry.getKey(),zookeeperEntry.getValue());
						continue;
					}
				}
			}
			// here we can not just replace object, we would be erasing the local not yet committed to zookeeper counters
			cachedPropensityState.setPresentationCount(zookeeperEntry.getValue().getPresentationCount());
			cachedPropensityState.setAcceptanceCount(zookeeperEntry.getValue().getAcceptanceCount());
		}
	}

	@Override
	protected JSONObject toJson() {
		JSONObject toReturn = new JSONObject();
		JSONArray propensityStatesJson = new JSONArray();
		for(Map.Entry<List<String>,PropensityState> entry:propensityStates.entrySet()){
			JSONObject jsonObject = new JSONObject();
			JSONArray propensityStratumJson = new JSONArray();
			propensityStratumJson.addAll(entry.getKey());
			jsonObject.put(propensityStratumJsonKey,propensityStratumJson);
			jsonObject.putAll(entry.getValue().toJson());
			propensityStatesJson.add(jsonObject);
		}
		toReturn.put(propensityStatesJsonKey,propensityStatesJson);
		return toReturn;
	}

	@Override
	protected PropensityOffer fromJson(JSONObject jsonObject) {
		// SHOULD BE A DEEP COPY, NOT "this"
		PropensityOffer toRet = new PropensityOffer();
		JSONArray propensityStatesJson = JSONUtilities.decodeJSONArray(jsonObject,propensityStatesJsonKey,new JSONArray());
		for(Object propensityStateJson:propensityStatesJson){
			List<String> strat = JSONUtilities.decodeJSONArray((JSONObject)propensityStateJson,propensityStratumJsonKey,new JSONArray());
			PropensityState propensityState = new PropensityState((JSONObject)propensityStateJson);
			toRet.addPropensityState(strat,propensityState);
		}
		return toRet;
	}

	@Override
	protected PropensityOffer updateAtomic(PropensityOffer storedInZookeeperObject) {
		// this is call by zookeeper to add my local counter to the stored global one
		// so arg storedInZookeeperObject is the stored one

		// we "increment" all the stored one by our "local" ones first
		for(Map.Entry<List<String>,PropensityState> entry:propensityStates.entrySet()){
			PropensityState stored = storedInZookeeperObject.propensityStates.get(entry.getKey());
			// don't exist adding it
			if(stored==null){
				storedInZookeeperObject.propensityStates.put(entry.getKey(),entry.getValue());
				continue;
			}
			// get value for commit
			stored.addAcceptanceCount(entry.getValue().getLocalAcceptanceCountForCommit());
			stored.addPresentationCount(entry.getValue().getLocalPresentationCountForCommit());
		}
		return storedInZookeeperObject;
	}

	@Override
	protected void onUpdateAtomicSucces() {
		for(PropensityState propensityState:propensityStates.values()){
			propensityState.confirmCommit();
		}
	}

}
