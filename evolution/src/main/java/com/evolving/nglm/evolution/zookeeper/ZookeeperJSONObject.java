package com.evolving.nglm.evolution.zookeeper;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ZookeeperJSONObject<T extends ZookeeperStoredObject<T>> extends ZookeeperStoredObject<T> {

	private static final Logger log = LoggerFactory.getLogger(ZookeeperJSONObject.class);
	private static final JSONParser jsonParser = new JSONParser();

	@Override
	protected T getFromZookeeperData(byte[] zookeeperData) {
		synchronized (jsonParser){
			try {
				String jsonString = new String(zookeeperData,Configuration.ENCODING_CHARSET);
				if(log.isDebugEnabled()) log.debug("ZookeeperJSONObject.getFromZookeeperData : will try to parse as JSON object "+jsonString);
				return fromJson((JSONObject)jsonParser.parse(jsonString));
			} catch (ParseException e) {
				log.warn("ZookeeperJSONObject.getFromZookeeperData : exception ",e);
			}
		}
		return null;
	}

	@Override
	protected byte[] getFromObject() {
		return toJson().toJSONString().getBytes(Configuration.ENCODING_CHARSET);
	}

	protected abstract T fromJson(JSONObject jsonObject);
	protected abstract JSONObject toJson();

}
