package com.evolving.nglm.evolution;

import java.util.Properties;

public final class ConfigUtils {

    public static final Properties envPropertiesWithPrefix(String prefix) {
        if (prefix == null || "" == prefix.trim()) {
            throw new UnsupportedOperationException("ENV properties prefix should be non empty");
        }
        final Properties properties = new Properties();
        System.getenv().keySet().stream()
                .filter(key -> key.startsWith(prefix))
                .forEach(key -> properties.setProperty(envVarToProp(key, prefix), System.getenv(key)));
        return properties;
    }

    private static final String envVarToProp(String name, String prefix) {
        //KAFKA_STREAMS_PRODUCER_BATCH_SIZE becomes producer.batch.size with a KAFKA_STREAMS prefix
        final String maybePrefix = String.format("%s_", prefix);
        return name
                .replaceAll(maybePrefix, "")
                .replaceAll(prefix, "")
                .replaceAll("_", ".")
                .toLowerCase();
    }

}

