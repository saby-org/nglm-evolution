package com.evolving.nglm.evolution.retention;

import java.util.Date;

public interface Expirable {

	Date getExpirationDate(RetentionService retentionService, int tenantID);

}
