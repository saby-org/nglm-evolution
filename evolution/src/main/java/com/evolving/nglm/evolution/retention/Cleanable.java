package com.evolving.nglm.evolution.retention;

import java.time.Duration;

public interface Cleanable extends Expirable {

	enum RetentionType{
		KAFKA_DELETION // used to know when to delete from kafka
	}

	// NOTE Duration vs Period
	// Duration is just a number of "seconds" at the end, it does not take care about anything like timezone and day time saving while operating
	// it means we will completely ignore day time saving here (which can lead to inexact retention of 1h, but saved CPU time operating with it instead of Period and ZonedDateTime)
	Duration getRetention(RetentionType retentionType, RetentionService retentionService, int tenantID);

}
