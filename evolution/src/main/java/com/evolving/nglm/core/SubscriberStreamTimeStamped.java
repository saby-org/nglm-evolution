package com.evolving.nglm.core;

import java.util.Date;

public interface SubscriberStreamTimeStamped {

  Date getEventDate();// time the ORIGINAL external event was done (there is a stickiness for this to all internal events that are produced)
  String getEventID();// stickiness as well of this to the ORIGINAL exeternal event (all internal events trigger by it have same value)

}
