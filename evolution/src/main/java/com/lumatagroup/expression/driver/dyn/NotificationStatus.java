package com.lumatagroup.expression.driver.dyn;

public enum NotificationStatus {
  SENT(1),
  DELIVERED(2),
  NOT_SENT(3),
  ERROR(4),
  EXPIRED(5);
  
  private final int value;

  NotificationStatus(final int newValue) {
      value = newValue;
  }
  public int getValue() { return value; }
}
