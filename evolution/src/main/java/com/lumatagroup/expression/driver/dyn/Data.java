package com.lumatagroup.expression.driver.dyn;

public class Data {
	private Delivered[] delivered;
	private Opens[] opens;
	private Bounces[] bounces;
	private Clicks[] clicks;
	
	public Clicks[] getClicks() {
		return clicks;
	}

	public void setClicks(Clicks[] clicks) {
		this.clicks = clicks;
	}

	public Bounces[] getBounces() {
		return bounces;
	}

	public void setBounces(Bounces[] bounces) {
		this.bounces = bounces;
	}

	public Opens[] getOpens() {
		return opens;
	}

	public void setOpens(Opens[] opens) {
		this.opens = opens;
	}

	public Delivered[] getDelivered() {
		return delivered;
	}

	public void setDelivered(Delivered[] delivered) {
		this.delivered = delivered;
	}
}
