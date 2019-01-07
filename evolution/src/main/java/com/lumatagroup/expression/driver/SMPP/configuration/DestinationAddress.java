package com.lumatagroup.expression.driver.SMPP.configuration;

public class DestinationAddress {
	private String destAddrTon;
	private String destAddrNpi;
	private String destAddrSubunit;
	
	public String getDestAddrTon() {
		return destAddrTon;
	}
	public void setDestAddrTon(final String destAddrTon) {
		this.destAddrTon = destAddrTon;
	}
	public String getDestAddrNpi() {
		return destAddrNpi;
	}
	public void setDestAddrNpi(final String destAddrNpi) {
		this.destAddrNpi = destAddrNpi;
	}
	public String getDestAddrSubunit() {
		return destAddrSubunit;
	}
	public void setDestAddrSubunit(final String destAddrSubunit) {
		this.destAddrSubunit = destAddrSubunit;
	}
}
