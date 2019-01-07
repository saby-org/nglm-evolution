package com.lumatagroup.expression.driver.SMPP.configuration;


public class SMPPDriverConfiguration {

    private String name;
    private boolean isMT;
    private boolean isMO;
    private String dataCoding;
    private String encodingCharset;
    private String expirationPeriod;
    private String maxAttempts;
    private String maxPerSec;
    private String intervalRetry;
    private String formatter;
    private String smppReceiverThreadNumber;
    private String dataPacking;
    private String delayOnQueueFull;
    private String throttleByAllSM;
    private String supportSar;
    private String supportUdh;
    private String serviceType;
    private String deliveryReceiptDecodingHexDec;
    
    private DestinationAddress destinationAdd;
    private SourceAddress sourceAdd;
    private SystemAccount systemAccount;
    private SMPPConnectionTimeCheck connTimeCheck;    
    private Connection conn;
    
    public Connection getConn() {
		return conn;
	}

	public void setConn(Connection conn) {
		this.conn = conn;
	}

	public DestinationAddress getDestinationAdd() {
		return destinationAdd;
	}

	public void setDestinationAdd(DestinationAddress destinationAdd) {
		this.destinationAdd = destinationAdd;
	}

	public SourceAddress getSourceAdd() {
		return sourceAdd;
	}

	public void setSourceAdd(SourceAddress sourceAdd) {
		this.sourceAdd = sourceAdd;
	}

	public SystemAccount getSystemAccount() {
		return systemAccount;
	}

	public void setSystemAccount(SystemAccount systemAccount) {
		this.systemAccount = systemAccount;
	}

	public SMPPConnectionTimeCheck getConnTimeCheck() {
		return connTimeCheck;
	}

	public void setConnTimeCheck(SMPPConnectionTimeCheck connTimeCheck) {
		this.connTimeCheck = connTimeCheck;
	}


    
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    public boolean isMT(){
    	return isMT;
    }
    
    public void setMt(boolean v){
    	this.isMT = v;
    }

    public boolean isMO(){
    	return isMO;
    }
    
    public void setMo(boolean v){
    	this.isMO = v;
    }

    
    public String getDataCoding() {
        return dataCoding;
    }

    public void setDataCoding(String dataCoding) {
        this.dataCoding = dataCoding;
    }

    public String getEncodingCharset() {
        return encodingCharset;
    }

    public void setEncodingCharset(String encodingCharset) {
        this.encodingCharset = encodingCharset;
    }

    public String getExpirationPeriod() {
        return expirationPeriod;
    }

    public void setExpirationPeriod(String expirationPeriod) {
        this.expirationPeriod = expirationPeriod;
    }

    public String getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(String maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public String getMaxPerSec() {
        return maxPerSec;
    }

    public void setMaxPerSec(String maxPerSec) {
        this.maxPerSec = maxPerSec;
    }

    public String getIntervalRetry() {
        return intervalRetry;
    }

    public void setIntervalRetry(String intervalRetry) {
        this.intervalRetry = intervalRetry;
    }

    public String getFormatter() {
        return formatter;
    }

    public void setFormatter(String formatter) {
        this.formatter = formatter;
    }

    

    public String getSmppReceiverThreadNumber() {
        return smppReceiverThreadNumber;
    }

    public void setSmppReceiverThreadNumber(String smppReceiverThreadNumber) {
        this.smppReceiverThreadNumber = smppReceiverThreadNumber;
    }

    public String getDataPacking() {
        return dataPacking;
    }

    public void setDataPacking(String dataPacking) {
        this.dataPacking = dataPacking;
    }

    public String getDelayOnQueueFull() {
        return delayOnQueueFull;
    }

    public void setDelayOnQueueFull(String delayOnQueueFull) {
        this.delayOnQueueFull = delayOnQueueFull;
    }

    public String getThrottleByAllSM() {
        return throttleByAllSM;
    }

    public void setThrottleByAllSM(String throttleByAllSM) {
        this.throttleByAllSM = throttleByAllSM;
    }

    public String getSupportSar() {
        return supportSar;
    }

    public void setSupportSar(String supportSar) {
        this.supportSar = supportSar;
    }

    public String getSupportUdh() {
        return supportUdh;
    }

    public void setSupportUdh(String supportUdh) {
        this.supportUdh = supportUdh;
    }

   
    public String getServiceType() {
        return serviceType;
    }

    public void setServiceType(String serviceType) {
        this.serviceType = serviceType;
    }

    public String getDeliveryReceiptDecodingHexDec() {
        return deliveryReceiptDecodingHexDec;
    }

    public void setDeliveryReceiptDecodingHexDec(String deliveryReceiptDecodingHexDec) {
        this.deliveryReceiptDecodingHexDec = deliveryReceiptDecodingHexDec;
    }
    
    
    @Override
    public String toString() {
    	// TODO Auto-generated method stub
    	return super.toString();
    }

}
