package com.lumatagroup.expression.driver.dyn;

public class Response {
	private String message;
	private int status;
	private Data data;
	
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public Data getData() {
		return data;
	}
	public void setData(Data data) {
		this.data = data;
	}
	
//	 @Override
//	    public String toString(){
//	        StringBuilder sb = new StringBuilder();
//	        sb.append("***** Employee Details *****\n");
//	        sb.append("Message ="+getMessage()+"\n");
//	        sb.append("status="+getStatus()+"\n");
//	        Delivered [] delivered = getData().getDelivered();
//	        for(int i=0; i<delivered.length; i++){
//	            sb.append("X headers messageId="+delivered[i].getXheaders().getXmessageId()+"\n");
//	            sb.append("UserId="+delivered[i].getUserid()+"\n");
//	            sb.append("Sent Time="+delivered[i].getSenttime()+"\n");
//	            sb.append("Email Address="+delivered[i].getEmailaddress()+"\n");
//	            sb.append("MS senttime="+delivered[i].getMssenttime()+"\n");
//
//	        }
//	        //sb.append("Phone Numbers="+Arrays.toString(getPhoneNumbers())+"\n");
//	        sb.append("*****************************");
//	         
//	        return sb.toString();
//	    }
	
}
