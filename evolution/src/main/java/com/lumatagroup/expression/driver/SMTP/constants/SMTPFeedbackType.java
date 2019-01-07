package com.lumatagroup.expression.driver.SMTP.constants;

public enum SMTPFeedbackType {
	
	SENT{
		@Override
		public boolean getStatus(){
			return status;
		}

		@Override
		public void setStatus(boolean status) {
			this.status = status;
		}
	},
	DELIVERED{

		@Override
		public boolean getStatus(){
			return status;
		}
		
		@Override
		public void setStatus(boolean status) {
			this.status = status;
		}
	}, 
	OPEN{

		@Override
		public boolean getStatus(){
			return status;
		}
		
		@Override
		public void setStatus(boolean status) {
			this.status = status;
		}
	}, 
	CLICKED{

		@Override
		public boolean getStatus(){
			return status;
		}
		
		@Override
		public void setStatus(boolean status) {
			this.status = status;
		}
	},
	BOUNCED{

		@Override
		public boolean getStatus(){
			return status;
		}
		
		@Override
		public void setStatus(boolean status) {
			this.status = status;
		}
	},
	ERROR{

		@Override
		public boolean getStatus(){
			return status;
		}

		@Override
		public void setStatus(boolean status) {
			this.status = status;
		}
		
	};

	boolean status;
	public abstract boolean getStatus();
	public abstract void setStatus(boolean status);

}
