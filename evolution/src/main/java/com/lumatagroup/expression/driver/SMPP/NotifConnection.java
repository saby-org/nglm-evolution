package com.lumatagroup.expression.driver.SMPP;

/**
 * Interface of a connection to SMSC in the connectivity point of view
 * @author fduclos
 *
 */
public interface NotifConnection {
	public boolean start();
	public boolean stop();
	public boolean isConnected();
}

