package com.lumatagroup.expression.driver.SMPP;

import ie.omk.smpp.Connection;
import ie.omk.smpp.NotBoundException;
import ie.omk.smpp.event.ConnectionObserver;
import ie.omk.smpp.event.ReceiverExceptionEvent;
import ie.omk.smpp.event.ReceiverExitEvent;
import ie.omk.smpp.event.SMPPEvent;
import ie.omk.smpp.message.EnquireLink;
import ie.omk.smpp.message.GenericNack;
import ie.omk.smpp.message.SMPPPacket;
import ie.omk.smpp.message.SMPPProtocolException;
import ie.omk.smpp.net.TcpLink;
import ie.omk.smpp.util.PacketStatus;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.SMSNotificationManager.SMSNotificationManagerRequest;
import com.evolving.nglm.evolution.DeliveryRequest;
import com.evolving.nglm.evolution.INotificationRequest;
import com.lumatagroup.expression.driver.SMPP.receiver.ReceiverWorkerThread;

/**
 * Connect (asynchronously) to a configured SMSC and handle all control messages (incoming / outgoing).
 * <p>Also handles the stats for SMSC layer
 * @author oaudo
 *
 */
public class SMPPConnection implements ConnectionObserver, NotifConnection {
    private static final Logger logger = LoggerFactory.getLogger(SMPPConnection.class);
	private static Timer ELCheck_timer = new Timer("SMPPConnection-ELCheck", false/*no daemon*/); // single instance for all SMPPConnection instances
	private static Timer Rebind_timer = new Timer("SMPPConnection-ReBind", false/*no daemon*/); // single instance for all SMPPConnection instances
	private static final String RECEIVER = "RX";
	private static final String TRANSCEIVER = "TRX";
	private static final String TRANSMITTER = "TX";
    private boolean started = false; // not yet
    private boolean being_restarted = false; // not yet
    /**controlled by JMX*/
    private boolean running = true;
	private Connection trans = null;
	private TimerTask regularEnquireLinkThread = null;
	private SMSSenderListener listener = null;
	private Object initLock = new Object(); // to ensure the creation of the connection and adding of observer are synchronous
	private final String name;
	private int type;
	private final String remote_address;
	private final int remote_port;
	private final String system_id;
	private final String password;
	/** Optional,is used to categorize the type of ESME that is binding to the SMSC. Examples include "VMS" (voice mail system) and "OTA" (over-the-air activation system)*/
	private final String system_type;
	private final boolean retry_connection;
	private final long time_between_connection_check;
	private final long time_between_reconnection;
	private final int receiverThreadNumber;
	private final GenericThreadPool receiverPool;
	private final String magic;
	//private SMPPDriver driver = null;
	
	Connection physicalConnection = null; 
	
	/**
	 * unique message id incremented for each SMS for the current connection
	 */
	private int messageIndex = 12;
	
	private HashMap<String, SubmitSMCorrectionDeliveryRequest> messageId2NotifBean = new HashMap<String, SubmitSMCorrectionDeliveryRequest>();
	
	public SMPPConnection(String name, String address, String port, String system_id, String password, String system_type, boolean retry_connection, String time_between_reconnection, String time_between_connection_check, String receiver_thread_number, String magic_number_configuration ) {
		this(name, TRANSCEIVER, address, port, system_id, password, system_type, retry_connection, time_between_reconnection, time_between_connection_check, receiver_thread_number, magic_number_configuration);
	}
	
	public SMPPConnection(
			String name, 
			String address, 
			String port, 
			String connectionType, 
			String system_id, 
			String password, 
			String system_type, 
			boolean retry_connection, 
			String time_between_reconnection, 
			String time_between_connection_check, 
			String receiver_thread_number,
			String magic_number_configuration // systemid and or ip and or port, by example systemid_ip_port or systemid_port or system_id ...
			) {
		if (address == null || port == null || system_id == null || password == null || time_between_reconnection == null || time_between_connection_check == null) {
			throw new NullPointerException("Missing argument for SMPPConnection constructor");
		}
		if (name == null || name.trim().isEmpty()) {
			name = "SMPP";
		}
		
		if(connectionType == null || connectionType.isEmpty()){
			logger.info("SMPPConnection.SMPPConnection: connection type is not defined in YAML so TRANSCEIVER (TRX) connection is being set by default.");
			this.type = Connection.TRANSCEIVER;
		}else{
			if(connectionType.equalsIgnoreCase(SMPPConnection.RECEIVER)){
				this.type = Connection.RECEIVER;
				logger.info("SMPPConnection.SMPPConnection: connection type is RECEIVER (RX).");
			}else if(connectionType.equalsIgnoreCase(SMPPConnection.TRANSCEIVER)){
				this.type = Connection.TRANSCEIVER;
				logger.info("SMPPConnection.SMPPConnection: connection type is TRANSCEIVER (TRX).");
			}
			else if(connectionType.equalsIgnoreCase(SMPPConnection.TRANSMITTER)){
				this.type = Connection.TRANSMITTER;
				logger.info("SMPPConnection.SMPPConnection: connection type is TRANSMITTER (TX).");
			}else{
				logger.error("Check the connection type parameter, please choose one among RX, TRX, TX");
			}
		}
		
		this.name = name;
//		this.driver = smppdriver;
		this.remote_address = address;
		this.remote_port = Integer.parseInt(port);
		this.system_id = system_id;
		this.password = password;
		this.system_type = system_type;
		this.retry_connection = retry_connection;
		this.time_between_reconnection = Long.parseLong(time_between_reconnection);
		this.time_between_connection_check = Long.parseLong(time_between_connection_check);
		if(receiver_thread_number != null){
			if (logger.isInfoEnabled()) {
				logger.info("SMPPConnection.SMPPConnection: Set receiver thread number to " + receiver_thread_number);
			}
			this.receiverThreadNumber = Integer.parseInt(receiver_thread_number.trim());
		}
		else {
			this.receiverThreadNumber = 5;
			if (logger.isInfoEnabled()) {
				logger.info("SMPPConnection.SMPPConnection: Use default receiver thread number " + this.receiverThreadNumber);
			}
		}
		
		this.receiverPool = new GenericThreadPool(this.name+"_Receiver");
		ReceiverWorkerThread[] threads = new ReceiverWorkerThread[receiverThreadNumber];
		for(int i = 0 ; i < receiverThreadNumber; i++){
			threads[i] = new ReceiverWorkerThread(receiverPool, this.name+"_Receiver_" + i);
		}		
		
		this.receiverPool.init(threads);
		
		String default_magic_string = system_id + address + port;
		String new_magic_string = "";
		if(magic_number_configuration != null && magic_number_configuration.trim().length() > 0){
			for(String current : magic_number_configuration.split("_")){
				if(current.trim().toLowerCase().equals("systemid")){
					new_magic_string = new_magic_string + system_id;
				}
				else if(current.trim().toLowerCase().equals("ip")){
					new_magic_string = new_magic_string + address;
				}
				else if(current.trim().toLowerCase().equals("port")){
					new_magic_string = new_magic_string + port;
				}
				else {
					logger.warn("SMPPConnection.SMPPConnection key " + current + " not understood for magic configuration must be either systemid, ip or port");
				}
			}
			if (logger.isInfoEnabled()) {
				logger.info("SMPPConnection.SMPPConnection Configured magic id configuration " + new_magic_string);
			}			
		}
		if(!new_magic_string.equals("")){
			this.magic = "" + new_magic_string.hashCode();
			if (logger.isInfoEnabled()) {
				logger.info("SMPPConnection.SMPPConnection DriverID magic " + this.magic + " from " + new_magic_string);
			}
		}
		else {
			this.magic = "" + default_magic_string.hashCode();
			if (logger.isInfoEnabled()) {
				logger.info("SMPPConnection.SMPPConnection DriverID magic " + this.magic + " from default " + default_magic_string);
			}
		}		
	}
	
	public void setListener(SMSSenderListener listener) {
		// keep obervers in memory in case of reconnection
		synchronized (initLock) {
			if(this.listener == null){
				this.listener = listener; // we consider the SMSSenderListener, i.e. SimpleSMSSender as stateless, so we don't care about which instance handles the ansers
				if (logger.isInfoEnabled()) {
					logger.info("SMPPConnection.addObserver " + listener);
				}
			}				
		}
	}
	

	@Override
	public void packetReceived(Connection source, SMPPPacket packet) {
		if(logger.isDebugEnabled()) logger.debug("SMPPConnection.packetReceived() : execution started... source: "+source);
		if (source != null) {
			if (logger.isTraceEnabled()) logger.trace("SMPPConnection.packetReceived("+source+" #"+source.hashCode()+", "+packet+") ["+this.name+"]"+"packet.getCommandId["+packet.getCommandId() +"] packet.getCommandStatus()["+packet.getCommandStatus()+"]");
			switch(packet.getCommandId()) {
				case SMPPPacket.BIND_TRANSMITTER_RESP: // A Transmitter is authorised to send short messages to the SMSC and to	receive the corresponding SMPP responses from the SMSC.
				case SMPPPacket.BIND_TRANSCEIVER_RESP: // A Transceiver is allowed to send messages to the SMSC and receive messages from the SMSC over a single SMPP session.
				case SMPPPacket.BIND_RECEIVER_RESP:	// A Receiver can received deliver_sm but should not send submit_sm
					if (packet.getCommandStatus() == PacketStatus.OK) {
						if (logger.isInfoEnabled()) logger.info("SMPPConnection.packetReceived: ["+this.name+"] bind successful, connection established with object "+source.hashCode());
						/////////////////////////////////////////////////////////////
						// update the trans attribute when we are really connected //
						/////////////////////////////////////////////////////////////
						this.trans = source; 
						if (time_between_connection_check > 0L) {
							if (this.regularEnquireLinkThread != null) {
								this.regularEnquireLinkThread.cancel();
							}
							this.regularEnquireLinkThread = new TimerTask() {
								@Override
								public void run() {
									if (SMPPConnection.this.trans != null) {
										try {
											// Check link status
											if (logger.isDebugEnabled()) logger.debug("SMPPConnection.ELCheck.run ["+SMPPConnection.this.name+"] check link, send EnquireLink");
											SMPPConnection.this.trans.enquireLink();
										} catch (SMPPProtocolException e) { // should never happen since we are asynchronous
											logger.error("SMPPConnection.ELCheck.run " + e, e);
										} catch (IOException e) {
											logger.error("SMPPConnection.ELCheck.run " + e, e);
											SMPPConnection.this.restart();
										} catch (NotBoundException e) {
											logger.error("SMPPConnection.ELCheck.run " + e, e);
											SMPPConnection.this.restart();
										} catch (Throwable e) {
											logger.error("SMPPConnection.ELCheck.run " + e, e);
											SMPPConnection.this.restart();
										}
									} else {
										logger.warn("SMPPConnection.ELCheck.run ["+SMPPConnection.this.name+"] connection is not established");
									}
								}
							};
							ELCheck_timer.scheduleAtFixedRate(regularEnquireLinkThread, 1000L, time_between_connection_check);
						}
					} else {
						//incrementSmscStat("BIND_KO");
						logger.error("SMPPConnection.packetReceived: ["+this.name+"] BindTransmitterResp/BindTransceiverResp unable to connect to the SMPP server received status is "+ packet.getCommandStatus()+" (0x"+Integer.toHexString(packet.getCommandStatus())+"="+PacketStatusUtils.getMessage(packet.getCommandStatus())+")");
						disconnect(source); // local trans
						restart();
					}
					break;
				case SMPPPacket.ENQUIRE_LINK_RESP:
					if (logger.isDebugEnabled()) {
						logger.debug("SMPPConnection.packetReceived: ["+this.name+"] EnquireLinkResp");
					}
					break;
				case SMPPPacket.ENQUIRE_LINK: // used by the SMSC after a period of inactivity to decide whether to close the link.
					if (logger.isDebugEnabled()) {
						logger.debug("SMPPConnection.packetReceived: ["+this.name+"] EnquireLink");
					}
					EnquireLink enquireReceived = (EnquireLink) packet;
					try {
						trans.ackEnquireLink(enquireReceived);
					} catch (Exception e) {
						logger.error("SMPPConnection.packetReceived: ["+this.name+"] Error while sending a EnquireLinkResp "+e, e);
					}
					break;
				case SMPPPacket.UNBIND_RESP:
					if (logger.isInfoEnabled()) logger.info("SMPPConnection.packetReceived: ["+this.name+"] UnbindResp");
					// nothing here, not very clean
					break;
				case SMPPPacket.SUBMIT_SM_RESP:
				case SMPPPacket.DELIVER_SM:
					if(listener != null){
						listener.packetReceived(source, packet);
					}
					else {
						logger.warn("SMPPConnection.packetReceived No Listener to handle Submit_Sm_Response or Deliver_Sm " + packet);
					}
					break;
				
				case SMPPPacket.GENERIC_NACK:
					GenericNack genericNack = (GenericNack) packet;
					logger.error("SMPPConnection.packetReceived: ["+this.name+"] GenericNack with status "+genericNack.getCommandStatus()+" ("+PacketStatusUtils.getMessage(genericNack.getCommandStatus())+")");
					break;
				default:
					logger.error("SMPPConnection.packetReceived: ["+this.name+"] Unknown command id ("+packet.getCommandId()+") with SMPPPacket of class "+packet.getClass().toString());
					break;
			}
		} else {
			if (logger.isWarnEnabled()) {
				logger.warn("SMPPConnection.packetReceived: ["+this.name+"] receive null object");
			}
		}
	}

	

	@Override
	public void update(Connection source, SMPPEvent event) {
		if (logger.isTraceEnabled()) logger.trace("SMPPConnection.update("+source+" #"+source.hashCode()+", "+event+") ["+this.name+"] event.Type["+event.getType()+"]");
		if (source != null) {
			if(logger.isDebugEnabled()) logger.debug("source[state] "+source.getState()+" source[isBound] "+source.isBound()+" source[connectionType] "+source.getConnectionType());
			switch(event.getType()) {
				case SMPPEvent.RECEIVER_START:
					// useless
					break;
				case SMPPEvent.RECEIVER_EXIT:
					int reason = ((ReceiverExitEvent)event).getReason();
					String tmp;
					switch(reason) {
					case ReceiverExitEvent.BIND_TIMEOUT:
						tmp = "BIND_TIMEOUT";
						break;
					case ReceiverExitEvent.EXCEPTION:
						tmp = "EXCEPTION";
						break;
					case ReceiverExitEvent.UNKNOWN:
					default:
						tmp = "UNKNOWN";
					}
					logger.error("SMPPConnection.update ["+this.name+"] ReceiverExitEvent reason="+tmp+" ("+reason+"), exception="+((ReceiverExitEvent)event).getException());
					disconnect(source); // local trans, do not touch "main" trans if any
					restart();
					break;
				case SMPPEvent.RECEIVER_EXCEPTION:
					if (((ReceiverExceptionEvent)event).getState() != Connection.BOUND) {
						logger.error("SMPPConnection.update ["+this.name+"] ReceiverExceptionEvent exception="+((ReceiverExceptionEvent)event).getException());
						disconnect(source); // local trans, do not touch "main" trans if any
						restart();
						break;
					}
				default: // SMPPEvent.RECEIVER_EXCEPTION (BOUND)
					if (logger.isDebugEnabled()) logger.debug("SMPPConnection.update: ["+this.name+"] " + event.toString());
			}
		} else {
			if (logger.isWarnEnabled()) {
				logger.warn("SMPPConnection.update: ["+this.name+"] receive null object");
			}
		}
	}
	
	/**
	 * Non-blocking call, unless it cannot connect to the SMSC
	 * <p>One Thread at a time here
	 */
	private synchronized boolean connect()
    {
		// When connect is called, let ensure the current connection is not yet exisitn, if yes, disconnect it
		if(physicalConnection != null){
			logger.warn("SMPPConnection.connect The physical connection " + name + " already exists");
			try{
				disconnect(physicalConnection);
				physicalConnection = null;
				if (logger.isInfoEnabled()) {
					logger.info("SMPPConnection.connect Disconnection finished for connection "+ name);
				}
			}
			catch(Exception e){
				logger.warn("SMPPConnection.connect Excpetion " + e.getClass().getName() + " " + e.getMessage() + " happened while disconnecting the SMPP connection for name " + name, e);
				
			}
		}
		
		boolean retry = false;
		while (started && !retry) {
			try {
				if (logger.isTraceEnabled())
					logger.trace("SMPPConnection.connect ["+this.name+"] attempt to get address from "+ remote_address);
				InetAddress smscAddr = InetAddress.getByName(remote_address);
				if (logger.isInfoEnabled()) {
					logger.info("SMPPConnection.connect ["+this.name+"] ready to start a SMSC connection on "+ remote_address);
				}
				TcpLink smscLink = new TcpLink(smscAddr, remote_port);
				if (logger.isTraceEnabled())
					logger.trace("SMPPConnection.connect ["+this.name+"] attempt to open SMSC connection");
				smscLink.open();
				if (logger.isInfoEnabled()) {
					logger.info("SMPPConnection.connect ["+this.name+"] link to "+remote_address+" opened");
				}
				physicalConnection = new Connection(smscLink/*cannot be null*/, true/*async*/); // create an internal Thread called "ReceiverDaemon"

				physicalConnection.addObserver(this);
				if (logger.isInfoEnabled()) {
					logger.info("SMPPConnection.connect Wait for synchronistation to add observers");
				}
				
				if (logger.isTraceEnabled())
					logger.trace("SMPPConnection.connect ["+this.name+"] attempt to bind");
				physicalConnection.bind(type, system_id, password, system_type/*can be null*/, 0, 0, null);
				// as we use async mode, there's no answer to this bind yet
				if (logger.isInfoEnabled()) {
					logger.info("SMPPConnection.connect: ["+this.name+"] connection is on-going with object "+physicalConnection.hashCode());
				}
				retry = true; // to break the loop
			} catch (IOException e) { // can be NoRouteToHostException or ConnectException is SMSC cannot be reached
				logger.error("SMPPConnection.connect ["+this.name+"] cannot connect to SMSC on "+remote_address+" due to "+e);
//				this.driver.sendConnectionStatusTrap(false,"SMPPConnection not Loaded Successfully. Trying to reconnect.... ");
				//incrementSmscStat("BIND_KO");
				disconnect(physicalConnection); // to clear local trans
				physicalConnection = null;
				if (started && this.retry_connection) {
					try
					{
						if (logger.isTraceEnabled()) logger.trace("SMPPConnection.connect ["+this.name+"] sleep for "+ time_between_reconnection + " ms");
						Thread.sleep(time_between_reconnection);
					}
					catch (InterruptedException e2)
					{
						logger.warn("SMPPConnection.connect ["+this.name+"] woken up by "+e2,e2);
					}
				} else {
					if (logger.isInfoEnabled()) logger.info("SMPPConnection.connect ["+this.name+"] does not reconnect");
					break;
				}
			}
		} // end while loop
		return (physicalConnection!=null);
    }

	/**
	 * One Thread at a time here
	 */
	private synchronized void disconnect() {
		disconnect(trans);
		trans = null;
	}
	
	private void disconnect(Connection trans)
    {
        if (trans != null)
        {
        	int hashCode = trans.hashCode();
    		if (logger.isTraceEnabled()) logger.trace("SMPPConnection.disconnect ["+this.name+"] object "+hashCode);
            try
            { 
            	if (trans.isBound()) {
	            	if (logger.isDebugEnabled()) logger.debug("SMPPConnection.disconnect ["+this.name+"] attempt to unbind");
	            	trans.unbind();
	            	if (logger.isDebugEnabled()) logger.debug("SMPPConnection.disconnect ["+this.name+"] unbind successful");
	            	// as we use async mode, there's no answer to this unbind yet, so closeLink below will throw IllegalStateException
	            	try
					{
						if (logger.isTraceEnabled()) logger.trace("SMPPConnection.disconnect ["+this.name+"] sleep for 100 ms");
						Thread.sleep(100);
					}
					catch (InterruptedException e)
					{
						logger.warn("SMPPConnection.disconnect ["+this.name+"] woken up by "+e,e);
					}
					// here we might have received the UnbindResp packet, so we assume we can close the link.
					// if we don't have received the UnbindResp packet, anyway we have to close the link.
            	}
            	if (logger.isDebugEnabled()) logger.debug("SMPPConnection.disconnect ["+this.name+"] attempt to close link");
            	trans.closeLink();
            	if (logger.isInfoEnabled()) logger.info("SMPPConnection.disconnect ["+this.name+"] link to "+remote_address+" successfully closed");
            	trans = null; // ok, done
            }
            catch (Throwable e)
            {
            	logger.error("SMPPConnection.disconnect ["+this.name+"] could not disconnect from SMSC on "+remote_address+" due to "+e);
            }
            finally
            {
            	if (trans != null) {
            		try {
            			if (logger.isDebugEnabled()) logger.debug("SMPPConnection.disconnect ["+this.name+"] attempt to force unbind");
        				trans.force_unbind();
        				if (logger.isDebugEnabled()) logger.debug("SMPPConnection.disconnect ["+this.name+"] force unbind successful");
        			} catch (Throwable e) {
        				logger.error("SMPPConnection.disconnect ["+this.name+"] could not disconnect from SMSC due to "+e);
        			}
        			trans = null; // ok, done
            	}
            }
            if (logger.isInfoEnabled()) logger.info("SMPPConnection.disconnect ["+this.name+"] object "+hashCode+" "+((trans == null)?"successful":"unsuccessful"));
        } else {
            // ok, nothing to do
        	if (logger.isDebugEnabled()) logger.debug("SMPPConnection.disconnect ["+this.name+"] nothing to disconnect");
        }
    }

	public Connection getConnection() {
		return trans;
	}
	
	@Override
	public boolean isConnected() {
		return (running && isStarted());
	}
	
	private boolean isStarted() {
		return (started && trans != null);
	}
	
	/**
	 * Non-blocking call, attempt to connect asynchronously to SMSC.
	 * <p>However, it blocks if it cannot connect to SMSC
	 */
	@Override
	public boolean start() {
		if (logger.isInfoEnabled()) logger.info("SMPPConnection.start");
		if (!started) {
			if (logger.isInfoEnabled()) {
				logger.info("SMPPConnection.start Not Started, let disconnect and reconnect " + name);
			}
			disconnect();
			started = true;
			boolean isConnect =  connect();
			if(isConnect){
//				this.driver.sendConnectionStatusTrap(true,null);
			}
			return isConnect;
		}
//		this.driver.sendConnectionStatusTrap(true,null);
		return true; // already started
	}

	/**
	 * Close the connection to SMSC
	 */
	@Override
	public boolean stop() {
		if (logger.isInfoEnabled()) logger.info("SMPPConnection.stop ["+this.name+"]");
		if (regularEnquireLinkThread != null) {
			regularEnquireLinkThread.cancel(); // here the ELThread may be the caller, don't worry that works too 
			regularEnquireLinkThread = null;
		}
		if (started) {
			started = false;
			disconnect();
			if (logger.isInfoEnabled()) {
				logger.info("SMPPConnection.stop disconnected " + name);
			}
		}
//		this.driver.sendConnectionStatusTrap(false,null);
		return true;
	}
	
	private void restart()
	{
		if (logger.isInfoEnabled()) {
			logger.info("SMPPConnection.restart Try to restart " + name);
		}
		boolean restart = false;
		synchronized(this) {
			if (!being_restarted) { // are we already restarting ?
				being_restarted = true;
				restart = true;
			}
		}
		// out of synchro block, the code below must not be executed twice at the same time 
		if (restart) {
			if (logger.isInfoEnabled()) logger.info("SMPPConnection.restart ["+this.name+"] Thread-"+Thread.currentThread().getId());
			if (logger.isTraceEnabled()) logger.trace("SMPPConnection.restart ["+this.name+"] calling stop to terminate current connection");
			stop();
			if (this.retry_connection) {
				// asynchronous call so we don't block potential ReceiverExitEvent coming on the same socket / Thread
				// if we block them, we would call "start" and then the ReceiverExitEvent would call "stop + start" ... forever loop
				TimerTask startTask = new TimerTask() {
					@Override
					public void run() {
						if (logger.isDebugEnabled()) logger.debug("SMPPConnection.TimerTask.restart ["+SMPPConnection.this.name+"] calling start to reconnect");
						start();
						being_restarted = false;
					}
				};
				if (logger.isDebugEnabled()) logger.debug("SMPPConnection.restart ["+this.name+"] plan the reconnection in "+ time_between_reconnection + " ms");
				Rebind_timer.schedule(startTask, time_between_reconnection); // one-shot execution
			} else {
				if (logger.isInfoEnabled()) logger.info("SMPPConnection.restart ["+this.name+"] does not reconnect");
				being_restarted = false;
			}
		} else {
			if (logger.isInfoEnabled()) logger.info("SMPPConnection.restart ["+this.name+"] already being restarted Thread-"+Thread.currentThread().getId());
		}
	}
	
	public GenericThreadPool getReceiverThreadPool(){
		return receiverPool;
	}
	
	/**
	 * Generate a new SMPP message id and register the notification bean. This is used to correlate Submit SM and submit SM response
	 * @param notif represents the information about the current SMS
	 * @param isMultiPart defines if the current SMPP message belongs to a multi part SMS
	 * @param if the current SMPP message belongs to a multi part SMS then this field represents the ordinal number of this part. (from 0 to n-1)
	 * @return
	 */
	public synchronized int referenceMessageId(INotificationRequest deliveryRequest, String text, boolean isMultiPart, int partOrdianl, int totalPartNumber){

		int id = messageIndex++;
		if (logger.isTraceEnabled()) {
			logger.trace("SMPPConnection.referenceMessageId smppmessageid " + id);
		}
		SubmitSMCorrectionDeliveryRequest content = new SubmitSMCorrectionDeliveryRequest(deliveryRequest, text, isMultiPart, partOrdianl, totalPartNumber);
		messageId2NotifBean.put(""+id, content);
		return id;
	}
	
	public SubmitSMCorrectionDeliveryRequest getReferencedMessageId(String smppMessageId){
	  SubmitSMCorrectionDeliveryRequest result = messageId2NotifBean.get(""+smppMessageId);
		if (logger.isTraceEnabled()) {
			logger.trace("SMPPConnection.getReferencedMessageId smppMessageId " + smppMessageId);
		}
		return result;		
	}

    /**
     * Remove delivery request reference from the hashmap that store message objects for correlating with Submit SM
     * Response packet
     * 
     * @param notif
     * @return
     */
    public void deleteReferencedMessageId(String smppMessageId) {
      SubmitSMCorrectionDeliveryRequest toRemove = getReferencedMessageId(smppMessageId);
    	if(toRemove != null){
    		// it has been well filtered by the getReferenceMessageId, so can be deleted...
    		messageId2NotifBean.remove(smppMessageId);
    	}
       
    }
    
    
    /**
     * Class that makes the link between a submit SM and submit SM response (through SMPP sequence number). It allows to:
     * - retrieve the delivery request for the submit SM response,
     * - know if the notification is or not a MultiPart SMS
     * - if it is a multi part SMS, it tells if it is related the first part of the SMS or the following ones. 
     * @author fduclos
     *
     */
    public class SubmitSMCorrectionDeliveryRequest {
        private INotificationRequest deliveryRequest;
    	private String messageContent;
    	private boolean isMultiPart;
    	private int partOrdinal;
    	private int totalPartsNumber;
   	
    	public SubmitSMCorrectionDeliveryRequest(INotificationRequest deliveryRequest, String messageContent, boolean isMultiPart, int partOridnal, int totalPartsNumber){
    		this.setDeliveryRequest(deliveryRequest);
    		this.setMessageContent(messageContent);
    		this.setMultiPart(isMultiPart);
    		this.setPartOrdinal(partOridnal);
    		this.setTotalPartsNumber(totalPartsNumber);
    		if(logger.isTraceEnabled()){
    			logger.trace("SMPPConnection.SubmitSMCorrectionExpandedMessageContent create " + this.toString());
    		}
    	}

		public String getMessageContent() {
			return messageContent;
		}

		public void setMessageContent(String messageContent) {
			this.messageContent = messageContent;
		}

		public boolean isMultiPart() {
			return isMultiPart;
		}

		public void setMultiPart(boolean isMultiPart) {
			this.isMultiPart = isMultiPart;
		}
		
		public void setDeliveryRequest(INotificationRequest deliveryRequest){
		  this.deliveryRequest = deliveryRequest;
		}
		
		public INotificationRequest getDeliveryRequest(){
		  return deliveryRequest;
		}

		public String toString(){
			return messageContent.toString() + " isMultiPart: " + isMultiPart + " partOrdianl:" +partOrdinal +" totalPartNumber:" +totalPartsNumber ;
		}

		public int getPartOrdinal() {
			return partOrdinal;
		}

		public void setPartOrdinal(int partOrdinal) {
			this.partOrdinal = partOrdinal;
		}

		public int getTotalPartsNumber() {
			return totalPartsNumber;
		}

		public void setTotalPartsNumber(int totalPartsNumber) {
			this.totalPartsNumber = totalPartsNumber;
		}

    }


	public String getMagic() {
		return magic;
	}
}
