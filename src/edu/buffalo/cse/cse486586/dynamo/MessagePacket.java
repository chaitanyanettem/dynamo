package edu.buffalo.cse.cse486586.dynamo;

import java.io.Serializable;

import android.database.Cursor;
import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.util.Log;

/**
 * Class which implements the MessagePacket Protocol for communication between nodes
 * @author Chaitanya Nettem
 *
 */
public class MessagePacket implements Serializable{
	/**
	 * Serial Version UID 
	 */
	public static final String TAG = MessagePacket.class.getSimpleName();
	private static final long serialVersionUID = 5393198541861509332L;	
	
	private String msgId;
	private String msgContent;
	private MSG_OPER msgOperation;
	private MSG_TYPE msgType;
	private String msgInitiator;
	
	public static final String MSG_DELIMITER = "---";
	public static final String COL_DELIMITER = "~~~";
	public static final String ROW_DELIMITER = "@@@";
	public static final String QUEUE_DELIMITER = "###";
	
	/**
	 * Default constructor
	 */
	public MessagePacket() {
		super();
		msgId = null;
		msgContent = "";
		msgInitiator = "";		
	}
	
	/** 
	 * Parameterized constructor initialize with Message ID and Message Content
	 * @param msgId
	 * @param msgContent
	 */
	public MessagePacket(String msgId, String msgContent) {
		super();
		this.msgId = msgId;
		this.msgContent = msgContent;		
	}
	
	/** 
	 * Parameterized constructor initialize with Message ID, Message Content and Sequence Number
	 * @param msgId - Message ID
	 * @param msgContent - Message Content
	 * @param seqNumber - Sequnce Number of the message
	 */
	public MessagePacket(String msgId, String msgContent, MSG_TYPE msgType) {
		super();
		this.msgId = msgId;
		this.msgContent = msgContent;		
		this.msgType = msgType;
	}
	
	/**
	 * @return the msgId
	 */
	public String getMsgId() {
		return msgId;
	}
	/**
	 * @param msgId the msgId to set
	 */
	public void setMsgId(String msgId) {
		this.msgId = msgId;
	}
	
	/**
	 * @return the msgContent
	 */
	public String getMsgContent() {
		return msgContent;
	}
	/**
	 * @param msgContent the msgContent to set
	 */
	public void setMsgContent(String msgContent) {
		this.msgContent = msgContent;
	}	
	
	/**
	 * @return the msgType
	 */
	public MSG_TYPE getMsgType() {
		return msgType;
	}
	/**
	 * @param msgType the msgType to set
	 */
	public void setMsgType(MSG_TYPE msgType) {
		this.msgType = msgType;
	}

	/**
	 * @return the msgOperation
	 */
	public MSG_OPER getMsgOperation() {
		return msgOperation;
	}
	/**
	 * @param msgOperation the msgOperation to set
	 */
	public void setMsgOperation(MSG_OPER msgOperation) {
		this.msgOperation = msgOperation;
	}

	/**
	 * @return the msgInitiator
	 */
	public String getMsgInitiator() {
		return msgInitiator;
	}

	/**
	 * @param msgInitiator the msgInitiator to set
	 */
	public void setMsgInitiator(String msgInitiator) {
		this.msgInitiator = msgInitiator;
	}
	//
}

/**
 * Enum to define the Message Type 
 * during the message communication
 * @author Chaitanya Nettem
 *
 */
enum MSG_TYPE
{
	REQUEST,
	DUPLICATE,
	RESPONSE
};

/**
 * Enum to define the Message Type 
 * during the message communication
 * @author Chaitanya Nettem
 *
 */
enum MSG_OPER
{
	INSERT,
	QUERY,
	DELETE,	
	RECOVERY,
	NODEJOIN,
	NODEUPDATE
};

