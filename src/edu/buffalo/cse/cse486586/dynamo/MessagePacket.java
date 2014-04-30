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

	/**
	 * Serialize Message Packet while sending the message
	 * @param msgPacket Message Packet to be serialized
	 * @return serialized message string
	 */
	public static String serializeMessage(MessagePacket msgPacket)
	{
		String msgToBeSent = msgPacket.msgId + MSG_DELIMITER 
				+ msgPacket.msgContent + MSG_DELIMITER 
				+ msgPacket.msgType.toString() + MSG_DELIMITER
				+ msgPacket.msgOperation.toString() + MSG_DELIMITER
				+ msgPacket.msgInitiator;
	  	return msgToBeSent;
	}
	
	/**
	 * De-serialize the message packet and while receiving message
	 * @param messageString - Message to be de-serialized
	 * @return Message Packet Object
	 */
	public static MessagePacket deSerializeMessage(String messageString)
	{
		String[] msgArray = messageString.split(MSG_DELIMITER); 
		MessagePacket msgPacket = new MessagePacket(msgArray[0], msgArray[1], MSG_TYPE.valueOf(msgArray[2]));
		msgPacket.msgOperation = MSG_OPER.valueOf(msgArray[3]);
		msgPacket.msgInitiator = msgArray[4];
		return msgPacket;
	}
	
	/**
	 * Serialize cursor object for message sending
	 * @param cursor - Cursor object to be serialized
	 * @return serialized cursor string
	 * @author Chaitanya Nettem
	 */
	public static String serializeCursor(Cursor cursor)
	{
		String serializedString = "";
		int i=0;
		while (cursor.moveToNext()) 
		{
			// To avoid row delimiter for the first row
			if(i!=0)
				serializedString += ROW_DELIMITER;				
			serializedString += cursor.getString(0) + COL_DELIMITER	+ cursor.getString(1) + COL_DELIMITER + cursor.getString(2);		
			i++;
		}
		return serializedString;
	}
	
	/**
	 * De-serialize cursor object from message received
	 * @param serializedString - Serialized String
	 * @return Deserialized cursor object
	 * @author Chaitanya Nettem
	 */
	public static MatrixCursor deSerializeCursor(String serializedString)	
	{
		MatrixCursor cursor = new MatrixCursor(new String[]{"key","value", "version"});
		String[] rows = serializedString.split(ROW_DELIMITER);
		for (String row : rows) 
		{
			String[] columns = row.split(COL_DELIMITER);
			if(columns.length > 2 && !columns[0].isEmpty() && !columns[1].isEmpty() && !columns[2].isEmpty())
				cursor.addRow(columns);
		}
		cursor.close();
		return cursor;		
	}
	
	
	/**     * 
	* Function to Send message to target Host
	* @param targetHost  - Target to which the message should be sent 
	* @param msgPacket - Message Packet to be sent 
	* @author Chaitanya Nettem
	*/
	public static void sendMessage(String target, MessagePacket msgPacket) {
	
		//synchronized(lock)
		{
		  	// Attach Unique ID to the messageMSG_DELIMITER
			String msgToBeSent = MessagePacket.serializeMessage(msgPacket);
			String targetPort = DeviceInfo.getDevicePortNo(target);
			Log.d(TAG, "Message Sent - Target : " + target);
			Log.d(TAG, "Message Sent - Body - : " + msgToBeSent);
			
			// Add the message to queue so that responses can be queued and concurrency problems can be avoided
			//SimpleDynamoProvider.requestQueue.add(target + QUEUE_DELIMITER + msgPacket.msgType + QUEUE_DELIMITER + msgPacket.msgId);
			
		  	new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, targetPort, msgToBeSent);
		} 		
	}
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

