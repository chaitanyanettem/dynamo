package edu.buffalo.cse.cse486586.dynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Queue;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.content.SharedPreferences.Editor;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	public static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	private static final String VERSION = "###version";	
	private final long TIMEOUT = 100;
	private final long TIMEOUT_SEC = 3;
	
	private boolean timeOutOccured = false;
	private boolean isRecovered = false;
	
	private String resultCursorString = "";
	private String resultParentString = "";
	private String resultReplicaString = "";
	
	private int resultDeletedRows = 0;
	
	private boolean isProcessCompleteInsert = false;
	private boolean isReplicaCompleteInsert = false;
	private int replicaCountInsert = 0;
	
	private boolean isProcessCompleteQuery = false;
	@SuppressWarnings("unused")
	private boolean isReplicaCompleteQuery = false;	
	private int replicaCountQuery = 0;
	
	private boolean isProcessCompleteDelete = false;
	@SuppressWarnings("unused")
	private boolean isReplicaCompleteDelete = false;
	private int replicaCountDelete = 0;
	
	private int replicaCountRecovery = 0;
	private int parentCountRecovery = 0;
	
	
	private Object lock = new Object();
	
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
    	
    	synchronized (lock) 
    	{
    		// Incase of recovery, wait till recovery operation gets completed.
    		while(isRecovered);
    		
    		int retVal = 0;
	    	try {
				/*
				 * Code to query key and values from the storage
				 * from the shared preference used during insert function
				 */
				Log.v(TAG,"delete content provider - start");
				Log.v(TAG,"uri :" + uri);
				Log.v(TAG,"selection :" + selection);
				
				Node currentNode = Node.getInstance();
				SharedPreferences sharedPreference = getContext().getSharedPreferences("SimpleDynamoSP", Context.MODE_PRIVATE);
				String target = "";
				boolean isDuplicate = false;
				isReplicaCompleteDelete = false;
				isProcessCompleteDelete = false;
				replicaCountDelete = 0;
				
				// Forming MessagePacket incase of forwarding
				MessagePacket msgPacket = new MessagePacket();
				msgPacket.setMsgId(selection);
				msgPacket.setMsgType(MSG_TYPE.REQUEST);
				msgPacket.setMsgOperation(MSG_OPER.DELETE);
				msgPacket.setMsgInitiator(currentNode.getDeviceID());
				
				Node lookupNode = Node.nodeLookupInMembership(selection);
				if(selectionArgs != null)
				{
					if(selectionArgs.length > 0 && MSG_TYPE.valueOf(selectionArgs[0]) == MSG_TYPE.DUPLICATE)
					{						
						//lookupNode =  NODE.CURRENT;
						isDuplicate = true;
					}
				}
				
				/* 
				 * If key to be deleted lies between the previous node id and the current node id 
				 * then insert into the local storage
				 */				
				if(lookupNode == currentNode || isDuplicate)		
				{
					if(selection.compareTo("*") == 0 || selection.compareTo("@") == 0)
					{	
						Log.v(TAG,"Deleting all rows...");
						retVal = sharedPreference.getAll().size();
						Editor editor = sharedPreference.edit();				
						editor.clear();
						editor.commit();				
						Log.v(TAG,"All rows deleted...");
						
						// Forward the request if the selection is '*' and current node is the initiator
						if(selection.compareTo("*") == 0 && currentNode.getNodeID().compareTo(currentNode.getNextNodeID()) != 0)
						{							
							msgPacket.setMsgContent(String.valueOf(retVal));
							for (String member : currentNode.getMembershipDetails()) 
							{
								if(currentNode.getDeviceID().compareTo(member) != 0)
								{
									MessagePacket.sendMessage(member, msgPacket);
									Log.i(TAG, "DELETE ALL : Sending * to device :"+member);
								}
							}							
							isProcessCompleteDelete = false;
						}						
					}
					else
					{
						Log.v(TAG, "Deleting one row ..");
						Editor editor = sharedPreference.edit();
						editor.remove(selection);
						editor.remove(selection + VERSION);
						editor.commit();
						retVal = 1;
						isProcessCompleteDelete = true;						

						// If duplicate message then dont need to replicate operation in current node's quorum 
						if(!isDuplicate)// && selection.compareTo("*") != 0)
						{
							for (String replicaTarget : currentNode.getQuorumReplicas())
							{
								msgPacket.setMsgType(MSG_TYPE.DUPLICATE);
								MessagePacket.sendMessage(replicaTarget, msgPacket);								
							}	
							isReplicaCompleteDelete = false;
							
						}
						else
							isReplicaCompleteDelete = true;
					}										
				}
				
				/* 
				 * Forward the insert request to next ID if the node id is greater than current node id
				 * or lesser the previous node id
				 */
				else			
				{
					target = lookupNode.getDeviceID();					
					msgPacket.setMsgType(MSG_TYPE.DUPLICATE);
					Log.d(TAG, "Delete Does not belong to current node. So sending to - " + target);
					MessagePacket.sendMessage(target, msgPacket);
					isProcessCompleteDelete = false;
					
					for (String replicaTarget : lookupNode.getQuorumReplicas())
					{				
						// If the current node is duplicate then dont send
						if(currentNode.getDeviceID().compareTo(replicaTarget) != 0)
						{
							Log.d(TAG, "Sending to target duplicates - "+replicaTarget);
							MessagePacket.sendMessage(replicaTarget, msgPacket);
						}
						else
						{
							Log.v(TAG, "As the current node is target's replica, Deleting one row ..");
							Editor editor = sharedPreference.edit();
							editor.remove(selection);
							editor.remove(selection + VERSION);
							editor.commit();
							retVal = 1;
							replicaCountDelete++;
						}
					}			
					isReplicaCompleteDelete = false;					
					timeOutOccured = false;
				}
				
					
				// Don't return until the initiator node gets value from all the nodes
				long j = 0;
				while(!isProcessCompleteDelete)// || !isReplicaCompleteDelete)
				{
					Thread.sleep(TIMEOUT_SEC);
					if(j > TIMEOUT)
					{
						Log.v(TAG, "INSIDE - Breaking bcoz of timeout");
						break;
					}
					j++;
				}
				
				// Adding total rows deleted across all nodes
				retVal += resultDeletedRows;				
				resultDeletedRows = 0;
				
				Log.v(TAG,"delete content provider - end ");
				
			} catch (Exception e) {
				Log.e(TAG, "Exception occured. ");
				e.printStackTrace();
				retVal = 0;
			}    
	        return retVal;
    	}
    }

    public int deleteForMessage(Uri uri, String selection, String[] selectionArgs) {
    	
		int retVal = 0;
    	try {
    		
    		// Incase of recovery, wait till recovery operation gets completed.
    		while(isRecovered);
    		
			/*
			 * Code to query key and values from the storage
			 * from the shared preference used during insert function
			 */
			Log.v(TAG,"delete for message - start");
			Log.v(TAG,"uri :" + uri);
			Log.v(TAG,"selection :" + selection);
			
			SharedPreferences sharedPreference = getContext().getSharedPreferences("SimpleDynamoSP", Context.MODE_PRIVATE);
	
			if(selection.compareTo("*") == 0 || selection.compareTo("@") == 0)
			{	
				Log.v(TAG,"Deleting all rows...");
				retVal = sharedPreference.getAll().size();
				Editor editor = sharedPreference.edit();				
				editor.clear();
				editor.commit();				
				Log.v(TAG,"All rows deleted...");
								
			}
			else
			{
				Log.v(TAG, "Deleting one row ..");
				Editor editor = sharedPreference.edit();
				editor.remove(selection);
				editor.remove(selection + VERSION);
				editor.commit();
				retVal = 1;
				Log.v(TAG, "row deleted");
			}
		
			Log.v(TAG,"delete for message - end ");
			
		} catch (Exception e) {
			Log.e(TAG, "Exception occured. ");
			e.printStackTrace();
			retVal = 0;
		}    
        return retVal;
    	
    }

    
    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

    	synchronized (lock) 
    	{
    		// Incase of recovery, wait till recovery operation gets completed.
    		while(isRecovered);
    		
	    	Log.v("SimpleDynamoProvider","insert Content Provider - start");
	    	Log.v("SimpleDynamoProvider", "Content Values" + values.toString());
	    	
	        try {
	        	
	        	
	        	/* Fetch the device node and then insert the data*/
	        	Node currentNode = Node.getInstance();
	        	
				// Declaring SharedPreference as Data Storage
				SharedPreferences sharedPreference = getContext().getSharedPreferences("SimpleDynamoSP", Context.MODE_PRIVATE);
				String keyToInsert = values.getAsString("key").trim();
				String valueToInsert = values.getAsString("value").trim();
				String target = "";
				boolean isDuplicate = false;
				isReplicaCompleteInsert = false;
				isProcessCompleteInsert = false;
				replicaCountInsert = 0;
				
				// Forming MessagePacket incase of forwarding
				MessagePacket msgPacket = new MessagePacket(keyToInsert, valueToInsert);
				msgPacket.setMsgType(MSG_TYPE.REQUEST);
				msgPacket.setMsgOperation(MSG_OPER.INSERT);
				msgPacket.setMsgInitiator(currentNode.getDeviceID());
				
				Node lookupNode = Node.nodeLookupInMembership(keyToInsert);
				if(values.containsKey("duplicate"))
				{
					Log.d(TAG, "Duplicate insert");
					isDuplicate = values.getAsBoolean("duplicate");
					Log.d(TAG, "duplicate boolean value - " + isDuplicate);
				}
				
				/* 
				 * If key to be inserted lies between the previous node id and the current node id 
				 * then insert into the local storage
				 */
				
				if(lookupNode == currentNode || isDuplicate)		
				{
					Log.v(TAG,"Inserting key/value pair as key belongs to current node...");
					Log.v(TAG,"key : "+keyToInsert);
					Log.v(TAG,"value : "+valueToInsert);
					int version = sharedPreference.getInt(keyToInsert + VERSION, 0);
					Log.v(TAG,"version : "+version);
					Editor editor = sharedPreference.edit();
					editor.putString(keyToInsert, valueToInsert);
					editor.putInt(keyToInsert+ VERSION, version + 1);
					editor.commit();		
					isProcessCompleteInsert = true;
					
					// If duplicate message then dont need to replicate in current node's quorum 
					if(!isDuplicate)
					{
						Log.d(TAG, "Not duplicate so sending insert message to all quroum replicas");
						for (String replicaTarget : currentNode.getQuorumReplicas())
						{
							Log.d(TAG, "Sending to current node duplicates - "+replicaTarget);
							msgPacket.setMsgType(MSG_TYPE.DUPLICATE);
							MessagePacket.sendMessage(replicaTarget, msgPacket);
						}
						replicaCountInsert++;
						isReplicaCompleteInsert = false;
					}
					else
						isReplicaCompleteInsert = true;				
				}
				
				/* 
				 * Forward the insert request to next ID if the node id is greater than current node id
				 * or lesser the previous node id
				 */
				else			
				{
					// If duplicate message then dont need to replicate in current node's quorum 
					if(!isDuplicate)
					{
						target = lookupNode.getDeviceID();
						msgPacket.setMsgType(MSG_TYPE.DUPLICATE);
						Log.d(TAG, "Insert Does not belong to current node. So sending to - " + target);
						MessagePacket.sendMessage(target, msgPacket);
						isProcessCompleteInsert = false;	
						
						for (String replicaTarget : lookupNode.getQuorumReplicas())
						{
							// If the current node is duplicate then dont send
							if(currentNode.getDeviceID().compareTo(replicaTarget) != 0)
							{
								Log.d(TAG, "Sending to target duplicates - "+replicaTarget);							
								MessagePacket.sendMessage(replicaTarget, msgPacket);
							}
							else
							{
								Log.d(TAG, "Inserting in local as it is the replica node for target. ");
								int version = sharedPreference.getInt(keyToInsert + VERSION, 0);
								Log.v(TAG,"version : "+version);
								Editor editor = sharedPreference.edit();
								editor.putString(keyToInsert, valueToInsert);
								editor.putInt(keyToInsert+ VERSION, version + 1);
								editor.commit();
								replicaCountInsert++;
							}
						}
						isReplicaCompleteInsert = false;
						timeOutOccured = false;
					}
					else
						Log.e(TAG, "ERROR - invalid duplicate message");
				}
				
		        // Dont return until the initiator node gets value from all the nodes
				long i=0;
				while(!isProcessCompleteInsert || !isReplicaCompleteInsert)
				{
					// Wait till the timeout
					Thread.sleep(TIMEOUT_SEC);
					if(i > TIMEOUT)
					{
						Log.v(TAG, "INSIDE - Breaking bcoz of timeout");
						break;
					}
					i++;
				}

				Log.i(TAG, "Resetting all values at the end of insert");
				replicaCountInsert = 0;
				Log.i(TAG, "Insert After loop : counter - " + i);
				//Thread.sleep(5000);	
			} catch (Exception e) {
				Log.e(TAG, "Exception occured. ");
				e.printStackTrace();
			}
	        
	        Log.v(TAG,"insert Content Provider - end");
	        return uri;
    	}
    }

    public Uri insertForMessage(Uri uri, ContentValues values) {
    		
	        try {
	        	// Incase of recovery, wait till recovery operation gets completed.
	    		while(isRecovered);    	
	    

		    	Log.v("SimpleDynamoProvider","insert for message - start");
		    	Log.v("SimpleDynamoProvider", "Content Values" + values.toString());
		    
		    	
				// Declaring SharedPreference as Data Storage
				SharedPreferences sharedPreference = getContext().getSharedPreferences("SimpleDynamoSP", Context.MODE_PRIVATE);
				String keyToInsert = values.getAsString("key").trim();
				String valueToInsert = values.getAsString("value").trim();
				
				Log.v(TAG,"Inserting key value pair to current node...");
				Log.v(TAG,"key : "+keyToInsert);
				Log.v(TAG,"Hash key : "+Node.genHash(keyToInsert));
				Log.v(TAG,"value : "+valueToInsert);
				
				int version = sharedPreference.getInt(keyToInsert + VERSION, 0);
				Log.v(TAG,"version : "+version);
				Editor editor = sharedPreference.edit();
				editor.putString(keyToInsert, valueToInsert);
				editor.putInt(keyToInsert+ VERSION, version + 1);
				editor.commit();		
				
				Log.v(TAG, "value inserted.");
				//Thread.sleep(5000);	
			} catch (Exception e) {
				Log.e(TAG, "Exception occured. ");
				e.printStackTrace();
			}
	        
	        Log.v(TAG,"insert for message - end");
	        return uri;
    }


    
    @Override
    public boolean onCreate() {
    
		  try {
			  
			  // Setting onRecovery flag before start listening to incoming requests to avoid concurrency issues
			  isRecovered = true;
			  
		      /*
		       * Create a server socket as well as a thread (AsyncTask) that listens on the server
		       * port.
		       * */
			  Log.v(TAG, "Server Socket Start ...");			  					  
		      ServerSocket serverSocket = new ServerSocket(DeviceInfo.SERVER_PORT);
		      new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		      
		      
		      Log.v(TAG, "Server Socket End ...");
		      
		      CheckForRecovery();
		      return true;
		  } catch (IOException e) {
		    
		      Log.e(TAG, "Can't create a ServerSocket");		      
		  }
		  return false;
    }
    
    /**
     * On recovery load the corresponding messages from replicas and 
     * make the values consistent
     */
    public void CheckForRecovery()
    {
    	try {
			Log.v(TAG, "Check For recovery - start");
    		
			final String INITIALIZED = "Initialized";
			
			if(!Node.isNodeInitialized())
			{
				TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
				String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
			    final String emulatorPort = String.valueOf((Integer.parseInt(portStr) * 2));
			    Node.initNodeInstance(emulatorPort);
			}
			
			Node currentNode = Node.getInstance();
			// Inside recovery
			Log.d(TAG, "Inside recovery checking the node device id : "+currentNode.getDeviceID());
			
			replicaCountRecovery = 0;
			parentCountRecovery = 0;
			
			// Forming MessagePacket incase of forwarding
			MessagePacket msgPacket = new MessagePacket();
			msgPacket.setMsgId("@");
			msgPacket.setMsgType(MSG_TYPE.REQUEST);
			msgPacket.setMsgOperation(MSG_OPER.RECOVERY);
			msgPacket.setMsgInitiator(currentNode.getDeviceID());
			
			// Declaring SharedPreference as Data Storage
			SharedPreferences sharedPreferenceRecovery = getContext().getSharedPreferences("SimpleDynamoSPRecovery", Context.MODE_PRIVATE);
			Node[] parentNode = new Node[2];
			
			isRecovered = sharedPreferenceRecovery.getBoolean(INITIALIZED, false);
			// Checking for recovery..
			Log.v(TAG, "Checking for recovery - " +isRecovered);
			if(isRecovered)
			{							
				isProcessCompleteQuery = false;					
				Log.v(TAG, "Sending query message to replicas for stabilization");
				for (String replicaTarget : currentNode.getQuorumReplicas())
				{					
					MessagePacket.sendMessage(replicaTarget, msgPacket);
				}			
				isReplicaCompleteQuery = false;
				timeOutOccured = false;
				
				int n = 0;
				Log.v(TAG, "Sending query message to parent nodes for storing their replicas..");
				for (String parentTarget : currentNode.getParentNodes()) 
				{
					parentNode[n] = Node.getInstance(parentTarget);
					MessagePacket.sendMessage(parentTarget, msgPacket);
					n++;
				}
				
				// Setting replica Count Query to -1 as we are expecting messge from 2 parent nodes and 2 replica nodes so the total count
				// of message recieved will be equal to 3 to satify the condition in RESPONSE method.
				//replicaCountQuery = -1;
				
				// Changine the replicaCountQuery to 2 as if we get atleast reply from replica we proceed with recovery as tester in phase 6 does
				// not give ample time between successive crashes.
				replicaCountQuery = 2;
				
				Log.i(TAG, "recovery end : ");
				Log.i(TAG, "Result String : "+resultCursorString);

			}
			else
			{
				Log.v(TAG, "Inserting recovery details ");
				Editor editor = sharedPreferenceRecovery.edit();
				editor.putBoolean(INITIALIZED, true);
				editor.commit();
			}
			Log.v(TAG, "Check For recovery - end");
			
		} catch (Exception e) {
			Log.e(TAG, "Exception on recovery");
			e.printStackTrace();
		}
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {

    	synchronized (lock)    	
    	{
	    	try {
				/*
				 * Code to query key and values from the storage
				 * from the shared preference used during query function
				 */
	    		
	    		// Incase of recovery, wait till recovery operation gets completed.
	    		while(isRecovered);
	    		
				Log.v(TAG,"query content provider - start");
				Log.v(TAG,"uri :" + uri);
				Log.v(TAG,"selection :" + selection);        
				
				Node currentNode = Node.getInstance();
				// Declaring SharedPreference as Data Storage
				SharedPreferences sharedPreference = getContext().getSharedPreferences("SimpleDynamoSP", Context.MODE_PRIVATE);
				MatrixCursor cursor = null;
				String target = "";
				
				isReplicaCompleteQuery = false;
				isProcessCompleteQuery = false;
				replicaCountQuery = 0;
				
				// Forming MessagePacket incase of forwarding
				MessagePacket msgPacket = new MessagePacket();
				msgPacket.setMsgId(selection);
				msgPacket.setMsgType(MSG_TYPE.REQUEST);
				msgPacket.setMsgOperation(MSG_OPER.QUERY);
				msgPacket.setMsgInitiator(currentNode.getDeviceID());
				
				Node lookupNode = Node.nodeLookupInMembership(selection);
				
				/* 
				 * If key to be queried lies between the previous node id and the current node id 
				 * then insert into the local storage
				 */
				if(lookupNode == currentNode)
				{
					/*
					 * Build cursor from the data retrieved
					 */
					Log.d(TAG, "looking up in the current node");
					cursor = new MatrixCursor(new String[]{"key","value","version"});
					if(selection.compareTo("*") == 0 || selection.compareTo("@") == 0)			
					{	
						for (Entry<String, ?> entry : sharedPreference.getAll().entrySet()) {
							String key = entry.getKey().toString();
							if(!key.contains(VERSION))
							{
								String value = entry.getValue().toString();
								int version = sharedPreference.getInt(key + VERSION, 0);
								cursor.addRow(new String[]{key, value, String.valueOf(version)});
							}
							isProcessCompleteQuery = true;
							isReplicaCompleteQuery = true;
						}
						Log.v(TAG, "Closing cursor");
						cursor.close();				
						
						// Forward the request if the selection is '*' and current node is the initiator
						if(selection.compareTo("*") == 0 && currentNode.getNodeID().compareTo(currentNode.getNextNodeID()) != 0)
						{
							msgPacket.setMsgContent(currentNode.getDeviceID());							
							for (String member : currentNode.getMembershipDetails()) 
							{
								if(currentNode.getDeviceID().compareTo(member) != 0)
								{
									MessagePacket.sendMessage(member, msgPacket);
									Log.i(TAG, "QUERY ALL : Sending * to device :"+member);
								}
							}	isReplicaCompleteQuery = true;
							isProcessCompleteQuery = false;
						}
					}
					else
					{
						// Find the selection and add the value to the cursor
						String value = sharedPreference.getString(selection, "null");
						int version = sharedPreference.getInt(selection + VERSION, 0);
						cursor.addRow(new String[]{selection,value, String.valueOf(version)});
						cursor.close();
						isProcessCompleteQuery = true;
						
						if(value.equals("null"))        
							Log.w(TAG, "Key does not exist");
						else
							Log.v(TAG,"Adding row to cursor");

					}					
				
				}
				
				/* 
				 * Forward the insert request to next ID if the node id is greater than current node id
				 * or lesser the previous node id
				 */
				else			
				{				  
					
					// Before sending the request to other nodes check whether the current node has 
					// the key as part of replica store
					String[] parentDeviceIds = currentNode.getParentNodes();
					boolean valueFound = false;
					if(lookupNode.getDeviceID().compareTo(currentNode.getDeviceID()) == 0 || 
							lookupNode.getDeviceID().compareTo(parentDeviceIds[0]) == 0 || 
							lookupNode.getDeviceID().compareTo(parentDeviceIds[1]) == 0)
					{
						Log.d(TAG, "Current node is the replica. So querying the key instead of forwarding.");
						
						// Find the selection and add the value to the cursor
						String value = sharedPreference.getString(selection, "null");
						int version = sharedPreference.getInt(selection + VERSION, 0);
						
						cursor = new MatrixCursor(new String[]{"key","value","version"});
						cursor.addRow(new String[]{selection,value, String.valueOf(version)});
						cursor.close();
						isProcessCompleteQuery = true;
						
						if(value.compareTo("null") == 0)        
							Log.e(TAG, "Key does not exist");
						else
						{
							Log.v(TAG,"Adding row to cursor");
							valueFound = true;
						}

					}
					
					// Even after querying the local if the value is not found then forward to respective node
					if(!valueFound)
					{
						target = lookupNode.getDeviceID();
						msgPacket.setMsgType(MSG_TYPE.DUPLICATE);
						Log.d(TAG, "Query Does not belong to current node. So sending to - " + target);
						MessagePacket.sendMessage(target, msgPacket);
						isProcessCompleteQuery = false;							
						timeOutOccured = false;		
						
						long j = 0;
						while(true)
						{
							Thread.sleep(TIMEOUT_SEC);
							
							// break if we get what we are waiting for
							if(isProcessCompleteQuery && resultCursorString.contains(selection))
								break;
							else
							{
								Log.e(TAG, "recieved incorrect message. so resetting flag... 0");
								isProcessCompleteQuery = false;
							}
							
							if(j > TIMEOUT)
							{
								Log.v(TAG, "INSIDE - Breaking bcoz of timeout");
								break;
							}
							j++;
						}
						Log.v(TAG, "Query waiting over..  2 ");
						
						// Even after the timeout if the query is not processed then
						// send to its replicas						
						for (String replicaTarget : lookupNode.getQuorumReplicas())
						{
							Log.v(TAG,"Sending to its replicas as timeout occured - " +replicaTarget);
							MessagePacket.sendMessage(replicaTarget, msgPacket);
						
							long k = 0;
							while(true)
							{
								Thread.sleep(TIMEOUT_SEC);
								
								// break if we get what we are waiting for
								if(isProcessCompleteQuery && resultCursorString.contains(selection))
									break;
								else
								{
									Log.e(TAG, "recieved incorrect message. so resetting flag... 1");
									isProcessCompleteQuery = false;
								}
								
								if(k > TIMEOUT)
								{
									Log.v(TAG, "INSIDE - Breaking bcoz of timeout");
									break;
								}
								k++;
							}
							Log.v(TAG, "Query waiting over..  3 ");
						
							if(isProcessCompleteQuery && resultCursorString.contains(selection))
								break;
							else
							{
								if(isProcessCompleteQuery)
								{
									isProcessCompleteQuery = false;
									Log.e(TAG, "recieved incorrect message. so resetting flag... 2");
								}
								Log.v(TAG, "timeout when waiting for replica. so continuing to next...");
							}
						}						
					}
					
				}
				
				// Detect node failures based on the message timeout
				// Dont return until the initiator node gets value from all the nodes
				long i = 0;
				if(selection.compareTo("@") != 0)
				{
					while(true)
					{
						if(timeOutOccured)
							break;
						
						Thread.sleep(TIMEOUT_SEC);
						
						// break if we get what we are waiting for
						if(isProcessCompleteQuery)
						{
							if(selection.compareTo("*") == 0)
								break;
							else if(resultCursorString.contains(selection))
								break;
							else
							{
								isProcessCompleteQuery = false;
								Log.e(TAG, "recieved incorrect message. so resetting flag... 3");
							}
						}
						
						// Wait till the timeout
						if(i > TIMEOUT)
						{
							Log.i(TAG, "Query Breakin bcoz of time out");
							break;
						}
						i++;
					}
					Log.v(TAG, "Query waiting over..  4 ");

					Log.i(TAG, "Query After loop : counter - " + i);
				
					// Appending all the result cursors to form one cursor object				
					if(cursor != null && selection.compareTo("*") == 0)
					{	
						Log.d(TAG, "Result cursor string before combine- " + resultCursorString);
						if(!resultCursorString.isEmpty())
							resultCursorString += MessagePacket.ROW_DELIMITER;					
						resultCursorString += MessagePacket.serializeCursor(cursor);
						cursor = MessagePacket.deSerializeCursor(resultCursorString);
						Log.d(TAG, "Result cursor string after combine- " + resultCursorString);
					}
					
					if(cursor == null)
					{
						Log.d(TAG, "Cursor is null so fetching value from result string and forming cursor...");
						Log.d(TAG, "Result cursor string - " + resultCursorString);
						if(!resultCursorString.isEmpty())
							cursor = MessagePacket.deSerializeCursor(resultCursorString);
					}
				}
				resultCursorString = "";
				
				// Sortorder should be null as invoked from content provider
				if(sortOrder != null)
				{
					Log.e(TAG, "invalid sort order");
					throw new Exception("Invalid sort order");
				}
				
				// Forming final cursor for returning to the caller
				Log.v(TAG, "constructing final cursor....");
				MatrixCursor finalCursor = new MatrixCursor(new String[]{"key","value"});
				if(cursor != null)
				{
					while(cursor.moveToNext())
					{
						finalCursor.addRow(new String[]{cursor.getString(0), cursor.getString(1)});
					}
					finalCursor.close();
				}
				Log.v(TAG,"query content provider - end ");
				
				return finalCursor;
				
			} catch (Exception e) {
				Log.e(TAG, "Exception occured. ");
				e.printStackTrace();
			}
	        
	        return null;      
    	}        
    }
    
    

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {

    	
        return 0;
    }
	
    
    public Cursor queryForMessage(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {

    //	synchronized (lock) 
    	{
	    	try {
	    		
	    		// Incase of recovery, wait till recovery operation gets completed.
	    		while(isRecovered);
	    		
				/*
				 * Code to query key and values from the storage
				 * from the shared preference used during query function
				 */
				Log.v(TAG,"query for message - start");
				Log.v(TAG,"uri :" + uri);
				Log.v(TAG,"selection :" + selection);        
				
				// Declaring SharedPreference as Data Storage
				SharedPreferences sharedPreference = getContext().getSharedPreferences("SimpleDynamoSP", Context.MODE_PRIVATE);
				MatrixCursor cursor = null;
				
				/*
				 * Build cursor from the data retrieved
				 */
				Log.d(TAG, "looking up in the current node");
				cursor = new MatrixCursor(new String[]{"key","value","version"});
				if(selection.compareTo("*") == 0 || selection.compareTo("@") == 0)			
				{	
					Log.v(TAG,"Querying all rows...");
					for (Entry<String, ?> entry : sharedPreference.getAll().entrySet()) {
						String key = entry.getKey().toString();
						if(!key.contains(VERSION))
						{
							String value = entry.getValue().toString();
							int version = sharedPreference.getInt(key + VERSION, 0);
							Log.d(TAG, "querying all key - " + key + " value - "+ value + " version - " + version);
							cursor.addRow(new String[]{key, value, String.valueOf(version)});
						}
					}
					Log.v(TAG, "Closing cursor");
					cursor.close();				
					
				}
				else
				{
					// Find the selection and add the value to the cursor
					String value = sharedPreference.getString(selection, "null");
					int version = sharedPreference.getInt(selection + VERSION, 0);
					cursor.addRow(new String[]{selection,value, String.valueOf(version)});
					cursor.close();
					
					if(value.equals("null"))        
						Log.e(TAG, "Key does not exist");
					else
						Log.v(TAG,"Adding row to cursor");
					
				}	
				Log.v(TAG,"query for message - end ");
				return cursor;
			} catch (Exception e) {
				Log.e(TAG, "Exception occured in queryForMessage ");
				e.printStackTrace();
				return null;
			}
	              
    	}        
    }

    
    
    
    public AsyncTask<ServerSocket, String, Void> getServerTask()
    {
    	return new ServerTask();
    }
		  
	  /*** 
	   * Server Task in each device to receive the message from the socket
	   * and deliver based on the sequence algorithm 
	   */
	   public class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		   int msgCount = 0, trackerLength = 5;
		   boolean trackerArrived = false;
	  	   int replicaLength = 0;
		   	
	      @Override
	      protected Void doInBackground(ServerSocket... sockets) {
	          ServerSocket serverSocket = sockets[0];
	  		  String msgRecieved = "";

	  		  // Create a Queue to manage incoming requests
	  		  Queue<String> messageQueue = new LinkedList<String>();
	  		  
	          /*
	           * TODO: Fill in your server code that receives messages and passes them
	           * to onProgressUpdate().
	           */
	  		
	  		  /*
	           * Code to accept the client socket connection and read message from the socket. Message read  
	           * from the socket is updated in the Server UI through ProgressUpdate Call.
	           */  
	        	
				while (true) {   // Infinite while loop in order to enable continuous two-way communication
					
					try
					{
					    // Default Buffer Size is assigned as 4098 
					    //byte buffer[] = new byte[4098];
					    // Setting default socket time out
					    //serverSocket.setSoTimeout(socketTimeOut);
						Socket socket = serverSocket.accept();	  				
						BufferedReader buf =  new BufferedReader(new InputStreamReader(socket.getInputStream()));
						msgRecieved = buf.readLine();
						if (msgRecieved != null) 
						{
							msgRecieved = msgRecieved.trim();
							Log.d(TAG, "Message Recieved - " + msgRecieved);
							
							// if recovering then dont send it for processing, instead queue it
							if(isRecovered)
							{
								if(!msgRecieved.contains("RESPONSE"))
								{
									Log.d(TAG, "Recovery mode. So adding data to Queue");
									messageQueue.add(msgRecieved);
									continue;
								}
								else
									Log.d(TAG, "Recovery mode. But getting reponse. so allowing");
							}
							else
							{
								if(messageQueue.size() != 0)
								{
									while(messageQueue.size() != 0)
									{
										Log.d(TAG, "recovered. So sending all data for processing.");
										publishProgress(messageQueue.poll());
										Thread.sleep(10);
									}
								}
							}
							
							// Send to publish progress for further message processing
							publishProgress(msgRecieved);
							Log.d(TAG, "publish progress invoked.");
							Thread.sleep(10);
						}
						buf.close();
					}
						catch (SocketException e) {
						Log.w(TAG, "Timeout occured in socket connection");
						timeOutOccured = true;
						e.printStackTrace();
					} catch (IOException e) {
						Log.e(TAG, "Error in socket connection.");
						timeOutOccured = false;
						e.printStackTrace();
					} catch (InterruptedException e) {
						Log.e(TAG, "Error in thread waiting.");
						e.printStackTrace();
					}					
				}
	      }

	      @Override
	      protected void onProgressUpdate(String...strings) {
	             	
	      	
	    	  try {
	    		  //synchronized (lock) 
	    		  
	    		  Log.v(TAG, "Prgress Update CHECK");
				
				/* Fetch the device node and then insert the data*/
				  Node currentNode = Node.getInstance();
				  
				  MessagePacket msgPacket = MessagePacket.deSerializeMessage(strings[0].trim());
				  String target = "";					
				  
				  if(msgPacket.getMsgId().compareTo("*") == 0 && msgPacket.getMsgInitiator().compareTo(currentNode.getDeviceID()) == 0)
					  msgPacket.setMsgType(MSG_TYPE.RESPONSE);
				  if(msgPacket.getMsgType() == MSG_TYPE.REQUEST)
				  {  
					  Log.v(TAG, "inside message as REQUEST block");
					
					  if(msgPacket.getMsgInitiator().compareTo(currentNode.getDeviceID()) != 0)
					  {							  							  						
						  Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
						  target = msgPacket.getMsgInitiator();
						  					  
						  MessagePacket responseMsgPacket = new MessagePacket();							  
						  responseMsgPacket.setMsgType(MSG_TYPE.RESPONSE);
						  responseMsgPacket.setMsgId(msgPacket.getMsgId());
						  responseMsgPacket.setMsgOperation(msgPacket.getMsgOperation());
						  responseMsgPacket.setMsgInitiator(currentNode.getDeviceID());
						  
						  if(msgPacket.getMsgOperation() == MSG_OPER.INSERT)
						  {
							  ContentValues contentValue = new ContentValues();         
							  contentValue.put("key", msgPacket.getMsgId());
							  contentValue.put("value", msgPacket.getMsgContent());
							  Uri newUri =  insertForMessage(uri,contentValue);
							  
							  // Send Response Message to initiator
							  responseMsgPacket.setMsgContent(newUri.toString());								  
							  MessagePacket.sendMessage(target, responseMsgPacket);
						  }
						  else if(msgPacket.getMsgOperation() == MSG_OPER.QUERY || msgPacket.getMsgOperation() == MSG_OPER.RECOVERY)
						  {
							  Cursor resultCursor = queryForMessage(uri, null, 
									  				(msgPacket.getMsgId().compareTo("*")==0)?"@":msgPacket.getMsgId(), null, "MESSAGE");
							  
							  // Send Response Message to initiator
							  responseMsgPacket.setMsgContent(MessagePacket.serializeCursor(resultCursor));								  
							  MessagePacket.sendMessage(target, responseMsgPacket);								  
						  }
						  else if(msgPacket.getMsgOperation() == MSG_OPER.DELETE)
						  {
							  int noRowsDeleted = deleteForMessage(uri, 
									  				(msgPacket.getMsgId().compareTo("*")==0)?"@":msgPacket.getMsgId(), null);

							  // Send Response Message to initiator
							  responseMsgPacket.setMsgContent(String.valueOf(noRowsDeleted));								  
							  MessagePacket.sendMessage(target, responseMsgPacket);
						  }
						  else if(msgPacket.getMsgOperation() == MSG_OPER.NODEJOIN)
						  {									
							  responseMsgPacket.setMsgContent(currentNode.getDeviceID() + ":::" + currentNode.getPrevDeviceID());
							  currentNode.setPrevDeviceID(msgPacket.getMsgId());
							  currentNode.setPrevNodeID(Node.genHash(msgPacket.getMsgId()));
							  MessagePacket.sendMessage(target, responseMsgPacket);
						  }
					  }						  
				  }

				  // only initiator gets Message Type Response, and in that case announce that process is complete
				  else if(msgPacket.getMsgType() == MSG_TYPE.RESPONSE)
				  {		
					  Log.v(TAG, "response thread section");
					  switch(msgPacket.getMsgOperation())
					  {
					  	case INSERT:		
					  	{
					  		// wait until we get response from the original insert and duplicate
					  		Log.d(TAG, "Getting response from " + msgPacket.getMsgInitiator());
					  		isProcessCompleteInsert = true;
					  		replicaCountInsert++;
					  		if(replicaCountInsert == 3) // 3 bcoz including the lookupnode with 2 replicas
					  		{
					  			Log.d(TAG, "Got insert reply from all replicas " + replicaCountInsert);
					  			isReplicaCompleteInsert = true;
					  			replicaCountInsert = 0;
					  		}
					  		else
					  			Log.d(TAG, "Got insert reply from replicas " + replicaCountInsert);
					  		
					  		break;
					  	}
					  	case DELETE:
					  	{
				  			resultDeletedRows += Integer.parseInt(msgPacket.getMsgContent());
				  			
				  			// wait until we get response from the original insert and duplicate
				  			Log.d(TAG, "Getting response from " + msgPacket.getMsgInitiator());
					  		isProcessCompleteDelete = true;
					  		replicaCountDelete++;
					  		if(replicaCountDelete == 3) // 3 bcoz including the lookupnode with 2 replicas
					  		{
					  			Log.d(TAG, "Got delete reply from both replicas " + replicaCountDelete);
					  			isReplicaCompleteDelete = true;
					  			replicaCountDelete = 0;
					  		}						  		
					  		else
					  			Log.d(TAG, "Got delete reply from one replicas " + replicaCountDelete);
					  		break;
					  	}
					  	case QUERY:
					  	{
					  		Log.d(TAG, "Getting response from " + msgPacket.getMsgInitiator());
					  		if(msgPacket.getMsgId().compareTo("*") == 0)
					  		{						  				
				  				if(!resultCursorString.isEmpty())
				  					resultCursorString += MessagePacket.ROW_DELIMITER;
				  				resultCursorString += msgPacket.getMsgContent();
				  				msgCount++;
				  				Log.i(TAG, "Message in Query * : "+msgPacket.getMsgContent());
				  				Log.i(TAG, "Message Count : "+msgCount);
				  				if(trackerLength -1 == msgCount)
				  				{
				  					Log.d(TAG,"Got query message from all devices - "+msgCount);
				  					isProcessCompleteQuery = true;
				  					msgCount = 0;						  					
				  				}
				  				else
				  					Log.d(TAG,"Got query message only devices count - "+msgCount);
					  		}
					  		else
					  		{
					  			Log.v(TAG, "Assigning to cursor string");
					  			resultCursorString = msgPacket.getMsgContent();
		
					  			replicaCountQuery++;
					  			Log.i(TAG, "Message in Query : "+msgPacket.getMsgId());
					  			Log.i(TAG, "Message in Query : "+msgPacket.getMsgContent());
				  				Log.i(TAG, "Message Count : "+ replicaCountQuery);
				  				Log.i(TAG, "Result Cursor Str : " + resultCursorString);
					  		
				  				// wait until we get response from the original insert and duplicate
						  		isProcessCompleteQuery = true;
						  		if(replicaCountQuery == 3) // 3 bcoz including the lookupnode with 2 replicas
						  		{
						  			Log.d(TAG, "Got query reply from all replicas " + replicaCountQuery);
						  			isReplicaCompleteQuery = true;
						  			replicaCountQuery = 0;
						  		}						  		
						  		else
						  			Log.d(TAG, "Got query reply from  replicas " + replicaCountQuery);
					  		}
					  		break;
					  	}				
						case RECOVERY:
					  	{
					  		Log.d(TAG, "Getting response from " + msgPacket.getMsgInitiator());
					  		Log.i(TAG, "Message in Recovery : "+msgPacket.getMsgId());
				  			String finalCursorString = "";
				  			if(currentNode.isReplicaMember(msgPacket.getMsgInitiator()))
				  			{
					  			//  wait for replicas						  			
					  			resultReplicaString = msgPacket.getMsgContent();
					  			replicaCountRecovery++;
				  				Log.i(TAG, "Replica recovery Count : "+ replicaCountRecovery);
					  			
						  		if(replicaCountRecovery == 2) 
						  		{
						  			Log.d(TAG, "Got recovery reply all replica " + replicaCountRecovery);
						  		}						  		
						  		else
						  			Log.d(TAG, "Got recovery reply from one replicas " + replicaCountRecovery);
						  		finalCursorString = resultReplicaString;
				  			}	
				  			else if(currentNode.isParentMember(msgPacket.getMsgInitiator()))
				  			{
				  				//  wait for replicas						  				
					  			resultParentString = msgPacket.getMsgContent();
					  			parentCountRecovery++;
					  			
				  				Log.i(TAG, "Parent recovery Count : "+ parentCountRecovery);
					  			
						  		if(parentCountRecovery == 2) 
						  		{
						  			Log.d(TAG, "Got recovery reply all parent " + parentCountRecovery);
						  		}						  		
						  		else
						  			Log.d(TAG, "Got recovery reply from one parent " + parentCountRecovery);
						  		finalCursorString = resultParentString;
				  			}
				  			
					  		// Incase of recovery the current node will get the information from
					  		// replicas and parent nodes. Insert it and proceed.
					  		if(replicaCountRecovery > 0 || parentCountRecovery > 0)
					  		{
					  			Log.d(TAG,"Entering in recovery logic inside query...");
								SharedPreferences sharedPreference = getContext().getSharedPreferences("SimpleDynamoSP", Context.MODE_PRIVATE);

								MatrixCursor cursor = null;
								
								if(!finalCursorString.isEmpty())
									cursor = MessagePacket.deSerializeCursor(finalCursorString);				
								else
									Log.e(TAG, "final cursor string is empty");
								
								Editor editor = sharedPreference.edit();		
								String[] parentDeviceIds = currentNode.getParentNodes();
								if(cursor != null)
								{
									while(cursor.moveToNext())
									{
										String key  = cursor.getString(0);
										Node lookupNode = Node.nodeLookupInMembership(key);
										
										// Checking the key whether it belongs to current node or parent nodes then perform insert
										Log.d(TAG, "Node looked up for key : "+ key + " is node : "+lookupNode.getDeviceID());
										if(lookupNode.getDeviceID().compareTo(currentNode.getDeviceID()) == 0 || 
												lookupNode.getDeviceID().compareTo(parentDeviceIds[0]) == 0 || 
												lookupNode.getDeviceID().compareTo(parentDeviceIds[1]) == 0)
										{
											Log.d(TAG, "inserting key in current node as it belongs to current/parent : "+key);
											String value = cursor.getString(1);
											int version = Integer.parseInt(cursor.getString(2));
											if(sharedPreference.contains(key))
											{
												String existingValue = sharedPreference.getString(key, "null");
												int existingVersion = sharedPreference.getInt(key + VERSION, 0);
												if(version <  existingVersion)
												{
													Log.e(TAG, "Existing value greater than replica value");
													value = existingValue;
													version = existingVersion;
												}
											}
											Log.d(TAG,"Inserted value : "+value + " version : "+version);
											editor.putString(key, value);
											editor.putInt(key + VERSION, version);
										}
									}
									editor.commit();
								}
								if(replicaCountRecovery > 0)
									isRecovered = false;
								if(parentCountRecovery == 2)
									parentCountRecovery = 0;
								if(replicaCountRecovery == 2)
									replicaCountRecovery = 0;
								Log.v(TAG,"Data recovered and rows are consitent with replicas");				
					  		}
					  		break;
					  	}				
					  	case NODEJOIN:
					  	{
					  		String[] placeHolderNodes = msgPacket.getMsgContent().split(":::");
					  		currentNode.setNextDeviceID(placeHolderNodes[0]);
					  		currentNode.setNextNodeID(Node.genHash(placeHolderNodes[0]));
					  		currentNode.setPrevDeviceID(placeHolderNodes[1]);
					  		currentNode.setPrevNodeID(Node.genHash(placeHolderNodes[1]));
					  		
					  		target = placeHolderNodes[1];
					  		MessagePacket nodeUpdateMsg = new MessagePacket();
					  		nodeUpdateMsg.setMsgId(currentNode.getDeviceID());						  		
					  		nodeUpdateMsg.setMsgInitiator(currentNode.getDeviceID());						  		
					  		nodeUpdateMsg.setMsgType(MSG_TYPE.RESPONSE);
					  		nodeUpdateMsg.setMsgOperation(MSG_OPER.NODEUPDATE);
					  		MessagePacket.sendMessage(target, nodeUpdateMsg);
					  		break;
					  	}
					  	case NODEUPDATE:
					  	{
					  		currentNode.setNextDeviceID(msgPacket.getMsgId());
					  		currentNode.setNextNodeID(Node.genHash(msgPacket.getMsgId()));	
					  		break;
					  	}
					  	default:
						Log.e(TAG, "Invalid Message RESPONSE Operation :"+ msgPacket.getMsgOperation());
						break;
					  }
				  }
				  else if(msgPacket.getMsgType() == MSG_TYPE.DUPLICATE)
				  {
					
					   // If its a duplicate message then dont have to perform node lookup. Rather just perform the operation requested.
					  Log.d(TAG, "Duplicate key recieved");
					  if(msgPacket.getMsgInitiator().compareTo(currentNode.getDeviceID()) != 0)
					  {
						  							  						
						  Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
						  target = msgPacket.getMsgInitiator();
						  					  
						  MessagePacket responseMsgPacket = new MessagePacket();							  
						  responseMsgPacket.setMsgType(MSG_TYPE.RESPONSE);
						  responseMsgPacket.setMsgId(msgPacket.getMsgId());
						  responseMsgPacket.setMsgOperation(msgPacket.getMsgOperation());
						  responseMsgPacket.setMsgInitiator(currentNode.getDeviceID());
						  
						  if(msgPacket.getMsgOperation() == MSG_OPER.INSERT)
						  {
							  ContentValues contentValue = new ContentValues();         
							  contentValue.put("key", msgPacket.getMsgId());
							  contentValue.put("value", msgPacket.getMsgContent());
							  contentValue.put("duplicate", true);
							  Uri newUri =  insertForMessage(uri,contentValue);
							  
							  // Send Response Message to initiator
							  responseMsgPacket.setMsgContent(newUri.toString());								  
							  MessagePacket.sendMessage(target, responseMsgPacket);								
						  }
						  else if(msgPacket.getMsgOperation() == MSG_OPER.QUERY)
						  {
							  Cursor resultCursor = queryForMessage(uri, null, 
									  				(msgPacket.getMsgId().compareTo("*")==0)?"@":msgPacket.getMsgId(), new String[]{MSG_TYPE.DUPLICATE.toString()}, "MESSAGE");

							  // Send Response Message to initiator
							  responseMsgPacket.setMsgContent(MessagePacket.serializeCursor(resultCursor));								  
							  MessagePacket.sendMessage(target, responseMsgPacket);							  
						  }
						  else if(msgPacket.getMsgOperation() == MSG_OPER.DELETE)
						  {
							  int noRowsDeleted = deleteForMessage(uri, 
									  				(msgPacket.getMsgId().compareTo("*")==0)?"@":msgPacket.getMsgId(), new String[]{MSG_TYPE.DUPLICATE.toString()});
							  
							  // Send Response Message to initiator
							  responseMsgPacket.setMsgContent(String.valueOf(noRowsDeleted));								  
							  MessagePacket.sendMessage(target, responseMsgPacket);
						  }						  						  
					  }
				  }
				  else
				  {
					  Log.e(TAG,"Invalid Message Type");
				  }	
	    		
				  
			} catch (Exception e) {
				Log.e(TAG, e.getMessage());
				e.printStackTrace();
			}

	          return;
	      }
	  }
}
