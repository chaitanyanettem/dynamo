package edu.buffalo.cse.cse486586.dynamo;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import android.os.AsyncTask;
import android.util.Log;

/***
 * ClientTask is an AsyncTask that should send a string over the network.
 * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
 * an enter key press event.
 * 
 * @author Chaitanya Nettem
 *
 */
public class ClientTask extends AsyncTask<String, Void, Void> {
	
	public static final String TAG =  SimpleDynamoActivity.class.getSimpleName();

    @Override
    protected Void doInBackground(String... msgs) {
        try {              

    	 
	        /*
	         * Code to get the outputstream from the socket created for client-server connection and sending 
	         * the message through the socket 
	         */                 
	    	String targetHost = msgs[0]; // target
	        String msgToSend = msgs[1]; // Message to be sent	                
    		
    		Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(targetHost));
            OutputStream out = socket.getOutputStream();            
            out.write(msgToSend.getBytes());                
            out.flush();  
            out.close();
            Log.d(TAG, "Message Sent - " + msgToSend); 
            socket.close();
            
        } catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException");
            e.printStackTrace();
        } catch (IOException e) {
            Log.e(TAG, "ClientTask socket IOException");
            e.printStackTrace();
        }

        return null;
    }
}
