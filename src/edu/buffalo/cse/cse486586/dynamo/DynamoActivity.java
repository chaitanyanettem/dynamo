package edu.buffalo.cse.cse486586.dynamo;

import java.io.IOException;

import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {

	SimpleDynamoProvider simpleDynamoProvider = null;
	public static final String TAG = SimpleDynamoActivity.class.getSimpleName();
	
	
    @Override
    protected void onCreate(Bundle savedInstanceState) {
    	
    	try {    		
    		
		        super.onCreate(savedInstanceState);
		        setContentView(R.layout.activity_simple_dynamo);
		        
		        final TextView tv = (TextView) findViewById(R.id.textView1);
		        tv.setMovementMethod(new ScrollingMovementMethod());
		        findViewById(R.id.button3).setOnClickListener(
		                new OnTestClickListener(tv, getContentResolver()));
		        
		        final Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
		        
		        findViewById(R.id.button1).setOnClickListener(new OnClickListener() {
					
					@Override
					public void onClick(View v) {
						// TODO Auto-generated method stub
						try {
							Node node = Node.getInstance();
							Cursor cursor = getContentResolver().query(uri, null, "@", null, null);
							String result = "Device ID : " + node.getDeviceID();
							while(cursor.moveToNext())
							{
								result += "\n" + cursor.getString(0) + " : " + cursor.getString(1); 
							}
							tv.setText(result);
								
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
						
					}
				});
		        
		        /*
		         * Calculate the port number that this AVD listens on.
		         * It is just a hack that I came up with to get around the networking limitations of AVDs.
		         * The explanation is provided in the PA1 spec.
		         */
		        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
		        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		        final String emulatorPort = String.valueOf((Integer.parseInt(portStr) * 2));
		        
		        findViewById(R.id.button4).setOnClickListener(new OnClickListener() {
					
					@Override
					public void onClick(View v) {
						Node node;
						try {
							node = Node.getInstance();
							tv.setText("\n Device ID : "+node.getDeviceID() +
									"\nDevice HashID : "+node.getNodeID() +
									"\nPrev DeviceID : "+node.getPrevDeviceID() +
									"\nNext DeviceID : "+node.getNextDeviceID());							
									
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					}
				});
		        
		        findViewById(R.id.button5).setOnClickListener(new OnClickListener() {
					
					@Override
					public void onClick(View v) {
						Node node;
						try {
							node = Node.getInstance();
							tv.setText("\n Inserting : "+node.getDeviceID());// +
							ContentValues contentValue = new ContentValues();         
							
							if(node.getDeviceID() == "5554")
							{
								contentValue.put("key","52NMGXlFKbm8mywSznhdhgF4zfE6N3fB");
								contentValue.put("value", "5duHjPVgb7PJMzJWhKdRVxgDa0hkq7US");							
								getContentResolver().insert(uri,contentValue);
								
								contentValue.put("key","94BGfwN6IEgeHqkT0OquRVsptx4L1jDa");
								contentValue.put("value", "ZjTvJgE5hC6pBE4EJm2v7Vi8FuDkbAwI");							
								getContentResolver().insert(uri,contentValue);
								
								contentValue.put("key","6yhgjBvCQNPwsPLQnqr8qrS0KXt8Vw9s");
								contentValue.put("value", "VCTlyrB6yOfTPhFwxz58Japl5Ga1GuC0");							
								getContentResolver().insert(uri,contentValue);
							}
							if(node.getDeviceID() == "5556")
							{
								contentValue.put("key","83LhwdPv8W53aCWZIfWmpfRzlNIpPw5N");
								contentValue.put("value", "Ww3DqACtPesmkNUdpeCESzeIzdlOaqEr");							
								getContentResolver().insert(uri,contentValue);
								
								contentValue.put("key","4xfJqxaaPpfj6Yxci0CO1OwoVph36wHL");
								contentValue.put("value", "VezgBZdNr8icni7qeYXZuYRthMEv22Us");							
								getContentResolver().insert(uri,contentValue);
								
								contentValue.put("key","H2JV9wwTbZKnXqYJpaz2tBC9Zt0qFOXG");
								contentValue.put("value", "lMUcHkK6dNai1eBD6rWfJo9pa4smWgiz");							
								getContentResolver().insert(uri,contentValue);
							}
							
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					}
				});
		        
		        findViewById(R.id.button6).setOnClickListener(new OnClickListener() {
					
					@Override
					public void onClick(View v) {
						Node node;
						try {
							node = Node.getInstance();
							  getContentResolver().query(uri, null,"*", null, null);
							  tv.setText("\n Querying : "+node.getDeviceID());// +
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					}
				});
		        
		        if(!Node.isNodeInitialized())
		        {
		        	// Initialize Node Instance
		        	Node.initNodeInstance(emulatorPort);       
		        	Node node = Node.getInstance();
		        	Log.v(TAG, "Node instance initialized : "+ node.getDeviceID());
		        }
		   
		  } catch (IOException e) {		    
		      Log.e(TAG, "Can't create a ServerSocket");		      
		  } catch (Exception e) {
		      Log.e(TAG, "Exception in oncreate - "+e.getMessage());
			e.printStackTrace();
		}
		
			 
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.simple_dynamo, menu);
        return true;
    }
}