package edu.buffalo.cse.cse486586.dynamo;

/**
 * Device Information - Static Class for storing the Port information and
 * fetching the device name from the port defined
 * 
 * @author Chaitanya Nettem
 */
public class DeviceInfo {
	
	static final String[] REMOTE_PORTS = {	"11108", 
											"11112",
											"11116",
											"11120",
											"11124"};
	
	static final String[] DEVICE_IDS = {"5554",
										"5556",
										"5558",
										"5560",
										"5562"};
	
	static final String BASE_DEVICE_ID = DEVICE_IDS[0];
	
	static final int SERVER_PORT = 10000;
	
	/**
	 * Get device name from the Port Number
	 * @param portNo -  Port No
	 * @return - Device Name
	 */
	public static String getDeviceName(String portNo)
	{			
		for (int i = 0; i < REMOTE_PORTS.length; i++)		 
			if ((REMOTE_PORTS[i].compareTo(portNo) == 0)) 
				return DEVICE_IDS[i];		
		return "InvalidPortNo";
	}
	
	/**
	 * Get port no from the Device name
	 * @param deviceID - Device Id from which the port no is generated
	 * @return - Device Port no
	 */
	public static String getDevicePortNo(String deviceID)
	{			
		for (int i = 0; i < DEVICE_IDS.length; i++)		 
			if ((DEVICE_IDS[i].compareTo(deviceID) == 0)) 
				return REMOTE_PORTS[i];		
		return "InvalidDevice";
	}

}