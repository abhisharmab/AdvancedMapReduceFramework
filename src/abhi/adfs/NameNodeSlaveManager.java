/**
 * 
 */
package abhi.adfs;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;



/**
 * @author abhisheksharma, dkrew
 *
 */
public class NameNodeSlaveManager {
	
	private static String ipAddress;
	private static String portNumber;
	private static NameNodeMaster nameNodeMaster;

	public static void main(String args[])
    {
		System.out.println(ManagementFactory.getRuntimeMXBean().getName());
    	try {
			System.out.println(InetAddress.getLocalHost());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (args.length != 2) {
			System.err.println("Usage: NameNodeSlaveManager <RMI_IP> <RMI_Port>");
			return;
		}
		
		ipAddress = args[0];
		portNumber = args[1];
		
		lookUpNameNodeMaster();
		bindDataNode();

        

     
    }

	public static void lookUpNameNodeMaster(){
	       try
	        {
	        	System.setSecurityManager(new RMISecurityManager());
	        //	String lookup_name = "rmi://10.0.0.4:1099/NameNodeMaster"; 
	        	String lookup_name = "rmi://" +ipAddress + ":"+ portNumber+ "/NameNodeMaster";
	        
	        	System.out.println(lookup_name);
	    		nameNodeMaster = (NameNodeMaster) Naming.lookup(lookup_name);
	    		System.out.println("NameNodeMaster has been looked up.");
	    		nameNodeMaster.print();
	        		
	    	} catch (Exception e){
	    		e.printStackTrace();
	    		System.out.println("Manager: Exception thrown looking up " + "NameNodeSlaveManager");
	    		
	    	}
	}
	public static void bindDataNode(){
		
		// Generated an unique name.
		String name= "DataNode_"+getPID();
		String bindName = "rmi://" +ipAddress + ":"+ portNumber+ "/" + name; 
		System.out.println("Bind Name : " + bindName);
        System.out.println("Registering DataNode as : " + bindName);
     
        try{
        	DataNodeImpl dataNode = new DataNodeImpl();
        	Naming.bind(bindName, dataNode);
            System.out.println("DataNode: Ready...");
            
            // Register to the master
            if(nameNodeMaster.registerToMaster(bindName)){
            	System.out.println("DataNode has been registered to the NameNodeMaster.");
            } else {
            	System.out.println("Error in registering.");
            }
        
        } catch (RemoteException e){
        	e.printStackTrace();
	    } catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static String getPID(){
		String name = ManagementFactory.getRuntimeMXBean().getName();
		int index = name.indexOf('@');
		String pid = (String) name.subSequence(0,  index);
		return pid;
	}
}
 