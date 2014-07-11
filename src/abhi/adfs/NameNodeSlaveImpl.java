/**
 * 
 */
package abhi.adfs;

import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * @author Douglas Rew
 *
 */
public class NameNodeSlaveImpl extends UnicastRemoteObject implements NameNodeSlave{

	protected NameNodeSlaveImpl() throws RemoteException {
		super();
		// TODO Auto-generated constructor stub
	}
	


	public static void main(String args[])
    {
		if (args.length != 2) {
			System.err.println("Usage: NameNodeSlave <RMI_IP> <RMI_Port>");
			return;
		}
		
		
        try
        {
        	System.setSecurityManager(new RMISecurityManager());
        	String ip = args[0];
        	String port = args[1];
        	
      
        	//String name = "rmi://128.237.205.17:1099/NameNodeMaster"; 
        	String name = "rmi://" +ip + ":"+ port+ "/NameNodeMaster"; 
        	System.out.println(name);
        	NameNodeMaster nameNodeMaster = null;
    		nameNodeMaster = (NameNodeMaster) Naming.lookup(name);
    		nameNodeMaster.print();
        		
    	} catch (Exception e){
    		e.printStackTrace();
    		System.out.println("Slave: Exception thrown looking up " + "NameNodeMaster");
    	}
    }

}
