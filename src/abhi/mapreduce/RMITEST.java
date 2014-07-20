/**
 * 
 */
package abhi.mapreduce;

import java.net.InetAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import abhi.adfs.NameNodeSlave;

/**
 * @author abhisheksharma
 *
 */
public class RMITEST {

	
	public static void main(String[] args)
	{
		
		//Get the NameNodeSlave
		String identifer = "Eclat";
        String slave_Name = SystemConstants.getConfig(SystemConstants.NAMENODE_SLAVE_SERVICE);
        String lookupName = slave_Name +"_" + "128.237.205.17";
    
    	try {
			Registry diff =  LocateRegistry.getRegistry("128.237.205.17", 1099);
			for(String entry : diff.list())
				System.out.println(entry);
			
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
}
