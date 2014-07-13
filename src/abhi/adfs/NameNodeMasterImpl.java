/**
 * 
 */
package abhi.adfs;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Douglas Rew
 *
 */
public class NameNodeMasterImpl extends UnicastRemoteObject implements NameNodeMaster{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2917579042804203973L;
	private static String master_Name;
	private static HashMap<String, ArrayList<String>> list_distributed_files; 
	
	private static HashMap<String, DataNode> list_dataNode;
	
	public NameNodeMasterImpl() throws RemoteException {
		super();
		list_distributed_files = new HashMap<String, ArrayList<String>>();
		list_dataNode = new HashMap<String, DataNode>();
		// TODO Auto-generated constructor stub
	}
	
	public boolean registerToMaster(String slave_name) 
			throws RemoteException {
		// TODO Auto-generated method stub
		if(list_distributed_files.containsKey(slave_name)){
			// This means that they were errors previously and we are starting a new list
			// for the distributed files
			
			ArrayList<String> list = new  ArrayList<String>();
			list_distributed_files.put(slave_name, list);
			
			
			// This is for testing
			System.out.println(list_distributed_files);

			
			
			// This should trigger restructure in the files.
		} else {
			ArrayList<String> list = new  ArrayList<String>();
			list_distributed_files.put(slave_name, list);
			
		}
	
		// Grabbing the stub for future use.
		registerDataNode(slave_name);
		return true;
		

	}

	@Override
	public boolean registerFileToMaster(String slave_name, String file_name)
			throws RemoteException {
		// TODO Auto-generated method stub
		if(list_distributed_files.containsKey(slave_name)){
			if(list_distributed_files.get(slave_name).contains(file_name)){
				System.out.println("This file is already registered Filename : " + file_name + " in slave : " + slave_name);
				// Might need to throw exception
				return false;
			} else {
				list_distributed_files.get(slave_name).add(file_name);
				return true;
			}
			
		} else {
			return false;	
		}
		
	}
	
	public static void main(String args[])
    {
		if (args.length != 1) {
			System.err.println("Usage: NameNodeMaster <RMI_port> ");
			return;
		}
		
		
        try
        {
        	master_Name = "NameNodeMaster";
            System.out.println("Registering NameNodeMaster as : NameNodeMaster");
            NameNodeMasterImpl master = new NameNodeMasterImpl();
            Naming.rebind(master_Name, master);
            System.out.println("NameNodeMaster: Ready...");
        }
        catch (Exception e)
        {
            System.out.println("Server: Failed to register NameNodeMaster: " + e);
        }
    }

	@Override
	public void print() throws RemoteException {
		// TODO Auto-generated method stub
		System.out.println("This is me!!!!!");
	}

	void registerDataNode(String dataNodeName){
		
        try {
			DataNode dataNode = (DataNode) Naming.lookup(dataNodeName);
			System.out.println("Looked up datanNode : " + dataNodeName);
			
			list_dataNode.put(dataNodeName, dataNode);
			
			System.out.println(list_dataNode);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
	}
}
