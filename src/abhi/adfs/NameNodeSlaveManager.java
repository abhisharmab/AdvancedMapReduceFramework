/**
 * 
 */
package abhi.adfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;

import com.sun.corba.se.spi.orbutil.fsm.Input;






/**
 * @author abhisheksharma, dkrew
 *
 */
public class NameNodeSlaveManager implements Runnable {
	
	private static String ipAddress = null;
	private static String portNumber = null;
	private static NameNodeMaster nameNodeMaster;
	private static HashMap<String, DataNode> list_DataNode;
	private static DataNode myDataNode = null;
	private static String myDataNodeName = null;
	private static List<InputFileInfo> list_inputFileInfo;

	public NameNodeSlaveManager(String ip, String port){
		this.ipAddress = ip;
		this.portNumber = port;
		
	}
	public static void main(String args[])
    {
		


		if (args.length != 2) {
			System.err.println("Usage: NameNodeSlaveManager <RMI_IP> <RMI_Port>");
			return;
		}

		
	   Thread manager =  new Thread(new NameNodeSlaveManager(args[0],args[1]));
	   manager.start();
	


 
    }


	public static void lookUpNameNodeMaster(){
	       try
	        {
	    	   if(System.getSecurityManager() == null){
	    		   System.setSecurityManager(new RMISecurityManager());   
	    	   }
	        	
	        //	String lookup_name = "rmi://10.0.0.4:1099/NameNodeMaster"; 
	        	String lookup_name = "rmi://" +ipAddress + ":"+ portNumber+ "/NameNodeMaster";
	        
	        	System.out.println(lookup_name);
	    		nameNodeMaster = (NameNodeMaster) Naming.lookup(lookup_name);
	    		System.out.println("NameNodeMaster has been looked up.");
	    		nameNodeMaster.print();
	        		
	    	} catch (Exception e){
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
        	// Debug
        	DataNodeImpl dataNode = new DataNodeImpl(getPID());
        	
        	Naming.bind(bindName, dataNode);
        	
        	myDataNode = dataNode;
        	myDataNodeName = bindName;
            System.out.println("DataNode: Ready...");
            
            // Register to the master
            if(nameNodeMaster.registerToMaster(bindName)){
            	System.out.println("DataNode has been registered to the NameNodeMaster.");
            	
            	// Adding the DataNode into the List
            	list_DataNode.put(bindName, dataNode);
            	
            	
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
	
	public static void updateDataNodes(){
		
		try {
			List<String> live_DataNodes = nameNodeMaster.getDataNodes();
			list_DataNode.clear();
			
			for(String live_nodes : live_DataNodes){
				// If we do not have the Reference of the DataNode we need to get it.
				System.out.println("Looking up : " + live_nodes);
				DataNode new_dataNode;
				try {
					new_dataNode = (DataNode) Naming.lookup(live_nodes);
					list_DataNode.put(live_nodes,new_dataNode);
					
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (NotBoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			
			
			
			
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			System.out.println("Error when accessing the NameNodeMaster.");
			System.out.println("Try to reconnect to the Master.");
//			e.printStackTrace();
		}
		
		
		// Debug method
		System.out.println("updateDataNodes");
		for(String names : list_DataNode.keySet()){
			System.out.println(names);
		}
		
		
	}


	@Override
	public void run() {

		// These are debugging prints.
    	try {
    		System.out.println(ManagementFactory.getRuntimeMXBean().getName());
			System.out.println(InetAddress.getLocalHost());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		list_DataNode = new HashMap<String, DataNode>();
		list_inputFileInfo = new ArrayList<InputFileInfo>();
		lookUpNameNodeMaster();
		bindDataNode();
		
		
		// Debug

		command_debug();
	}
	
	public void command_debug(){
		Scanner scanner = new Scanner(new InputStreamReader(System.in));

		System.out.println("1: update data Nodes.");
		System.out.println("2: Partition Files");
		System.out.println("3: Print my info");
		System.out.println("4: remove file");
		System.out.println("5: reconnect to master");
		
		while(true){
				String input = scanner.nextLine();
				if(input.equals("1")){
					updateDataNodes();


				} else if( input.equals("2")){
					String input1 = scanner.nextLine();
					partitionFile(input1);
				}else if( input.equals("3")){
					System.out.println("MyInfo");
					
					System.out.println(myDataNodeName);
					System.out.println(myDataNode);
				} else if ( input.equals("4")){
					
					System.out.println("removing file");
					String input1 = scanner.nextLine();
					removeFile(input1);
				}	else if ( input.equals("5")){
						
						System.out.println("Reconnect to master");
						reconnectToMaster();
				} else {

					System.out.println("1: update data Nodes.");
					System.out.println("2: Partition Files");
					System.out.println("3: Print my info");
					System.out.println("4: remove file");
				}
		}
					
					
	}
	public void partitionFile(String fileName){
		if(isExist(fileName)){
			try {
				if(nameNodeMaster.checkFileExistance(fileName)){
					System.out.println("File already exist and is distributed.");
					System.out.println("Please remove the file : " + fileName);
					System.out.println("And try again.");
				} else { // We need to partition the file now
					
					// but for now lets just save it in multiple locations.
					InputFileInfo fileInfo = new InputFileInfo(fileName);
					fileInfo.setParitionNumber(1);
					String partition_name = fileName+"_"+getPID();
					
					
					
					
					// Reading in file
					StringBuilder builder = new StringBuilder();
					File file = new File(fileName);
					
					
					Scanner scanner;
					try {
						scanner = new Scanner(file);
						while(scanner.hasNextLine()){
							builder.append(scanner.nextLine());
						}
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					
					
					
					
					
					
					// Updated the data Nodes now to send out files.
					updateDataNodes();
					
					for(Entry<String, DataNode> entry : list_DataNode.entrySet()){
						
						System.out.println(entry.getKey());
						System.out.println(entry.getValue());
						entry.getValue().submit(partition_name, builder.toString());
						fileInfo.addFileParitionInfo(entry.getKey(), partition_name);
						
					}
					
					
					
					list_inputFileInfo.add(fileInfo);
					nameNodeMaster.registerFileInformation(fileInfo);
					
					
					
					
				}
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			System.out.println("File that Needs to be partitioned does not exist.");
			System.out.println("Please try again.");
		}
	
	}
	
	
	public void removeFile(String fileName){
		try {
			if(nameNodeMaster.checkFileExistance(fileName)){
					nameNodeMaster.removeInputFileInfo(fileName);
			} else {
				System.out.println("File : " + fileName + "  does not exist in the adsf.");
				
			}
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public boolean isExist(String filename) {
		
		File file = new File(filename);
		if( file.exists() && file.isFile()){
			
			return true;
	
		} else {
			return false;	
		}
	}
	
	// Try to reconnect to the Master
	public void reconnectToMaster(){
		try{
			if(nameNodeMaster.ping()){
				System.out.println("Already connected to the master.");
			}
		} catch (RemoteException e){
			// If there was a problem reconnect to the master and register itself
			System.out.println("Master is not reachable. Trying to reconnect.");
			lookUpNameNodeMaster();
			try {
				nameNodeMaster.registerToMaster(myDataNodeName);
				for(InputFileInfo info : list_inputFileInfo){
					nameNodeMaster.registerFileInformation(info);
				}
			} catch (RemoteException e1) {
				// TODO Auto-generated catch block
//				e1.printStackTrace();
				System.out.println("Master is not up yet, please try again later.");
			}
		}
	}
}
 