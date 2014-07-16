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
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;

import abhi.mapreduce.SystemConstants;







/**
 * @author abhisheksharma, dkrew
 *
 */
public class NameNodeSlaveImpl extends UnicastRemoteObject implements NameNodeSlave {
	

	/**
	 * 
	 */
	private static final long serialVersionUID = 5556881182151765004L;
	
	
	private static String ipAddress = null;
	private static String portNumber = null;
	private static NameNodeMaster nameNodeMaster;
	private static LinkedHashMap<String, DataNode> list_DataNode;
	private static DataNode myDataNode = null;
	private static String myDataNodeName = null;
	private static List<InputFileInfo> list_inputFileInfo;
	private static String identifier = null;
	
	private static Integer rotationIndex = null;


	
	protected NameNodeSlaveImpl() throws RemoteException {
		super();

	}
	
	public static void main(String args[])
    {
		



		

		// These are debugging prints.
    	try {
    		System.out.println(ManagementFactory.getRuntimeMXBean().getName());
			System.out.println(InetAddress.getLocalHost().getHostAddress());
			setIdentifier(InetAddress.getLocalHost().getCanonicalHostName());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
    	rotationIndex = 1;
    	
    	// Grab rmiregistry information
		portNumber = SystemConstants.getConfig(SystemConstants.REGISTRY_PORT);
        ipAddress = SystemConstants.getConfig(SystemConstants.REGISTRY_HOST);
		
        //Initialize lists
		list_DataNode = new LinkedHashMap<String, DataNode>();
		list_inputFileInfo = new ArrayList<InputFileInfo>();
		

 	   if(System.getSecurityManager() == null){
 		   System.setSecurityManager(new RMISecurityManager());   
 	   }
 	   
		registerToRmi();
		lookUpNameNodeMaster();
		bindDataNode();
		
		
		// Debug

//		command_debug();


 
    }
	
	public static void registerToRmi(){
 		
        
        try {
        	NameNodeSlaveImpl slave = new NameNodeSlaveImpl();
        	String local_Ipaddress = InetAddress.getLocalHost().getHostAddress();
           	String name= "NameNodeSlave_"+getIdentifier();
    		String bindName = "rmi://" +local_Ipaddress + ":"+ portNumber+ "/" + name; 
    		System.out.println("Registering NameNodeSlave as : " + bindName);
    		
    		Naming.rebind(bindName, slave);
    		
    		System.out.println("NameNodeSlaveImpl: Ready...");
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

        
        

	}


	public static void lookUpNameNodeMaster(){
	       try
	        {
	        	
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
		
	
    	try {
    		String name= "DataNode_"+getIdentifier();
        	String local_Ipaddress = InetAddress.getLocalHost().getHostAddress();
			String bindName = "rmi://" +local_Ipaddress + ":"+ portNumber+ "/" + name; 
    		System.out.println("Bind Name : " + bindName);
            System.out.println("Registering DataNode as : " + bindName);
           
            
        	DataNodeImpl dataNode = new DataNodeImpl();
        	Naming.rebind(bindName, dataNode);
           	
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
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemoteException e){
        	e.printStackTrace();
		} 
	}
	
//	public static String getPID(){
//		String name = ManagementFactory.getRuntimeMXBean().getName();
//		int index = name.indexOf('@');
//		String pid = (String) name.subSequence(0,  index);
//		return pid;
//	}
	
	public static void updateDataNodes(){
		
		try {
			List<String> live_DataNodes = nameNodeMaster.getDataNodes();
			
			
			// Debug
			System.out.println("Print out data nodes from the master");
			for(String name : live_DataNodes){
				System.out.println("+++++++" + name);
			}
			
			
			if(!checkDataNodes(live_DataNodes)){
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

	public static boolean checkDataNodes(List<String> live_DataNodes){
		if(live_DataNodes.size() != list_DataNode.size()){
			return false;
		} else {
			for(String name : live_DataNodes){
				if(!list_DataNode.containsKey(name)){
					return false;
				}
			}
		}
		return true;
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
					//dump(input1);
				}else if( input.equals("3")){
					System.out.println("MyInfo");
					
					System.out.println(myDataNodeName);
					System.out.println(myDataNode);
			
				} else if ( input.equals("4")){
					
					System.out.println("removing file");
					String input1 = scanner.nextLine();
				//	removeFile(input1);
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
	

	

	
	public static boolean isExist(String filename) {
		
		File file = new File(filename);
		if( file.exists() && file.isFile()){
			
			return true;
	
		} else {
			return false;	
		}
	}
	

	public static void reconnectToMaster(){
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
	@Override
	public boolean ping() throws RemoteException {
		// TODO Auto-generated method stub
		return true;
	}
	@Override
	public void print() throws RemoteException {
		System.out.println("This is from the NameNodeSlaveImpl");
		
	}

	public static String getIdentifier() {
		return identifier;
	}

	public static void setIdentifier(String identifier) {
		NameNodeSlaveImpl.identifier = identifier;
	}

	@Override
	public boolean dump(String fileName) throws RemoteException{
		if(isExist(fileName)){
			updateDataNodes();
			try {
				if(nameNodeMaster.checkFileExistance(fileName)){
					System.out.println("File already exist and is distributed.");
					System.out.println("Please remove the file : " + fileName);
					System.out.println("And try again.");
				} else { // We need to partition the file now
					
					// Get file partition Size
					Double partition_size = Double.parseDouble(SystemConstants.getConfig(SystemConstants.FILE_PARITION_SIZE));
					System.out.println("fileSize   " + partition_size);
					
					// 1024*1024 will be 1MB. We are adjusting the size by ratio of 1MB
					Double file_size = 1024*1024*partition_size;
					
					
					// Get replication number for the file
					Integer replication = Integer.parseInt(SystemConstants.getConfig(SystemConstants.REPLICATION));
					
					// but for now lets just save it in multiple locations.
					InputFileInfo fileInfo = new InputFileInfo(fileName);
					
					FileSpliter spliter;
						try {
							spliter = new FileSpliter(fileName, file_size);
							String data = spliter.getNextBlock();
							while (data != null){
								String paritioned_fileName = fileName+"_"+spliter.getParitionSize().toString();
								sendOutFile(data,paritioned_fileName,replication,fileInfo);
								
								data = new String();
								data = spliter.getNextBlock();
							}

						} catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
											
					
					
					list_inputFileInfo.add(fileInfo);
					nameNodeMaster.registerFileInformation(fileInfo);
					
					
					return true;
					
				}
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			} 
		} else {
			System.out.println("File that Needs to be partitioned does not exist.");
			System.out.println("Please try again.");
			return false;
		}
		return false;
	
	}

	@Override
	public List<String> getDataNodeFiles() throws RemoteException {
		// TODO Auto-generated method stub
		if(myDataNode != null){
			return myDataNode.getFileList();
		}
		return null;
	}

	// This
	@Override
	public boolean remove(String fileName) throws RemoteException {
		
		try {
			if(nameNodeMaster.checkFileExistance(fileName)){
					nameNodeMaster.removeInputFileInfo(fileName);
					return true;
			} else {
				System.out.println("File : " + fileName + "  does not exist in the adsf.");
				return false;
			}
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	public void sendOutFile(String data, String fileName, Integer replication, InputFileInfo info){
		
		List<DataNode> values = new ArrayList<DataNode>(list_DataNode.values());
		List<String> keys = new ArrayList<String>(list_DataNode.keySet());
		for(int i = 0; i < replication; i++){
			Integer index = getRotationIndex();
			System.out.println("this is the index   " + index);
			try {
				System.out.println("Sending file too   " + keys.get(index) + "   : " + fileName);
				info.addFileParitionInfo(keys.get(index), fileName);
				values.get(index).submit(fileName, data);
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	public static Integer getRotationIndex() {
		Integer index = rotationIndex++;
		
		return index % list_DataNode.size();
	}

	
}
 