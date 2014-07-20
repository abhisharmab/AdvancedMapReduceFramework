/**
 * 
 */
package abhi.adfs;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
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
 * This is the implementation of the NameNodeSlave.
 * This will be in the middle the communication of the Master / DataNode / Manager
 */
public class NameNodeSlaveImpl extends UnicastRemoteObject implements NameNodeSlave {
	

	/**
	 * 
	 */
	private static final long serialVersionUID = 5556881182151765004L;
	
	
	// This will be the information about the main RMI
	// Where the NameNodeMaster lives
	private static String ipAddress = null;
	private static String portNumber = null;
	
	
	//Reference of the NameNodeMaster
	private static NameNodeMaster nameNodeMaster;
	
	// This will be the list of all DataNode that Live in the DFS
	private static LinkedHashMap<String, DataNode> list_DataNode;
	
	// This will be the local dataNode
	private static DataNode myDataNode = null;
	// This will be the local DataNodeName
	private static String myDataNodeName = null;
	
	// This will be the List of InputFileInfo which the this slave have created.
	// At the end all information will go to the Master
	private static List<InputFileInfo> list_inputFileInfo;
	
	// This will be the identifier for any Nodes that is being registered on the local RMI
	// By doing so other nodes could identify the origin of the binding
	private static String identifier = null;
	
	// This will be used in rotating through the DataNodes while submiting files.
	private static Integer rotationIndex = null;


	
	protected NameNodeSlaveImpl() throws RemoteException {
		super();
	}
	
	public static void main(String args[])
    {

    	try {
			setIdentifier(InetAddress.getLocalHost().getHostName());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
    	rotationIndex = 1;
    	
    	// Grab rmiregistry information for the Master look Up
		portNumber = SystemConstants.getConfig(SystemConstants.NAMENODE_REGISTRY_PORT);
        ipAddress = SystemConstants.getConfig(SystemConstants.NAMENODE_REGISTRY_HOST);
		
        //Initialize lists
		list_DataNode = new LinkedHashMap<String, DataNode>();
		list_inputFileInfo = new ArrayList<InputFileInfo>();
		
		
		// Setting up the security manager
 	   if(System.getSecurityManager() == null){
 		   System.setSecurityManager(new RMISecurityManager());   
 	   }
 	   
 	   // Register the Slave to the Local RMI
		registerToRmi();
		// Look up the Master
		lookUpNameNodeMaster();
		// Register the DataNode to the local RMI
		// And register this on the Master
		bindDataNode();


 
    }
	
	// This is used to register the Slave to the local RMI
	public static void registerToRmi(){
        try {
        	NameNodeSlaveImpl slave = new NameNodeSlaveImpl();
        	String local_Ipaddress = InetAddress.getLocalHost().getHostAddress();
        	String slaveName = SystemConstants.getConfig(SystemConstants.NAMENODE_SLAVE_SERVICE);
           	String name= slaveName+"_"+getIdentifier();
    		String bindName = "rmi://" +local_Ipaddress + ":"+ portNumber+ "/" + name; 
    		System.out.println("Registering NameNodeSlave as : " + bindName);
    		Naming.rebind(bindName, slave);
    		
    		System.out.println("NameNodeSlaveImpl: Ready...");
		} catch (RemoteException e) {
			System.out.println("Error while accessing the remote object check on the RMI.");
			System.exit(0);
		} catch (UnknownHostException e) {
			System.out.println("Error while accessing the RMI, please retry");
			System.exit(0);
		} catch (MalformedURLException e) {
			System.out.println("Error while binding the slave, please retry.");
			System.exit(0);
		} 

        
        

	}


	
	// This method will look up the NameNodeMaster from the Main RMI
	public static void lookUpNameNodeMaster(){
	       try
	        {
	    		String master_Name = SystemConstants.getConfig(SystemConstants.NAMENODE_SERVICE_NAME);
	        	String lookup_name = "rmi://" +ipAddress + ":"+ portNumber+ "/" + master_Name;
	    		nameNodeMaster = (NameNodeMaster) Naming.lookup(lookup_name);
	    		System.out.println("NameNodeMaster has been looked up.");
	        		
	    	} catch (Exception e){
	    		System.out.println("Manager: Exception thrown looking up " + "NameNodeSlaveManager");
	    		System.exit(0);
	    	}
	}
	
	
	// The method will instantiate the DataNode on and bind it on to the local RMI
	// and register is on to the NameNode Master
	public static void bindDataNode(){
		
    	try {
    		String name= "DataNode_"+getIdentifier();
        	String local_Ipaddress = InetAddress.getLocalHost().getHostAddress();
			String bindName = "rmi://" +local_Ipaddress + ":"+ portNumber+ "/" + name; 
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
			System.out.println("Error while looking up the host name, please try again.");
			System.exit(0);
		} catch (MalformedURLException e) {
			System.out.println("Error while binding the DataNode, please try again.");
			System.exit(0);
		} catch (RemoteException e){
        	System.out.println("Error while accessing the remote object, please try agian.");
        	System.exit(0);
		} 
	}
	
	// This method is to update the list of all dataNodes on the Slave.
	// By having the list of all dataNode, the slave could access different file locates
	// and perform file actions.
	public static void updateDataNodes(){
		
		try {
			// The master will have all the information of the nodes so we ask the master
			List<String> live_DataNodes = nameNodeMaster.getDataNodes();
			
			// Debug
//			System.out.println("Print out data nodes from the master");
//			for(String name : live_DataNodes){
//				System.out.println("+++++++" + name);
//			}
			
			
			// After getting the list of updated DataNodes
			// We check with our current list.
			// If there are any difference we update our list.
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
						System.out.println("Error while looking up the DataNodes, please try again.");
					} catch (NotBoundException e) {
						System.out.println("Error while looking up the DataNodes, please try again.");
					}
				}
			}
		} catch (RemoteException e) {
			System.out.println("Error when accessing the NameNodeMaster.");
			System.out.println("Try to reconnect to the Master.");
		}
		
		
		// Debug method
//		System.out.println("updateDataNodes");
//		for(String names : list_DataNode.keySet()){
//			System.out.println(names);
//		}
		
		
	}

	// This method will compare the live_DataNode with the current list of dataNode the Slave has
	// if the list are the same it will return true. If it is not the same it will return false.
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


	// This method will be used when we are generating the partitioned input files.
	public static boolean isExist(String filename) {
		
		File file = new File(filename);
		if( file.exists() && file.isFile()){
			return true;
		} else {
			return false;	
		}
	}
	// This is used to check the liveness of the Slave
	@Override
	public boolean ping() throws RemoteException {
		return true;
	}
	
	
	// Debug
//	@Override
//	public void print() throws RemoteException {
//		System.out.println("This is from the NameNodeSlaveImpl");
//		
//	}

	public static String getIdentifier() {
		return identifier;
	}

	public static void setIdentifier(String identifier) {
		NameNodeSlaveImpl.identifier = identifier;
	}

	// This method will take care of partitioning the files and distributing it.
	// This will also register the InputFileInfo to the Master so that
	// the job tracker could look up information about where are the partitioned file are
	// distributed. 
	@Override
	public boolean dump(String fileName) throws RemoteException{
		if(isExist(fileName)){
			updateDataNodes();
			try {
				if(nameNodeMaster.checkFileExistence(fileName)){
					System.out.println("File already exist and is distributed.");
					System.out.println("Please remove the file : " + fileName);
					System.out.println("And try again.");
				} else { // We need to partition the file now.
					
					// Get file partition Size
					Double partition_size = Double.parseDouble(SystemConstants.getConfig(SystemConstants.FILE_PARITION_SIZE));
					System.out.println("fileSize   " + partition_size);
					
					// 1024*1024 will be 1MB. We are adjusting the size by ratio of 1MB
					Double file_size = 1024*1024*partition_size;
					
					
					// Get replication number for the file
					Integer replication = Integer.parseInt(SystemConstants.getConfig(SystemConstants.REPLICATION));
					
					InputFileInfo fileInfo = new InputFileInfo(fileName);
					
					
					// The FileSpliter will handling the partitioning of the input.
					FileSpliter spliter;
					try {
						spliter = new FileSpliter(fileName, file_size);
						
						// This is get the first block of the partitioned file.
						String data = spliter.getNextBlock();

						while (data != null){
							
							String paritioned_fileName = fileName+"_"+spliter.getParitionSize().toString();
							
							// After getting the first block of the file, we send it out to 
							// other DataNodes and make replication according to the configuration.
							sendOutFile(data,paritioned_fileName,replication,fileInfo);
							
							data = new String();
							// Then get the new block.
							data = spliter.getNextBlock();
						}

					} catch (FileNotFoundException e) {
						System.out.println("Error while accessing file : " + fileName);
						System.out.println("Check on the file and retry.");
					} catch (IOException e) {
						System.out.println("Error while opening the file please try again.");
					}
											
					
					// Add the fileInfo on my local				
					list_inputFileInfo.add(fileInfo);
					// Register the FileInfo onto the Master.
					nameNodeMaster.registerFileInformation(fileInfo);
					return true;
					
				}
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				System.out.println("Error while accessing the remote object, please try again.");
				return false;
			} 
		} else {
			System.out.println("File that Needs to be partitioned does not exist.");
			System.out.println("Please try again.");
			return false;
		}
		return false;
	
	}

	// This method will return all registered files on the local Data Node
	@Override
	public List<String> getDataNodeFiles() throws RemoteException {
		// TODO Auto-generated method stub
		if(myDataNode != null){
			return myDataNode.getFileList();
		}
		return null;
	}

	// This method will trigger the file removal to the Master
	// And the Master will ask all DataNode which has the partition of the file to be removed.
	@Override
	public boolean remove(String fileName) throws RemoteException {
		
		try {
			if(nameNodeMaster.checkFileExistence(fileName)){
					nameNodeMaster.removeInputFileInfo(fileName);
					return true;
			} else {
				System.out.println("File : " + fileName + "  does not exist in the adsf.");
				return false;
			}
		} catch (RemoteException e) {
			System.out.println("Error Accessing the remote Object.");
			return false;
		}
	}

	// This method is used to send out file to data nodes with replication.
	public void sendOutFile(String data, String fileName, Integer replication, InputFileInfo info){
		
		List<DataNode> values = new ArrayList<DataNode>(list_DataNode.values());
		List<String> keys = new ArrayList<String>(list_DataNode.keySet());
		for(int i = 0; i < replication; i++){
			// The rotation index will determine that we send a replication of the file in different locations.
			Integer index = getRotationIndex();
			try {
				System.out.println("Sending file too   " + keys.get(index) + "   : " + fileName);
				info.addFileParitionInfo(keys.get(index), fileName);
				values.get(index).submit(fileName, data);
			} catch (RemoteException e) {
				System.out.println("Error while accessing the remote object.");
			}
		}
		
	}

	// This is used to rotatate through the List of DataNodes
	public static Integer getRotationIndex() {
		Integer index = rotationIndex++;
		return index % list_DataNode.size();
	}

	// This method will take care of the distribution of the JAR file.
	@Override
	public boolean dumpJar(String fileName) throws RemoteException {
		
		// We update our Data Node List first.
		updateDataNodes();
		
		// Open the file and read it into a byte array.
		File file = new File(fileName);
		byte buffer[] = new byte[(int)file.length()];
		try {
		     BufferedInputStream input = new
		     BufferedInputStream(new FileInputStream(fileName));
		     input.read(buffer,0,buffer.length);
		     input.close();
		     
		} catch(Exception e) {
		     System.out.println("Error while accesin the Jar file.");
		     return false;
		}
		
		// Send the JAR file to all data nodes.
		List<DataNode> nodes = new ArrayList<DataNode>(list_DataNode.values());
		for(DataNode node : nodes){
			node.submitJar(fileName, buffer, (int) buffer.length);
		}
			
		return true;

	}

	// This method will register the filename into the DataNode
	// This will be called from the Mapper to register the intermediate files.
	@Override
	public boolean registerToLocalDataNode(String fileName) throws RemoteException {
		// TODO Auto-generated method stub
		if(myDataNode.registerFileName(fileName)){
			System.out.println("FileName : " + fileName + " has been registered to the local DataNode.");
			return true;
		} else {
			System.out.println("Error while registering fileName : " + fileName + " to the local DataNode");
			return false;
		}
		
	}

	// This method will retrieve data from the DataNode
	// This will be called from the Reduver to retrive the intermediate files.
	@Override
	public String retrieveFromLocalDataNode(String fileName)
			throws RemoteException {
		return myDataNode.retrieve(fileName);
	}

	// This method will check the existence of a file in the DFS
	@Override
	public boolean checkFileExistences(String fileName) throws RemoteException {
		return nameNodeMaster.checkFileExistence(fileName);
	}

	// This is used to save a file to the Local Data Node
	@Override
	public boolean saveFileToLocalDataNode(String fileName, String data)
			throws RemoteException {
		
		return myDataNode.submit(fileName, data);
	}

	
}
 