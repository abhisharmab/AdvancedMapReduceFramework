/**
 * 
 */
package abhi.adfs;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import abhi.mapreduce.SystemConstants;

/**
 * @author Douglas Rew
 * This is the implemenation of the NameNodeMaster
 */
public class NameNodeMasterImpl extends UnicastRemoteObject implements NameNodeMaster{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2917579042804203973L;
	
	// Master Name will come from the config file
	private static String master_Name;
	
	// This will keep track of the file information.
	private static List<InputFileInfo> list_fileInfo;
	
	// This will keep track of the dataNodes
	private static ConcurrentHashMap <String, DataNode> list_dataNode;
	
	// This will be used in distributing the files.
	private static Integer rotationIndex;
	
	public NameNodeMasterImpl() throws RemoteException {
		super();
		list_dataNode = new ConcurrentHashMap <String, DataNode>();
		setList_fileInfo(new ArrayList<InputFileInfo>());
		rotationIndex = 1;
	}
	
	// This method will be called when a dataNode starts
	@Override
	public boolean registerToMaster(String dataNodeName) 
			throws RemoteException {
		registerDataNode(dataNodeName);
		return true;
	}
	
	
	public static void main(String args[])
    {


        try
        {
            
        	master_Name = SystemConstants.getConfig(SystemConstants.NAMENODE_SERVICE_NAME);
            System.out.println("Registering NameNodeMaster as : " + master_Name);
            NameNodeMasterImpl master = new NameNodeMasterImpl();
            Naming.rebind(master_Name, master);
            System.out.println("NameNodeMaster: Ready...");
            
            
            // This is for periodical ping on to the dataNodes that are registered.
            // By doing this we could detect dead Nodes and start the replication process.
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                        	checkDataNodes();
                            Thread.sleep(1000);
                        } catch (InterruptedException ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            });
            t.start();
        }
        catch (Exception e)
        {
            System.out.println("Server: Failed to register NameNodeMaster: " + e);
        }
    }
	
// Debug
//	@Override
//	public void print() throws RemoteException {
//		// TODO Auto-generated method stub
//		System.out.println("This is me!!!!!");
//	}

	// This method will register the dataNode to the master
	void registerDataNode(String dataNodeName){
		
        try {
        	
        	// Looking up the DataNode and saving it.
        	// The dataNodeName will be constructed with the remote rmi location
			DataNode dataNode = (DataNode) Naming.lookup(dataNodeName);
			System.out.println("Looked up datanNode : " + dataNodeName);
			list_dataNode.put(dataNodeName, dataNode);
			
		} catch (MalformedURLException e) {
			System.out.println("Error in looking up the DataName " + dataNodeName);
		} catch (RemoteException e) {
			System.out.println("Error in remote call");
		} catch (NotBoundException e) {
			System.out.println("DataNode : " + dataNodeName + " is not binded");
		}
        
	}

	
	// This method will be used to register the InputFileInfo on the Master
	@Override
	public void registerFileInformation(InputFileInfo fileInfo)
			throws RemoteException {
		if(list_fileInfo.isEmpty()){
			list_fileInfo.add(fileInfo);	
		} else {
			if(!checkFileExistence(fileInfo.getFileName())){
				list_fileInfo.add(fileInfo); 
			}
		}
		
//		Debug
//		System.out.println("registerFileInformation    : " + fileInfo.getFileName());
//		for(InputFileInfo info : list_fileInfo){
//			System.out.println("-------"+info.getFileName());
//		}
		
	}

	public static List<InputFileInfo> getList_fileInfo() {
		return list_fileInfo;
	}

	public static void setList_fileInfo(List<InputFileInfo> list_fileInfo) {
		NameNodeMasterImpl.list_fileInfo = list_fileInfo;
	}

	// This method will return the all names of the DataNode registered on the Master
	@Override
	public List<String> getDataNodes() throws RemoteException {
		List<String> dataNodes = new ArrayList<String>(list_dataNode.keySet());
		return dataNodes;
	}
	
	// This method will used for the replication
	public static void replicateFiles(String dataNodeName){
		
		System.out.println("This Data node is dead  " + dataNodeName);
		System.out.println("Starting the replication process.");
		
		// Retrieve the InpurFileInfo that is related to the Dead Data Node
		List<InputFileInfo> infoList = needValidations(dataNodeName);
		
		
		// Going through the info list to identify the files needed to be replicated 
		for(InputFileInfo info : infoList){
			System.out.println("Info   " +info.getFileName());
			for(String file : info.filesFromDeadDataNode(dataNodeName)){
				System.out.println("This is the missing file  " + file);
				String dataNode = info.fileExistInDataNode(file);
				System.out.println("This is where the existing file is. " + dataNode);
				Boolean found = Boolean.FALSE;
				
				Integer counter = 0;
				while(!found){
					
					// This will get the next data node in rotation.
					// By doing so we could replicate the file in different location
					Entry<String, DataNode> entry = getNextDataNodeEntry();
					
					try {
						// We will look into the data node and check whether 
						// the existing file exist in the data node
						// If the file does not exist in the data Node
						// Add that file in.
						if(!entry.getValue().isExist(file)){
							
							found = Boolean.TRUE;
							
							// Got the dataNode from an existing Node 
							DataNode node = list_dataNode.get(dataNode);
							
//							Debug
//							System.out.println("I am about to retrieve   " + file);
//							System.out.println("from    " + dataNode);
//							for(String fileaaa : node.getFileList()){
//								System.out.println("-------"+fileaaa);
//							}
							
							// Getting the data from the dataNode
							String data = node.retrieve(file);
							
							// Get the new Node and submit the file
							DataNode newNode = entry.getValue();
							newNode.submit(file, data);
							
							// update the info with the new file.
							info.addFileParitionInfo(entry.getKey(), file);
						} else {
							// This is a limitation of the replication retry process.
							// If the counter is over the size of the dataNode
							// There is no need for replication. All files exist in the filesystem.
							counter++;
							if(counter >  list_dataNode.size()){
								found = Boolean.TRUE;
							}
						}
					} catch (RemoteException e) {
						System.out.println("Error while asking the remote object");
					}
				}
			}
			
		}
		
		
		
		
	}
	
	// This method will identify the InputFileInfo's that affected by the dead DataNode
	public static List<InputFileInfo> needValidations(String dataNode){
		List<InputFileInfo> list = new ArrayList<InputFileInfo>();
		for(InputFileInfo info : list_fileInfo){
			if(info.isPartitionedInDataNode(dataNode)){
				list.add(info);
			}
		}
		
		return list;
	}


	// This method will check the existence of the fileName in the DFS
	@Override
	public boolean checkFileExistence(String fileName) throws RemoteException {
		
//		Debug
//		System.out.println("checkFileExistance   " + fileName);
//		
//		for(InputFileInfo info : list_fileInfo){
//			System.out.println("Info name  " + info.getFileName());
//		}
		
		
		for(InputFileInfo info : list_fileInfo){
			// Locate the info with the filename
			if(info.getFileName().equals(fileName)){
				
				// Check whether the info is valid
				return info.isValid();
			}
		}
		return false;
		
	}

	// This method will retrieve the InputFileInfo by fileName
	@Override
	public InputFileInfo getInputFileInfo(String fileName)
			throws RemoteException {
		for(InputFileInfo info : list_fileInfo){
			if( info.getFileName().equals(fileName)){
				return info;
			}
		}
		return null;
	}

	// This method will remove all files that is partition by the fileName
	@Override
	public void removeInputFileInfo(String fileName)
			throws RemoteException {
		
		Iterator<InputFileInfo> iter = list_fileInfo.iterator();
		while(iter.hasNext()){
			InputFileInfo info = iter.next();
			if(info.getFileName().equals(fileName)){
				// Go clean up all the partition files in all data Nodes
				cleanUpDataNodes(info);
				// Remove the entry from the master
				iter.remove();
			}
		}
		
	}
	
	// This method will clean up all the partition file with in the inputFileinfo
	public void cleanUpDataNodes(InputFileInfo info){
		for(Entry<String, List<String>> entry : info.getPartitions().entrySet()){
			DataNode dn = list_dataNode.get(entry.getKey());
			try {
				if(dn.ping()){
					System.out.println("Cleaning up all partitioned files.");
					for(String filename : entry.getValue()){
						if(dn.remove(filename)){
							System.out.println("File : " + filename + " has been removed.");
							
						} else {
							System.out.println("Error has been occured while removing file : " + filename);
						}
					}
				} else {
					System.out.println("DataNode : " + entry.getKey() + " is dead.");
					// Check upon the data node and clean up if necessary 
					checkDataNodes();
					
				}
			} catch (RemoteException e) {
				System.out.println("Error while access the remote object");
			}
		}
		
	}

	// This is used to check the liveness
	@Override
	public boolean ping() throws RemoteException {
		return true;
	}
	
	// This is used to rotation through the DataNode 
	public static Entry<String,DataNode> getNextDataNodeEntry(){
		rotationIndex++;
		Integer index = rotationIndex % list_dataNode.size(); 
		List<String> keys = new ArrayList<String>(list_dataNode.keySet());
		Map.Entry<String, DataNode> entry = 
				new AbstractMap.SimpleEntry<String,DataNode>(keys.get(index), list_dataNode.get(keys.get(index)));
		return entry;
		
	}
	
	
	// This method will go through all the DataNodes and check whether there are alive
	public static void checkDataNodes(){
		
		Iterator<Entry<String, DataNode>> iter = list_dataNode.entrySet().iterator();
		while(iter.hasNext()){
			@SuppressWarnings("unchecked")
			Entry<String, DataNode> dn = (Entry<String, DataNode>) iter.next();
		
			try{
				if(dn.getValue().ping()){
					// Debug 
//					System.out.println(dn.getKey() + " is alive");
					continue;
				} 
			} catch(RemoteException e){
				System.out.println("There are Dead DataNode this should be addressed");
				String deadNode = dn.getKey();
				iter.remove();
				replicateFiles(deadNode);
			}
			
			
		}
				
	}
}
